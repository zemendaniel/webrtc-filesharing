import aiofiles
import json
import os
import asyncio
import sys
import argparse
import asyncio
import logging
import time
import pickle
from aiortc import RTCIceCandidate, RTCPeerConnection, RTCSessionDescription, RTCConfiguration, RTCIceServer
from aiortc.contrib.signaling import BYE, add_signaling_arguments, create_signaling
import aioconsole
import hashlib

CHUNK_SIZE = 16 * 1024


class WebSocketSignaling:
    def __init__(self, uri):
        self._uri = uri
        self._ws = None

    async def connect(self):
        import websockets
        self._ws = await websockets.connect(self._uri)

    async def send(self, message):
        await self._ws.send(pickle.dumps(message))

    async def receive(self):
        data = await self._ws.recv()
        return pickle.loads(data)

    async def close(self):
        await self._ws.close()


class ControlMessage:
    def __init__(self, msg_type, data):
        self.msg_type = msg_type
        self.data = data

    @staticmethod
    def create_json(msg_type, data):
        return json.dumps({
            "type": msg_type,
            "data": data
        })

    @staticmethod
    def from_json(data):
        parsed = json.loads(data)
        match parsed["type"]:
            case "metadata":
                return ControlMessage(parsed["type"], json.loads(parsed["data"]))
            case _:
                return ControlMessage(parsed["type"], parsed["data"])


class Progress:
    def __init__(self, total):
        self.total = total
        self.current = 0
        self._last_print = 0

    def update(self, data):
        self.current += len(data)
        now = time.time()
        if now - self._last_print >= 1:  # print at most once per second
            percent = (self.current / self.total) * 100
            print(f"Progress: {self.current}/{self.total} bytes ({percent:.2f}%)")
            self._last_print = now


def compute_hash(file_path):
    sha = hashlib.sha256()
    with open(file_path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            sha.update(chunk)
    return sha.hexdigest()


class FileSender:
    def __init__(self, file_path):
        self._file_path = file_path
        self._file_channel = None
        self._control_channel = None
        self._sending_task = None
        self._done = asyncio.Future()

    def set_channel(self, channel_type, channel):
        if channel_type == "file":
            self._file_channel = channel
            self._file_channel.on("open", self._on_both_channels_open)
        elif channel_type == "control":
            self._control_channel = channel
            self._control_channel.on("open", self._on_both_channels_open)

    def _on_both_channels_open(self):
        if self._file_channel.readyState == "open" and self._control_channel.readyState == "open":
            self._sending_task = asyncio.create_task(self._start_file_transfer())

    async def wait_until_done(self):
        await self._done

    @staticmethod
    def _construct_metadata(file_path):
        metadata = {
            "file_name": os.path.basename(file_path),
            "file_size": os.path.getsize(file_path),
            "hash": compute_hash(file_path)
        }
        return metadata

    async def _start_file_transfer(self):
        if not self._control_channel or not self._file_channel or not self._file_path:
            print("[ERROR] Channels or file path not ready")
            return

        metadata = self._construct_metadata(self._file_path)
        self._control_channel.send(ControlMessage.create_json("metadata", json.dumps(metadata)))
        await asyncio.sleep(0)

        progress = Progress(metadata["file_size"])

        async with aiofiles.open(self._file_path, "rb") as fp:
            while chunk := await fp.read(CHUNK_SIZE):
                chunk = bytes(chunk)
                while self._file_channel.bufferedAmount > 4 * CHUNK_SIZE:
                    await asyncio.sleep(0)

                self._file_channel.send(chunk)
                progress.update(chunk)
                await asyncio.sleep(0)

        self._control_channel.send(ControlMessage.create_json("eof", metadata["file_name"]))

        if not self._done.done():
            self._done.set_result(None)


class FileReceiver:
    def __init__(self, path):
        self._file_channel = None
        self._control_channel = None
        self._metadata = None
        self._location = None
        self._file_obj = None
        self._progress = None
        self._path = path
        self._done = asyncio.Future()

    async def wait_until_done(self):
        await self._done

    def set_channel(self, channel_type, channel):
        if channel_type == "file":
            self._file_channel = channel
            self._file_channel.on("message", lambda message: asyncio.create_task(self._on_file_chunk(message)))
        elif channel_type == "control":
            self._control_channel = channel
            self._control_channel.on("message", lambda message: asyncio.create_task(self._on_control_message(message)))

    async def _on_file_chunk(self, chunk):
        if not self._file_obj:
            if not self._metadata or not self._path:
                print("[ERROR] Metadata or path not set before receiving file")
                return

            self._file_obj = await aiofiles.open(self._location, "wb")
            self._progress = Progress(self._metadata["file_size"])
            print(f"Receiving file: {self._metadata['file_name']} ({self._metadata['file_size']} bytes)")

        try:
            if isinstance(chunk, str):
                chunk = chunk.encode()
            await self._file_obj.write(chunk)
            self._progress.update(chunk)
        except Exception as e:
            await self._file_obj.close()
            self._file_obj = None
            raise e

    async def _on_control_message(self, message):
        control_message = ControlMessage.from_json(message)
        match control_message.msg_type:
            case "metadata":
                self._metadata = control_message.data
                self._location = os.path.join(self._path, self._metadata["file_name"])
            case "eof":
                if self._file_obj:
                    await self._file_obj.close()
                    self._file_obj = None
                expected_hash = compute_hash(self._location)
                actual_hash = self._metadata["hash"]
                print(f"Expected hash: {expected_hash} {type(expected_hash)}")
                print(f"Actual hash: {actual_hash} {type(actual_hash)}")
                if expected_hash == actual_hash:
                    print("File received successfully")
                else:
                    print("[ERROR} File corrupted")
                self._done.set_result(None)
        

class Peer:
    def __init__(self, role):
        self.signaling = WebSocketSignaling("ws://152.53.123.174:8001")
        ice_servers = [RTCIceServer(urls="stun:stun.l.google.com:19302")]
        rtc_config = RTCConfiguration(iceServers=ice_servers)
        self.pc = RTCPeerConnection(configuration=rtc_config)
        self.role = role

        if role == "send":
            self.file_handler = FileSender(args.path)
            self.coro = self.run_offer()
        elif role == "receive":
            self.file_handler = FileReceiver(args.path)
            self.coro = self.run_answer()

    async def start(self):
        await self.coro
        if self.role == "send":
            print("Waiting for file transfer to finish...")
        elif self.role == "receive":
            print("Waiting for file reception to finish...")
        await self.file_handler.wait_until_done()

    async def consume_signaling(self):
        obj = await self.signaling.receive()

        if isinstance(obj, RTCSessionDescription):
            await self.pc.setRemoteDescription(obj)

            if obj.type == "offer":
                await self.pc.setLocalDescription(await self.pc.createAnswer())
                await self.signaling.send(self.pc.localDescription)

        elif isinstance(obj, RTCIceCandidate):
            await self.pc.addIceCandidate(obj)

    async def run_answer(self):
        """
        The receiver uses this function to receive the offer from the signaling server
        """
        await self.signaling.connect()

        @self.pc.on("datachannel")
        def on_datachannel(channel):
            if channel.label == "file":
                self.file_handler.set_channel("file", channel)
            elif channel.label == "control":
                self.file_handler.set_channel("control", channel)

        await self.consume_signaling()

    async def run_offer(self):
        """
        The sender uses this function to send the offer to the signaling server
        """
        await self.signaling.connect()

        file_channel = self.pc.createDataChannel("file")
        control_channel = self.pc.createDataChannel("control")

        self.file_handler.set_channel("file", file_channel)
        self.file_handler.set_channel("control", control_channel)

        await self.pc.setLocalDescription(await self.pc.createOffer())
        await self.signaling.send(self.pc.localDescription)

        await self.consume_signaling()

    async def close(self):
        await self.signaling.close()
        await self.pc.close()


async def main():
    peer = Peer(args.role)
    await peer.start()
    await peer.close()


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(description="Data channel file transfer")
    parser.add_argument("role", choices=["send", "receive"])
    parser.add_argument("path")
    parser.add_argument("--verbose", "-v", action="count")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    asyncio.run(main())

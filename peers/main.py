import aiofiles
import json
import os
import sys
import argparse
import asyncio
import logging
import time
import pickle
from aiortc import RTCIceCandidate, RTCPeerConnection, RTCSessionDescription, RTCConfiguration, RTCIceServer
import hashlib

CHUNK_SIZE = 128 * 1024
# it represents the largest backlog you’re comfortable letting the channel reach before you stop pushing more chunks.
MAX_BUFFERED_AMOUNT = 32 * CHUNK_SIZE


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
        self._last_bytes = 0  # track how many bytes we had last time

    def update(self, data):
        self.current += len(data)
        now = time.time()

        if now - self._last_print >= 1:  # update at most once per second
            elapsed = now - self._last_print
            bytes_since_last = self.current - self._last_bytes
            speed_bps = bytes_since_last / elapsed  # bytes per second
            speed_mbps = (speed_bps * 8) / 1e6     # convert to megabits/sec

            percent = (self.current / self.total) * 100
            print(
                f"Progress: {self.current}/{self.total} bytes "
                f"({percent:.2f}%) | Speed: {speed_mbps:.2f} Mb/s"
            )

            self._last_print = now
            self._last_bytes = self.current


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
        self._buffer_event = asyncio.Event()
        self._buffer_event.set()

    def set_channel(self, channel_type, channel):
        if channel_type == "file":
            self._file_channel = channel
            self._file_channel.on("open", self._on_both_channels_open)
            self._file_channel.bufferedAmountLowThreshold = MAX_BUFFERED_AMOUNT
            self._file_channel.on("bufferedamountlow", self._on_buffered_amount_low)
        elif channel_type == "control":
            self._control_channel = channel
            self._control_channel.on("open", self._on_both_channels_open)
            self._control_channel.on("message", lambda message: asyncio.create_task(self._on_control_message(message)))

    def _on_both_channels_open(self):
        if self._file_channel.readyState == "open" and self._control_channel.readyState == "open":
            self._sending_task = asyncio.create_task(self._start_file_transfer())

    async def _on_control_message(self, message):
        control_message = ControlMessage.from_json(message)
        match control_message.msg_type:
            case "transfer_complete":
                print(f"Receiver confirmed transfer complete: {control_message.data}")
                self._done.set_result(None)
                self._file_channel.close()
                self._control_channel.close()

    def _on_buffered_amount_low(self):
        if self._buffer_event.is_set() is False:
            # print("Buffered amount too high, waiting...")
            self._buffer_event.set()

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
                while self._file_channel.bufferedAmount > MAX_BUFFERED_AMOUNT:
                    self._buffer_event.clear()
                    await self._buffer_event.wait()

                self._file_channel.send(chunk)
                progress.update(chunk)

        self._control_channel.send(ControlMessage.create_json("eof", metadata["file_name"]))


class FileReceiver:
    def __init__(self, path):
        self._file_channel = None
        self._control_channel = None
        self._metadata = None
        self._location = None
        self._progress = None
        self._path = path
        self._done = asyncio.Future()
        self._chunk_queue = asyncio.Queue()
        self._eof_event = asyncio.Event()
        self._writer_task = None

    async def wait_until_done(self):
        await self._done

    def set_channel(self, channel_type, channel):
        if channel_type == "file":
            self._file_channel = channel
            self._file_channel.on("message", lambda message: self._chunk_queue.put_nowait(message))
        elif channel_type == "control":
            self._control_channel = channel
            self._control_channel.on("message", lambda message: asyncio.create_task(self._on_control_message(message)))

    async def _process_file(self):
        if not self._metadata or not self._path:
            print("[ERROR] Metadata or path not set before receiving file")
            return

        async with aiofiles.open(self._location, "wb") as f:
            while True:
                if self._chunk_queue.empty() and self._eof_event.is_set():
                    break
                chunk = await self._chunk_queue.get()
                if not isinstance(chunk, (bytes, bytearray)):
                    raise ValueError(f"Expected bytes but got {type(chunk)}")

                await f.write(chunk)
                self._progress.update(chunk)

        if compute_hash(self._location) != self._metadata["hash"]:
            print("[ERROR] File corrupted")
        else:
            print("File received successfully")
        # todo success or not
        self._control_channel.send(ControlMessage.create_json("transfer_complete", self._metadata["file_name"]))

        self._done.set_result(None)
        self._file_channel.close()
        self._control_channel.close()

    async def _on_control_message(self, message):
        control_message = ControlMessage.from_json(message)
        match control_message.msg_type:
            case "metadata":
                self._metadata = control_message.data
                self._location = os.path.join(self._path, self._metadata["file_name"])
                self._progress = Progress(self._metadata["file_size"])
                self._writer_task = asyncio.create_task(self._process_file())
                print(f"Receiving file: {self._metadata['file_name']} ({self._metadata['file_size']} bytes)")
            case "eof":
                self._eof_event.set()


class Peer:
    def __init__(self, role):
        self._signaling = WebSocketSignaling("ws://152.53.123.174:8001")
        ice_servers = [RTCIceServer(urls="stun:stun.l.google.com:19302")]
        rtc_config = RTCConfiguration(iceServers=ice_servers)
        self._pc = RTCPeerConnection(configuration=rtc_config)
        self._role = role

        if role == "send":
            self._file_handler = FileSender(args.path)
            self._coro = self._run_offer()
        elif role == "receive":
            self._file_handler = FileReceiver(args.path)
            self._coro = self._run_answer()

    async def start(self):
        await self._coro
        if self._role == "send":
            print("Waiting for file transfer to finish...")
        elif self._role == "receive":
            print("Waiting for file reception to finish...")
        await self._file_handler.wait_until_done()

    async def _consume_signaling(self):
        obj = await self._signaling.receive()

        if isinstance(obj, RTCSessionDescription):
            await self._pc.setRemoteDescription(obj)

            if obj.type == "offer":
                await self._pc.setLocalDescription(await self._pc.createAnswer())
                await self._signaling.send(self._pc.localDescription)

        elif isinstance(obj, RTCIceCandidate):
            await self._pc.addIceCandidate(obj)

    async def _run_answer(self):
        """
        The receiver uses this function to receive the offer from the signaling server
        """
        await self._signaling.connect()

        @self._pc.on("datachannel")
        def _on_datachannel(channel):
            if channel.label == "file":
                self._file_handler.set_channel("file", channel)
            elif channel.label == "control":
                self._file_handler.set_channel("control", channel)

        await self._consume_signaling()

    async def _run_offer(self):
        """
        The sender uses this function to send the offer to the signaling server
        """
        await self._signaling.connect()

        file_channel = self._pc.createDataChannel("file")
        control_channel = self._pc.createDataChannel("control")

        self._file_handler.set_channel("file", file_channel)
        self._file_handler.set_channel("control", control_channel)

        await self._pc.setLocalDescription(await self._pc.createOffer())
        await self._signaling.send(self._pc.localDescription)

        await self._consume_signaling()

    async def close(self):
        await self._signaling.close()
        await self._pc.close()


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

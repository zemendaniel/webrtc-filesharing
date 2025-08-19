import asyncio
import sys
import argparse
import asyncio
import logging
import time
import pickle
from aiortc import RTCIceCandidate, RTCPeerConnection, RTCSessionDescription, RTCConfiguration, RTCIceServer
from aiortc.contrib.signaling import BYE, add_signaling_arguments, create_signaling
import threading


class WebSocketSignaling:
    def __init__(self, uri):
        self.uri = uri
        self.ws = None

    async def connect(self):
        import websockets
        self.ws = await websockets.connect(self.uri)

    async def send(self, message):
        await self.ws.send(pickle.dumps(message))

    async def receive(self):
        data = await self.ws.recv()
        return pickle.loads(data)

    async def close(self):
        await self.ws.close()


class Messaging:
    def __init__(self):
        self.chat_channel = None
        self.control_channel = None
        self.input_task = None

    def set_channel(self, channel_type, channel):
        if channel_type == "chat":
            self.chat_channel = channel
        elif channel_type == "control":
            self.control_channel = channel

        if not (self.control_channel and self.chat_channel):
            return

        self.chat_channel.on("message", lambda msg: Messaging.__on_chat_message(msg))
        self.control_channel.on("message", lambda msg: Messaging.__on_control_message(msg))

        if self.chat_channel.readyState == "open":
            self.input_task = asyncio.create_task(self.input_loop())
        else:
            self.chat_channel.on("open", self.__on_chat_open)
    @staticmethod
    def __on_chat_message(message: str):
        print(f"<<< {message}")

    def __on_chat_open(self):
        print("Type messages below...")
        asyncio.create_task(self.input_loop())

    async def input_loop(self):
        loop = asyncio.get_running_loop()
        while True:
            msg = await loop.run_in_executor(None, input, ">>> ")
            if msg == "/close":
                if self.control_channel.readyState == "open":
                    self.control_channel.send("close")
                self.chat_channel.close()
                print("[You have closed the channel]")
                break

            if self.chat_channel.readyState == "open":
                self.chat_channel.send(msg)
            else:
                print("[The peer has closed the channel]")
    @staticmethod
    def __on_control_message(message):
        if message == "close":
            print("[Peer is disconnecting]")


async def consume_signaling(pc, signaling):
    while True:
        obj = await signaling.receive()

        if isinstance(obj, RTCSessionDescription):
            await pc.setRemoteDescription(obj)

            if obj.type == "offer":
                await pc.setLocalDescription(await pc.createAnswer())
                await signaling.send(pc.localDescription)

        elif isinstance(obj, RTCIceCandidate):
            await pc.addIceCandidate(obj)

        elif obj is BYE:
            print("[Signaling received BYE, exiting]")
            break


async def run_answer(pc, signaling):
    await signaling.connect()

    @pc.on("datachannel")
    def on_datachannel(channel):
        if channel.label == "chat":
            messaging.set_channel("chat", channel)
        elif channel.label == "control":
            messaging.set_channel("control", channel)

    await consume_signaling(pc, signaling)


async def run_offer(pc, signaling):
    global messaging
    await signaling.connect()

    # create channels
    chat_channel = pc.createDataChannel("chat")
    control_channel = pc.createDataChannel("control")

    # assign channels to messaging
    messaging.set_channel("chat", chat_channel)
    messaging.set_channel("control", control_channel)

    # send offer
    await pc.setLocalDescription(await pc.createOffer())
    await signaling.send(pc.localDescription)

    await consume_signaling(pc, signaling)


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    parser = argparse.ArgumentParser(description="Data channel file transfer")
    parser.add_argument("role", choices=["send", "receive"])
    # parser.add_argument("filename")
    parser.add_argument("--verbose", "-v", action="count")
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    signaling = WebSocketSignaling("ws://127.0.0.1:8001")
    ice_servers = [RTCIceServer(urls="stun:stun.l.google.com:19302")]
    rtc_config = RTCConfiguration(iceServers=ice_servers)
    pc = RTCPeerConnection(configuration=rtc_config)
    messaging = Messaging()
    if args.role == "send":
        coro = run_offer(pc, signaling)
    else:
        coro = run_answer(pc, signaling)

    # run event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(coro)
    except KeyboardInterrupt:
        pass
    finally:
        if messaging.input_task:
            messaging.input_task.cancel()
        loop.run_until_complete(pc.close())
        loop.run_until_complete(signaling.close())

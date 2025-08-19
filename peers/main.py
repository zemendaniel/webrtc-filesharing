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
    def __init__(self, stop_event):
        self.chat_channel = None
        self.control_channel = None
        self.input_task = None
        self.stop_event = stop_event

    def set_channel(self, channel_type, channel):
        if channel_type == "chat":
            self.chat_channel = channel
        elif channel_type == "control":
            self.control_channel = channel

        if not (self.control_channel and self.chat_channel):
            return

        self.chat_channel.on("message", lambda msg: Messaging.__on_chat_message(msg))
        self.control_channel.on("message", lambda msg: self.__on_control_message(msg))

        self.input_task = asyncio.create_task(self.__input_loop())

    @staticmethod
    def __on_chat_message(message: str):
        print(f"<<< {message}")

    async def __input_loop(self):
        print("Type message to send or /close to close the channel")
        try:
            while not self.stop_event.is_set():
                msg = await aioconsole.ainput(">>> ")
                if msg == "/close":
                    if self.control_channel.readyState == "open":
                        await self.control_channel.send("close")
                    await self.chat_channel.close()
                    print("[You have closed the channel]")
                    self.stop_event.set()
                    break

                if self.chat_channel.readyState == "open":
                    self.chat_channel.send(msg)
                else:
                    print("[The chanel got disconnected]")
                    self.stop_event.set()
                    break
        except asyncio.CancelledError:
            pass

    def __on_control_message(self, message):
        if message == "close":
            print("[Peer is disconnecting]")
            self.stop_event.set()
            if self.input_task:
                self.input_task.cancel()


class Peer:
    def __init__(self, role):
        self.signaling = WebSocketSignaling("ws://152.53.123.174:8001")
        self.stop_event = asyncio.Event()
        self.messaging = Messaging(stop_event=self.stop_event)

        ice_servers = [RTCIceServer(urls="stun:stun.l.google.com:19302")]
        rtc_config = RTCConfiguration(iceServers=ice_servers)
        self.pc = RTCPeerConnection(configuration=rtc_config)

        if role == "send":
            self.coro = self.run_offer()
        elif role == "receive":
            self.coro = self.run_answer()

    async def start(self):
        await self.coro

    async def consume_signaling(self):
        """
        The main loop of the peer
        """
        try:
            while not self.stop_event.is_set():
                obj = await self.signaling.receive()

                if isinstance(obj, RTCSessionDescription):
                    await self.pc.setRemoteDescription(obj)

                    if obj.type == "offer":
                        await self.pc.setLocalDescription(await self.pc.createAnswer())
                        await self.signaling.send(self.pc.localDescription)

                elif isinstance(obj, RTCIceCandidate):
                    await self.pc.addIceCandidate(obj)
        except asyncio.CancelledError:
            pass
        finally:
            await self.pc.close()
            await self.signaling.close()

    async def run_answer(self):
        """
        The receiver uses this function to receive the offer from the signaling server and start the loop
        """
        await self.signaling.connect()

        @self.pc.on("datachannel")
        def on_datachannel(channel):
            if channel.label == "chat":
                self.messaging.set_channel("chat", channel)
            elif channel.label == "control":
                self.messaging.set_channel("control", channel)

        await self.consume_signaling()

    async def run_offer(self):
        """
        The sender uses this function to send the offer to the signaling server and start the loop
        """
        await self.signaling.connect()

        chat_channel = self.pc.createDataChannel("chat")
        control_channel = self.pc.createDataChannel("control")

        self.messaging.set_channel("chat", chat_channel)
        self.messaging.set_channel("control", control_channel)

        await self.pc.setLocalDescription(await self.pc.createOffer())
        await self.signaling.send(self.pc.localDescription)

        await self.consume_signaling()


async def main():
    peer = Peer(args.role)
    await peer.start()


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(description="Data channel file transfer")
    parser.add_argument("role", choices=["send", "receive"])
    parser.add_argument("--verbose", "-v", action="count")

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    # loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(loop)
    asyncio.run(main())
    # try:
    #     loop.run_until_complete(main_task)
    # except KeyboardInterrupt:
    #     messaging.stop_event.set()
    #     if messaging.input_task:
    #         messaging.input_task.cancel()
    #     loop.run_until_complete(main_task)
    # finally:
    #     if messaging.input_task:
    #         messaging.input_task.cancel()
    #     loop.run_until_complete(pc.close())
    #     loop.run_until_complete(signaling.close())
    #     loop.close()

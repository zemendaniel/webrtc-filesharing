import asyncio
from websockets.asyncio.server import serve


clients = set()


async def handler(websocket):
    clients.add(websocket)
    try:
        while True:
            message = await websocket.recv()
            print(message)
            for client in clients:
                if client != websocket:
                    await client.send(message)
    except:
        print('A client disconnected.')
    finally:
        clients.remove(websocket)


async def main():
    async with serve(handler, "", 8001) as server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())

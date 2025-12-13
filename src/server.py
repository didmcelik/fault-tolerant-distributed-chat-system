import asyncio
import json
import socket

from common import read_json, write_json

DISCOVERY_PORT = 37020

class ChatServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.clients = set()

    async def start(self):
        asyncio.create_task(self.discovery_listener())

        server = await asyncio.start_server(
            self.handle_client, self.host, self.port
        )

        print(f"[SERVER] Running on {self.host}:{self.port}")
        async with server:
            await server.serve_forever()

    async def discovery_listener(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", DISCOVERY_PORT))
        sock.setblocking(False)

        loop = asyncio.get_running_loop()
        print("[DISCOVERY] UDP listener started")

        while True:
            data, addr = await loop.sock_recvfrom(sock, 1024)
            msg = json.loads(data.decode())

            if msg.get("type") == "DISCOVER":
                reply = {
                    "type": "DISCOVER_REPLY",
                    "server_host": self.host,
                    "server_port": self.port
                }
                await loop.sock_sendto(
                    sock, json.dumps(reply).encode(), addr
                )

    async def handle_client(self, reader, writer):
        self.clients.add(writer)
        print("[CLIENT] Connected")

        try:
            while True:
                msg = await read_json(reader)
                if msg["type"] == "CHAT":
                    await self.broadcast(msg)
        except:
            pass
        finally:
            self.clients.remove(writer)
            writer.close()
            await writer.wait_closed()
            print("[CLIENT] Disconnected")

    async def broadcast(self, msg: dict):
        dead = []
        for w in self.clients:
            try:
                await write_json(w, msg)
            except:
                dead.append(w)
        for w in dead:
            self.clients.remove(w)

async def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, required=True)
    args = p.parse_args()

    server = ChatServer(args.host, args.port)
    await server.start()

if __name__ == "__main__":
    asyncio.run(main())

import asyncio
import json
import socket

DISCOVERY_PORT = 37020

def get_local_ip() -> str:
    """Return the LAN IP address that other devices can connect to."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # No real traffic is sent; this just selects the outbound interface.
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

class ChatServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.clients: set[asyncio.StreamWriter] = set()

    async def start(self):
        asyncio.create_task(self.discovery_listener())

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"[SERVER] TCP chat listening on {self.host}:{self.port}")
        print(f"[DISCOVERY] UDP listening on 0.0.0.0:{DISCOVERY_PORT}")

        async with server:
            await server.serve_forever()

    async def discovery_listener(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", DISCOVERY_PORT))
        sock.setblocking(False)

        loop = asyncio.get_running_loop()
        while True:
            data, addr = await loop.sock_recvfrom(sock, 2048)
            try:
                msg = json.loads(data.decode("utf-8"))
            except json.JSONDecodeError:
                continue

            if msg.get("type") == "DISCOVER":
                reply = {
                    "type": "DISCOVER_REPLY",
                    "server_host": get_local_ip(),
                    "server_port": self.port,
                }
                await loop.sock_sendto(sock, json.dumps(reply).encode("utf-8"), addr)

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.clients.add(writer)
        peer = writer.get_extra_info("peername")
        print(f"[CLIENT] Connected: {peer}")

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                msg = json.loads(line.decode("utf-8"))
                if msg.get("type") == "CHAT":
                    await self.broadcast(msg)
        except Exception:
            pass
        finally:
            self.clients.discard(writer)
            writer.close()
            await writer.wait_closed()
            print(f"[CLIENT] Disconnected: {peer}")

    async def broadcast(self, msg: dict):
        dead = []
        payload = (json.dumps(msg) + "\n").encode("utf-8")

        for w in self.clients:
            try:
                w.write(payload)
                await w.drain()
            except Exception:
                dead.append(w)

        for w in dead:
            self.clients.discard(w)

async def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0")  # listen on all interfaces
    p.add_argument("--port", type=int, default=5001)
    args = p.parse_args()

    server = ChatServer(args.host, args.port)
    await server.start()

if __name__ == "__main__":
    asyncio.run(main())

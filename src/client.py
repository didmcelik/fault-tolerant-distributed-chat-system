import asyncio
import json
import socket

from common import read_json, write_json

DISCOVERY_PORT = 37020

async def discover_server(timeout=3):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(timeout)

    msg = json.dumps({"type": "DISCOVER"}).encode()
    sock.sendto(msg, ("255.255.255.255", DISCOVERY_PORT))

    data, _ = sock.recvfrom(1024)
    reply = json.loads(data.decode())
    return reply["server_host"], reply["server_port"]

async def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--user", required=True)
    args = p.parse_args()

    host, port = await discover_server()
    print(f"[CLIENT] Discovered server {host}:{port}")

    reader, writer = await asyncio.open_connection(host, port)

    async def sender():
        loop = asyncio.get_running_loop()
        while True:
            text = await loop.run_in_executor(None, input, "")
            await write_json(writer, {
                "type": "CHAT",
                "from": args.user,
                "text": text
            })

    async def receiver():
        while True:
            msg = await read_json(reader)
            print(f"{msg['from']}: {msg['text']}")

    await asyncio.gather(sender(), receiver())

if __name__ == "__main__":
    asyncio.run(main())

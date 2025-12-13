import asyncio
import json
import socket
from typing import Optional, Tuple

from common import read_json_line, write_json_line

DISCOVERY_PORT = 37020
DISCOVERY_MAGIC = "DS1_CHAT_DISCOVERY_V1"

async def discover_server(timeout_sec: float = 1.0) -> Tuple[str, int]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(timeout_sec)

    msg = {"magic": DISCOVERY_MAGIC, "type": "DISCOVER_SERVER"}
    sock.sendto(json.dumps(msg).encode("utf-8"), ("255.255.255.255", DISCOVERY_PORT))

    data, _ = sock.recvfrom(64 * 1024)
    reply = json.loads(data.decode("utf-8"))

    leader_host = reply["leader_host"]
    # reply also has leader_peer_port, but client connects to server_port
    server_port = reply["server_port"]
    return leader_host, int(server_port)

async def run_client(username: str) -> None:
    host, port = await discover_server()
    print(f"[client] discovered server at {host}:{port}")

    reader, writer = await asyncio.open_connection(host, port)
    print("[client] connected. Type messages and press Enter.")

    async def receiver():
        while True:
            msg = await read_json_line(reader)
            if msg.get("type") == "CHAT_DELIVER":
                print(f"[{msg['seq']}] {msg['from']}: {msg['text']}")

    async def sender():
        loop = asyncio.get_running_loop()
        while True:
            text = await loop.run_in_executor(None, input, "")
            await write_json_line(writer, {
                "type": "CHAT_SEND",
                "room": "main",
                "from": username,
                "text": text
            })

    await asyncio.gather(receiver(), sender())

async def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--user", required=True)
    args = p.parse_args()
    await run_client(args.user)

if __name__ == "__main__":
    asyncio.run(main())

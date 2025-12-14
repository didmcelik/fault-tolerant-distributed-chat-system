import asyncio
import json
import socket

DISCOVERY_PORT = 37020
DISCOVERY_TIMEOUT = 3.0

def discover_server() -> tuple[str, int]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(DISCOVERY_TIMEOUT)

    msg = {"type": "DISCOVER"}
    sock.sendto(json.dumps(msg).encode("utf-8"), ("255.255.255.255", DISCOVERY_PORT))

    data, _ = sock.recvfrom(2048)
    reply = json.loads(data.decode("utf-8"))

    if reply.get("type") != "DISCOVER_REPLY":
        raise RuntimeError(f"Unexpected reply: {reply}")

    return reply["server_host"], int(reply["server_port"])

async def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--user", required=True)
    args = p.parse_args()

    host, port = discover_server()
    print(f"[CLIENT] Discovered server at {host}:{port}")

    reader, writer = await asyncio.open_connection(host, port)

    async def sender():
        loop = asyncio.get_running_loop()
        while True:
            text = await loop.run_in_executor(None, input, "")
            msg = {"type": "CHAT", "from": args.user, "text": text}
            writer.write((json.dumps(msg) + "\n").encode("utf-8"))
            await writer.drain()

    async def receiver():
        while True:
            line = await reader.readline()
            if not line:
                break
            msg = json.loads(line.decode("utf-8"))
            print(f"{msg['from']}: {msg['text']}")

    await asyncio.gather(sender(), receiver())

if __name__ == "__main__":
    asyncio.run(main())

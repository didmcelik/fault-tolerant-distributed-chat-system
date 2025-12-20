import argparse
import asyncio
import json
import socket
from typing import Optional, Tuple

DISCOVERY_PORT = 37020
DISCOVERY_TIMEOUT = 3.0
RECONNECT_DELAY_SECONDS = 1.0
MAX_QUEUE_SIZE = 200


def discover_server() -> Tuple[str, int]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    sock.settimeout(DISCOVERY_TIMEOUT)
    sock.sendto(json.dumps({"type": "DISCOVER"}).encode("utf-8"), ("255.255.255.255", DISCOVERY_PORT))
    data, _ = sock.recvfrom(2048)
    reply = json.loads(data.decode("utf-8"))
    if reply.get("type") != "DISCOVER_REPLY":
        raise RuntimeError(f"Unexpected reply: {reply}")
    return str(reply["server_host"]), int(reply["server_port"])


async def send_json_line(writer: asyncio.StreamWriter, msg: dict) -> None:
    writer.write((json.dumps(msg) + "\n").encode("utf-8"))
    await writer.drain()


async def read_json_line(reader: asyncio.StreamReader) -> Optional[dict]:
    line = await reader.readline()
    if not line:
        return None
    return json.loads(line.decode("utf-8"))


class ClientApp:
    def __init__(self, username: str, room: str):
        self.username = username
        self.room = room
        self.outgoing_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self._stop = False

    async def input_loop(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop:
            text = await loop.run_in_executor(None, input, "")
            text = text.strip()
            if not text:
                continue
            if text.lower() in ("/quit", "/exit"):
                self._stop = True
                break
            msg = {"type": "CHAT", "from": self.username, "room": self.room, "text": text}
            try:
                self.outgoing_queue.put_nowait(msg)
            except asyncio.QueueFull:
                print("[CLIENT] Outgoing queue full; dropping message.")

    async def join_via_coordinator(self) -> Tuple[str, int]:
        """
        Join via coordinator (leader):
          - DISCOVER to get any coordinator endpoint
          - JOIN_REQUEST(room)
          - if REDIRECT -> connect to leader coordinator and retry
          - expect JOIN_ASSIGN with assigned_host + assigned_room_port
        Returns (assigned_host, assigned_room_port).
        """
        host, port = discover_server()
        print(f"[CLIENT] Discovered coordinator at {host}:{port}")

        reader, writer = await asyncio.open_connection(host, port)
        await send_json_line(writer, {"type": "JOIN_REQUEST", "from": self.username, "room": self.room})

        resp = await read_json_line(reader)
        if resp is None:
            writer.close()
            await writer.wait_closed()
            raise ConnectionError("Coordinator closed during join.")

        if resp.get("type") == "REDIRECT":
            leader_host = str(resp.get("leader_host"))
            leader_port = int(resp.get("leader_port"))
            writer.close()
            await writer.wait_closed()

            print(f"[CLIENT] Redirected to leader coordinator at {leader_host}:{leader_port}")
            reader, writer = await asyncio.open_connection(leader_host, leader_port)
            await send_json_line(writer, {"type": "JOIN_REQUEST", "from": self.username, "room": self.room})
            resp = await read_json_line(reader)
            if resp is None:
                writer.close()
                await writer.wait_closed()
                raise ConnectionError("Leader coordinator closed during join.")

        if resp.get("type") == "JOIN_ASSIGN":
            assigned_host = str(resp.get("assigned_host"))
            assigned_room_port = int(resp.get("assigned_room_port"))
            assigned_sid = str(resp.get("assigned_server_id", ""))
            print(f"[CLIENT] Assigned room='{self.room}' to server={assigned_sid[:8]} at {assigned_host}:{assigned_room_port}")
            writer.close()
            await writer.wait_closed()
            return assigned_host, assigned_room_port

        if resp.get("type") == "ERROR":
            writer.close()
            await writer.wait_closed()
            raise RuntimeError(f"Join failed: {resp.get('message')}")

        writer.close()
        await writer.wait_closed()
        raise RuntimeError(f"Unexpected join response: {resp}")

    async def connect_room(self) -> Tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        assigned_host, assigned_room_port = await self.join_via_coordinator()
        reader, writer = await asyncio.open_connection(assigned_host, assigned_room_port)
        print(f"[CLIENT] Connected to room endpoint {assigned_host}:{assigned_room_port}")
        return reader, writer

    async def sender_loop(self, writer: asyncio.StreamWriter) -> None:
        while not self._stop:
            msg = await self.outgoing_queue.get()
            try:
                await send_json_line(writer, msg)
            except Exception:
                try:
                    self.outgoing_queue.put_nowait(msg)
                except asyncio.QueueFull:
                    pass
                raise

    async def receiver_loop(self, reader: asyncio.StreamReader) -> None:
        while not self._stop:
            msg = await read_json_line(reader)
            if msg is None:
                raise ConnectionError("Room server disconnected.")
            if msg.get("type") == "CHAT":
                print(f"{msg.get('from', '?')}: {msg.get('text', '')}")

    async def connection_loop(self) -> None:
        while not self._stop:
            try:
                reader, writer = await self.connect_room()
                try:
                    await asyncio.gather(self.sender_loop(writer), self.receiver_loop(reader))
                finally:
                    writer.close()
                    try:
                        await writer.wait_closed()
                    except Exception:
                        pass
            except Exception as e:
                if self._stop:
                    break
                print(f"[CLIENT] Connection lost/join failed: {e}")
                print(f"[CLIENT] Reconnecting in {RECONNECT_DELAY_SECONDS:.1f}s...")
                await asyncio.sleep(RECONNECT_DELAY_SECONDS)

        print("[CLIENT] Exiting.")

    async def run(self) -> None:
        await asyncio.gather(self.input_loop(), self.connection_loop())


async def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--user", required=True)
    p.add_argument("--room", required=True)
    args = p.parse_args()

    app = ClientApp(args.user, args.room)
    await app.run()


if __name__ == "__main__":
    asyncio.run(main())

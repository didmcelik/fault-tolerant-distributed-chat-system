import argparse
import asyncio
import json
import socket
import uuid
from typing import Optional, Tuple, Dict, Any, List

DISCOVERY_PORT = 37020
DISCOVERY_TIMEOUT = 3.0
RECONNECT_DELAY_SECONDS = 1.0
MAX_QUEUE_SIZE = 200

HOLD_BACK_TICK_SECONDS = 0.05


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


def _vc_get(vc: Dict[str, int], pid: str) -> int:
    return int(vc.get(pid, 0))


def _vc_merge_max(local_vc: Dict[str, int], received_vc: Dict[str, int]) -> None:
    for pid, ts in received_vc.items():
        try:
            ts_i = int(ts)
        except Exception:
            continue
        local_vc[pid] = max(int(local_vc.get(pid, 0)), ts_i)


def _is_deliverable(local_vc: Dict[str, int], sender_id: str, received_vc: Dict[str, int]) -> bool:
    """
    Classic causal delivery rule:
      deliverable iff
        received_vc[sender] == local_vc[sender] + 1
        and for all k != sender: received_vc[k] <= local_vc[k]
    """
    if not sender_id:
        return True

    if _vc_get(received_vc, sender_id) != _vc_get(local_vc, sender_id) + 1:
        return False

    for pid, ts in received_vc.items():
        if pid == sender_id:
            continue
        if _vc_get(received_vc, pid) > _vc_get(local_vc, pid):
            return False

    return True


class ClientApp:
    def __init__(self, username: str, room: str):
        self.username = username
        self.room = room

        # Stable process id (better than IP for reconnect / portability).
        # The example uses IP as the key; this is the DS1-safe equivalent.
        self.client_id = str(uuid.uuid4())

        # Vector clock stored in-memory (dict: pid -> int).
        # Initialize own entry.
        self.vc: Dict[str, int] = {self.client_id: 0}

        self.outgoing_queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)
        self._stop = False

        # Hold-back queue (client-side causal ordering like the example project)
        self._holdback: List[dict] = []
        self._hb_lock = asyncio.Lock()
        self._holdback_task: Optional[asyncio.Task] = None

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

            # SEND event: increment own VC entry, attach snapshot (example-style)
            self.vc[self.client_id] = _vc_get(self.vc, self.client_id) + 1

            msg = {
                "type": "CHAT",
                "from": self.username,
                "sender_id": self.client_id,
                "room": self.room,
                "text": text,
                "vc": dict(self.vc),  # snapshot
            }

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
                # Re-queue best-effort
                try:
                    self.outgoing_queue.put_nowait(msg)
                except asyncio.QueueFull:
                    pass
                raise

    async def _deliver_chat(self, msg: dict) -> None:
        """
        Deliver a chat message:
          - merge VC via max (example updates local clock based on received)
          - print
        """
        sender_id = str(msg.get("sender_id", ""))
        received_vc = msg.get("vc")
        if isinstance(received_vc, dict):
            received_vc_int: Dict[str, int] = {}
            for k, v in received_vc.items():
                try:
                    received_vc_int[str(k)] = int(v)
                except Exception:
                    pass

            # RECEIVE merge: VC = max(VC, received_vc)
            _vc_merge_max(self.vc, received_vc_int)

            # Ensure sender entry is at least what we received (already covered by max)
            if sender_id:
                self.vc[sender_id] = max(_vc_get(self.vc, sender_id), _vc_get(received_vc_int, sender_id))

        print(f"{msg.get('from', '?')}: {msg.get('text', '')}")

    async def _enqueue_holdback(self, msg: dict) -> None:
        async with self._hb_lock:
            self._holdback.append(msg)

    async def _try_deliver_from_holdback_once(self) -> bool:
        """
        Try delivering any deliverable message from holdback.
        Returns True if progress was made.
        """
        async with self._hb_lock:
            for i, msg in enumerate(self._holdback):
                sender_id = str(msg.get("sender_id", ""))
                received_vc = msg.get("vc")
                if not isinstance(received_vc, dict):
                    # If no VC, deliver immediately
                    self._holdback.pop(i)
                    await self._deliver_chat(msg)
                    return True

                received_vc_int: Dict[str, int] = {}
                for k, v in received_vc.items():
                    try:
                        received_vc_int[str(k)] = int(v)
                    except Exception:
                        pass

                if _is_deliverable(self.vc, sender_id, received_vc_int):
                    self._holdback.pop(i)
                    await self._deliver_chat(msg)
                    return True

        return False

    async def holdback_loop(self) -> None:
        """
        Example-style background processing:
        keep checking holdback queue and deliver when possible.
        """
        while not self._stop:
            progressed = await self._try_deliver_from_holdback_once()
            if not progressed:
                await asyncio.sleep(HOLD_BACK_TICK_SECONDS)

    async def receiver_loop(self, reader: asyncio.StreamReader) -> None:
        while not self._stop:
            msg = await read_json_line(reader)
            if msg is None:
                raise ConnectionError("Room server disconnected.")

            if msg.get("type") != "CHAT":
                continue
            if msg.get("room") != self.room:
                # Safety: ignore any cross-room traffic
                continue

            sender_id = str(msg.get("sender_id", ""))
            received_vc = msg.get("vc")

            if not isinstance(received_vc, dict):
                await self._deliver_chat(msg)
                continue

            received_vc_int: Dict[str, int] = {}
            for k, v in received_vc.items():
                try:
                    received_vc_int[str(k)] = int(v)
                except Exception:
                    pass

            if _is_deliverable(self.vc, sender_id, received_vc_int):
                await self._deliver_chat(msg)

                # After delivering one message, try draining holdback as much as possible
                while await self._try_deliver_from_holdback_once():
                    pass
            else:
                await self._enqueue_holdback(msg)

    async def connection_loop(self) -> None:
        while not self._stop:
            try:
                reader, writer = await self.connect_room()

                # Start holdback processing when connected (example starts it with output)
                if self._holdback_task is None or self._holdback_task.done():
                    self._holdback_task = asyncio.create_task(self.holdback_loop())

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

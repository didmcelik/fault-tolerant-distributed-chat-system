import asyncio
import json
import socket
import uuid
import time
from dataclasses import dataclass, asdict
from typing import Dict, Optional, Set, List

# Client discovery
DISCOVERY_PORT = 37020

# Server control plane: membership + heartbeats (later election)
SERVER_CONTROL_PORT = 37021

# Membership gossip (leader)
LEADER_GOSSIP_INTERVAL = 2.0

# Heartbeats
HEARTBEAT_INTERVAL = 1.0
FAILURE_TIMEOUT = 4.0  # seconds without hearing from a server -> suspect failed


def now() -> float:
    return time.time()


def get_local_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    finally:
        s.close()


@dataclass(frozen=True)
class ServerInfo:
    id: str
    host: str
    chat_port: int
    control_port: int = SERVER_CONTROL_PORT


class ChatServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

        self.server_id = str(uuid.uuid4())
        self.advertised_host = get_local_ip()

        # Membership view replicated at every server
        self.membership: Dict[str, ServerInfo] = {
            self.server_id: ServerInfo(
                id=self.server_id,
                host=self.advertised_host,
                chat_port=self.port,
                control_port=SERVER_CONTROL_PORT,
            )
        }

        # last_seen timestamps for failure detection (include self)
        self.last_seen: Dict[str, float] = {self.server_id: now()}

        # Deterministic leader placeholder (until LCR)
        self.leader_id: str = self.server_id
        self._recompute_leader()

        # TCP clients
        self.clients: Set[asyncio.StreamWriter] = set()

        # UDP control socket (shared for send)
        self._control_send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._control_send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._control_send_sock.setblocking(False)

    # ---------------- Utilities ----------------
    def _recompute_leader(self) -> None:
        self.leader_id = min(self.membership.keys())

    def _print_membership(self) -> None:
        leader = self.leader_id
        servers = sorted(self.membership.values(), key=lambda s: s.id)
        print(f"[MEMBERSHIP] me={self.server_id[:8]} leader={leader[:8]} servers={len(servers)}")
        for s in servers:
            role = " (LEADER)" if s.id == leader else ""
            age = now() - self.last_seen.get(s.id, 0.0)
            print(f"  - {s.id[:8]} {s.host}:{s.chat_port} ctrl:{s.control_port}{role} last_seen={age:0.1f}s ago")

    def _serverinfo_from_dict(self, d: dict) -> Optional[ServerInfo]:
        try:
            sid = str(d["id"])
            host = str(d["host"])
            chat_port = int(d["chat_port"])
            control_port = int(d.get("control_port", SERVER_CONTROL_PORT))
            return ServerInfo(id=sid, host=host, chat_port=chat_port, control_port=control_port)
        except Exception:
            return None

    def _membership_payload(self, mtype: str) -> dict:
        return {
            "type": mtype,
            "leader_id": self.leader_id,
            "servers": [asdict(s) for s in self.membership.values()],
        }

    def _touch(self, sid: str) -> None:
        self.last_seen[sid] = now()

    def _merge_membership(self, servers: List[dict], leader_id: Optional[str]) -> bool:
        changed = False

        for s in servers:
            info = self._serverinfo_from_dict(s)
            if not info or info.id == self.server_id:
                continue

            if info.id not in self.membership or self.membership[info.id] != info:
                self.membership[info.id] = info
                changed = True

            # seeing a server in a membership snapshot means it's likely alive recently
            if info.id not in self.last_seen:
                self.last_seen[info.id] = now()
                changed = True

        if isinstance(leader_id, str) and leader_id and leader_id != self.leader_id:
            self.leader_id = leader_id
            changed = True

        if changed:
            self._recompute_leader()

        return changed

    # ---------------- Main start ----------------
    async def start(self) -> None:
        asyncio.create_task(self.discovery_listener())
        asyncio.create_task(self.server_control_listener())
        asyncio.create_task(self.broadcast_server_hello())
        asyncio.create_task(self.leader_gossip_loop())
        asyncio.create_task(self.heartbeat_loop())
        asyncio.create_task(self.failure_detector_loop())

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"[SERVER] id={self.server_id[:8]}")
        print(f"[SERVER] TCP chat listening on {self.host}:{self.port} (advertise {self.advertised_host}:{self.port})")
        print(f"[DISCOVERY] UDP listening on 0.0.0.0:{DISCOVERY_PORT}")
        print(f"[CONTROL] UDP listening on 0.0.0.0:{SERVER_CONTROL_PORT}")
        self._print_membership()

        async with server:
            await server.serve_forever()

    # ---------------- Client discovery (UDP) ----------------
    async def discovery_listener(self) -> None:
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
                reply = {"type": "DISCOVER_REPLY", "server_host": self.advertised_host, "server_port": self.port}
                await loop.sock_sendto(sock, json.dumps(reply).encode("utf-8"), addr)

    # ---------------- Server control (UDP) ----------------
    async def server_control_listener(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", SERVER_CONTROL_PORT))
        sock.setblocking(False)

        loop = asyncio.get_running_loop()
        while True:
            data, addr = await loop.sock_recvfrom(sock, 65535)
            try:
                msg = json.loads(data.decode("utf-8"))
            except json.JSONDecodeError:
                continue

            mtype = msg.get("type")

            if mtype == "SERVER_HELLO":
                info = self._serverinfo_from_dict(msg)
                if info and info.id != self.server_id:
                    # mark sender alive
                    self._touch(info.id)

                    changed = False
                    if info.id not in self.membership or self.membership[info.id] != info:
                        self.membership[info.id] = info
                        changed = True
                        self._recompute_leader()

                    # reply with our current view (any server can do this)
                    welcome = self._membership_payload("SERVER_WELCOME")
                    await loop.sock_sendto(sock, json.dumps(welcome).encode("utf-8"), addr)

                    if changed:
                        self._print_membership()

            elif mtype in ("SERVER_WELCOME", "MEMBERSHIP"):
                servers = msg.get("servers", [])
                leader_id = msg.get("leader_id")
                if isinstance(servers, list):
                    changed = self._merge_membership(servers, leader_id)
                    if changed:
                        self._print_membership()

            elif mtype == "HEARTBEAT":
                leader_id = str(msg.get("leader_id", ""))
                sender_id = str(msg.get("from", ""))

                # mark leader/sender alive
                if sender_id:
                    self._touch(sender_id)

                # reply ACK back to the sender address
                ack = {"type": "HEARTBEAT_ACK", "from": self.server_id}
                await loop.sock_sendto(sock, json.dumps(ack).encode("utf-8"), addr)

            elif mtype == "HEARTBEAT_ACK":
                sender_id = str(msg.get("from", ""))
                if sender_id:
                    self._touch(sender_id)

            # else: ignore unknown types for now (we’ll add election next)

    async def broadcast_server_hello(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(False)

        loop = asyncio.get_running_loop()
        hello = {
            "type": "SERVER_HELLO",
            "id": self.server_id,
            "host": self.advertised_host,
            "chat_port": self.port,
            "control_port": SERVER_CONTROL_PORT,
        }
        payload = json.dumps(hello).encode("utf-8")
        bcast_addr = ("255.255.255.255", SERVER_CONTROL_PORT)

        for _ in range(3):
            try:
                await loop.sock_sendto(sock, payload, bcast_addr)
            except Exception:
                pass
            await asyncio.sleep(0.3)

        sock.close()

    async def leader_gossip_loop(self) -> None:
        await asyncio.sleep(1.0)
        loop = asyncio.get_running_loop()

        while True:
            await asyncio.sleep(LEADER_GOSSIP_INTERVAL)

            if self.leader_id != self.server_id:
                continue

            msg = self._membership_payload("MEMBERSHIP")
            payload = json.dumps(msg).encode("utf-8")

            for sid, sinfo in list(self.membership.items()):
                if sid == self.server_id:
                    continue
                try:
                    await loop.sock_sendto(self._control_send_sock, payload, (sinfo.host, sinfo.control_port))
                except Exception:
                    pass

    # ---------------- Heartbeats + failure detection ----------------
    async def heartbeat_loop(self) -> None:
        """Leader sends periodic heartbeats to all known servers."""
        await asyncio.sleep(1.0)
        loop = asyncio.get_running_loop()

        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            if self.leader_id != self.server_id:
                continue

            hb = {"type": "HEARTBEAT", "leader_id": self.leader_id, "from": self.server_id, "ts": now()}
            payload = json.dumps(hb).encode("utf-8")

            for sid, sinfo in list(self.membership.items()):
                if sid == self.server_id:
                    continue
                try:
                    await loop.sock_sendto(self._control_send_sock, payload, (sinfo.host, sinfo.control_port))
                except Exception:
                    pass

    async def failure_detector_loop(self) -> None:
        """Remove servers not seen for FAILURE_TIMEOUT; detect leader suspicion."""
        while True:
            await asyncio.sleep(1.0)

            # Leader suspicion for followers
            if self.leader_id != self.server_id:
                leader_last = self.last_seen.get(self.leader_id, 0.0)
                if now() - leader_last > FAILURE_TIMEOUT:
                    print(f"[FAILURE] Leader suspected failed: {self.leader_id[:8]} (no heartbeat).")
                    # Election will be started here later (LCR)
                    # For now just log.

            # Remove failed servers (including old leader entries)
            to_remove = []
            for sid in list(self.membership.keys()):
                if sid == self.server_id:
                    continue
                last = self.last_seen.get(sid, 0.0)
                if now() - last > FAILURE_TIMEOUT:
                    to_remove.append(sid)

            if to_remove:
                for sid in to_remove:
                    info = self.membership.pop(sid, None)
                    self.last_seen.pop(sid, None)
                    if info:
                        print(f"[FAILURE] Removing server {sid[:8]} from membership (timeout).")

                self._recompute_leader()
                self._print_membership()

    # ---------------- TCP chat ----------------
    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
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
                    await self.broadcast_chat(msg)
        except Exception:
            pass
        finally:
            self.clients.discard(writer)
            writer.close()
            await writer.wait_closed()
            print(f"[CLIENT] Disconnected: {peer}")

    async def broadcast_chat(self, msg: dict) -> None:
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


async def main() -> None:
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=5001)
    args = p.parse_args()

    server = ChatServer(args.host, args.port)
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())

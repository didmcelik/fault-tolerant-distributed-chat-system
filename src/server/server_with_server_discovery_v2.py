import asyncio
import json
import socket
import uuid
from dataclasses import dataclass, asdict
from typing import Dict, Optional, Set, Tuple, List

# Client discovery (client -> broadcast DISCOVER, server -> unicast DISCOVER_REPLY)
DISCOVERY_PORT = 37020

# Server-to-server control plane (membership; later heartbeats/election)
SERVER_CONTROL_PORT = 37021

# How often the leader gossips the current membership view (seconds)
LEADER_GOSSIP_INTERVAL = 2.0


def get_local_ip() -> str:
    """Return the LAN IP address that other devices can connect to."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # No real traffic is sent; this selects the outbound interface.
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
        self.host = host                 # bind address for TCP chat
        self.port = port                 # TCP chat port
        self.server_id = str(uuid.uuid4())

        # IP we advertise to others (must be a real LAN address, not 0.0.0.0)
        self.advertised_host = get_local_ip()

        # Membership view: id -> ServerInfo
        self.membership: Dict[str, ServerInfo] = {}
        self.membership[self.server_id] = ServerInfo(
            id=self.server_id,
            host=self.advertised_host,
            chat_port=self.port,
            control_port=SERVER_CONTROL_PORT,
        )

        # Leader: for now deterministic = lexicographically smallest server_id
        self.leader_id: str = self.server_id
        self._recompute_leader()

        # TCP clients (for chat)
        self.clients: Set[asyncio.StreamWriter] = set()

    # ------------------------
    # Utilities
    # ------------------------
    def _recompute_leader(self) -> None:
        self.leader_id = min(self.membership.keys())

    def _print_membership(self) -> None:
        leader = self.leader_id
        servers = sorted(self.membership.values(), key=lambda s: s.id)
        print(f"[MEMBERSHIP] leader={leader} servers={len(servers)}")
        for s in servers:
            role = " (LEADER)" if s.id == leader else ""
            print(f"  - {s.id[:8]} {s.host}:{s.chat_port} ctrl:{s.control_port}{role}")

    def _serverinfo_from_msg(self, msg: dict) -> Optional[ServerInfo]:
        try:
            sid = str(msg["id"])
            host = str(msg["host"])
            chat_port = int(msg["chat_port"])
            control_port = int(msg.get("control_port", SERVER_CONTROL_PORT))
            return ServerInfo(id=sid, host=host, chat_port=chat_port, control_port=control_port)
        except Exception:
            return None

    def _membership_payload(self, mtype: str) -> dict:
        return {
            "type": mtype,
            "leader_id": self.leader_id,
            "servers": [asdict(s) for s in self.membership.values()],
        }

    def _merge_membership(self, servers: List[dict], leader_id: Optional[str]) -> bool:
        """Merge a membership snapshot into our local view. Returns True if changed."""
        changed = False

        for s in servers:
            info = self._serverinfo_from_msg(s)
            if not info:
                continue
            if info.id == self.server_id:
                # Ignore our own entry coming back with potentially stale host/port
                continue
            if info.id not in self.membership or self.membership[info.id] != info:
                self.membership[info.id] = info
                changed = True

        if isinstance(leader_id, str) and leader_id:
            # We accept the advertised leader_id but still recompute deterministically
            # (until we implement real election).
            if leader_id != self.leader_id:
                self.leader_id = leader_id
                changed = True

        if changed:
            self._recompute_leader()
        return changed

    async def start(self) -> None:
        # Start background tasks
        asyncio.create_task(self.discovery_listener())
        asyncio.create_task(self.server_control_listener())
        asyncio.create_task(self.broadcast_server_hello())
        asyncio.create_task(self.leader_gossip_loop())

        # Start TCP chat server
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"[SERVER] id={self.server_id[:8]}")
        print(f"[SERVER] TCP chat listening on {self.host}:{self.port} (advertise {self.advertised_host}:{self.port})")
        print(f"[DISCOVERY] UDP listening on 0.0.0.0:{DISCOVERY_PORT}")
        print(f"[CONTROL] UDP listening on 0.0.0.0:{SERVER_CONTROL_PORT}")
        self._print_membership()

        async with server:
            await server.serve_forever()

    # ------------------------
    # Client discovery (UDP)
    # ------------------------
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

    # ------------------------
    # Server-to-server discovery & membership (UDP)
    # ------------------------
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
                info = self._serverinfo_from_msg(msg)
                if info and info.id != self.server_id:
                    changed = False
                    if info.id not in self.membership or self.membership[info.id] != info:
                        self.membership[info.id] = info
                        changed = True
                        self._recompute_leader()

                    # IMPORTANT: reply from *any* server (not only leader) with its current view.
                    # This makes the joining server learn about servers that existed before it joined.
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

            # Ignore unknown control messages for now (we'll add heartbeat/election later)

    async def broadcast_server_hello(self) -> None:
        """Broadcast our presence so other servers can add us to membership and reply with their views."""
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

        # Send a few times to reduce loss (still best-effort)
        for _ in range(3):
            try:
                await loop.sock_sendto(sock, payload, bcast_addr)
            except Exception:
                pass
            await asyncio.sleep(0.3)

        sock.close()

    async def leader_gossip_loop(self) -> None:
        """Leader periodically gossips the current membership to all known servers (helps convergence)."""
        await asyncio.sleep(1.0)  # let startup discovery happen first
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(False)
        loop = asyncio.get_running_loop()

        while True:
            await asyncio.sleep(LEADER_GOSSIP_INTERVAL)
            if self.leader_id != self.server_id:
                continue

            msg = self._membership_payload("MEMBERSHIP")
            payload = json.dumps(msg).encode("utf-8")

            # Unicast to all known servers (except self)
            for sid, sinfo in list(self.membership.items()):
                if sid == self.server_id:
                    continue
                try:
                    await loop.sock_sendto(sock, payload, (sinfo.host, sinfo.control_port))
                except Exception:
                    # Best-effort: ignore send errors
                    pass

    # ------------------------
    # TCP chat handling
    # ------------------------
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
    p.add_argument("--host", default="0.0.0.0")  # listen on all interfaces
    p.add_argument("--port", type=int, default=5001)
    args = p.parse_args()

    server = ChatServer(args.host, args.port)
    await server.start()

if __name__ == "__main__":
    asyncio.run(main())

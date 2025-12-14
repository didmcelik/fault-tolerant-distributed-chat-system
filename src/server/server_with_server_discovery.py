import asyncio
import json
import socket
import uuid
from dataclasses import dataclass, asdict
from typing import Dict, Tuple, Optional, List

# Client discovery (client -> broadcast DISCOVER, server -> unicast DISCOVER_REPLY)
DISCOVERY_PORT = 37020

# Server-to-server control plane (membership, later: heartbeats/election)
SERVER_CONTROL_PORT = 37021

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


@dataclass(frozen=True)
class ServerInfo:
    id: str
    host: str
    chat_port: int
    control_port: int


class ChatServer:
    def __init__(self, host: str, port: int):
        # TCP chat bind host/port
        self.host = host
        self.port = port

        # Stable identity for this server process
        self.server_id = str(uuid.uuid4())

        # The host we advertise to other machines (never 0.0.0.0)
        self.advertised_host = get_local_ip()

        # TCP clients connected to THIS server
        self.clients: set[asyncio.StreamWriter] = set()

        # Membership / leader state (server layer)
        self.servers: Dict[str, ServerInfo] = {}
        self.leader_id: str = self.server_id

        # Register self in membership
        self._upsert_server(
            ServerInfo(
                id=self.server_id,
                host=self.advertised_host,
                chat_port=self.port,
                control_port=SERVER_CONTROL_PORT,
            )
        )

    # ------------------------
    # Public start
    # ------------------------
    async def start(self):
        # Start UDP listeners (client discovery + server control)
        asyncio.create_task(self.discovery_listener())
        asyncio.create_task(self.server_control_listener())

        # Announce ourselves to the server group (broadcast)
        asyncio.create_task(self.broadcast_server_hello())

        server = await asyncio.start_server(self.handle_client, self.host, self.port)

        print(f"[SERVER] id={self.server_id}")
        print(f"[SERVER] TCP chat listening on {self.host}:{self.port} (advertise {self.advertised_host}:{self.port})")
        print(f"[DISCOVERY] UDP listening on 0.0.0.0:{DISCOVERY_PORT}")
        print(f"[CONTROL] UDP listening on 0.0.0.0:{SERVER_CONTROL_PORT}")
        self._print_membership()

        async with server:
            await server.serve_forever()

    # ------------------------
    # Client discovery (UDP)
    # ------------------------
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
                    "server_host": self.advertised_host,
                    "server_port": self.port,
                }
                await loop.sock_sendto(sock, json.dumps(reply).encode("utf-8"), addr)

    # ------------------------
    # Server-to-server control (UDP)
    # ------------------------
    async def server_control_listener(self):
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

            # Another server announcing itself
            if mtype == "SERVER_HELLO":
                info = self._parse_server_info(msg)
                if info and info.id != self.server_id:
                    changed = self._upsert_server(info)
                    if changed:
                        self._recompute_leader()
                        self._print_membership()

                    # If I am leader, reply with membership snapshot to the sender
                    if self.leader_id == self.server_id:
                        welcome = self._membership_payload("SERVER_WELCOME")
                        await loop.sock_sendto(sock, json.dumps(welcome).encode("utf-8"), addr)

            # Leader replying to my hello, or leader pushing membership updates
            elif mtype in ("SERVER_WELCOME", "MEMBERSHIP"):
                leader_id = msg.get("leader_id")
                servers = msg.get("servers", [])
                changed = False

                for s in servers:
                    info = self._parse_server_info(s)
                    if info:
                        # ignore accidental self with wrong host
                        if info.id == self.server_id:
                            continue
                        changed |= self._upsert_server(info)

                if isinstance(leader_id, str) and leader_id:
                    if leader_id != self.leader_id:
                        self.leader_id = leader_id
                        changed = True

                if changed:
                    self._recompute_leader()  # keep deterministic if needed
                    self._print_membership()

    async def broadcast_server_hello(self):
        """Broadcast our presence so other servers can add us to membership."""
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

        # Send a few times to reduce loss (still best-effort)
        payload = json.dumps(hello).encode("utf-8")
        bcast_addr = ("255.255.255.255", SERVER_CONTROL_PORT)

        for _ in range(3):
            try:
                await loop.sock_sendto(sock, payload, bcast_addr)
            except Exception:
                pass
            await asyncio.sleep(0.3)

        sock.close()

    # ------------------------
    # TCP chat handling
    # ------------------------
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
                    await self.broadcast_chat(msg)
        except Exception:
            pass
        finally:
            self.clients.discard(writer)
            writer.close()
            await writer.wait_closed()
            print(f"[CLIENT] Disconnected: {peer}")

    async def broadcast_chat(self, msg: dict):
        dead: List[asyncio.StreamWriter] = []
        payload = (json.dumps(msg) + "\n").encode("utf-8")

        for w in self.clients:
            try:
                w.write(payload)
                await w.drain()
            except Exception:
                dead.append(w)

        for w in dead:
            self.clients.discard(w)

    # ------------------------
    # Helpers: membership / leader
    # ------------------------
    def _parse_server_info(self, obj: dict) -> Optional[ServerInfo]:
        """Accept both top-level HELLO fields and nested membership dicts."""
        try:
            sid = obj.get("id")
            host = obj.get("host")
            chat_port = int(obj.get("chat_port"))
            control_port = int(obj.get("control_port"))
            if not (isinstance(sid, str) and isinstance(host, str) and host):
                return None
            return ServerInfo(id=sid, host=host, chat_port=chat_port, control_port=control_port)
        except Exception:
            return None

    def _upsert_server(self, info: ServerInfo) -> bool:
        """Insert/update server info. Returns True if membership changed."""
        prev = self.servers.get(info.id)
        if prev == info:
            return False
        self.servers[info.id] = info
        return True

    def _recompute_leader(self):
        """Deterministic leader until we implement Chang–Roberts: smallest server_id wins."""
        # This avoids split-brain when two servers start at the same time.
        all_ids = sorted(self.servers.keys())
        if all_ids:
            self.leader_id = all_ids[0]

    def _membership_payload(self, mtype: str) -> dict:
        return {
            "type": mtype,
            "leader_id": self.leader_id,
            "servers": [asdict(s) for s in self.servers.values()],
        }

    def _print_membership(self):
        leader = self.servers.get(self.leader_id)
        leader_str = f"{self.leader_id}"
        if leader:
            leader_str += f" ({leader.host}:{leader.chat_port})"

        print(f"[MEMBERSHIP] leader={leader_str}; servers={len(self.servers)}")
        for s in sorted(self.servers.values(), key=lambda x: x.id):
            flag = "(me)" if s.id == self.server_id else ""
            print(f"  - {s.id} {flag} {s.host}:{s.chat_port} ctrl:{s.control_port}")


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

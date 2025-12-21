import asyncio
import json
import socket
import time
import uuid
from dataclasses import dataclass, asdict
from typing import Dict, Optional, Set, List, Tuple

DISCOVERY_PORT = 37020
SERVER_CONTROL_PORT = 37021

ROOM_PORT_BASE = 5100
ROOM_PORT_MAX = 5200

LEADER_GOSSIP_INTERVAL = 2.0
LEADER_HEARTBEAT_INTERVAL = 1.0
FOLLOWER_ALIVE_INTERVAL = 1.0
FAILURE_TIMEOUT = 5.0


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
    """
    Example-style room isolation:
      - Leader assigns a room to a server AND allocates a dedicated TCP port for that room on that server.
      - Clients connect to the assigned server's room TCP port for chat traffic.
      - Each room TCP server keeps its own client set -> no cross-room leakage.

    Causal ordering is implemented on the CLIENT (holdback + vector clocks),
    so this server only forwards/broadcasts chat messages as they arrive.
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port  # coordinator TCP port

        self.server_id = str(uuid.uuid4())
        self.advertised_host = get_local_ip()

        self.membership: Dict[str, ServerInfo] = {
            self.server_id: ServerInfo(
                id=self.server_id,
                host=self.advertised_host,
                chat_port=self.port,
                control_port=SERVER_CONTROL_PORT,
            )
        }
        self.last_seen: Dict[str, float] = {self.server_id: now()}

        self.leader_id: str = self.server_id
        self.term: int = 0

        self.in_election: bool = False
        self._election_term: int = 0
        self._last_elected: Optional[Tuple[int, str]] = None

        self.room_owner: Dict[str, str] = {}
        self.room_port: Dict[str, int] = {}
        self.server_load: Dict[str, int] = {self.server_id: 0}

        self._room_servers: Dict[str, asyncio.base_events.Server] = {}
        self._room_clients: Dict[str, Set[asyncio.StreamWriter]] = {}

        self._ctrl_send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._ctrl_send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._ctrl_send_sock.setblocking(False)

    # -----------------------------
    # Helpers
    # -----------------------------
    def _touch(self, sid: str) -> None:
        self.last_seen[sid] = now()

    def _serverinfo_from_dict(self, d: dict) -> Optional[ServerInfo]:
        try:
            sid = str(d["id"])
            host = str(d["host"])
            chat_port = int(d["chat_port"])
            control_port = int(d.get("control_port", SERVER_CONTROL_PORT))
            return ServerInfo(id=sid, host=host, chat_port=chat_port, control_port=control_port)
        except Exception:
            return None

    def _ring_ids(self) -> List[str]:
        return sorted(self.membership.keys())

    def _ring_successor_id(self) -> Optional[str]:
        ring = self._ring_ids()
        if len(ring) <= 1:
            return None
        try:
            i = ring.index(self.server_id)
        except ValueError:
            return None
        return ring[(i + 1) % len(ring)]

    def _elect_self_if_alone(self) -> None:
        if len(self.membership) == 1:
            self.leader_id = self.server_id
            self.in_election = False
            self._election_term = 0
            self._last_elected = (self.term, self.leader_id)

    def _ensure_load_entries(self) -> None:
        for sid in self.membership.keys():
            self.server_load.setdefault(sid, 0)

    def _leader_endpoint(self) -> Optional[Tuple[str, int]]:
        info = self.membership.get(self.leader_id)
        if not info:
            return None
        return info.host, info.chat_port

    def _get_leader_control_endpoint(self) -> Optional[Tuple[str, int]]:
        info = self.membership.get(self.leader_id)
        if not info:
            return None
        return info.host, info.control_port

    def _print_membership(self) -> None:
        leader = self.leader_id
        servers = sorted(self.membership.values(), key=lambda s: s.id)
        print(f"[MEMBERSHIP] me={self.server_id[:8]} leader={leader[:8]} term={self.term} servers={len(servers)}")
        for s in servers:
            role = " (LEADER)" if s.id == leader else ""
            age = now() - self.last_seen.get(s.id, 0.0)
            print(f"  - {s.id[:8]} {s.host}:{s.chat_port} ctrl:{s.control_port}{role} last_seen={age:0.1f}s ago")

        if self.room_owner:
            print("[ROOMS]")
            for r in sorted(self.room_owner.keys()):
                owner = self.room_owner[r]
                port = self.room_port.get(r)
                print(f"  - room='{r}' owner={owner[:8]} port={port}")

    def _choose_server_for_room(self, room: str) -> str:
        if room in self.room_owner and self.room_owner[room] in self.membership:
            return self.room_owner[room]
        self._ensure_load_entries()
        alive_ids = list(self.membership.keys())
        chosen = min(alive_ids, key=lambda sid: (self.server_load.get(sid, 0), sid))
        self.room_owner[room] = chosen
        return chosen

    def _ensure_room_port_assigned(self, room: str, owner_sid: str) -> int:
        if room in self.room_port:
            return self.room_port[room]

        port = ROOM_PORT_BASE + (abs(hash(room)) % (ROOM_PORT_MAX - ROOM_PORT_BASE + 1))
        shift = 0
        while True:
            candidate = port + shift
            if candidate > ROOM_PORT_MAX:
                candidate = ROOM_PORT_BASE + (candidate - ROOM_PORT_MAX - 1)
            if candidate != self.port:
                self.room_port[room] = candidate
                return candidate
            shift += 1

    async def _ensure_local_room_server(self, room: str) -> None:
        if self.room_owner.get(room) != self.server_id:
            return
        if room in self._room_servers:
            return

        port = self.room_port.get(room)
        if not isinstance(port, int):
            return

        srv = await asyncio.start_server(
            lambda r, w: self._handle_room_client(room, r, w),
            self.host,
            port,
        )
        self._room_servers[room] = srv
        self._room_clients[room] = set()
        print(f"[ROOM] Started room='{room}' TCP on {self.host}:{port}")

    async def _handle_room_client(self, room: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._room_clients.setdefault(room, set()).add(writer)
        peer = writer.get_extra_info("peername")
        print(f"[ROOM] Client connected room='{room}' peer={peer}")

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    msg = json.loads(line.decode("utf-8"))
                except Exception:
                    continue

                if msg.get("type") == "CHAT":
                    # Enforce correct room tag for this room-port.
                    msg["room"] = room
                    await self._broadcast_room(room, msg)
        finally:
            self._room_clients.get(room, set()).discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            print(f"[ROOM] Client disconnected room='{room}' peer={peer}")

    async def _broadcast_room(self, room: str, msg: dict) -> None:
        payload = (json.dumps(msg) + "\n").encode("utf-8")
        dead: List[asyncio.StreamWriter] = []
        for w in list(self._room_clients.get(room, set())):
            try:
                w.write(payload)
                await w.drain()
            except Exception:
                dead.append(w)
        for w in dead:
            self._room_clients.get(room, set()).discard(w)

    # -----------------------------
    # Membership payload
    # -----------------------------
    def _membership_payload(self, mtype: str) -> dict:
        return {
            "type": mtype,
            "leader_id": self.leader_id,
            "term": self.term,
            "servers": [asdict(s) for s in self.membership.values()],
            "ts": now(),
            "room_owner": self.room_owner,
            "room_port": self.room_port,
            "server_load": self.server_load,
        }

    def _merge_membership(self, msg: dict) -> bool:
        servers = msg.get("servers", [])
        leader_id = msg.get("leader_id")
        term = msg.get("term")
        room_owner = msg.get("room_owner")
        room_port = msg.get("room_port")
        server_load = msg.get("server_load")

        changed = False

        if isinstance(servers, list):
            for s in servers:
                info = self._serverinfo_from_dict(s)
                if not info or info.id == self.server_id:
                    continue
                if info.id not in self.membership or self.membership[info.id] != info:
                    self.membership[info.id] = info
                    changed = True
                if info.id not in self.last_seen:
                    self.last_seen[info.id] = now()
                    changed = True

        incoming_term: Optional[int] = None
        try:
            if term is not None:
                incoming_term = int(term)
        except Exception:
            incoming_term = None

        if incoming_term is not None and incoming_term > self.term:
            self.term = incoming_term
            self.in_election = False
            self._election_term = 0
            changed = True

        if (
            isinstance(leader_id, str)
            and leader_id
            and leader_id in self.membership
            and leader_id != self.leader_id
            and (incoming_term is None or incoming_term == self.term)
        ):
            self.leader_id = leader_id
            changed = True

        if incoming_term is not None and incoming_term >= self.term:
            if isinstance(room_owner, dict):
                filtered = {str(r): str(sid) for r, sid in room_owner.items() if str(sid) in self.membership}
                if filtered != self.room_owner:
                    self.room_owner = filtered
                    changed = True
            if isinstance(room_port, dict):
                new_ports: Dict[str, int] = {}
                for r, p in room_port.items():
                    try:
                        new_ports[str(r)] = int(p)
                    except Exception:
                        pass
                if new_ports and new_ports != self.room_port:
                    self.room_port = new_ports
                    changed = True
            if isinstance(server_load, dict):
                new_load: Dict[str, int] = {}
                for sid, v in server_load.items():
                    sid_s = str(sid)
                    if sid_s in self.membership:
                        try:
                            new_load[sid_s] = int(v)
                        except Exception:
                            pass
                if new_load:
                    new_load.setdefault(self.server_id, self.server_load.get(self.server_id, 0))
                    if new_load != self.server_load:
                        self.server_load = new_load
                        changed = True

        self._elect_self_if_alone()
        self._ensure_load_entries()
        return changed

    # -----------------------------
    # Startup
    # -----------------------------
    async def start(self) -> None:
        asyncio.create_task(self.discovery_listener())
        asyncio.create_task(self.server_control_listener())
        asyncio.create_task(self.broadcast_server_hello())
        asyncio.create_task(self.leader_gossip_loop())
        asyncio.create_task(self.leader_heartbeat_loop())
        asyncio.create_task(self.follower_alive_loop())
        asyncio.create_task(self.failure_detector_loop())

        coordinator_srv = await asyncio.start_server(self._handle_coordinator_client, self.host, self.port)

        print(f"[SERVER] id={self.server_id[:8]}")
        print(f"[SERVER] Coordinator TCP listening on {self.host}:{self.port} (advertise {self.advertised_host}:{self.port})")
        print(f"[DISCOVERY] UDP listening on 0.0.0.0:{DISCOVERY_PORT}")
        print(f"[CONTROL] UDP listening on 0.0.0.0:{SERVER_CONTROL_PORT}")
        self._print_membership()

        async with coordinator_srv:
            await coordinator_srv.serve_forever()

    # -----------------------------
    # UDP discovery for clients
    # -----------------------------
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
                reply = {
                    "type": "DISCOVER_REPLY",
                    "server_host": self.advertised_host,
                    "server_port": self.port,
                }
                await loop.sock_sendto(sock, json.dumps(reply).encode("utf-8"), addr)

    async def _send_control_to_server(self, sid: str, msg: dict) -> None:
        sinfo = self.membership.get(sid)
        if not sinfo or sid == self.server_id:
            return
        loop = asyncio.get_running_loop()
        try:
            await loop.sock_sendto(
                self._ctrl_send_sock,
                json.dumps(msg).encode("utf-8"),
                (sinfo.host, sinfo.control_port),
            )
        except Exception:
            pass

    # -----------------------------
    # LCR election (unchanged)
    # -----------------------------
    async def _lcr_start_election(self, reason: str) -> None:
        if len(self.membership) <= 1:
            return
        if not self.in_election:
            self._election_term = self.term + 1
            self.in_election = True

        succ = self._ring_successor_id()
        print(
            f"[ELECTION] Start LCR reason={reason} term={self._election_term} "
            f"me={self.server_id[:8]} ring_size={len(self.membership)} successor={succ[:8] if succ else None}"
        )
        if not succ:
            return

        msg = {
            "type": "ELECTION",
            "from": self.server_id,
            "term": self._election_term,
            "candidate_id": self.server_id,
            "ts": now(),
            "reason": reason,
        }
        await self._send_control_to_server(succ, msg)

    # -----------------------------
    # UDP server control listener
    # -----------------------------
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
            sender_id = msg.get("from") or msg.get("id")
            if isinstance(sender_id, str) and sender_id:
                self._touch(sender_id)

            if mtype == "SERVER_HELLO":
                info = self._serverinfo_from_dict(msg)
                if info and info.id != self.server_id:
                    changed = False
                    if info.id not in self.membership or self.membership[info.id] != info:
                        self.membership[info.id] = info
                        self.server_load.setdefault(info.id, 0)
                        changed = True

                    welcome = self._membership_payload("SERVER_WELCOME")
                    await loop.sock_sendto(sock, json.dumps(welcome).encode("utf-8"), addr)

                    if changed:
                        self._print_membership()

            elif mtype in ("SERVER_WELCOME", "MEMBERSHIP"):
                changed = self._merge_membership(msg)
                if changed:
                    for room, owner in self.room_owner.items():
                        if owner == self.server_id:
                            await self._ensure_local_room_server(room)
                    self._print_membership()

            elif mtype in ("LEADER_HEARTBEAT", "ALIVE"):
                pass

            elif mtype == "ELECTION":
                try:
                    term = int(msg.get("term", 0))
                except Exception:
                    continue
                cand = msg.get("candidate_id")
                if not isinstance(cand, str) or not cand:
                    continue
                if term <= self.term:
                    continue

                print(f"[ELECTION] RX ELECTION term={term} candidate={cand[:8]} at={self.server_id[:8]}")
                self.in_election = True
                self._election_term = max(self._election_term, term)

                if cand == self.server_id:
                    self.term = term
                    self.leader_id = self.server_id
                    self.in_election = False
                    self._last_elected = (self.term, self.leader_id)

                    print(f"[ELECTION] WIN leader={self.server_id[:8]} term={term} -> sending ELECTED")
                    self._print_membership()

                    succ = self._ring_successor_id()
                    if succ:
                        await self._send_control_to_server(
                            succ,
                            {"type": "ELECTED", "from": self.server_id, "term": self.term, "leader_id": self.leader_id, "ts": now()},
                        )
                    continue

                next_cand = cand if cand > self.server_id else self.server_id
                succ = self._ring_successor_id()
                if succ:
                    print(f"[ELECTION] FWD ELECTION term={term} candidate={next_cand[:8]} -> succ={succ[:8]}")
                    await self._send_control_to_server(
                        succ,
                        {"type": "ELECTION", "from": self.server_id, "term": term, "candidate_id": next_cand, "ts": now()},
                    )

            elif mtype == "ELECTED":
                try:
                    term = int(msg.get("term", 0))
                except Exception:
                    continue
                leader_id = msg.get("leader_id")
                if not isinstance(leader_id, str) or not leader_id:
                    continue
                if term < self.term:
                    continue

                already_seen = self._last_elected == (term, leader_id)
                print(f"[ELECTION] RX ELECTED term={term} leader={leader_id[:8]} at={self.server_id[:8]} already_seen={already_seen}")

                self.term = term
                if leader_id in self.membership:
                    self.leader_id = leader_id
                self.in_election = False
                self._election_term = 0
                self._last_elected = (term, leader_id)

                if not already_seen:
                    self._print_membership()

                if self.server_id == leader_id and already_seen:
                    continue

                succ = self._ring_successor_id()
                if succ:
                    print(f"[ELECTION] FWD ELECTED term={term} leader={leader_id[:8]} -> succ={succ[:8]}")
                    await self._send_control_to_server(
                        succ,
                        {"type": "ELECTED", "from": self.server_id, "term": term, "leader_id": leader_id, "ts": now()},
                    )

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
            "from": self.server_id,
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
                    await loop.sock_sendto(self._ctrl_send_sock, payload, (sinfo.host, sinfo.control_port))
                except Exception:
                    pass

    async def leader_heartbeat_loop(self) -> None:
        await asyncio.sleep(1.0)
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(LEADER_HEARTBEAT_INTERVAL)
            if self.leader_id != self.server_id:
                continue
            hb = {"type": "LEADER_HEARTBEAT", "from": self.server_id, "leader_id": self.leader_id, "term": self.term, "ts": now()}
            payload = json.dumps(hb).encode("utf-8")
            for sid, sinfo in list(self.membership.items()):
                if sid == self.server_id:
                    continue
                try:
                    await loop.sock_sendto(self._ctrl_send_sock, payload, (sinfo.host, sinfo.control_port))
                except Exception:
                    pass

    async def follower_alive_loop(self) -> None:
        await asyncio.sleep(1.0)
        loop = asyncio.get_running_loop()
        while True:
            await asyncio.sleep(FOLLOWER_ALIVE_INTERVAL)
            if self.leader_id == self.server_id:
                continue
            leader_ep = self._get_leader_control_endpoint()
            if not leader_ep:
                continue
            msg = {"type": "ALIVE", "from": self.server_id, "leader_id": self.leader_id, "term": self.term, "ts": now()}
            payload = json.dumps(msg).encode("utf-8")
            try:
                await loop.sock_sendto(self._ctrl_send_sock, payload, leader_ep)
            except Exception:
                pass

    def _reassign_rooms_from_dead_server(self, dead_sid: str) -> None:
        rooms_to_move = [r for r, owner in self.room_owner.items() if owner == dead_sid]
        if not rooms_to_move:
            return
        alive_ids = [sid for sid in self.membership.keys() if sid != dead_sid]
        if not alive_ids:
            return

        print(f"[COORD] Reassigning {len(rooms_to_move)} room(s) from dead server {dead_sid[:8]}.")
        for room in rooms_to_move:
            new_owner = min(alive_ids, key=lambda sid: (self.server_load.get(sid, 0), sid))
            self.room_owner[room] = new_owner

    async def failure_detector_loop(self) -> None:
        await asyncio.sleep(2.0)
        while True:
            await asyncio.sleep(1.0)

            if self.leader_id != self.server_id and self.leader_id in self.membership:
                last = self.last_seen.get(self.leader_id, 0.0)
                if now() - last > FAILURE_TIMEOUT:
                    print(f"[FAILURE] Leader suspected failed: {self.leader_id[:8]} (timeout).")
                    dead_leader = self.leader_id
                    self.membership.pop(dead_leader, None)
                    self.last_seen.pop(dead_leader, None)
                    self.server_load.pop(dead_leader, None)

                    if len(self.membership) == 1:
                        self._elect_self_if_alone()
                        print("[ELECTION] Single node remaining; self-elected as leader.")
                        self._print_membership()
                        continue

                    self._print_membership()
                    await self._lcr_start_election(reason="leader_timeout")

            to_remove: List[str] = []
            for sid in list(self.membership.keys()):
                if sid == self.server_id:
                    continue
                last = self.last_seen.get(sid, 0.0)
                if now() - last > FAILURE_TIMEOUT:
                    to_remove.append(sid)

            if to_remove:
                removed_leader = False
                for sid in to_remove:
                    if sid in self.membership:
                        print(f"[FAILURE] Removing server {sid[:8]} from membership (timeout).")
                    self.membership.pop(sid, None)
                    self.last_seen.pop(sid, None)
                    self.server_load.pop(sid, None)
                    if sid == self.leader_id:
                        removed_leader = True

                if self.leader_id == self.server_id:
                    for dead_sid in to_remove:
                        self._reassign_rooms_from_dead_server(dead_sid)

                if len(self.membership) == 1:
                    self._elect_self_if_alone()
                    self._print_membership()
                    continue

                self._print_membership()

                if removed_leader and len(self.membership) > 1:
                    await self._lcr_start_election(reason="member_timeout")

    async def _send_tcp(self, writer: asyncio.StreamWriter, msg: dict) -> None:
        writer.write((json.dumps(msg) + "\n").encode("utf-8"))
        await writer.drain()

    async def _handle_coordinator_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        print(f"[COORD] Client connected: {peer}")

        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    msg = json.loads(line.decode("utf-8"))
                except Exception:
                    continue

                if msg.get("type") != "JOIN_REQUEST":
                    await self._send_tcp(writer, {"type": "ERROR", "message": "Only JOIN_REQUEST is supported on coordinator port."})
                    continue

                room = msg.get("room")
                if not isinstance(room, str) or not room.strip():
                    await self._send_tcp(writer, {"type": "ERROR", "message": "Invalid room."})
                    continue
                room = room.strip()

                if self.leader_id != self.server_id:
                    leader_ep = self._leader_endpoint()
                    if leader_ep:
                        await self._send_tcp(
                            writer,
                            {"type": "REDIRECT", "leader_host": leader_ep[0], "leader_port": leader_ep[1], "leader_id": self.leader_id, "term": self.term},
                        )
                    else:
                        await self._send_tcp(writer, {"type": "ERROR", "message": "Leader unknown."})
                    continue

                owner_sid = self._choose_server_for_room(room)
                port = self._ensure_room_port_assigned(room, owner_sid)
                owner_info = self.membership.get(owner_sid)
                if not owner_info:
                    await self._send_tcp(writer, {"type": "ERROR", "message": "No server available."})
                    continue

                self.server_load[owner_sid] = self.server_load.get(owner_sid, 0) + 1

                await self._send_tcp(
                    writer,
                    {
                        "type": "JOIN_ASSIGN",
                        "room": room,
                        "assigned_server_id": owner_sid,
                        "assigned_host": owner_info.host,
                        "assigned_room_port": port,
                        "leader_id": self.leader_id,
                        "term": self.term,
                    },
                )

                print(f"[COORD] Assigned room='{room}' owner={owner_sid[:8]} room_port={port}")

                if owner_sid == self.server_id:
                    await self._ensure_local_room_server(room)

        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            print(f"[COORD] Client disconnected: {peer}")


async def main() -> None:
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=5001, help="Coordinator TCP port")
    args = p.parse_args()

    server = ChatServer(args.host, args.port)
    await server.start()


if __name__ == "__main__":
    asyncio.run(main())

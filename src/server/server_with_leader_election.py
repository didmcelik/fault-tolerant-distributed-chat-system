import asyncio
import json
import socket
import time
import uuid
from dataclasses import dataclass, asdict
from typing import Dict, Optional, Set, List, Tuple


# -----------------------------
# Ports
# -----------------------------
# Client discovery: client broadcasts DISCOVER, server replies DISCOVER_REPLY
DISCOVERY_PORT = 37020

# Server control plane: membership + heartbeats + election
SERVER_CONTROL_PORT = 37021

# -----------------------------
# Timers (seconds)
# -----------------------------
LEADER_GOSSIP_INTERVAL = 2.0         # leader gossips membership snapshot
LEADER_HEARTBEAT_INTERVAL = 1.0      # leader -> followers
FOLLOWER_ALIVE_INTERVAL = 1.0        # follower -> leader
FAILURE_TIMEOUT = 5.0                # no message from peer within this window -> suspect failed


def now() -> float:
    return time.time()


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
    """
    Multi-server chat backend with:
      - Client discovery over UDP (DISCOVER / DISCOVER_REPLY)
      - Server-to-server discovery/membership over UDP
            SERVER_HELLO / SERVER_WELCOME / MEMBERSHIP
      - Heartbeat-based failure detection using periodic beacons (no ACKs):
            * leader sends LEADER_HEARTBEAT to followers
            * followers send ALIVE to leader
      - Timeout-based suspected failure detection (asynchronous system model)
      - Leader election using ring-based LCR (Chang–Roberts):
            * election messages circulate on a logical ring
            * lexicographically largest server_id wins

    Note:
      - Discovery uses UDP broadcast (SERVER_HELLO).
      - Control traffic uses UDP unicast (membership gossip, heartbeats, election).
    """

    def __init__(self, host: str, port: int):
        self.host = host          # bind address for TCP chat
        self.port = port          # TCP chat port

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

        # Liveness timestamps (updated when ANY control message is received from that server)
        self.last_seen: Dict[str, float] = {self.server_id: now()}

        # Leader + term (updated via election or membership gossip)
        self.leader_id: str = self.server_id
        self.term: int = 0

        # Election state (LCR)
        self.in_election: bool = False
        self._election_term: int = 0
        self._last_elected: Optional[Tuple[int, str]] = None  # (term, leader_id)

        # TCP clients for chat
        self.clients: Set[asyncio.StreamWriter] = set()

        # A shared UDP socket for sending control messages (unicast)
        self._ctrl_send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._ctrl_send_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._ctrl_send_sock.setblocking(False)

    # -----------------------------
    # Membership + ring helpers
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
        """Deterministic ring order for LCR (lexicographic by server_id)."""
        return sorted(self.membership.keys())

    def _ring_successor_id(self) -> Optional[str]:
        """Return the next server_id in the ring after me (wrap-around)."""
        ring = self._ring_ids()
        if len(ring) <= 1:
            return None
        try:
            i = ring.index(self.server_id)
        except ValueError:
            return None
        return ring[(i + 1) % len(ring)]

    def _elect_self_if_alone(self) -> None:
        """If I'm the only known server, I must be the leader."""
        if len(self.membership) == 1:
            self.leader_id = self.server_id
            self.in_election = False
            self._election_term = 0

    def _membership_payload(self, mtype: str) -> dict:
        return {
            "type": mtype,
            "leader_id": self.leader_id,
            "term": self.term,
            "servers": [asdict(s) for s in self.membership.values()],
            "ts": now(),
        }

    def _merge_membership(self, servers: List[dict], leader_id: Optional[str], term: Optional[int]) -> bool:
        """
        Merge a membership snapshot into our local view. Returns True if changed.

        We also accept leader/term updates:
          - Higher term always wins.
          - For the current term, adopt leader_id only if it exists in membership.
        """
        changed = False

        for s in servers:
            info = self._serverinfo_from_dict(s)
            if not info or info.id == self.server_id:
                continue

            if info.id not in self.membership or self.membership[info.id] != info:
                self.membership[info.id] = info
                changed = True

            # Seeing a server in a membership snapshot suggests it's alive recently.
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

        self._elect_self_if_alone()
        return changed

    def _get_leader_endpoint(self) -> Optional[Tuple[str, int]]:
        """Return (host, control_port) for current leader."""
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

        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"[SERVER] id={self.server_id[:8]}")
        print(f"[SERVER] TCP chat listening on {self.host}:{self.port} (advertise {self.advertised_host}:{self.port})")
        print(f"[DISCOVERY] UDP listening on 0.0.0.0:{DISCOVERY_PORT}")
        print(f"[CONTROL] UDP listening on 0.0.0.0:{SERVER_CONTROL_PORT}")
        self._print_membership()

        async with server:
            await server.serve_forever()

    # -----------------------------
    # Client discovery (UDP)
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

    # -----------------------------
    # Server control plane (UDP)
    # -----------------------------
    async def _send_control_to_server(self, sid: str, msg: dict) -> None:
        """Best-effort unicast control message to a given server_id."""
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

    async def _lcr_start_election(self, reason: str) -> None:
        """Start a Chang–Roberts (LCR) ring election."""
        self._elect_self_if_alone()
        if len(self.membership) <= 1:
            # With a single node there is no ring; self-election is sufficient.
            return

        # Bump election term if we're not already running one.
        if not self.in_election:
            self._election_term = self.term + 1
            self.in_election = True

        succ = self._ring_successor_id()

        # ---- LCR DEBUG LOGS (START) ----
        print(
            f"[ELECTION] Start LCR reason={reason} term={self._election_term} "
            f"me={self.server_id[:8]} ring_size={len(self.membership)} successor={succ[:8] if succ else None}"
        )
        # ---- LCR DEBUG LOGS (END) ----

        if not succ:
            # Ring successor unknown (should not happen if membership includes self).
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

            # Any control-plane message proves the sender is alive (touch it)
            sender_id = msg.get("from") or msg.get("id")
            if isinstance(sender_id, str) and sender_id:
                self._touch(sender_id)

            if mtype == "SERVER_HELLO":
                info = self._serverinfo_from_dict(msg)
                if info and info.id != self.server_id:
                    changed = False
                    if info.id not in self.membership or self.membership[info.id] != info:
                        self.membership[info.id] = info
                        changed = True

                    self._elect_self_if_alone()

                    # Reply from any server with its current view so joiner learns older members
                    welcome = self._membership_payload("SERVER_WELCOME")
                    await loop.sock_sendto(sock, json.dumps(welcome).encode("utf-8"), addr)

                    if changed:
                        self._print_membership()

            elif mtype in ("SERVER_WELCOME", "MEMBERSHIP"):
                servers = msg.get("servers", [])
                leader_id = msg.get("leader_id")
                term = msg.get("term")
                if isinstance(servers, list):
                    changed = self._merge_membership(servers, leader_id, term)
                    if changed:
                        self._print_membership()

            elif mtype == "LEADER_HEARTBEAT":
                # Followers receive this from leader; touching above already updated last_seen.
                pass

            elif mtype == "ALIVE":
                # Leader receives periodic ALIVE from followers; touching above already updated last_seen.
                pass

            elif mtype == "ELECTION":
                # Chang–Roberts election (ring-based). We elect the lexicographically largest server_id.
                try:
                    term = int(msg.get("term", 0))
                except Exception:
                    continue

                cand = msg.get("candidate_id")
                if not isinstance(cand, str) or not cand:
                    continue

                # Ignore stale elections.
                if term <= self.term:
                    continue

                # ---- LCR DEBUG LOGS (RX ELECTION) ----
                print(f"[ELECTION] RX ELECTION term={term} candidate={cand[:8]} at={self.server_id[:8]}")
                # ---- LCR DEBUG LOGS (END) ----

                self.in_election = True
                self._election_term = max(self._election_term, term)

                # If my own id comes back, I win.
                if cand == self.server_id:
                    self.term = term
                    self.leader_id = self.server_id
                    self.in_election = False
                    self._last_elected = (self.term, self.leader_id)

                    # ---- LCR DEBUG LOGS (WIN) ----
                    print(f"[ELECTION] WIN leader={self.server_id[:8]} term={term} -> sending ELECTED")
                    # ---- LCR DEBUG LOGS (END) ----

                    self._print_membership()

                    # Announce elected leader around the ring.
                    succ = self._ring_successor_id()
                    if succ:
                        elected = {
                            "type": "ELECTED",
                            "from": self.server_id,
                            "term": self.term,
                            "leader_id": self.leader_id,
                            "ts": now(),
                        }
                        await self._send_control_to_server(succ, elected)
                    continue

                # Otherwise forward the larger of (candidate_id, my_id).
                next_cand = cand if cand > self.server_id else self.server_id
                succ = self._ring_successor_id()
                if succ:
                    # ---- LCR DEBUG LOGS (FWD ELECTION) ----
                    print(f"[ELECTION] FWD ELECTION term={term} candidate={next_cand[:8]} -> succ={succ[:8]}")
                    # ---- LCR DEBUG LOGS (END) ----

                    fwd = {
                        "type": "ELECTION",
                        "from": self.server_id,
                        "term": term,
                        "candidate_id": next_cand,
                        "ts": now(),
                    }
                    await self._send_control_to_server(succ, fwd)

            elif mtype == "ELECTED":
                try:
                    term = int(msg.get("term", 0))
                except Exception:
                    continue

                leader_id = msg.get("leader_id")
                if not isinstance(leader_id, str) or not leader_id:
                    continue

                # Drop stale elected announcements.
                if term < self.term:
                    continue

                already_seen = self._last_elected == (term, leader_id)

                # ---- LCR DEBUG LOGS (RX ELECTED) ----
                print(
                    f"[ELECTION] RX ELECTED term={term} leader={leader_id[:8]} at={self.server_id[:8]} "
                    f"already_seen={already_seen}"
                )
                # ---- LCR DEBUG LOGS (END) ----

                self.term = term
                if leader_id in self.membership:
                    self.leader_id = leader_id

                self.in_election = False
                self._election_term = 0
                self._last_elected = (term, leader_id)

                if not already_seen:
                    self._print_membership()

                # Forward until it returns to the elected leader.
                if self.server_id == leader_id and already_seen:
                    # Leader received its own ELECTED again -> stop.
                    return

                succ = self._ring_successor_id()
                if succ:
                    # ---- LCR DEBUG LOGS (FWD ELECTED) ----
                    print(f"[ELECTION] FWD ELECTED term={term} leader={leader_id[:8]} -> succ={succ[:8]}")
                    # ---- LCR DEBUG LOGS (END) ----

                    await self._send_control_to_server(
                        succ,
                        {
                            "type": "ELECTED",
                            "from": self.server_id,
                            "term": term,
                            "leader_id": leader_id,
                            "ts": now(),
                        },
                    )

            # Ignore unknown types

    async def broadcast_server_hello(self) -> None:
        """Broadcast our presence so other servers add us and reply with their views."""
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

        # Best-effort: send a few times to reduce loss
        for _ in range(3):
            try:
                await loop.sock_sendto(sock, payload, bcast_addr)
            except Exception:
                pass
            await asyncio.sleep(0.3)

        sock.close()

    # -----------------------------
    # Membership dissemination (leader gossip)
    # -----------------------------
    async def leader_gossip_loop(self) -> None:
        """Leader periodically unicasts the membership snapshot to all known servers."""
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

    # -----------------------------
    # Heartbeats (NO ACKs): periodic beacons
    # -----------------------------
    async def leader_heartbeat_loop(self) -> None:
        """If I am leader, periodically send LEADER_HEARTBEAT to all known servers."""
        await asyncio.sleep(1.0)
        loop = asyncio.get_running_loop()

        while True:
            await asyncio.sleep(LEADER_HEARTBEAT_INTERVAL)

            if self.leader_id != self.server_id:
                continue

            hb = {
                "type": "LEADER_HEARTBEAT",
                "from": self.server_id,
                "leader_id": self.leader_id,
                "term": self.term,
                "ts": now(),
            }
            payload = json.dumps(hb).encode("utf-8")

            for sid, sinfo in list(self.membership.items()):
                if sid == self.server_id:
                    continue
                try:
                    await loop.sock_sendto(self._ctrl_send_sock, payload, (sinfo.host, sinfo.control_port))
                except Exception:
                    pass

    async def follower_alive_loop(self) -> None:
        """If I am follower, periodically send ALIVE to leader (no ACK expected)."""
        await asyncio.sleep(1.0)
        loop = asyncio.get_running_loop()

        while True:
            await asyncio.sleep(FOLLOWER_ALIVE_INTERVAL)

            if self.leader_id == self.server_id:
                continue  # I'm leader; no need to send ALIVE

            leader_ep = self._get_leader_endpoint()
            if not leader_ep:
                continue

            msg = {
                "type": "ALIVE",
                "from": self.server_id,
                "leader_id": self.leader_id,
                "term": self.term,
                "ts": now(),
            }
            payload = json.dumps(msg).encode("utf-8")

            try:
                await loop.sock_sendto(self._ctrl_send_sock, payload, leader_ep)
            except Exception:
                pass

    # -----------------------------
    # Failure detection
    # -----------------------------
    async def failure_detector_loop(self) -> None:
        """
        Timeout-based suspected failure detection in an asynchronous system:
          - Followers suspect leader if no control msg from leader within FAILURE_TIMEOUT
          - Any server prunes peers if no control msg from that peer within FAILURE_TIMEOUT
          - If the leader is removed/suspected, start an LCR election
        """
        await asyncio.sleep(2.0)

        while True:
            await asyncio.sleep(1.0)

            # Followers: suspect leader and trigger election
            if self.leader_id != self.server_id and self.leader_id in self.membership:
                last = self.last_seen.get(self.leader_id, 0.0)
                if now() - last > FAILURE_TIMEOUT:
                    print(f"[FAILURE] Leader suspected failed: {self.leader_id[:8]} (timeout).")

                    # Remove suspected leader locally so the ring can progress.
                    dead_leader = self.leader_id
                    self.membership.pop(dead_leader, None)
                    self.last_seen.pop(dead_leader, None)

                    # If we are now alone, immediately self-elect (2-node edge case).
                    if len(self.membership) == 1:
                        self.leader_id = self.server_id
                        self.in_election = False
                        self._election_term = 0
                        self._last_elected = (self.term, self.leader_id)
                        print("[ELECTION] Single node remaining; self-elected as leader.")
                        self._print_membership()
                    else:
                        self._print_membership()
                        await self._lcr_start_election(reason="leader_timeout")

            # Prune any failed peers (including followers if I'm leader)
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
                    removed = self.membership.pop(sid, None)
                    self.last_seen.pop(sid, None)
                    if removed:
                        print(f"[FAILURE] Removing server {sid[:8]} from membership (timeout).")
                    if sid == self.leader_id:
                        removed_leader = True

                self._elect_self_if_alone()
                self._print_membership()

                if removed_leader and len(self.membership) > 0:
                    await self._lcr_start_election(reason="member_timeout")

    # -----------------------------
    # TCP chat (unchanged)
    # -----------------------------
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

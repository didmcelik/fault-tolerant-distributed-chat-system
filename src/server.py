import asyncio
import json
import socket
import uuid
from typing import Dict, Set, Tuple, Optional

from common import read_json_line, write_json_line, now_ms, NodeInfo

DISCOVERY_PORT = 37020          # UDP broadcast port
DISCOVERY_MAGIC = "DS1_CHAT_DISCOVERY_V1"

HEARTBEAT_INTERVAL_SEC = 1.0
HEARTBEAT_TIMEOUT_SEC = 3.5

RETRY_INTERVAL_SEC = 0.5
ACK_TIMEOUT_SEC = 1.0

class ServerNode:
    def __init__(self, host: str, server_port: int, peer_port: int):
        self.info = NodeInfo(
            node_id=str(uuid.uuid4()),
            host=host,
            server_port=server_port,
            peer_port=peer_port,
        )

        # Leader state
        self.is_leader: bool = False
        self.leader_id: Optional[str] = None
        self.leader_addr: Optional[Tuple[str, int]] = None  # (host, peer_port)

        # Membership (known servers)
        self.members: Dict[str, NodeInfo] = {self.info.node_id: self.info}

        # Clients connected to THIS server
        self.client_writers: Set[asyncio.StreamWriter] = set()

        # Sequencing / delivery
        self.global_seq: int = 0               # leader only
        self.next_deliver_seq: int = 1         # all nodes
        self.delivery_buffer: Dict[int, dict] = {}

        # Heartbeat
        self.last_heartbeat_ms: int = now_ms()

        # Server-to-server connections (leader -> follower)
        self.peer_writers: Dict[str, asyncio.StreamWriter] = {}  # node_id -> writer

        # Pending ACKs: seq -> set(node_id)
        self.pending_acks: Dict[int, Set[str]] = {}

        # Concurrency guards
        self._lock = asyncio.Lock()

    # ------------------------
    # UDP discovery
    # ------------------------
    async def udp_discovery_listener(self) -> None:
        """
        Listen for broadcast discovery messages from clients or servers.
        Reply with leader address (if known) or self if leader.
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", DISCOVERY_PORT))
        sock.setblocking(False)

        loop = asyncio.get_running_loop()
        print(f"[discovery] Listening UDP on 0.0.0.0:{DISCOVERY_PORT}")

        while True:
            data, addr = await loop.sock_recvfrom(sock, 64 * 1024)
            try:
                msg = json.loads(data.decode("utf-8"))
            except Exception:
                continue

            if msg.get("magic") != DISCOVERY_MAGIC:
                continue

            mtype = msg.get("type")
            if mtype == "DISCOVER_SERVER":
                # Reply with best-known leader; fallback to self.
                reply_target = None
                if self.is_leader:
                    reply_target = (self.info.host, self.info.peer_port, self.info.node_id)
                elif self.leader_addr and self.leader_id:
                    reply_target = (self.leader_addr[0], self.leader_addr[1], self.leader_id)
                else:
                    reply_target = (self.info.host, self.info.peer_port, self.info.node_id)

                reply = {
                    "magic": DISCOVERY_MAGIC,
                    "type": "DISCOVER_REPLY",
                    "leader_host": reply_target[0],
                    "leader_peer_port": reply_target[1],
                    "leader_id": reply_target[2],
                    "server_host": self.info.host,
                    "server_port": self.info.server_port,
                }
                await loop.sock_sendto(sock, json.dumps(reply).encode("utf-8"), addr)

            elif mtype == "SERVER_JOIN_ANNOUNCE":
                # A server wants to join; only leader should register it.
                if not self.is_leader:
                    continue
                node_id = msg["node_id"]
                nfo = NodeInfo(node_id=node_id, host=msg["host"], server_port=msg["server_port"], peer_port=msg["peer_port"])
                async with self._lock:
                    self.members[node_id] = nfo
                print(f"[leader] Registered new server {node_id[:8]} @ {nfo.host}:{nfo.peer_port}")

    async def udp_broadcast_server_join(self) -> None:
        """
        Non-leader servers broadcast a join announce until a leader is known.
        """
        if self.is_leader:
            return

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.setblocking(False)
        loop = asyncio.get_running_loop()

        announce = {
            "magic": DISCOVERY_MAGIC,
            "type": "SERVER_JOIN_ANNOUNCE",
            "node_id": self.info.node_id,
            "host": self.info.host,
            "server_port": self.info.server_port,
            "peer_port": self.info.peer_port,
        }

        while self.leader_id is None:
            await loop.sock_sendto(
                sock,
                json.dumps(announce).encode("utf-8"),
                ("255.255.255.255", DISCOVERY_PORT),
            )
            await asyncio.sleep(1.0)

    # ------------------------
    # TCP servers
    # ------------------------
    async def start(self) -> None:
        client_srv = await asyncio.start_server(self.handle_client_conn, self.info.host, self.info.server_port)
        peer_srv = await asyncio.start_server(self.handle_peer_conn, self.info.host, self.info.peer_port)

        print(f"[node] id={self.info.node_id}")
        print(f"[node] client TCP  {self.info.host}:{self.info.server_port}")
        print(f"[node] peer   TCP  {self.info.host}:{self.info.peer_port}")

        # Bootstrap: first server becomes leader (MVP).
        # (Sonraki adım: gerçek election + leader discovery.)
        self.become_leader()

        tasks = [
            asyncio.create_task(self.udp_discovery_listener()),
            asyncio.create_task(self.heartbeat_loop()),
        ]

        async with client_srv, peer_srv:
            await asyncio.gather(
                client_srv.serve_forever(),
                peer_srv.serve_forever(),
                *tasks
            )

    async def handle_client_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.client_writers.add(writer)
        addr = writer.get_extra_info("peername")
        print(f"[client] connected {addr}")

        try:
            # Simple protocol: client sends {"type":"CHAT_SEND","room":"main","from":"...","text":"..."}
            while True:
                msg = await read_json_line(reader)
                if msg.get("type") == "CHAT_SEND":
                    await self.on_client_chat(msg)
        except Exception:
            pass
        finally:
            self.client_writers.discard(writer)
            writer.close()
            await writer.wait_closed()
            print(f"[client] disconnected {addr}")

    async def handle_peer_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        addr = writer.get_extra_info("peername")
        try:
            hello = await read_json_line(reader)
            if hello.get("type") != "PEER_HELLO":
                raise ValueError("Expected PEER_HELLO")
            peer_id = hello["node_id"]
            async with self._lock:
                self.peer_writers[peer_id] = writer
            print(f"[peer] connected {peer_id[:8]} from {addr}")

            while True:
                msg = await read_json_line(reader)
                await self.on_peer_message(peer_id, msg)

        except Exception:
            pass
        finally:
            # remove
            for nid, w in list(self.peer_writers.items()):
                if w is writer:
                    del self.peer_writers[nid]
            writer.close()
            await writer.wait_closed()
            print(f"[peer] disconnected from {addr}")

    # ------------------------
    # Leader / follower actions
    # ------------------------
    def become_leader(self) -> None:
        self.is_leader = True
        self.leader_id = self.info.node_id
        self.leader_addr = (self.info.host, self.info.peer_port)
        print("[leader] I am the leader now.")

    async def connect_to_leader(self, host: str, peer_port: int, leader_id: str) -> None:
        reader, writer = await asyncio.open_connection(host, peer_port)
        await write_json_line(writer, {"type": "PEER_HELLO", "node_id": self.info.node_id})
        self.leader_id = leader_id
        self.leader_addr = (host, peer_port)
        print(f"[node] Connected to leader {leader_id[:8]} @ {host}:{peer_port}")

        # Keep reading leader publishes
        async def read_loop():
            while True:
                msg = await read_json_line(reader)
                await self.on_peer_message(leader_id, msg)

        asyncio.create_task(read_loop())

    async def on_client_chat(self, msg: dict) -> None:
        if self.is_leader:
            await self.leader_publish(msg)
        else:
            # Forward to leader
            if not self.leader_addr or not self.leader_id:
                print("[warn] No leader known, dropping client message")
                return
            # open a short-lived connection to leader (MVP)
            r, w = await asyncio.open_connection(self.leader_addr[0], self.leader_addr[1])
            await write_json_line(w, {"type": "PEER_HELLO", "node_id": self.info.node_id})
            await write_json_line(w, {"type": "CHAT_FORWARD", "payload": msg})
            w.close()
            await w.wait_closed()

    async def leader_publish(self, chat_send_msg: dict) -> None:
        async with self._lock:
            self.global_seq += 1
            seq = self.global_seq
            # who should ack? all other servers
            targets = set(self.members.keys()) - {self.info.node_id}
            self.pending_acks[seq] = set(targets)

        publish = {
            "type": "CHAT_PUBLISH",
            "seq": seq,
            "room": chat_send_msg.get("room", "main"),
            "from": chat_send_msg.get("from", "anon"),
            "text": chat_send_msg.get("text", ""),
        }

        # Deliver locally first (leader is also a server)
        await self.deliver_or_buffer(publish)

        # Send to followers reliably (MVP: best-effort retries)
        await self.send_publish_with_retries(seq, publish)

    async def send_publish_with_retries(self, seq: int, publish: dict) -> None:
        while True:
            async with self._lock:
                pending = self.pending_acks.get(seq, set()).copy()
            if not pending:
                return

            # Ensure we have connections; MVP: connect on demand
            for nid in list(pending):
                nfo = self.members.get(nid)
                if not nfo:
                    continue
                try:
                    reader, writer = await asyncio.open_connection(nfo.host, nfo.peer_port)
                    await write_json_line(writer, {"type": "PEER_HELLO", "node_id": self.info.node_id})
                    await write_json_line(writer, publish)
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    continue

            await asyncio.sleep(RETRY_INTERVAL_SEC)

    async def on_peer_message(self, peer_id: str, msg: dict) -> None:
        mtype = msg.get("type")
        if mtype == "HEARTBEAT":
            self.last_heartbeat_ms = now_ms()
            # leader info update
            self.leader_id = msg.get("leader_id", self.leader_id)
            self.leader_addr = (msg.get("host"), msg.get("peer_port"))
            return

        if mtype == "CHAT_PUBLISH":
            await self.deliver_or_buffer(msg)
            # ACK back to leader (short-lived)
            if self.leader_addr and self.leader_id:
                try:
                    r, w = await asyncio.open_connection(self.leader_addr[0], self.leader_addr[1])
                    await write_json_line(w, {"type": "PEER_HELLO", "node_id": self.info.node_id})
                    await write_json_line(w, {"type": "ACK", "seq": msg["seq"], "from": self.info.node_id})
                    w.close()
                    await w.wait_closed()
                except Exception:
                    pass
            return

        if mtype == "ACK" and self.is_leader:
            seq = int(msg["seq"])
            sender = msg.get("from")
            async with self._lock:
                if seq in self.pending_acks and sender in self.pending_acks[seq]:
                    self.pending_acks[seq].remove(sender)
                    if not self.pending_acks[seq]:
                        del self.pending_acks[seq]
            return

        if mtype == "CHAT_FORWARD" and self.is_leader:
            payload = msg["payload"]
            await self.leader_publish(payload)
            return

    async def deliver_or_buffer(self, publish: dict) -> None:
        seq = int(publish["seq"])
        async with self._lock:
            self.delivery_buffer[seq] = publish

            while self.next_deliver_seq in self.delivery_buffer:
                msg = self.delivery_buffer.pop(self.next_deliver_seq)
                self.next_deliver_seq += 1
                await self.deliver_to_local_clients(msg)

    async def deliver_to_local_clients(self, msg: dict) -> None:
        out = {"type": "CHAT_DELIVER", **msg}
        dead = []
        for w in self.client_writers:
            try:
                await write_json_line(w, out)
            except Exception:
                dead.append(w)
        for w in dead:
            self.client_writers.discard(w)

    async def heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)

            if self.is_leader:
                hb = {
                    "type": "HEARTBEAT",
                    "leader_id": self.info.node_id,
                    "host": self.info.host,
                    "peer_port": self.info.peer_port,
                    "ts": now_ms(),
                }
                # broadcast heartbeat to known members (MVP: short-lived TCP)
                async with self._lock:
                    targets = list(self.members.values())
                for nfo in targets:
                    if nfo.node_id == self.info.node_id:
                        continue
                    try:
                        r, w = await asyncio.open_connection(nfo.host, nfo.peer_port)
                        await write_json_line(w, {"type": "PEER_HELLO", "node_id": self.info.node_id})
                        await write_json_line(w, hb)
                        w.close()
                        await w.wait_closed()
                    except Exception:
                        continue
            else:
                # detect leader failure
                if (now_ms() - self.last_heartbeat_ms) > int(HEARTBEAT_TIMEOUT_SEC * 1000):
                    print("[election] Leader timeout detected. (MVP) Becoming leader.")
                    # MVP: just self-promote (next step: real election like LCR/HS)
                    self.become_leader()

async def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--server-port", type=int, required=True)
    p.add_argument("--peer-port", type=int, required=True)
    args = p.parse_args()

    node = ServerNode(args.host, args.server_port, args.peer_port)
    await node.start()

if __name__ == "__main__":
    asyncio.run(main())

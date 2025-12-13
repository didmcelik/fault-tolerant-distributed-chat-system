import asyncio
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

JSONDict = Dict[str, Any]


async def read_json(reader: asyncio.StreamReader) -> dict:
    data = await reader.readline()
    if not data:
        raise ConnectionError
    return json.loads(data.decode())

async def write_json(writer: asyncio.StreamWriter, msg: dict):
    writer.write((json.dumps(msg) + "\n").encode())
    await writer.drain()


def now_ms() -> int:
    import time
    return int(time.time() * 1000)

@dataclass(frozen=True)
class NodeInfo:
    node_id: str
    host: str
    server_port: int
    peer_port: int

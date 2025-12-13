import asyncio
import json
from dataclasses import dataclass
from typing import Any, Dict, Optional

JSONDict = Dict[str, Any]

async def read_json_line(reader: asyncio.StreamReader) -> JSONDict:
    line = await reader.readline()
    if not line:
        raise ConnectionError("Connection closed")
    return json.loads(line.decode("utf-8"))

async def write_json_line(writer: asyncio.StreamWriter, msg: JSONDict) -> None:
    data = (json.dumps(msg) + "\n").encode("utf-8")
    writer.write(data)
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

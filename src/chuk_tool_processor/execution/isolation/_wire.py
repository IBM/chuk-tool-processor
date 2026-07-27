# chuk_tool_processor/execution/isolation/_wire.py
"""
Length-prefixed JSON message framing for the host<->guest tool-broker channel.

This module MUST stay dependency-free (stdlib only, no chuk_tool_processor
imports): a copy of it is placed inside the isolated guest environment next to
the bootstrap, where the chuk_tool_processor package is not installed.

Wire format: a 4-byte big-endian unsigned length prefix followed by that many
bytes of UTF-8 JSON. JSON (never pickle) is used deliberately — the guest is
untrusted, and unpickling guest-controlled bytes on the host would hand code
execution straight back across the boundary.
"""

from __future__ import annotations

import asyncio
import json
import struct
from typing import Any

_LEN = struct.Struct(">I")

# Hard cap on a single frame so a malicious guest cannot force a huge allocation.
MAX_FRAME_BYTES = 8 * 1024 * 1024


def encode(obj: Any) -> bytes:
    """Encode a message object to a length-prefixed JSON frame."""
    body = json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(body) > MAX_FRAME_BYTES:
        raise ValueError(f"message too large: {len(body)} > {MAX_FRAME_BYTES}")
    return _LEN.pack(len(body)) + body


async def send(writer: asyncio.StreamWriter, obj: Any) -> None:
    """Send one framed message."""
    writer.write(encode(obj))
    await writer.drain()


async def recv(reader: asyncio.StreamReader) -> dict[str, Any]:
    """
    Read one framed message.

    Raises:
        EOFError: If the peer closed the connection cleanly at a frame boundary.
        ValueError: If the frame is malformed or exceeds ``MAX_FRAME_BYTES``.
    """
    header = await reader.readexactly(_LEN.size)
    (length,) = _LEN.unpack(header)
    if length > MAX_FRAME_BYTES:
        raise ValueError(f"declared frame length {length} exceeds cap {MAX_FRAME_BYTES}")
    body = await reader.readexactly(length)
    obj = json.loads(body.decode("utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("wire message must be a JSON object")
    return obj

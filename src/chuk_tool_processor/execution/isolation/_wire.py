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

# --------------------------------------------------------------------------- #
# Broker RPC protocol vocabulary.
#
# Shared between the host broker and the guest bootstrap. It lives in this
# dependency-free module (copied next to the guest) so both ends reference the
# same names instead of duplicating string/keys literals across the boundary.
# --------------------------------------------------------------------------- #

# Envelope / frame keys.
KEY_ID = "id"
KEY_METHOD = "method"
KEY_PARAMS = "params"
KEY_TOKEN = "token"
KEY_OK = "ok"
KEY_ERROR = "error"
KEY_VALUE = "value"

# ``call_tool`` parameter keys (also the ``list_tools`` reply entry keys).
KEY_NAME = "name"
KEY_NAMESPACE = "namespace"
KEY_ARGUMENTS = "arguments"

# RPC method names.
METHOD_HELLO = "hello"
METHOD_LIST_TOOLS = "list_tools"
METHOD_CALL_TOOL = "call_tool"
METHOD_RESULT = "result"

# Job payload keys (host ``GuestJob.payload`` -> guest bootstrap).
KEY_CODE = "code"
KEY_ENDPOINT = "endpoint"
KEY_TRANSPORT = "transport"
KEY_INITIAL_VARS = "initial_vars"
KEY_LIMITS = "limits"
KEY_CPU_TIMEOUT = "cpu_timeout"
KEY_MEMORY_BYTES = "memory_bytes"
KEY_MAX_PROCESSES = "max_processes"
KEY_MAX_OUTPUT_BYTES = "max_output_bytes"

# Transport kinds.
TRANSPORT_UNIX = "unix"
TRANSPORT_PIPE = "pipe"

# Default tool namespace when the host pins none.
DEFAULT_NAMESPACE = "default"

# Env flag that turns on host + guest connection checkpoints (to stderr).
GUEST_DEBUG_ENV = "CTP_GUEST_DEBUG"
GUEST_DEBUG_ON = "1"


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

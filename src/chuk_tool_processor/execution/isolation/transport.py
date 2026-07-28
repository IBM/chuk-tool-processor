# chuk_tool_processor/execution/isolation/transport.py
"""
Pluggable broker transport.

The host tool-broker needs a local, streaming, authenticated channel to the
guest. POSIX uses a unix-domain socket in a 0700 dir. Windows has no unix
sockets, and (for the AppContainer backend) TCP loopback is blocked without a
network-capability hole — so Windows uses a **named pipe** whose security
descriptor grants ``ALL APPLICATION PACKAGES`` and a low-integrity label, which
an AppContainer child can reach without any network access.

Both transports present the same interface to the broker: a callback receiving
``(asyncio.StreamReader, asyncio.StreamWriter)`` per connection, plus a string
``endpoint`` the guest connects to. The guest side lives in ``guest_bootstrap``
(dependency-free); this module is host-only.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from chuk_tool_processor.execution.isolation import _wire

IS_WINDOWS = os.name == "nt"


class MessageChannel:
    """A duplex, framed-JSON message channel to one guest connection.

    Abstracts over the concrete transport so the broker never touches sockets or
    pipes directly. The unix transport backs this with asyncio streams; the
    Windows pipe transport backs it with blocking pipe I/O in a thread.
    """

    async def recv(self) -> dict[str, Any]:
        raise NotImplementedError

    async def send(self, obj: dict[str, Any]) -> None:
        raise NotImplementedError

    async def aclose(self) -> None:
        raise NotImplementedError


class StreamChannel(MessageChannel):
    """MessageChannel over an asyncio (reader, writer) pair using ``_wire`` framing."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self._write_lock = asyncio.Lock()

    async def recv(self) -> dict[str, Any]:
        return await _wire.recv(self._reader)

    async def send(self, obj: dict[str, Any]) -> None:
        async with self._write_lock:
            await _wire.send(self._writer, obj)

    async def aclose(self) -> None:
        with contextlib.suppress(Exception):
            self._writer.close()


ClientHandler = Callable[[MessageChannel], Awaitable[None]]


def default_transport() -> str:
    """The transport kind for this platform: ``"pipe"`` on Windows, else ``"unix"``."""
    return "pipe" if IS_WINDOWS else "unix"


@dataclass
class BrokerListener:
    """A started broker server plus the endpoint the guest connects to."""

    endpoint: str
    transport: str
    _server: Any
    _cleanup: Callable[[], None] | None = None

    async def aclose(self) -> None:
        server = self._server
        with contextlib.suppress(Exception):  # teardown must not raise
            close = getattr(server, "close", None)
            if close:
                close()
            wait_closed = getattr(server, "wait_closed", None)
            if wait_closed:
                await wait_closed()
        if self._cleanup:
            with contextlib.suppress(Exception):
                self._cleanup()


async def start_listener(handle_client: ClientHandler) -> BrokerListener:
    """Start the platform-appropriate broker server and return its listener."""
    if IS_WINDOWS:
        # Imported lazily: the module uses Windows-only APIs.
        from chuk_tool_processor.execution.isolation import _winpipe

        return await _winpipe.start_pipe_listener(handle_client)
    return await _start_unix_listener(handle_client)


async def _start_unix_listener(handle_client: ClientHandler) -> BrokerListener:
    sock_dir = tempfile.mkdtemp(prefix="cticode-")
    os.chmod(sock_dir, 0o700)
    path = os.path.join(sock_dir, "broker.sock")

    async def _on_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        await handle_client(StreamChannel(reader, writer))

    server = await asyncio.start_unix_server(_on_client, path=path)
    os.chmod(path, 0o600)
    return BrokerListener(
        endpoint=path,
        transport="unix",
        _server=server,
        _cleanup=lambda: shutil.rmtree(sock_dir, ignore_errors=True),
    )

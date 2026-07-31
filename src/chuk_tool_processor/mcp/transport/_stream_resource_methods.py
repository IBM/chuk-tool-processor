# chuk_tool_processor/mcp/transport/_stream_resource_methods.py
"""Shared resource/prompt methods for stream-based MCP transports.

The stdio and HTTP-streamable transports drive the same ``send_*`` helpers over
a ``(read, write)`` stream pair, so their ``list_resources`` / ``list_prompts``
/ ``read_resource`` / ``get_prompt`` implementations are identical apart from
which streams they pass. This mixin captures that shared behaviour; each
transport only supplies :attr:`_send_streams`.

(The SSE transport uses a different request mechanism and does not use this.)
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from chuk_mcp_rs import (  # type: ignore[import-untyped]
    send_prompts_get,
    send_prompts_list,
    send_resources_list,
    send_resources_read,
)

from ._result_normalize import to_plain_dict

logger = logging.getLogger(__name__)


class StreamResourceMethodsMixin:
    """Resource/prompt queries shared by the stdio and HTTP-streamable transports."""

    if TYPE_CHECKING:  # provided by the concrete transport
        _initialized: bool
        _consecutive_failures: int
        default_timeout: float

        @property
        def _send_streams(self) -> tuple[Any, Any]: ...

    async def list_resources(self) -> dict[str, Any]:
        """Enhanced resource listing with error handling."""
        if not self._initialized:
            return {}

        try:
            response = await asyncio.wait_for(send_resources_list(*self._send_streams), timeout=self.default_timeout)
            self._consecutive_failures = 0  # Reset on success
            if isinstance(response, dict):
                return response
            # send_* helpers return a Result model (Pydantic model_dump/dict, or
            # the Rust-backed chuk-mcp's to_dict); normalise to a dict.
            normalized = to_plain_dict(response)
            return normalized if isinstance(normalized, dict) else {}
        except TimeoutError:
            logger.error("List resources timed out")
            self._consecutive_failures += 1
            return {}
        except Exception as e:
            logger.debug("Error listing resources: %s", e)
            self._consecutive_failures += 1
            return {}

    async def list_prompts(self) -> dict[str, Any]:
        """Enhanced prompt listing with error handling."""
        if not self._initialized:
            return {}

        try:
            response = await asyncio.wait_for(send_prompts_list(*self._send_streams), timeout=self.default_timeout)
            self._consecutive_failures = 0  # Reset on success
            if isinstance(response, dict):
                return response
            # send_* helpers return a Result model (Pydantic model_dump/dict, or
            # the Rust-backed chuk-mcp's to_dict); normalise to a dict.
            normalized = to_plain_dict(response)
            return normalized if isinstance(normalized, dict) else {}
        except TimeoutError:
            logger.error("List prompts timed out")
            self._consecutive_failures += 1
            return {}
        except Exception as e:
            logger.debug("Error listing prompts: %s", e)
            self._consecutive_failures += 1
            return {}

    async def read_resource(self, uri: str) -> dict[str, Any]:
        """Read a specific resource."""
        if not self._initialized:
            return {}

        try:
            response = await asyncio.wait_for(
                send_resources_read(*self._send_streams, uri), timeout=self.default_timeout
            )
            self._consecutive_failures = 0  # Reset on success
            if isinstance(response, dict):
                return response
            # send_resources_read returns a Result model (Pydantic model_dump/dict,
            # or the Rust-backed to_dict); normalize either form to a dict.
            normalized = to_plain_dict(response)
            return normalized if isinstance(normalized, dict) else {}
        except TimeoutError:
            logger.error("Read resource timed out")
            self._consecutive_failures += 1
            return {}
        except Exception as e:
            logger.debug("Error reading resource: %s", e)
            self._consecutive_failures += 1
            return {}

    async def get_prompt(self, name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        """Get a specific prompt."""
        if not self._initialized:
            return {}

        try:
            response = await asyncio.wait_for(
                send_prompts_get(*self._send_streams, name, arguments or {}),
                timeout=self.default_timeout,
            )
            self._consecutive_failures = 0  # Reset on success
            if isinstance(response, dict):
                return response
            # send_* helpers return a Result model (Pydantic model_dump/dict, or
            # the Rust-backed chuk-mcp's to_dict); normalise to a dict.
            normalized = to_plain_dict(response)
            return normalized if isinstance(normalized, dict) else {}
        except TimeoutError:
            logger.error("Get prompt timed out")
            self._consecutive_failures += 1
            return {}
        except Exception as e:
            logger.debug("Error getting prompt: %s", e)
            self._consecutive_failures += 1
            return {}

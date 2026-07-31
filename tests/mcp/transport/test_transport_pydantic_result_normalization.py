"""Regression tests: stream-based MCP transports must normalize the ``*Result``
objects returned by the ``send_*`` helpers into plain dicts.

Bug history
-----------
``list_resources`` / ``list_prompts`` / ``get_prompt`` on both the stdio and
HTTP-streamable transports used::

    return response if isinstance(response, dict) else {}

The ``send_*`` helpers return result *models* (``ListResourcesResult`` etc.),
*not* dicts, so the ``isinstance(..., dict)`` guard silently discarded every
result and the methods returned ``{}``. The transports now route non-dict
responses through ``to_plain_dict``.

These result models were Pydantic (``.model_dump()``) in the pure-Python
chuk-mcp and are PyO3 objects (``.to_dict()``) in the Rust-backed one. Rather
than construct either concrete type (their constructors differ across backends),
these tests use lightweight fakes exposing the same dump surface — which is
exactly what ``to_plain_dict`` keys off — and assert the transport methods
normalise both dump styles across both transports.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from chuk_tool_processor.mcp.transport.http_streamable_transport import HTTPStreamableTransport
from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

STDIO_MOD = "chuk_tool_processor.mcp.transport.stdio_transport"
HTTP_MOD = "chuk_tool_processor.mcp.transport.http_streamable_transport"
# stdio and HTTP share the resource/prompt methods (and thus the send_* helpers)
# via StreamResourceMethodsMixin, so patch the send_* where the mixin looks them up.
RES_MOD = "chuk_tool_processor.mcp.transport._stream_resource_methods"


class _ModelDumpResult:
    """Fake pydantic result model (exposes ``model_dump``)."""

    def __init__(self, payload):
        self._payload = payload

    def model_dump(self):
        return self._payload


class _ToDictResult:
    """Fake Rust-backed result object (exposes ``to_dict``)."""

    def __init__(self, payload):
        self._payload = payload

    def to_dict(self):
        return self._payload


# Both dump surfaces to_plain_dict handles, so every transport method is checked
# against the legacy pydantic shape and the modern Rust-backed shape.
RESULT_FLAVORS = [
    pytest.param(_ModelDumpResult, id="model_dump"),
    pytest.param(_ToDictResult, id="to_dict"),
]


def _make_stdio() -> StdioTransport:
    t = StdioTransport({"command": "python", "args": ["-m", "x"]})
    t._initialized = True
    t._streams = (Mock(), Mock())
    return t


def _make_http() -> HTTPStreamableTransport:
    t = HTTPStreamableTransport("http://test.com")
    t._initialized = True
    t._read_stream = Mock()
    t._write_stream = Mock()
    return t


# (transport factory, module to patch send_* in)
TRANSPORTS = [
    pytest.param(_make_stdio, STDIO_MOD, id="stdio"),
    pytest.param(_make_http, HTTP_MOD, id="http"),
]


class TestResultNormalization:
    """Every list/get transport method must dump result models to dicts."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("factory, mod", TRANSPORTS)
    @pytest.mark.parametrize("flavor", RESULT_FLAVORS)
    async def test_list_resources_normalizes_model(self, factory, mod, flavor):
        transport = factory()
        payload = {"resources": [{"uri": "demo://r1", "name": "r1"}]}
        with patch(f"{RES_MOD}.send_resources_list", AsyncMock(return_value=flavor(payload))):
            result = await transport.list_resources()
        assert result == payload
        assert result["resources"][0]["uri"] == "demo://r1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("factory, mod", TRANSPORTS)
    @pytest.mark.parametrize("flavor", RESULT_FLAVORS)
    async def test_list_prompts_normalizes_model(self, factory, mod, flavor):
        transport = factory()
        payload = {"prompts": [{"name": "greet"}]}
        with patch(f"{RES_MOD}.send_prompts_list", AsyncMock(return_value=flavor(payload))):
            result = await transport.list_prompts()
        assert result == payload
        assert result["prompts"][0]["name"] == "greet"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("factory, mod", TRANSPORTS)
    @pytest.mark.parametrize("flavor", RESULT_FLAVORS)
    async def test_get_prompt_normalizes_model(self, factory, mod, flavor):
        transport = factory()
        payload = {"messages": [{"role": "user", "content": {"type": "text", "text": "hi"}}]}
        with patch(f"{RES_MOD}.send_prompts_get", AsyncMock(return_value=flavor(payload))):
            result = await transport.get_prompt("greet", {})
        assert result == payload
        assert result["messages"][0]["role"] == "user"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("factory, mod", TRANSPORTS)
    @pytest.mark.parametrize("flavor", RESULT_FLAVORS)
    async def test_read_resource_normalizes_model(self, factory, mod, flavor):
        transport = factory()
        payload = {"contents": [{"uri": "demo://r1", "text": "hello"}]}
        with patch(f"{RES_MOD}.send_resources_read", AsyncMock(return_value=flavor(payload))):
            result = await transport.read_resource("demo://r1")
        assert result == payload
        assert result["contents"][0]["text"] == "hello"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("factory, mod", TRANSPORTS)
    async def test_dict_response_passes_through(self, factory, mod):
        # Helpers/servers that already yield a dict must keep working unchanged.
        transport = factory()
        payload = {"resources": [{"uri": "demo://r1", "name": "r1"}]}
        with patch(f"{RES_MOD}.send_resources_list", AsyncMock(return_value=payload)):
            result = await transport.list_resources()
        assert result == payload

    @pytest.mark.asyncio
    @pytest.mark.parametrize("factory, mod", TRANSPORTS)
    async def test_non_model_non_dict_is_safe(self, factory, mod):
        # An unexpected response type must degrade to {} rather than raise.
        transport = factory()
        with patch(f"{RES_MOD}.send_prompts_list", AsyncMock(return_value=None)):
            result = await transport.list_prompts()
        assert result == {}

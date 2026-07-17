"""
Regression tests: stream-based MCP transports must normalize the pydantic
``*Result`` models returned by chuk_mcp's ``send_*`` helpers into plain dicts.

Bug history
-----------
``list_resources`` / ``list_prompts`` / ``get_prompt`` on both the stdio and
HTTP-streamable transports used::

    return response if isinstance(response, dict) else {}

``send_resources_list`` / ``send_prompts_list`` / ``send_prompts_get`` return
pydantic models (``ListResourcesResult`` etc.), *not* dicts, so the
``isinstance(..., dict)`` guard silently discarded every result and the methods
returned ``{}``. ``read_resource`` already handled this correctly via
``model_dump()``.

The pre-existing tests masked the bug because they mocked the ``send_*`` helpers
with plain dicts (which trivially satisfy the ``isinstance`` check) rather than
with the pydantic models the real helpers return. These tests use the real
chuk_mcp models so they reflect production behaviour, and run against both
transports.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest
from chuk_mcp.protocol.messages.prompts.prompt import Prompt, PromptMessage
from chuk_mcp.protocol.messages.prompts.send_messages import (
    GetPromptResult,
    ListPromptsResult,
)
from chuk_mcp.protocol.messages.resources.resource import Resource
from chuk_mcp.protocol.messages.resources.resource_content import ResourceContent
from chuk_mcp.protocol.messages.resources.send_messages import (
    ListResourcesResult,
    ReadResourceResult,
)

from chuk_tool_processor.mcp.transport.http_streamable_transport import (
    HTTPStreamableTransport,
)
from chuk_tool_processor.mcp.transport.stdio_transport import StdioTransport

STDIO_MOD = "chuk_tool_processor.mcp.transport.stdio_transport"
HTTP_MOD = "chuk_tool_processor.mcp.transport.http_streamable_transport"


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


class TestPydanticResultNormalization:
    """Every list/get transport method must dump pydantic results to dicts."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize("factory, mod", TRANSPORTS)
    async def test_list_resources_normalizes_model(self, factory, mod):
        transport = factory()
        model = ListResourcesResult(resources=[Resource(uri="demo://r1", name="r1")])
        with patch(f"{mod}.send_resources_list", AsyncMock(return_value=model)):
            result = await transport.list_resources()
        assert isinstance(result, dict)
        assert result == model.model_dump()
        assert result["resources"][0]["uri"] == "demo://r1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("factory, mod", TRANSPORTS)
    async def test_list_prompts_normalizes_model(self, factory, mod):
        transport = factory()
        model = ListPromptsResult(prompts=[Prompt(name="greet")])
        with patch(f"{mod}.send_prompts_list", AsyncMock(return_value=model)):
            result = await transport.list_prompts()
        assert isinstance(result, dict)
        assert result == model.model_dump()
        assert result["prompts"][0]["name"] == "greet"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("factory, mod", TRANSPORTS)
    async def test_get_prompt_normalizes_model(self, factory, mod):
        transport = factory()
        model = GetPromptResult(messages=[PromptMessage(role="user", content={"type": "text", "text": "hi"})])
        with patch(f"{mod}.send_prompts_get", AsyncMock(return_value=model)):
            result = await transport.get_prompt("greet", {})
        assert isinstance(result, dict)
        assert result == model.model_dump()
        assert result["messages"][0]["role"] == "user"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("factory, mod", TRANSPORTS)
    async def test_read_resource_still_normalizes_model(self, factory, mod):
        # read_resource already handled models; guard against regressions.
        transport = factory()
        model = ReadResourceResult(contents=[ResourceContent(uri="demo://r1", text="hello")])
        with patch(f"{mod}.send_resources_read", AsyncMock(return_value=model)):
            result = await transport.read_resource("demo://r1")
        assert isinstance(result, dict)
        assert result == model.model_dump()
        assert result["contents"][0]["text"] == "hello"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("factory, mod", TRANSPORTS)
    async def test_dict_response_passes_through(self, factory, mod):
        # Helpers/servers that already yield a dict must keep working unchanged.
        transport = factory()
        payload = {"resources": [{"uri": "demo://r1", "name": "r1"}]}
        with patch(f"{mod}.send_resources_list", AsyncMock(return_value=payload)):
            result = await transport.list_resources()
        assert result == payload

    @pytest.mark.asyncio
    @pytest.mark.parametrize("factory, mod", TRANSPORTS)
    async def test_non_model_non_dict_is_safe(self, factory, mod):
        # An unexpected response type must degrade to {} rather than raise.
        transport = factory()
        with patch(f"{mod}.send_prompts_list", AsyncMock(return_value=None)):
            result = await transport.list_prompts()
        assert result == {}

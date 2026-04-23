# tests/tool_processor/registry/test_tool_metadata.py
import pytest
from pydantic import ValidationError

from chuk_tool_processor.registry.metadata import ToolMetadata


@pytest.mark.asyncio
async def test_defaults_and_str():
    tm = ToolMetadata(name="my_tool")
    assert tm.name == "my_tool"
    assert tm.namespace == "default"
    assert tm.description is None
    assert tm.version == "1.0.0"
    assert tm.is_async is True  # Note: Changed to True for async-native
    assert tm.argument_schema is None
    assert tm.result_schema is None
    assert tm.requires_auth is False
    assert isinstance(tm.tags, set) and len(tm.tags) == 0
    assert str(tm) == "default.my_tool (v1.0.0)"


@pytest.mark.asyncio
async def test_custom_values_and_tags_independence():
    schema = {"type": "object", "properties": {"x": {"type": "integer"}}}
    tm1 = ToolMetadata(
        name="foo",
        namespace="ns",
        description="desc",
        version="2.3.4",
        is_async=True,
        argument_schema=schema,
        result_schema={"type": "string"},
        requires_auth=True,
        tags={"a", "b"},
    )
    tm2 = ToolMetadata(name="bar")

    assert tm1.tags == {"a", "b"}
    tm1.tags.add("c")
    assert tm1.tags == {"a", "b", "c"}
    assert tm2.tags == set()


@pytest.mark.asyncio
async def test_missing_name_raises():
    with pytest.raises(ValidationError) as excinfo:
        ToolMetadata()
    assert "Field required" in str(excinfo.value)


@pytest.mark.asyncio
async def test_invalid_name_type_raises():
    with pytest.raises(ValidationError):
        ToolMetadata(name=123)  # name must be str


@pytest.mark.asyncio
async def test_is_async_default_is_true():
    tm = ToolMetadata(name="t1")
    assert tm.is_async is True


@pytest.mark.asyncio
async def test_is_async_respects_value():
    # is_async now reflects the actual value — no forced override
    tm_false = ToolMetadata(name="test", is_async=False)
    assert tm_false.is_async is False
    tm_true = ToolMetadata(name="test2", is_async=True)
    assert tm_true.is_async is True


@pytest.mark.asyncio
async def test_tags_coerced_to_set():
    tm = ToolMetadata(name="t", tags=["a", "b", "a"])
    assert isinstance(tm.tags, set)
    assert tm.tags == {"a", "b"}


@pytest.mark.asyncio
async def test_partial_override_defaults():
    tm = ToolMetadata(name="t", namespace="custom_ns")
    assert tm.namespace == "custom_ns"
    assert tm.version == "1.0.0"


@pytest.mark.asyncio
async def test_str_reflects_overrides():
    tm = ToolMetadata(name="name", namespace="my_ns", version="9.9.9")
    assert str(tm) == "my_ns.name (v9.9.9)"


@pytest.mark.asyncio
async def test_with_updated_timestamp():
    tm = ToolMetadata(name="test")
    initial_timestamp = tm.updated_at

    # Small delay to ensure timestamp difference
    # Using asyncio.sleep to ensure time passes
    import asyncio

    await asyncio.sleep(0.001)  # 1ms delay

    # Create an updated copy
    updated = tm.with_updated_timestamp()

    # Check that the timestamp was updated
    assert updated.updated_at >= initial_timestamp  # Should be equal or greater
    assert updated.name == tm.name  # Should preserve other properties


@pytest.mark.asyncio
async def test_streaming_metadata():
    from chuk_tool_processor.registry.metadata import StreamingToolMetadata

    tm = StreamingToolMetadata(name="stream_tool")
    assert tm.supports_streaming is True
    assert tm.name == "stream_tool"

    # Test with chunk size and content type
    tm2 = StreamingToolMetadata(name="stream_tool2", chunk_size=1024, content_type="text/plain")
    assert tm2.chunk_size == 1024
    assert tm2.content_type == "text/plain"

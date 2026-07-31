"""Unit tests for ``mcp/_config.py`` — ctp's replicated stdio config loader."""

import json

import pytest

from chuk_tool_processor.mcp._config import load_config


def _write_config(tmp_path, mcp_servers):
    path = tmp_path / "servers.json"
    path.write_text(json.dumps({"mcpServers": mcp_servers}), encoding="utf-8")
    return str(path)


@pytest.mark.asyncio
async def test_loads_command_args_and_env(tmp_path):
    path = _write_config(
        tmp_path,
        {"demo": {"command": "python", "args": ["-m", "server"], "env": {"KEY": "v"}}},
    )

    params, timeout = await load_config(path, "demo")

    assert params.command == "python"
    assert params.args == ["-m", "server"]
    assert params.env == {"KEY": "v"}
    assert timeout is None


@pytest.mark.asyncio
async def test_args_and_env_default_when_absent(tmp_path):
    path = _write_config(tmp_path, {"demo": {"command": "python"}})

    params, timeout = await load_config(path, "demo")

    assert params.command == "python"
    assert params.args == []
    assert timeout is None


@pytest.mark.asyncio
async def test_timeout_is_returned_as_float(tmp_path):
    path = _write_config(tmp_path, {"demo": {"command": "python", "timeout": 12}})

    _, timeout = await load_config(path, "demo")

    assert timeout == 12.0
    assert isinstance(timeout, float)


@pytest.mark.asyncio
async def test_missing_server_raises_value_error(tmp_path):
    path = _write_config(tmp_path, {"demo": {"command": "python"}})

    with pytest.raises(ValueError, match="not found"):
        await load_config(path, "absent")


@pytest.mark.asyncio
async def test_server_without_command_raises_value_error(tmp_path):
    path = _write_config(tmp_path, {"demo": {"args": ["x"]}})

    with pytest.raises(ValueError, match="stdio"):
        await load_config(path, "demo")


@pytest.mark.asyncio
async def test_missing_file_raises_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        await load_config(str(tmp_path / "nope.json"), "demo")


@pytest.mark.asyncio
async def test_invalid_json_raises_decode_error(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{ not json", encoding="utf-8")

    with pytest.raises(json.JSONDecodeError):
        await load_config(str(path), "demo")

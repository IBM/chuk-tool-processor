"""Load MCP stdio server parameters from a JSON config file.

Replaces the equivalent helper from the ``chuk-mcp`` facade so that
chuk-tool-processor depends on ``chuk-mcp-rs`` directly. The config format is
unchanged: ``{"mcpServers": {"<name>": {"command", "args", "env", "timeout"}}}``.
"""

from __future__ import annotations

import json
import logging

from chuk_mcp_rs import StdioParameters  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


async def load_config(config_path: str, server_name: str) -> tuple[StdioParameters, float | None]:
    """Return ``(StdioParameters, timeout)`` for ``server_name`` in ``config_path``.

    ``timeout`` is the per-server timeout in seconds, or ``None`` if unset.

    Raises:
        FileNotFoundError: the config file does not exist.
        json.JSONDecodeError: the file is not valid JSON.
        ValueError: the named server is absent (or is not a stdio/command server).
    """
    logger.debug("Loading MCP config from %s", config_path)
    with open(config_path, encoding="utf-8") as fh:
        config = json.load(fh)

    server_config = config.get("mcpServers", {}).get(server_name)
    if not server_config:
        raise ValueError(f"Server '{server_name}' not found in configuration file.")
    if "command" not in server_config:
        raise ValueError(f"Server '{server_name}' is not a stdio (command) server; load_config handles stdio only.")

    params = StdioParameters(
        command=server_config["command"],
        args=server_config.get("args", []),
        env=server_config.get("env"),
    )
    timeout = server_config.get("timeout")
    return params, (float(timeout) if timeout is not None else None)

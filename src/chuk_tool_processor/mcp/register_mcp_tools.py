#!/usr/bin/env python
# chuk_tool_processor/mcp/register_mcp_tools.py
"""
Discover the remote MCP tools exposed by a StreamManager and register them locally.

CLEAN & SIMPLE: Just the essentials - create MCPTool wrappers for remote tools.
"""

from __future__ import annotations

from typing import Any

from chuk_tool_processor.logging import get_logger
from chuk_tool_processor.mcp.mcp_tool import MCPTool, RecoveryConfig
from chuk_tool_processor.mcp.stream_manager import StreamManager
from chuk_tool_processor.registry.metadata import MCPToolFactoryParams
from chuk_tool_processor.registry.provider import ToolRegistryProvider

logger = get_logger("chuk_tool_processor.mcp.register")


async def register_mcp_tools(
    stream_manager: StreamManager,
    namespace: str = "mcp",
    *,
    # Optional resilience configuration
    default_timeout: float = 30.0,
    enable_resilience: bool = True,
    recovery_config: RecoveryConfig | None = None,
    # Deferred loading configuration
    defer_loading: bool = False,
    defer_all_except: list[str] | None = None,
    defer_only: list[str] | None = None,
    search_keywords_fn: Any = None,
) -> list[str]:
    """
    Pull the remote tool catalogue and create local MCPTool wrappers.

    Parameters
    ----------
    stream_manager
        An initialised StreamManager.
    namespace
        Tools are registered under their original name in the specified namespace.
    default_timeout
        Default timeout for tool execution
    enable_resilience
        Whether to enable resilience features (circuit breaker, retries)
    recovery_config
        Optional custom recovery configuration
    defer_loading
        If True, defer all tools by default (can be overridden by defer_all_except)
    defer_all_except
        List of tool names to NOT defer (load eagerly). Only used if defer_loading=True.
    defer_only
        List of tool names to defer. Only used if defer_loading=False.
    search_keywords_fn
        Optional function(tool_name, tool_def) -> list[str] to generate search keywords

    Returns
    -------
    list[str]
        The tool names that were registered.
    """
    registry = await ToolRegistryProvider.get_registry()
    registered: list[str] = []

    # Store stream_manager reference for deferred MCP tools
    if hasattr(registry, "set_stream_manager"):
        registry.set_stream_manager(namespace, stream_manager)

    # Get the remote tool catalogue
    mcp_tools: list[dict[str, Any]] = stream_manager.get_all_tools()

    for tool_def in mcp_tools:
        tool_name = tool_def.get("name")
        if not tool_name:
            logger.warning("Remote tool definition without a 'name' field - skipped")
            continue

        description = tool_def.get("description") or f"MCP tool • {tool_name}"

        # Determine if this tool should be deferred
        should_defer = False
        if defer_loading:
            # Defer all except those in the exception list
            should_defer = tool_name not in (defer_all_except or [])
        elif defer_only:
            # Only defer those in the defer list
            should_defer = tool_name in defer_only

        # Generate search keywords for deferred tools
        search_keywords = []
        if should_defer and search_keywords_fn:
            search_keywords = search_keywords_fn(tool_name, tool_def)
        elif should_defer:
            # Default: use tool name and description words
            search_keywords = [tool_name.lower()]
            if description:
                # Extract words from description
                words = description.lower().split()
                search_keywords.extend([w for w in words if len(w) > 3])

        meta: dict[str, Any] = {
            "description": description,
            "is_async": True,
            "tags": {"mcp", "remote"},
            "argument_schema": tool_def.get("inputSchema", {}),
            "defer_loading": should_defer,
        }

        # Add search keywords for deferred tools
        if should_defer and search_keywords:
            meta["search_keywords"] = search_keywords[:10]  # Limit to 10

        # Add icon if present (MCP spec 2025-11-25)
        if "icon" in tool_def:
            meta["icon"] = tool_def["icon"]

        # For deferred tools, store factory params as Pydantic model
        if should_defer:
            meta["mcp_factory_params"] = MCPToolFactoryParams(
                tool_name=tool_name,
                default_timeout=default_timeout,
                enable_resilience=enable_resilience,
                recovery_config=recovery_config,
                namespace=namespace,
            )

        try:
            # Create MCPTool wrapper with optional resilience configuration
            wrapper = MCPTool(
                tool_name=tool_name,
                stream_manager=stream_manager,
                default_timeout=default_timeout,
                enable_resilience=enable_resilience,
                recovery_config=recovery_config,
            )

            await registry.register_tool(
                wrapper,
                name=tool_name,
                namespace=namespace,
                metadata=meta,
            )

            registered.append(tool_name)
            defer_status = " (deferred)" if should_defer else ""
            logger.debug(
                "MCP tool '%s' registered as '%s:%s'%s",
                tool_name,
                namespace,
                tool_name,
                defer_status,
            )
        except Exception as exc:
            logger.error("Failed to register MCP tool '%s': %s", tool_name, exc)

    logger.debug("MCP registration complete - %d tool(s) available", len(registered))
    return registered


async def update_mcp_tools_stream_manager(
    namespace: str,
    new_stream_manager: StreamManager | None,
) -> int:
    """
    Update the StreamManager reference for all MCP tools in a namespace.

    Useful for reconnecting tools after StreamManager recovery at the service level.

    Parameters
    ----------
    namespace
        The namespace containing MCP tools to update
    new_stream_manager
        The new StreamManager to use, or None to disconnect

    Returns
    -------
    int
        Number of tools updated
    """
    registry = await ToolRegistryProvider.get_registry()
    updated_count = 0

    try:
        # List all tools in the namespace
        all_tools = await registry.list_tools()
        namespace_tools = [tool_info.name for tool_info in all_tools if tool_info.namespace == namespace]

        for tool_name in namespace_tools:
            try:
                tool = await registry.get_tool(tool_name, namespace)
                if tool and hasattr(tool, "set_stream_manager"):
                    tool.set_stream_manager(new_stream_manager)
                    updated_count += 1
                    logger.debug("Updated StreamManager for tool '%s:%s'", namespace, tool_name)
            except Exception as e:
                logger.warning("Failed to update StreamManager for tool '%s:%s': %s", namespace, tool_name, e)

        action = "connected" if new_stream_manager else "disconnected"
        logger.debug("StreamManager %s for %d tools in namespace '%s'", action, updated_count, namespace)

    except Exception as e:
        logger.error("Failed to update tools in namespace '%s': %s", namespace, e)

    return updated_count

# chuk_tool_processor/registry/metadata.py
"""
Tool metadata models for the registry with async-native support.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


class MCPToolFactoryParams(BaseModel):
    """
    Factory parameters for creating MCP tools on-demand.

    Used for deferred loading of MCP tools.
    """

    tool_name: str = Field(..., description="Name of the MCP tool")
    default_timeout: float = Field(30.0, description="Default timeout for tool execution")
    enable_resilience: bool = Field(True, description="Whether to enable resilience features")
    recovery_config: Any | None = Field(None, description="Optional recovery configuration")
    namespace: str = Field(..., description="Namespace where stream_manager is stored")


class ToolInfo(BaseModel):
    """
    Information about a registered tool (namespace and name).

    This is a clear, type-safe alternative to plain tuples for tool listings.

    Attributes:
        namespace: The namespace the tool belongs to.
        name: The name of the tool.

    Example:
        >>> tool = ToolInfo(namespace="math", name="add")
        >>> print(f"{tool.namespace}:{tool.name}")
        math:add
    """

    model_config = {"frozen": True}  # Make immutable and hashable

    namespace: str = Field(..., description="Namespace the tool belongs to")
    name: str = Field(..., description="Tool name")

    def __str__(self) -> str:
        """String representation in namespace:name format."""
        return f"{self.namespace}:{self.name}"


class ToolMetadata(BaseModel):
    """
    Metadata for registered tools.

    Attributes:
        name: The name of the tool.
        namespace: The namespace the tool belongs to.
        description: Optional description of the tool's functionality.
        version: Version of the tool implementation.
        is_async: Whether the tool's execute method is asynchronous.
        argument_schema: Optional schema for the tool's arguments.
        result_schema: Optional schema for the tool's result.
        requires_auth: Whether the tool requires authentication.
        tags: Set of tags associated with the tool.
        created_at: When the tool was first registered.
        updated_at: When the tool was last updated.
        source: Optional source information (e.g., "function", "class", "langchain").
        source_name: Optional source identifier.
        concurrency_limit: Optional maximum concurrent executions.
        timeout: Optional default timeout in seconds.
        rate_limit: Optional rate limiting configuration.
    """

    name: str = Field(..., description="Tool name")
    namespace: str = Field("default", description="Namespace the tool belongs to")
    description: str | None = Field(None, description="Tool description")
    version: str = Field("1.0.0", description="Tool implementation version")
    is_async: bool = Field(True, description="Whether the tool's execute method is asynchronous")
    argument_schema: dict[str, Any] | None = Field(None, description="Schema for the tool's arguments")
    result_schema: dict[str, Any] | None = Field(None, description="Schema for the tool's result")
    requires_auth: bool = Field(False, description="Whether the tool requires authentication")
    tags: set[str] = Field(default_factory=set, description="Tags associated with the tool")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC), description="When the tool was first registered"
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC), description="When the tool was last updated"
    )
    source: str | None = Field(None, description="Source of the tool (e.g., 'function', 'class', 'langchain')")
    source_name: str | None = Field(None, description="Source identifier (e.g., function name, class name)")
    concurrency_limit: int | None = Field(None, description="Maximum concurrent executions (None = unlimited)")
    timeout: float | None = Field(None, description="Default timeout in seconds (None = no timeout)")
    rate_limit: dict[str, Any] | None = Field(None, description="Rate limiting configuration")

    # Additional fields for async-native architecture
    supports_streaming: bool = Field(False, description="Whether the tool supports streaming responses")
    execution_options: dict[str, Any] = Field(default_factory=dict, description="Additional execution options")
    dependencies: list[str] = Field(default_factory=list, description="Dependencies on other tools")

    # Dynamic loading fields (advanced tool use)
    defer_loading: bool = Field(False, description="If True, tool is loaded on-demand rather than eagerly")
    search_keywords: list[str] = Field(default_factory=list, description="Keywords for tool discovery")
    import_path: str | None = Field(None, description="Import path for lazy loading (e.g., 'module.ClassName')")
    allowed_callers: list[str] | None = Field(None, description="Allowed callers: ['claude', 'programmatic']")
    mcp_factory_params: MCPToolFactoryParams | None = Field(
        None, description="Factory parameters for creating deferred MCP tools"
    )

    def with_updated_timestamp(self) -> ToolMetadata:
        """Create a copy with updated timestamp."""
        return self.model_copy(update={"updated_at": datetime.now(UTC)})

    def __str__(self) -> str:
        """String representation of the tool metadata."""
        return f"{self.namespace}.{self.name} (v{self.version})"


class RateLimitConfig(BaseModel):
    """Rate limiting configuration for tools."""

    requests: int = Field(..., description="Maximum number of requests")
    period: float = Field(..., description="Time period in seconds")
    scope: str = Field("global", description="Scope of rate limiting: 'global', 'user', 'ip'")


class StreamingToolMetadata(ToolMetadata):
    """Extended metadata for tools that support streaming responses."""

    supports_streaming: bool = Field(True, description="Whether the tool supports streaming responses")
    chunk_size: int | None = Field(None, description="Suggested chunk size for streaming")
    content_type: str | None = Field(None, description="Content type for streaming responses")

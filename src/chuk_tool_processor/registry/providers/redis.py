# chuk_tool_processor/registry/providers/redis.py
"""
Redis-backed implementation of the asynchronous tool registry.

This provider enables distributed state across multiple processes/machines
by storing tool registrations in Redis.

Note: Tool objects themselves cannot be stored in Redis (they are Python objects).
This provider stores metadata and allows tool loading from import paths.
For tools that are registered at runtime without import paths, you should
use a pattern where each process registers the same tools on startup.
"""

from __future__ import annotations

import asyncio
import inspect
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from chuk_tool_processor.core.exceptions import ToolNotFoundError
from chuk_tool_processor.registry.interface import ToolRegistryInterface
from chuk_tool_processor.registry.metadata import ToolInfo, ToolMetadata

if TYPE_CHECKING:
    from redis.asyncio import Redis


class RedisKeyType(StrEnum):
    """Redis key types for registry storage."""

    TOOLS = "tools"
    NAMESPACES = "namespaces"
    DEFERRED = "deferred"


class RedisConfig(BaseModel):
    """Configuration for Redis registry."""

    key_prefix: str = Field(default="chuk", description="Prefix for all Redis keys")
    local_cache_ttl: float = Field(default=60.0, description="TTL in seconds for local tool cache")
    redis_url: str = Field(default="redis://localhost:6379/0", description="Redis connection URL")


class RedisToolRegistry(ToolRegistryInterface):
    """
    Redis-backed implementation of the async ToolRegistryInterface.

    This registry stores tool metadata in Redis, enabling distributed state
    across multiple processes. Tool objects are cached locally but metadata
    is shared across all instances.

    Key structure in Redis:
    - {prefix}:tools:{namespace}:{name} -> Tool metadata JSON
    - {prefix}:namespaces -> Set of all namespaces
    - {prefix}:deferred:{namespace}:{name} -> Deferred tool metadata JSON

    Note: This provider requires the `redis` package with async support:
        pip install redis[hiredis]  # or: uv add redis[hiredis]
    """

    def __init__(
        self,
        redis_client: Redis,
        config: RedisConfig | None = None,
    ) -> None:
        """
        Initialize the Redis registry.

        Args:
            redis_client: Async Redis client instance
            config: Configuration for the registry (uses defaults if not provided)
        """
        self._redis = redis_client
        self._config = config or RedisConfig()

        # Local caches for tool objects (Redis stores metadata only)
        self._tools: dict[str, dict[str, Any]] = {}
        self._deferred_tools: dict[str, dict[str, Any]] = {}
        self._loaded_deferred_tools: set[str] = set()
        self._stream_managers: dict[str, Any] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------ #
    # Key helpers - use enum for key types
    # ------------------------------------------------------------------ #

    def _build_key(self, key_type: RedisKeyType, *parts: str) -> str:
        """Build a Redis key with prefix and type."""
        if parts:
            return f"{self._config.key_prefix}:{key_type.value}:{':'.join(parts)}"
        return f"{self._config.key_prefix}:{key_type.value}"

    def _tool_key(self, namespace: str, name: str) -> str:
        """Get Redis key for a tool."""
        return self._build_key(RedisKeyType.TOOLS, namespace, name)

    def _namespace_key(self) -> str:
        """Get Redis key for namespace set."""
        return self._build_key(RedisKeyType.NAMESPACES)

    def _deferred_key(self, namespace: str, name: str) -> str:
        """Get Redis key for a deferred tool."""
        return self._build_key(RedisKeyType.DEFERRED, namespace, name)

    def _tools_pattern(self, namespace: str | None = None) -> str:
        """Get pattern to match all tools in a namespace."""
        if namespace:
            return f"{self._config.key_prefix}:{RedisKeyType.TOOLS.value}:{namespace}:*"
        return f"{self._config.key_prefix}:{RedisKeyType.TOOLS.value}:*"

    def _deferred_pattern(self, namespace: str | None = None) -> str:
        """Get pattern to match all deferred tools in a namespace."""
        if namespace:
            return f"{self._config.key_prefix}:{RedisKeyType.DEFERRED.value}:{namespace}:*"
        return f"{self._config.key_prefix}:{RedisKeyType.DEFERRED.value}:*"

    # ------------------------------------------------------------------ #
    # Serialization helpers - use Pydantic native serialization
    # ------------------------------------------------------------------ #

    def _serialize_metadata(self, metadata: ToolMetadata) -> str:
        """Serialize ToolMetadata to JSON string using Pydantic."""
        return metadata.model_dump_json()

    def _deserialize_metadata(self, data: str | bytes) -> ToolMetadata:
        """Deserialize JSON string/bytes to ToolMetadata using Pydantic."""
        if isinstance(data, bytes):
            data = data.decode()
        return ToolMetadata.model_validate_json(data)

    def _parse_key_parts(self, key: str | bytes) -> tuple[str, str] | None:
        """Parse namespace and name from a Redis key."""
        key_str = key.decode() if isinstance(key, bytes) else key
        parts = key_str.split(":")
        if len(parts) >= 4:
            return parts[2], parts[3]
        return None

    # ------------------------------------------------------------------ #
    # Registration
    # ------------------------------------------------------------------ #

    async def register_tool(
        self,
        tool: Any,
        name: str | None = None,
        namespace: str = "default",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Register a tool in the registry.

        The tool object is cached locally, but metadata is stored in Redis
        for distributed access. Other processes can load the tool from its
        import_path if provided in metadata.
        """
        async with self._lock:
            # Determine the actual name and namespace
            key = name or getattr(tool, "__name__", None) or repr(tool)

            # Auto-parse dotted names: "web.fetch_user" -> namespace="web", name="fetch_user"
            if "." in key and namespace == "default":
                parts = key.split(".", 1)
                namespace = parts[0]
                key = parts[1]

            # Build metadata
            is_async = inspect.iscoroutinefunction(getattr(tool, "execute", None))
            description = (inspect.getdoc(tool) or "").strip() if not (metadata and "description" in metadata) else None

            meta_dict: dict[str, Any] = {
                "name": key,
                "namespace": namespace,
                "is_async": is_async,
            }
            if description:
                meta_dict["description"] = description
            if metadata:
                meta_dict.update(metadata)

            tool_metadata = ToolMetadata(**meta_dict)

            # Check if this is a deferred tool
            if tool_metadata.defer_loading:
                # Store metadata in Redis for deferred tools
                deferred_key = self._deferred_key(namespace, key)
                await self._redis.set(deferred_key, self._serialize_metadata(tool_metadata))

                # Store pre-instantiated tool locally if no import_path
                if tool_metadata.import_path is None and tool_metadata.mcp_factory_params is None:
                    self._deferred_tools.setdefault(namespace, {})[key] = tool
            else:
                # Eager loading - store metadata in Redis
                tool_key = self._tool_key(namespace, key)
                await self._redis.set(tool_key, self._serialize_metadata(tool_metadata))

                # Track namespace
                await self._redis.sadd(self._namespace_key(), namespace)

                # Cache tool locally
                self._tools.setdefault(namespace, {})[key] = tool

    # ------------------------------------------------------------------ #
    # Retrieval
    # ------------------------------------------------------------------ #

    async def get_tool(self, name: str, namespace: str = "default") -> Any | None:
        """
        Retrieve a tool by name and namespace.

        First checks local cache, then Redis metadata.
        If the tool is deferred, it will be loaded automatically.
        """
        # Check local cache first
        tool = self._tools.get(namespace, {}).get(name)
        if tool is not None:
            return tool

        # Check if it's a deferred tool
        loaded_key = f"{namespace}.{name}"
        if loaded_key not in self._loaded_deferred_tools:
            deferred_key = self._deferred_key(namespace, name)
            if await self._redis.exists(deferred_key):
                return await self.load_deferred_tool(name, namespace)

        # Check if metadata exists in Redis (tool might be registered elsewhere)
        tool_key = self._tool_key(namespace, name)
        metadata_bytes = await self._redis.get(tool_key)
        if metadata_bytes:
            metadata = self._deserialize_metadata(metadata_bytes)
            # Try to load from import_path
            if metadata.import_path:
                tool = await self._import_tool(metadata.import_path)
                self._tools.setdefault(namespace, {})[name] = tool
                return tool

        return None

    async def get_tool_strict(self, name: str, namespace: str = "default") -> Any:
        """Get a tool with strict validation, raising if not found."""
        tool = await self.get_tool(name, namespace)
        if tool is None:
            all_tools = await self.list_tools()
            available_tools = [(t.namespace, t.name) for t in all_tools]
            available_namespaces = await self.list_namespaces()

            raise ToolNotFoundError(
                tool_name=name,
                namespace=namespace,
                available_tools=available_tools,
                available_namespaces=available_namespaces,
            )
        return tool

    async def get_metadata(self, name: str, namespace: str = "default") -> ToolMetadata | None:
        """Get metadata for a tool."""
        # Check active tools
        tool_key = self._tool_key(namespace, name)
        metadata_bytes = await self._redis.get(tool_key)
        if metadata_bytes:
            return self._deserialize_metadata(metadata_bytes)

        # Check deferred tools
        deferred_key = self._deferred_key(namespace, name)
        deferred_bytes = await self._redis.get(deferred_key)
        if deferred_bytes:
            return self._deserialize_metadata(deferred_bytes)

        return None

    # ------------------------------------------------------------------ #
    # Listing helpers
    # ------------------------------------------------------------------ #

    async def list_tools(self, namespace: str | None = None) -> list[ToolInfo]:
        """Return a list of ToolInfo objects."""
        result: list[ToolInfo] = []
        pattern = self._tools_pattern(namespace)

        async for key in self._redis.scan_iter(match=pattern):
            parsed = self._parse_key_parts(key)
            if parsed:
                ns, name = parsed
                result.append(ToolInfo(namespace=ns, name=name))

        return result

    async def list_namespaces(self) -> list[str]:
        """List all namespaces."""
        namespaces = await self._redis.smembers(self._namespace_key())
        return [ns.decode() if isinstance(ns, bytes) else ns for ns in namespaces]

    async def list_metadata(self, namespace: str | None = None) -> list[ToolMetadata]:
        """Return all ToolMetadata objects."""
        result: list[ToolMetadata] = []
        tools = await self.list_tools(namespace)

        for tool_info in tools:
            metadata = await self.get_metadata(tool_info.name, tool_info.namespace)
            if metadata:
                result.append(metadata)

        return result

    # ------------------------------------------------------------------ #
    # Deferred loading support
    # ------------------------------------------------------------------ #

    async def search_deferred_tools(
        self,
        query: str,
        tags: list[str] | None = None,
        limit: int = 5,
    ) -> list[ToolMetadata]:
        """Search deferred tools by query and tags."""
        candidates: list[tuple[float, ToolMetadata]] = []
        query_lower = query.lower()

        # Scan all deferred tools
        pattern = self._deferred_pattern()
        async for key in self._redis.scan_iter(match=pattern):
            parsed = self._parse_key_parts(key)
            if not parsed:
                continue

            namespace, tool_name = parsed

            # Skip if already loaded
            loaded_key = f"{namespace}.{tool_name}"
            if loaded_key in self._loaded_deferred_tools:
                continue

            # Get metadata
            metadata_bytes = await self._redis.get(key)
            if not metadata_bytes:
                continue
            metadata = self._deserialize_metadata(metadata_bytes)

            # Tag filtering
            if tags and not any(tag in metadata.tags for tag in tags):
                continue

            # Calculate relevance score
            score = self._compute_relevance_score(query_lower, metadata)
            if score > 0:
                candidates.append((score, metadata))

        # Sort by relevance score (descending) and return top N
        candidates.sort(reverse=True, key=lambda x: x[0])
        return [metadata for _, metadata in candidates[:limit]]

    def _compute_relevance_score(self, query_lower: str, metadata: ToolMetadata) -> float:
        """Compute relevance score for a tool based on query."""
        score = 0.0

        # Exact name match (highest priority)
        if query_lower == metadata.name.lower():
            score += 100.0

        # Name contains query
        if query_lower in metadata.name.lower():
            score += 50.0

        # Description match
        if metadata.description:
            desc_lower = metadata.description.lower()
            if query_lower in desc_lower:
                score += 30.0
            query_words = query_lower.split()
            for word in query_words:
                if word in desc_lower:
                    score += 5.0

        # Search keywords match
        for keyword in metadata.search_keywords:
            if query_lower in keyword.lower():
                score += 20.0
            query_words = query_lower.split()
            for word in query_words:
                if word in keyword.lower():
                    score += 3.0

        # Tag matches
        for tag in metadata.tags:
            if query_lower in tag.lower():
                score += 10.0

        return score

    async def load_deferred_tool(self, name: str, namespace: str = "default") -> Any:
        """Lazy load a deferred tool on-demand."""
        loaded_key = f"{namespace}.{name}"

        # Check if already loaded
        if loaded_key in self._loaded_deferred_tools:
            return await self.get_tool(name, namespace)

        # Get metadata from Redis
        deferred_key = self._deferred_key(namespace, name)
        metadata_bytes = await self._redis.get(deferred_key)
        if not metadata_bytes:
            raise ToolNotFoundError(
                tool_name=name,
                namespace=namespace,
                available_tools=[],
                available_namespaces=await self.list_namespaces(),
            )

        metadata = self._deserialize_metadata(metadata_bytes)

        async with self._lock:
            # Double-check after acquiring lock
            if loaded_key in self._loaded_deferred_tools:
                return await self.get_tool(name, namespace)

            # Load the tool
            if metadata.mcp_factory_params is not None:
                tool = await self._create_mcp_tool(metadata)
            elif metadata.import_path:
                tool = await self._import_tool(metadata.import_path)
            elif name in self._deferred_tools.get(namespace, {}):
                tool = self._deferred_tools[namespace][name]
            else:
                raise ValueError(f"Tool {loaded_key} is deferred but has no import_path or pre-instantiated tool")

            # Move from deferred to active
            tool_key = self._tool_key(namespace, name)
            await self._redis.set(tool_key, self._serialize_metadata(metadata))
            await self._redis.sadd(self._namespace_key(), namespace)

            self._tools.setdefault(namespace, {})[name] = tool
            self._loaded_deferred_tools.add(loaded_key)

            return tool

    async def _import_tool(self, import_path: str) -> Any:
        """Dynamically import a tool from an import path."""
        parts = import_path.rsplit(".", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid import path: {import_path}")

        module_path, class_name = parts

        import importlib

        module = importlib.import_module(module_path)
        tool_class = getattr(module, class_name)

        return tool_class

    async def _create_mcp_tool(self, metadata: ToolMetadata) -> Any:
        """Create an MCP tool instance from stored factory parameters."""
        from chuk_tool_processor.mcp.mcp_tool import MCPTool

        if not metadata.mcp_factory_params:
            raise ValueError(f"Tool {metadata.name} has no mcp_factory_params")

        stream_manager = self._stream_managers.get(metadata.mcp_factory_params.namespace)

        if not stream_manager:
            raise ValueError(
                f"No StreamManager found for namespace '{metadata.mcp_factory_params.namespace}'. "
                f"Call set_stream_manager() before loading deferred MCP tools."
            )

        return MCPTool(
            tool_name=metadata.mcp_factory_params.tool_name,
            stream_manager=stream_manager,
            default_timeout=metadata.mcp_factory_params.default_timeout,
            enable_resilience=metadata.mcp_factory_params.enable_resilience,
            recovery_config=metadata.mcp_factory_params.recovery_config,
        )

    def set_stream_manager(self, namespace: str, stream_manager: Any) -> None:
        """Set the StreamManager for a namespace (used for deferred MCP tools)."""
        self._stream_managers[namespace] = stream_manager

    async def get_active_tools(self, namespace: str | None = None) -> list[ToolInfo]:
        """Get only currently loaded (active) tools."""
        return await self.list_tools(namespace)

    async def get_deferred_tools(self, namespace: str | None = None) -> list[ToolInfo]:
        """Get list of deferred tools that haven't been loaded yet."""
        result: list[ToolInfo] = []
        pattern = self._deferred_pattern(namespace)

        async for key in self._redis.scan_iter(match=pattern):
            parsed = self._parse_key_parts(key)
            if parsed:
                ns, name = parsed
                loaded_key = f"{ns}.{name}"
                if loaded_key not in self._loaded_deferred_tools:
                    result.append(ToolInfo(namespace=ns, name=name))

        return result

    # ------------------------------------------------------------------ #
    # Cleanup
    # ------------------------------------------------------------------ #

    async def clear(self) -> None:
        """Clear all tool registrations (useful for testing)."""
        async with self._lock:
            # Delete all keys with our prefix
            async for key in self._redis.scan_iter(match=f"{self._config.key_prefix}:*"):
                await self._redis.delete(key)

            # Clear local caches
            self._tools.clear()
            self._deferred_tools.clear()
            self._loaded_deferred_tools.clear()
            self._stream_managers.clear()


async def create_redis_registry(
    redis_url: str = "redis://localhost:6379/0",
    key_prefix: str = "chuk",
    local_cache_ttl: float = 60.0,
) -> RedisToolRegistry:
    """
    Factory function to create a Redis registry.

    Args:
        redis_url: Redis connection URL (default: redis://localhost:6379/0)
        key_prefix: Prefix for all Redis keys (default: "chuk")
        local_cache_ttl: TTL in seconds for local tool cache (default: 60s)

    Returns:
        Configured RedisToolRegistry instance

    Raises:
        ImportError: If the redis package is not installed

    Example:
        >>> registry = await create_redis_registry("redis://localhost:6379/0")
        >>> await registry.register_tool(MyTool, name="my_tool")
    """
    try:
        from redis.asyncio import Redis
    except ImportError as e:
        raise ImportError(
            "The redis package is required for RedisToolRegistry. Install it with: pip install redis[hiredis]"
        ) from e

    config = RedisConfig(
        redis_url=redis_url,
        key_prefix=key_prefix,
        local_cache_ttl=local_cache_ttl,
    )

    redis_client = Redis.from_url(redis_url, decode_responses=False)
    return RedisToolRegistry(redis_client=redis_client, config=config)

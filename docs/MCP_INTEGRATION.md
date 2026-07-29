# MCP Integration Guide

Connect to remote tool servers using the [Model Context Protocol](https://modelcontextprotocol.io).

## Table of Contents

- [Overview](#overview)
- [Transport Comparison](#transport-comparison)
- [HTTP Streamable](#http-streamable-recommended-for-cloud)
- [STDIO](#stdio-best-for-local-tools)
- [SSE](#sse-legacy-support)
- [Middleware Stack](#middleware-stack)
- [Real-World Examples](#real-world-examples)
  - [Notion Integration](#notion-integration-with-oauth)
  - [SQLite Database](#local-sqlite-database-access)
  - [Echo Server](#simple-stdio-echo-server)
- [OAuth Token Refresh](#oauth-token-refresh)
- [MCPConfig](#mcpconfig-clean-configuration)

---

## Overview

CHUK Tool Processor supports three MCP transport mechanisms:

```
    LLM Output
        ↓
  Tool Processor
        ↓
 ┌──────────────┬────────────────────┐
 │ Local Tools  │ Remote Tools (MCP) │
 └──────────────┴────────────────────┘
```

**Relationship with [chuk-mcp](https://pypi.org/project/chuk-mcp/):**
- `chuk-mcp` is a low-level MCP protocol client (handles transports, protocol negotiation)
- `chuk-tool-processor` wraps `chuk-mcp` to integrate external tools into your execution pipeline
- You can use local tools, remote MCP tools, or both in the same processor

---

## Transport Comparison

| Transport | Use Case | Real Examples |
|-----------|----------|---------------|
| **HTTP Streamable** | Cloud APIs, SaaS services | Notion (`mcp.notion.com`) |
| **STDIO** | Local tools, databases | SQLite (`mcp-server-sqlite`), Echo (`chuk-mcp-echo`) |
| **SSE** | Legacy cloud services | Atlassian (`mcp.atlassian.com`) |

### When to Use Each

| Scenario | Recommended Transport |
|----------|----------------------|
| Cloud SaaS with OAuth | HTTP Streamable |
| Local database access | STDIO |
| File system operations | STDIO |
| Legacy MCP servers | SSE |
| Development/testing | STDIO (Echo server) |

---

## HTTP Streamable (Recommended for Cloud)

Modern HTTP streaming transport for cloud-based MCP servers like Notion.

**Use for:** Cloud SaaS services (OAuth, long-running streams, resilient reconnects)

```python
from chuk_tool_processor.mcp import setup_mcp_http_streamable

# Connect to Notion MCP with OAuth
servers = [
    {
        "name": "notion",
        "url": "https://mcp.notion.com/mcp",
        "headers": {"Authorization": f"Bearer {access_token}"}
    }
]

processor, manager = await setup_mcp_http_streamable(
    servers=servers,
    namespace="notion",
    initialization_timeout=120.0,  # Some services need time to initialize
    enable_caching=True,
    enable_retries=True
)

# Use Notion tools through MCP
results = await processor.process(
    '<tool name="notion.search_pages" args=\'{"query": "meeting notes"}\'/>'
)
```

### HTTP Streamable Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `servers` | required | List of server configs with `name`, `url`, `headers` |
| `namespace` | `"mcp"` | Prefix for tool names |
| `initialization_timeout` | `30.0` | Timeout for server initialization |
| `enable_caching` | `False` | Enable result caching |
| `enable_retries` | `False` | Enable automatic retries |
| `oauth_refresh_callback` | `None` | Callback for token refresh |

---

## STDIO (Best for Local Tools)

Run local MCP servers as subprocesses. Great for databases, file systems, and local tools.

**Use for:** Local/embedded tools and databases (SQLite, file systems, local services)

```python
from chuk_tool_processor.mcp import setup_mcp_stdio
import json

# Configure SQLite MCP server
config = {
    "mcpServers": {
        "sqlite": {
            "command": "uvx",
            "args": ["mcp-server-sqlite", "--db-path", "/path/to/database.db"],
            "env": {"MCP_SERVER_NAME": "sqlite"},
            "transport": "stdio"
        }
    }
}

# Save config to file
with open("mcp_config.json", "w") as f:
    json.dump(config, f)

# Connect to local SQLite server
processor, manager = await setup_mcp_stdio(
    config_file="mcp_config.json",
    servers=["sqlite"],
    namespace="db",
    initialization_timeout=120.0  # First run downloads packages
)

# Query your local database via MCP
results = await processor.process(
    '<tool name="db.query" args=\'{"sql": "SELECT * FROM users LIMIT 10"}\'/>'
)
```

### STDIO Config File Format

```json
{
  "mcpServers": {
    "server_name": {
      "command": "uvx",
      "args": ["package-name", "--flag", "value"],
      "env": {"ENV_VAR": "value"},
      "transport": "stdio"
    }
  }
}
```

### STDIO Options

| Parameter | Default | Description |
|-----------|---------|-------------|
| `config_file` | required | Path to MCP config JSON |
| `servers` | required | List of server names to connect |
| `namespace` | `"mcp"` | Prefix for tool names |
| `initialization_timeout` | `30.0` | Timeout for server initialization |

---

## SSE (Legacy Support)

For backward compatibility with older MCP servers using Server-Sent Events.

**Use for:** Legacy compatibility only. Prefer HTTP Streamable for new integrations.

```python
from chuk_tool_processor.mcp import setup_mcp_sse

# Connect to Atlassian with OAuth via SSE
servers = [
    {
        "name": "atlassian",
        "url": "https://mcp.atlassian.com/v1/sse",
        "headers": {"Authorization": f"Bearer {access_token}"}
    }
]

processor, manager = await setup_mcp_sse(
    servers=servers,
    namespace="atlassian",
    initialization_timeout=120.0
)
```

---

## Tool Name Collisions

When you connect several MCP servers, two of them can advertise the **same tool
name** (common short names like `read_file`, `search`, `query` collide easily). A
bare tool name is not a trust boundary, so `StreamManager` uses a **first-wins**
policy: the first server to advertise a name owns it for default (unpinned)
routing, and a later server advertising the same name is **ignored for routing
and logged with a warning** — it can never silently take over calls that were
going to an already-connected server.

```python
sm = StreamManager()
await sm.initialize(servers=[...])   # trusted-fs, then community-server

# trusted-fs registered read_file first, so it owns the name:
await sm.call_tool("read_file", {"path": "..."})            # -> trusted-fs
# reach the shadowed copy deliberately by pinning the server:
await sm.call_tool("read_file", {"path": "..."}, server_name="community-server")
```

Inspect collisions so you can namespace or reconfigure:

```python
sm.get_server_for_tool("read_file")    # "trusted-fs"  (owns default routing)
sm.get_servers_for_tool("read_file")   # ["trusted-fs", "community-server"]
sm.get_tool_collisions()               # {"read_file": ["trusted-fs", "community-server"]}
```

To avoid collisions entirely, give each server a distinct `namespace=` in the
`setup_mcp_*` wrappers (e.g. `notion.search`, `db.query`) so names never clash.

## Middleware Stack

The `MiddlewareStack` provides production-grade resilience for MCP tool calls. It wraps MCP connections with configurable retry, circuit breaker, and rate limiting layers.

### Architecture

```
Tool Call
    ↓
┌─────────────────────────────────┐
│        Middleware Stack          │
│  (Applied in this order)         │
│   • Rate Limiting (outermost)    │
│   • Circuit Breaker              │
│   • Retry (innermost)            │
│   • Transport                    │
└─────────────────────────────────┘
    ↓
MCP Server
```

### Basic Usage

```python
from chuk_tool_processor.mcp.middleware import (
    MiddlewareConfig,
    MiddlewareStack,
    RetrySettings,
    CircuitBreakerSettings,
    RateLimitSettings,
)
from chuk_tool_processor.mcp import StreamManager

# Configure middleware layers
config = MiddlewareConfig(
    retry=RetrySettings(
        enabled=True,
        max_retries=3,
        base_delay=1.0,
        max_delay=30.0,
        jitter=True,
    ),
    circuit_breaker=CircuitBreakerSettings(
        enabled=True,
        failure_threshold=5,
        reset_timeout=60.0,
    ),
    rate_limiting=RateLimitSettings(
        enabled=True,
        global_limit=100,
        period=60.0,
    ),
)

# Create middleware stack around a StreamManager
stream_manager = StreamManager()
await stream_manager.initialize(servers)

middleware = MiddlewareStack(stream_manager, config=config)

# Execute tools through middleware
result = await middleware.call_tool(
    tool_name="notion.search_pages",
    arguments={"query": "docs"},
    timeout=30.0,
)

if result.success:
    print(f"Result: {result.result}")
else:
    print(f"Error: {result.error}")
```

### Middleware Configuration

#### RetrySettings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `True` | Enable retry middleware |
| `max_retries` | `int` | `3` | Maximum retry attempts |
| `base_delay` | `float` | `1.0` | Initial delay between retries (seconds) |
| `max_delay` | `float` | `30.0` | Maximum delay between retries |
| `jitter` | `bool` | `True` | Add randomness to delays |
| `retry_on_errors` | `list[str]` | Transport errors | Error patterns that trigger retry |
| `skip_on_errors` | `list[str]` | Auth errors | Error patterns that skip retry |

#### CircuitBreakerSettings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `True` | Enable circuit breaker |
| `failure_threshold` | `int` | `5` | Failures before opening circuit |
| `success_threshold` | `int` | `2` | Successes to close circuit |
| `reset_timeout` | `float` | `60.0` | Seconds before half-open state |
| `half_open_max_calls` | `int` | `1` | Calls allowed in half-open state |

#### RateLimitSettings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `False` | Enable rate limiting |
| `global_limit` | `int` | `100` | Max requests per period |
| `period` | `float` | `60.0` | Time window in seconds |
| `per_tool_limits` | `dict` | `{}` | Per-tool limits as `{tool: (limit, period)}` |

### Monitoring Middleware Status

```python
# Get current middleware status
status = middleware.get_status()

# Check retry status
if status.retry:
    print(f"Retry enabled: max_retries={status.retry.max_retries}")

# Check circuit breaker states per tool
if status.circuit_breaker:
    for tool, state in status.circuit_breaker.tool_states.items():
        print(f"{tool}: state={state.state}, failures={state.failure_count}")

# Check rate limiting
if status.rate_limiting:
    print(f"Rate limit: {status.rate_limiting.global_limit}/{status.rate_limiting.period}s")
```

### Error Classification

The middleware automatically classifies errors:

**Retryable Errors** (will trigger retry):
- `Transport not initialized`
- `connection` errors
- `timeout` errors
- `refused` connections
- `reset` connections
- `closed` connections

**Non-Retryable Errors** (fail immediately):
- `oauth` errors
- `unauthorized` errors
- `authentication` errors
- `invalid_grant` errors
- `No server found` errors

### ToolExecutionResult

Each middleware call returns a `ToolExecutionResult`:

```python
result = await middleware.call_tool("tool_name", {"arg": "value"})

# Fields available:
result.success       # bool: True if execution succeeded
result.result        # Any: The tool result (if success)
result.error         # str | None: Error message (if failed)
result.tool_name     # str: Name of the tool called
result.duration_ms   # float: Execution time in milliseconds
result.attempts      # int: Number of attempts (with retries)
result.from_cache    # bool: True if result was cached
```

---

## Real-World Examples

### Notion Integration with OAuth

Complete OAuth flow connecting to Notion's MCP server:

```python
from chuk_tool_processor.mcp import setup_mcp_http_streamable

# After completing OAuth flow (see examples/04_mcp_integration/notion_oauth.py)
processor, manager = await setup_mcp_http_streamable(
    servers=[{
        "name": "notion",
        "url": "https://mcp.notion.com/mcp",
        "headers": {"Authorization": f"Bearer {access_token}"}
    }],
    namespace="notion",
    initialization_timeout=120.0
)

# Get available Notion tools
tools = manager.get_all_tools()
print(f"Available tools: {[t['name'] for t in tools]}")

# Use Notion tools in your LLM workflow
results = await processor.process(
    '<tool name="notion.search_pages" args=\'{"query": "Q4 planning"}\'/>'
)
```

### Local SQLite Database Access

Run SQLite MCP server locally for database operations:

```python
from chuk_tool_processor.mcp import setup_mcp_stdio
import json

# Configure SQLite server
config = {
    "mcpServers": {
        "sqlite": {
            "command": "uvx",
            "args": ["mcp-server-sqlite", "--db-path", "./data/app.db"],
            "transport": "stdio"
        }
    }
}

with open("mcp_config.json", "w") as f:
    json.dump(config, f)

# Connect to local database
processor, manager = await setup_mcp_stdio(
    config_file="mcp_config.json",
    servers=["sqlite"],
    namespace="db",
    initialization_timeout=120.0  # First run downloads mcp-server-sqlite
)

# Query your database via LLM
results = await processor.process(
    '<tool name="db.query" args=\'{"sql": "SELECT COUNT(*) FROM users"}\'/>'
)
```

### Simple STDIO Echo Server

Minimal example for testing STDIO transport:

```python
from chuk_tool_processor.mcp import setup_mcp_stdio
import json

# Configure echo server (great for testing)
config = {
    "mcpServers": {
        "echo": {
            "command": "uvx",
            "args": ["chuk-mcp-echo", "stdio"],
            "transport": "stdio"
        }
    }
}

with open("echo_config.json", "w") as f:
    json.dump(config, f)

processor, manager = await setup_mcp_stdio(
    config_file="echo_config.json",
    servers=["echo"],
    namespace="echo",
    initialization_timeout=60.0
)

# Test echo functionality
results = await processor.process(
    '<tool name="echo.echo" args=\'{"message": "Hello MCP!"}\'/>'
)
```

---

## OAuth Token Refresh

For MCP servers that use OAuth authentication, CHUK Tool Processor supports automatic token refresh when access tokens expire.

### How It Works

1. When a tool call receives an OAuth-related error (e.g., "invalid_token", "expired token")
2. The processor automatically calls your refresh callback
3. Updates the authentication headers with the new token
4. Retries the tool call with fresh credentials

### Setup with HTTP Streamable

```python
from chuk_tool_processor.mcp import setup_mcp_http_streamable

async def refresh_oauth_token():
    """Called automatically when tokens expire."""
    new_token = await your_refresh_logic()
    return {"Authorization": f"Bearer {new_token}"}

processor, manager = await setup_mcp_http_streamable(
    servers=[{
        "name": "notion",
        "url": "https://mcp.notion.com/mcp",
        "headers": {"Authorization": f"Bearer {initial_access_token}"}
    }],
    namespace="notion",
    oauth_refresh_callback=refresh_oauth_token  # Enable auto-refresh
)
```

### Setup with SSE

```python
from chuk_tool_processor.mcp import setup_mcp_sse

async def refresh_oauth_token():
    """Refresh expired OAuth token."""
    new_access_token = await exchange_refresh_token(refresh_token)
    return {"Authorization": f"Bearer {new_access_token}"}

processor, manager = await setup_mcp_sse(
    servers=[{
        "name": "atlassian",
        "url": "https://mcp.atlassian.com/v1/sse",
        "headers": {"Authorization": f"Bearer {initial_token}"}
    }],
    namespace="atlassian",
    oauth_refresh_callback=refresh_oauth_token
)
```

### OAuth Errors Detected

The following error patterns trigger automatic refresh:
- `invalid_token`
- `expired token`
- `OAuth validation failed`
- `unauthorized`
- `token expired`
- `authentication failed`
- `invalid access token`

### Important Notes

- The refresh callback must return a dict with an `Authorization` key
- If refresh fails or returns invalid headers, the original error is returned
- Token refresh is attempted only once per tool call (no infinite retry loops)
- After successful refresh, the updated headers are used for all subsequent calls

---

## MCPConfig (Clean Configuration)

Use Pydantic models for clean, type-safe configuration:

```python
from chuk_tool_processor.mcp import setup_mcp_stdio, MCPConfig, MCPServerConfig

# Clean Pydantic config object instead of dict
processor, manager = await setup_mcp_stdio(
    config=MCPConfig(
        servers=[
            MCPServerConfig(
                name="echo",
                command="uvx",
                args=["mcp-echo"]
            )
        ],
        namespace="tools",
        enable_caching=True,
        cache_ttl=600,
    )
)
```

### MCPConfig Options

| Field | Type | Description |
|-------|------|-------------|
| `servers` | `list[MCPServerConfig]` | Server configurations |
| `namespace` | `str` | Tool name prefix |
| `enable_caching` | `bool` | Enable result caching |
| `cache_ttl` | `int` | Cache TTL in seconds |
| `enable_retries` | `bool` | Enable automatic retries |
| `max_retries` | `int` | Maximum retry attempts |
| `initialization_timeout` | `float` | Server init timeout |

### MCPServerConfig Options

| Field | Type | Description |
|-------|------|-------------|
| `name` | `str` | Server identifier |
| `command` | `str` | Command to run (STDIO) |
| `args` | `list[str]` | Command arguments (STDIO) |
| `env` | `dict[str, str]` | Environment variables (STDIO) |
| `url` | `str` | Server URL (HTTP/SSE) |
| `headers` | `dict[str, str]` | HTTP headers (HTTP/SSE) |

---

## Mixing Local and Remote Tools

You can use local tools and MCP tools in the same processor:

```python
from chuk_tool_processor import ToolProcessor, register_tool, initialize
from chuk_tool_processor.mcp import setup_mcp_http_streamable

# 1. Register local tools
@register_tool(name="local_calculator")
class Calculator:
    async def execute(self, a: int, b: int) -> int:
        return a + b

await initialize()

# 2. Add remote MCP tools
processor, manager = await setup_mcp_http_streamable(
    servers=[{
        "name": "notion",
        "url": "https://mcp.notion.com/mcp",
        "headers": {"Authorization": f"Bearer {token}"}
    }],
    namespace="notion"
)

# 3. Use both in the same call
async with processor:
    results = await processor.process('''
        <tool name="local_calculator" args='{"a": 5, "b": 3}'/>
        <tool name="notion.search_pages" args='{"query": "docs"}'/>
    ''')
```

---

## Related Documentation

- [CORE_CONCEPTS.md](CORE_CONCEPTS.md) - MCP architecture overview
- [ADVANCED_TOPICS.md](ADVANCED_TOPICS.md) - Deferred loading for MCP tools
- [CONFIGURATION.md](CONFIGURATION.md) - All configuration options
- [examples/04_mcp_integration/](../examples/04_mcp_integration/) - Complete working examples

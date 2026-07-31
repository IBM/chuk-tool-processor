# Core Concepts

This guide explains the fundamental building blocks of CHUK Tool Processor.

## What CHUK Tool Processor Is (and Isn't)

CHUK Tool Processor is a **tool execution runtime** for AI systems. It deliberately does not:
- Plan workflows or decide which tools to call
- Manage conversation history or memory
- Implement agent loops or reasoning chains

It **does**:
- Execute tool calls reliably, with timeouts, retries, caching, and rate limits
- Parse tool calls from any LLM format (Anthropic XML, OpenAI tool_calls, JSON)
- Run tools locally, in isolated subprocesses, or remotely via MCP
- Provide observability (tracing, metrics) for tool execution

This separation is intentional. Higher-level planners (your agent, LangChain, LlamaIndex) decide *which* tools to call. CHUK Tool Processor makes sure those calls **actually succeed**.

---

## Table of Contents

- [Tool Registry](#1-tool-registry)
- [Execution Strategies](#2-execution-strategies)
- [Execution Wrappers (Middleware)](#3-execution-wrappers-middleware)
- [Input Parsers (Plugins)](#4-input-parsers-plugins)
- [MCP Integration](#5-mcp-integration-external-tools)

---

## 1. Tool Registry

The **registry** is where you register tools for execution. Tools can be:

- **Simple classes** with an `async execute()` method
- **ValidatedTool** subclasses with Pydantic validation
- **StreamingTool** for real-time incremental results
- **Functions** registered via `register_fn_tool()`

> **Note:** The registry is global by default, but you can create isolated registries with `create_registry()` for multi-tenant apps. See [PRODUCTION_PATTERNS.md](PRODUCTION_PATTERNS.md#scoped-registries-multi-tenant-isolation).

### Basic Tool Registration

```python
from chuk_tool_processor import register_tool

@register_tool(name="calculator")
class Calculator:
    async def execute(self, a: float, b: float, operation: str) -> dict:
        ops = {"add": a + b, "multiply": a * b, "subtract": a - b}
        return {"result": ops.get(operation, 0)}
```

### Validated Tool with Pydantic

```python
from chuk_tool_processor import register_tool
from chuk_tool_processor.models.validated_tool import ValidatedTool
from pydantic import BaseModel, Field

@register_tool(name="weather")
class WeatherTool(ValidatedTool):
    class Arguments(BaseModel):
        location: str = Field(..., description="City name")
        units: str = Field("celsius", description="Temperature units")

    class Result(BaseModel):
        temperature: float
        conditions: str

    async def _execute(self, location: str, units: str) -> Result:
        # Your weather API logic here
        return self.Result(temperature=22.5, conditions="Sunny")
```

### Short Decorator Syntax

```python
from chuk_tool_processor import tool

@tool(name="search")  # Shorter alternative to @register_tool
class SearchTool:
    async def execute(self, query: str) -> dict:
        return {"results": [f"Found: {query}"]}
```

---

## 2. Execution Strategies

**Strategies** determine *how* tools run:

| Strategy | Use Case | Trade-offs |
|----------|----------|------------|
| **InProcessStrategy** | Fast, trusted tools | Speed ✅, Crash isolation ❌ |
| **IsolatedStrategy** | Tools that may crash/hang | Crash isolation ✅, Speed ❌ |

> `IsolatedStrategy` gives **crash/fault isolation** (separate worker processes),
> **not** a security boundary — workers are the same user and results cross via
> pickle. For untrusted or LLM-generated *code*, use
> [`IsolatedCodeRunner`](./isolated_execution.md), not a strategy.

### Parallel Execution

Both strategies execute tools **concurrently by default**. Results return in **completion order** (faster tools return first), not submission order. Use `ToolResult.tool` to match results to original calls.

### Using InProcessStrategy (Default)

```python
from chuk_tool_processor import ToolProcessor

async with ToolProcessor() as processor:
    # Tools run in the same process (fast)
    results = await processor.process(tool_calls)
```

### Using IsolatedStrategy

```python
from chuk_tool_processor import ToolProcessor, IsolatedStrategy, get_default_registry

async def main():
    registry = await get_default_registry()
    processor = ToolProcessor(
        strategy=IsolatedStrategy(
            registry=registry,
            max_workers=4,
            default_timeout=30.0
        )
    )
    async with processor:
        # Each tool call runs in a separate worker process (crash isolation)
        results = await processor.process(tool_calls)
```

> **Note:** `IsolatedStrategy` is an alias of `SubprocessStrategy` for backwards compatibility. It provides crash/fault isolation for tool dispatch, **not** a security sandbox; for untrusted/LLM code use [`IsolatedCodeRunner`](./isolated_execution.md).

### When to Use Each Strategy

| Scenario | Recommended |
|----------|-------------|
| Trusted internal tools | `InProcessStrategy` |
| Performance-critical path | `InProcessStrategy` |
| Tools that might crash or hang | `IsolatedStrategy` (crash isolation) |
| Untrusted / third-party / LLM-generated **code** | [`IsolatedCodeRunner`](./isolated_execution.md) — real isolation, not a strategy |

---

## 3. Execution Wrappers (Middleware)

**Wrappers** add production features as composable layers:

```python
processor = ToolProcessor(
    enable_caching=True,         # Cache expensive calls
    cache_ttl=600,               # 10 minutes
    enable_rate_limiting=True,   # Prevent abuse
    global_rate_limit=100,       # 100 req/min globally
    enable_retries=True,         # Auto-retry failures
    max_retries=3                # Up to 3 attempts
)
```

### Execution Order

The processor stacks wrappers automatically:

```
Request → Cache → Rate Limit → Retry → Strategy → Tool
                                              ↓
Response ← Cache ← Rate Limit ← Retry ← Strategy ← Tool
```

### Available Wrappers

| Wrapper | Purpose | Key Options |
|---------|---------|-------------|
| **Cache** | Avoid redundant calls | `cache_ttl`, `enable_caching` |
| **Rate Limit** | Prevent abuse | `global_rate_limit`, `tool_rate_limits` |
| **Retry** | Handle transient failures | `max_retries`, `retry_delay` |
| **Circuit Breaker** | Prevent cascading failures | `enable_circuit_breaker` |

See [CONFIGURATION.md](CONFIGURATION.md) for all options.

---

## 4. Input Parsers (Plugins)

**Parsers** extract tool calls from various LLM output formats. All formats work automatically—no configuration needed.

### XML Tags (Anthropic-style)

```xml
<tool name="search" args='{"query": "Python"}'/>
```

### OpenAI `tool_calls` (JSON)

```json
{
  "tool_calls": [
    {
      "type": "function",
      "function": {
        "name": "search",
        "arguments": "{\"query\": \"Python\"}"
      }
    }
  ]
}
```

### Direct JSON (array of calls)

```json
[
  { "tool": "search", "arguments": { "query": "Python" } }
]
```

### Input Format Compatibility

| Format | Example | Use Case |
|--------|---------|----------|
| **XML Tool Tag** | `<tool name="search" args='{"q":"Python"}'/>`| Anthropic Claude, XML-based LLMs |
| **OpenAI tool_calls** | JSON object (above) | OpenAI GPT-4 function calling |
| **Direct JSON** | `[{"tool": "search", "arguments": {"q": "Python"}}]` | Generic API integrations |
| **Single dict** | `{"tool": "search", "arguments": {"q": "Python"}}` | Programmatic calls |

---

## 5. MCP Integration (External Tools)

Connect to **remote tool servers** using the [Model Context Protocol](https://modelcontextprotocol.io). CHUK Tool Processor supports three transport mechanisms for different use cases.

### Transport Comparison

| Transport | Use Case | Real Examples |
|-----------|----------|---------------|
| **HTTP Streamable** | Cloud APIs, SaaS services | Notion (`mcp.notion.com`) |
| **STDIO** | Local tools, databases | SQLite (`mcp-server-sqlite`), Echo (`chuk-mcp-echo`) |
| **SSE** | Legacy cloud services | Atlassian (`mcp.atlassian.com`) |

### HTTP Streamable (Recommended for Cloud)

Modern HTTP streaming transport for cloud-based MCP servers:

```python
from chuk_tool_processor.mcp import setup_mcp_http_streamable

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
    initialization_timeout=120.0,
    enable_caching=True,
    enable_retries=True
)

# Use Notion tools through MCP
results = await processor.process(
    '<tool name="notion.search_pages" args=\'{"query": "meeting notes"}\'/>'
)
```

### STDIO (Best for Local Tools)

For running local MCP servers as subprocesses:

```python
from chuk_tool_processor.mcp import setup_mcp_stdio

processor, manager = await setup_mcp_stdio(
    config_file="mcp_config.json",
    servers=["sqlite"],
    namespace="db",
    initialization_timeout=120.0
)

results = await processor.process(
    '<tool name="db.query" args=\'{"sql": "SELECT * FROM users LIMIT 10"}\'/>'
)
```

### SSE (Legacy Support)

For backward compatibility with older MCP servers:

```python
from chuk_tool_processor.mcp import setup_mcp_sse

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

### Architecture with MCP

```
    LLM Output
        ↓
  Tool Processor
        ↓
 ┌──────────────┬────────────────────┐
 │ Local Tools  │ Remote Tools (MCP) │
 └──────────────┴────────────────────┘
```

**Relationship with [chuk-mcp-rs](https://github.com/IBM/chuk-mcp-rs):**
- `chuk-mcp-rs` is the low-level MCP protocol client — a Rust core (transports, protocol negotiation) exposed to Python as the `chuk_mcp_rs` extension
- `chuk-tool-processor` builds directly on `chuk-mcp-rs` to integrate external tools into your execution pipeline
- STDIO servers are connected with era-aware negotiation, so both legacy (`initialize` handshake) and modern (`2026-07-28` `server/discover`) MCP servers work transparently
- You can use local tools, remote MCP tools, or both in the same processor

For detailed MCP examples, see [MCP_INTEGRATION.md](MCP_INTEGRATION.md).

---

## Related Documentation

- [GETTING_STARTED.md](GETTING_STARTED.md) - Step-by-step tutorials
- [PRODUCTION_PATTERNS.md](PRODUCTION_PATTERNS.md) - Production-grade patterns
- [CONFIGURATION.md](CONFIGURATION.md) - All configuration options
- [MCP_INTEGRATION.md](MCP_INTEGRATION.md) - Detailed MCP guide

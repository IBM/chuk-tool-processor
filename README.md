# CHUK Tool Processor — A Tool Execution Runtime for AI Systems

[![PyPI](https://img.shields.io/pypi/v/chuk-tool-processor.svg)](https://pypi.org/project/chuk-tool-processor/)
[![Python](https://img.shields.io/pypi/pyversions/chuk-tool-processor.svg)](https://pypi.org/project/chuk-tool-processor/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Type Checked](https://img.shields.io/badge/type%20checked-PEP%20561-blue.svg)](https://www.python.org/dev/peps/pep-0561/)
[![Wheels](https://img.shields.io/badge/wheels-macOS%20%7C%20Linux%20%7C%20Windows-blue.svg)](https://pypi.org/project/chuk-tool-processor/)
[![OpenTelemetry](https://img.shields.io/badge/observability-OpenTelemetry%20%7C%20Prometheus-blue.svg)](docs/OBSERVABILITY.md)

**Reliable tool execution for LLMs — timeouts, retries, caching, rate limits, circuit breakers, and MCP integration — in one composable layer.**

---

## The Missing Runtime Layer

LLMs are good at *deciding which tools to call*. The hard part is **executing** those tools reliably.

**CHUK Tool Processor** is a **tool execution runtime** — it doesn't plan workflows or decide which tools to call. It executes tool calls reliably, under constraints, as directed by higher-level planners (your agent, LangChain, LlamaIndex, or a custom orchestrator).

**What it does:**
- Parses tool calls from any model (Anthropic XML, OpenAI `tool_calls`, JSON)
- Executes them with **timeouts, retries, caching, rate limits, circuit breaker, observability**
- Runs tools locally, in **isolated subprocesses**, or **remote via MCP**

Works with OpenAI, Anthropic, local models (Ollama/MLX/vLLM), and any framework.

---

## Architecture

```
    LLM Output
        ↓
CHUK Tool Processor
        ↓
 ┌──────────────┬────────────────────┐
 │ Local Tools  │ Remote Tools (MCP) │
 └──────────────┴────────────────────┘
```

**How it works internally:**

```
    LLM Output
        ↓
Parsers (XML / OpenAI / JSON)
        ↓
┌─────────────────────────────┐
│   Execution Middleware      │
│  (Applied in this order)    │
│   • Cache                   │
│   • Rate Limit              │
│   • Retry (with backoff)    │
│   • Circuit Breaker         │
│   • Bulkhead                │
└─────────────────────────────┘
        ↓
   Execution Strategy
   ┌──────────────────────┐
   │ • InProcess          │  ← Fast, trusted
   │ • Isolated/Subprocess│  ← Crash isolation
   │ • Remote via MCP     │  ← Distributed
   └──────────────────────┘
```

---

## Quick Start

### Installation

```bash
pip install chuk-tool-processor

# Or with uv (recommended)
uv pip install chuk-tool-processor
```

### 60-Second Example

```python
import asyncio
from chuk_tool_processor import ToolProcessor, create_registry

class Calculator:
    async def execute(self, operation: str, a: float, b: float) -> dict:
        ops = {"add": a + b, "multiply": a * b, "subtract": a - b}
        return {"result": ops.get(operation, 0)}

async def main():
    registry = create_registry()
    await registry.register_tool(Calculator, name="math.calculator")  # Dotted name → namespace="math"

    async with ToolProcessor(registry=registry, enable_caching=True, enable_retries=True) as p:
        # Works with OpenAI, Anthropic, or JSON formats
        result = await p.process('<tool name="math.calculator" args=\'{"operation": "multiply", "a": 15, "b": 23}\'/>')
        print(result[0].result)  # {'result': 345}

asyncio.run(main())
```

**That's it.** You now have production-ready tool execution with timeouts, retries, and caching.

### Dotted Names for Namespacing

Dotted names are auto-parsed into namespace and tool name:

```python
# These are equivalent:
await registry.register_tool(FetchUser, name="web.fetch_user")           # Auto-parsed
await registry.register_tool(FetchUser, name="fetch_user", namespace="web")  # Explicit

# Call using the full dotted name
result = await processor.process([{"tool": "web.fetch_user", "arguments": {"user_id": "123"}}])
```

### Works with Any LLM Format

```python
# Anthropic XML format
anthropic_output = '<tool name="search" args=\'{"query": "Python"}\'/>'

# OpenAI tool_calls format
openai_output = {
    "tool_calls": [{
        "type": "function",
        "function": {"name": "search", "arguments": '{"query": "Python"}'}
    }]
}

# Direct JSON
json_output = [{"tool": "search", "arguments": {"query": "Python"}}]

# All work identically
results = await processor.process(anthropic_output)
results = await processor.process(openai_output)
results = await processor.process(json_output)
```

---

## Key Features

### Production Reliability

| Feature | Description |
|---------|-------------|
| **Timeouts** | Every tool execution has proper timeout handling |
| **Retries** | Automatic retry with exponential backoff and jitter |
| **Rate Limiting** | Global and per-tool rate limits with sliding windows |
| **Caching** | Result caching with TTL and SHA256-based idempotency keys |
| **Circuit Breakers** | Prevent cascading failures with automatic recovery |
| **Structured Errors** | Machine-readable error categories with retry hints for planners |

### Multi-Tenant & Isolation

| Feature | Description |
|---------|-------------|
| **Bulkheads** | Per-tool/namespace concurrency limits to prevent resource starvation |
| **Pattern Bulkheads** | Glob patterns like `"db.*": 3` for grouped concurrency limits |
| **Scoped Registries** | Isolated registries for multi-tenant apps and testing |
| **ExecutionContext** | Request-scoped metadata propagation (user, tenant, tracing, deadlines) |
| **Isolated Strategy** | Subprocess tool execution for crash/fault isolation (zero crash blast radius). Not a security boundary — for untrusted/LLM *code*, see [`IsolatedCodeRunner`](docs/isolated_execution.md) |
| **Redis Registry** | Distributed tool registry for multi-process/multi-machine deployments |

### Advanced Scheduling

| Feature | Description |
|---------|-------------|
| **Return Order** | Choose completion order (fast first) or submission order (deterministic) |
| **SchedulerPolicy** | DAG-based scheduling with dependencies, deadlines, pool limits |
| **GreedyDagScheduler** | Built-in scheduler with topological sort and deadline-aware skipping |

### Runtime Guards (Constitution Layer)

| Guard | Description |
|-------|-------------|
| **SchemaStrictnessGuard** | Validates arguments against JSON schemas, optional type coercion |
| **SensitiveDataGuard** | Detects and blocks/redacts secrets (API keys, JWTs, private keys) |
| **NetworkPolicyGuard** | SSRF defense — blocks private IPs, metadata endpoints, enforces HTTPS |
| **SideEffectGuard** | Labels tools as read_only/write/destructive, enforces policies |
| **ConcurrencyGuard** | Limits simultaneous in-flight calls (global, per-tool, per-namespace) |
| **TimeoutBudgetGuard** | Enforces wall-clock time budgets with soft/hard limits |
| **OutputSizeGuard** | Prevents pathological payloads from blowing up context |
| **RetrySafetyGuard** | Guards retry behavior (backoff, idempotency keys, non-retryable errors) |
| **ProvenanceGuard** | Tracks output attribution and lineage |
| **PlanShapeGuard** | Detects pathological patterns (fan-out explosions, long chains) |
| **SaturationGuard** | Detects degenerate statistical outputs (extreme Z-scores, saturated CDFs) |

### Dynamic Tool Discovery

| Feature | Description |
|---------|-------------|
| **Intelligent Search** | Natural language queries find tools ("gaussian" → "normal_cdf") |
| **Synonym Expansion** | Built-in synonyms for math, statistics, file ops, networking |
| **Fuzzy Matching** | Typo tolerance ("multipley" finds "multiply") |
| **Session Boosting** | Recently used tools rank higher in search results |
| **Dynamic Provider** | Base class for LLM-driven tool discovery and execution |

### Integration & Observability

| Feature | Description |
|---------|-------------|
| **Multi-Format Parsing** | XML (Anthropic), OpenAI `tool_calls`, JSON — all work automatically |
| **MCP Integration** | Connect to remote tools via HTTP Streamable, STDIO, SSE |
| **OpenTelemetry** | Distributed tracing with automatic span creation |
| **Prometheus** | Metrics for error rates, latency, cache hits, circuit breaker state |
| **Type Safety** | PEP 561 compliant with full mypy support |

---

## Production Configuration

```python
async with ToolProcessor(
    # Execution settings
    default_timeout=30.0,
    max_concurrency=20,

    # Reliability features
    enable_caching=True,
    cache_ttl=600,
    enable_rate_limiting=True,
    global_rate_limit=100,
    tool_rate_limits={"expensive_api": (5, 60)},  # 5 req/min
    enable_retries=True,
    max_retries=3,
    enable_circuit_breaker=True,
    circuit_breaker_threshold=5,

    # Multi-tenant isolation
    enable_bulkhead=True,
    bulkhead_config=BulkheadConfig(
        default_limit=10,
        tool_limits={"slow_api": 2},
        patterns={"db.*": 3, "mcp.notion.*": 2},  # Pattern-based limits
    ),
) as processor:
    # Execute with request context
    ctx = ExecutionContext(
        request_id="req-123",
        user_id="user-456",
        tenant_id="acme-corp",
    )
    results = await processor.process(llm_output, context=ctx)
```

---

## Return Order & Scheduling

Control how results are returned and plan complex execution graphs:

```python
from chuk_tool_processor import ToolProcessor, ReturnOrder

async with ToolProcessor() as processor:
    # Results return as tools complete (fast tools first) - default
    results = await processor.process(calls, return_order="completion")

    # Results return in submission order (deterministic)
    results = await processor.process(calls, return_order="submission")
```

### DAG Scheduling with Dependencies

```python
from chuk_tool_processor import (
    GreedyDagScheduler,
    SchedulingConstraints,
    ToolCallSpec,
    ToolMetadata,
)

scheduler = GreedyDagScheduler()

# Define calls with dependencies
calls = [
    ToolCallSpec(call_id="fetch", tool_name="api.fetch",
                 metadata=ToolMetadata(pool="web", est_ms=300)),
    ToolCallSpec(call_id="transform", tool_name="compute.transform",
                 depends_on=("fetch",)),
    ToolCallSpec(call_id="store", tool_name="db.write",
                 depends_on=("transform",)),
]

# Plan execution with constraints
constraints = SchedulingConstraints(
    deadline_ms=5000,
    pool_limits={"web": 2, "db": 1},
)
plan = scheduler.plan(calls, constraints)

# plan.stages: (('fetch',), ('transform',), ('store',))
# plan.skip: () or low-priority calls that would miss deadline
```

---

## MCP Integration

Connect to remote tool servers using the [Model Context Protocol](https://modelcontextprotocol.io):

```python
from chuk_tool_processor.mcp import setup_mcp_http_streamable

# Cloud services (Notion, etc.)
processor, manager = await setup_mcp_http_streamable(
    servers=[{
        "name": "notion",
        "url": "https://mcp.notion.com/mcp",
        "headers": {"Authorization": f"Bearer {token}"}
    }],
    namespace="notion",
    enable_caching=True,
    enable_retries=True
)

# Use remote tools
results = await processor.process(
    '<tool name="notion.search_pages" args=\'{"query": "docs"}\'/>'
)
```

**Transport Options:**

| Transport | Use Case | Example |
|-----------|----------|---------|
| **HTTP Streamable** | Cloud SaaS with OAuth | Notion, custom APIs |
| **STDIO** | Local tools, databases | SQLite, file systems |
| **SSE** | Legacy MCP servers | Atlassian |

See [MCP_INTEGRATION.md](docs/MCP_INTEGRATION.md) for complete examples with OAuth token refresh.

### MCP Middleware Stack

For production deployments, wrap MCP connections with resilience middleware:

```python
from chuk_tool_processor.mcp.middleware import (
    MiddlewareConfig,
    MiddlewareStack,
    RetrySettings,
    CircuitBreakerSettings,
    RateLimitSettings,
)

# Configure middleware layers
config = MiddlewareConfig(
    retry=RetrySettings(max_retries=3, base_delay=1.0),
    circuit_breaker=CircuitBreakerSettings(failure_threshold=5),
    rate_limiting=RateLimitSettings(enabled=True, global_limit=100),
)

# Wrap StreamManager with middleware
middleware = MiddlewareStack(stream_manager, config=config)

# Execute with automatic retry, circuit breaking, and rate limiting
result = await middleware.call_tool("notion.search", {"query": "docs"})
```

---

## Distributed Deployments (Redis)

For multi-process or multi-machine deployments, configure Redis backends via environment variables:

```bash
# Enable Redis for everything
export CHUK_REGISTRY_BACKEND=redis
export CHUK_RESILIENCE_BACKEND=redis
export CHUK_REDIS_URL=redis://localhost:6379/0

# Enable resilience features
export CHUK_CIRCUIT_BREAKER_ENABLED=true
export CHUK_RATE_LIMIT_ENABLED=true
export CHUK_RATE_LIMIT_GLOBAL=100
```

```python
from chuk_tool_processor import ProcessorConfig

# Load from environment and create fully-configured processor
config = ProcessorConfig.from_env()
processor = await config.create_processor()

async with processor:
    results = await processor.process(llm_output)
```

Or configure programmatically:

```python
from chuk_tool_processor import ProcessorConfig, RegistryConfig, BackendType
from chuk_tool_processor.config import CircuitBreakerConfig, RateLimitConfig

config = ProcessorConfig(
    # Registry and resilience use Redis
    registry=RegistryConfig(backend=BackendType.REDIS),
    resilience_backend=BackendType.REDIS,
    redis_url="redis://localhost:6379/0",

    # Enable features
    circuit_breaker=CircuitBreakerConfig(enabled=True, failure_threshold=5),
    rate_limit=RateLimitConfig(enabled=True, global_limit=100),
)

processor = await config.create_processor()
```

**Key features:**
- **Distributed registry**: Tool metadata shared across processes
- **Distributed circuit breaker**: Failure counts shared (prevents cascading failures across instances)
- **Distributed rate limiting**: Global limits enforced across all instances
- **Multi-tenant isolation**: Key prefixes isolate data per tenant

**Installation:**
```bash
pip install chuk-tool-processor[redis]  # or: uv add chuk-tool-processor[redis]
```

See [examples/02_production_features/distributed_config_demo.py](examples/02_production_features/distributed_config_demo.py) for a complete example.

---

## Runtime Guards

Protect your tool execution with composable guards that enforce safety policies:

```python
from chuk_tool_processor.guards import (
    GuardChain,
    SchemaStrictnessGuard,
    SensitiveDataGuard,
    NetworkPolicyGuard,
    ConcurrencyGuard,
)

# Create individual guards
schema_guard = SchemaStrictnessGuard(get_schema=my_schema_getter)
sensitive_guard = SensitiveDataGuard()  # Detects API keys, JWTs, etc.
network_guard = NetworkPolicyGuard(block_private_ips=True)
concurrency_guard = ConcurrencyGuard(global_max=50, per_tool_max={"heavy_api": 2})

# Compose into a chain
chain = GuardChain([schema_guard, sensitive_guard, network_guard, concurrency_guard])

# Check before execution
result = await chain.check_all_async("api.fetch", {"url": "https://example.com"})
if result.blocked:
    print(f"Blocked by {result.stopped_at}: {result.reason}")
```

**Key Guards:**
- **SchemaStrictnessGuard** — Validate args against JSON schemas, auto-coerce types
- **SensitiveDataGuard** — Block or redact secrets (API keys, JWTs, private keys)
- **NetworkPolicyGuard** — SSRF defense (block localhost, private IPs, metadata endpoints)
- **SideEffectGuard** — Enforce read-only mode, block destructive ops in production
- **ConcurrencyGuard** — Limit in-flight calls globally, per-tool, or per-namespace
- **TimeoutBudgetGuard** — Enforce wall-clock budgets with soft/hard limits
- **OutputSizeGuard** — Prevent pathological payloads (size, depth, array length)
- **SaturationGuard** — Detect degenerate statistical outputs (extreme Z-scores, saturated CDFs)

See [GUARDS.md](docs/GUARDS.md) for complete documentation and examples.

---

## Dynamic Tool Discovery

When you have hundreds of tools, LLMs can't load all schemas upfront. The discovery module provides intelligent search and on-demand tool loading:

```python
from chuk_tool_processor.discovery import ToolSearchEngine, BaseDynamicToolProvider

# Create a search engine for your tools
engine = ToolSearchEngine()
engine.set_tools(my_tools)

# Natural language search with synonym expansion
results = engine.search("gaussian distribution")  # Finds "normal_cdf"
results = engine.search("find the average")       # Finds "calculate_mean"
results = engine.search("multipley")              # Finds "multiply" (typo tolerance)

# Session boosting - recently used tools rank higher
engine.record_tool_use("calculate_mean", success=True)
engine.advance_turn()
results = engine.search("calculate")  # "calculate_mean" now boosted
```

**Dynamic Provider Pattern** — give LLMs meta-tools for discovery:

```python
class MyToolProvider(BaseDynamicToolProvider):
    async def get_all_tools(self) -> list[Tool]:
        return self._tools

    async def execute_tool(self, name: str, args: dict) -> dict:
        return await self._tools[name].execute(**args)

provider = MyToolProvider()

# LLM gets 4 meta-tools: list_tools, search_tools, get_tool_schema, call_tool
tools_for_llm = provider.get_dynamic_tools()

# LLM workflow: search → get schema → call
results = await provider.search_tools("calculate average")
schema = await provider.get_tool_schema("calculate_mean")
result = await provider.call_tool("calculate_mean", {"values": [1, 2, 3]})
```

See [DISCOVERY.md](docs/DISCOVERY.md) for complete documentation.

---

## Observability

One-line setup for production monitoring:

```python
from chuk_tool_processor.observability import setup_observability

setup_observability(
    service_name="my-tool-service",
    enable_tracing=True,     # → OpenTelemetry traces
    enable_metrics=True,     # → Prometheus metrics at :9090/metrics
    metrics_port=9090
)
# Every tool execution is now automatically traced and metered
```

**What you get:**
- Distributed traces (Jaeger, Zipkin, any OTLP collector)
- Prometheus metrics (error rate, latency P50/P95/P99, cache hit rate)
- Circuit breaker state monitoring
- Zero code changes to your tools

See [OBSERVABILITY.md](docs/OBSERVABILITY.md) for complete setup guide.

---

## Structured Error Handling

Errors include machine-readable categories and retry hints for planner decision-making:

```python
from chuk_tool_processor.core.exceptions import ErrorCategory

results = await processor.process(llm_output)
for result in results:
    if result.error_info:
        match result.error_info.category:
            case ErrorCategory.RATE_LIMIT:
                await asyncio.sleep(result.retry_after_ms / 1000)
                return await retry()
            case ErrorCategory.CIRCUIT_OPEN:
                return await use_fallback_tool()
            case _ if not result.retryable:
                return await report_permanent_failure()
```

See [ERRORS.md](docs/ERRORS.md) for complete error taxonomy.

---

## Documentation

| Document | Description |
|----------|-------------|
| [**GETTING_STARTED.md**](docs/GETTING_STARTED.md) | Creating tools, using the processor, ValidatedTool, StreamingTool |
| [**CORE_CONCEPTS.md**](docs/CORE_CONCEPTS.md) | Registry, strategies, wrappers, parsers, MCP overview |
| [**PRODUCTION_PATTERNS.md**](docs/PRODUCTION_PATTERNS.md) | Bulkheads, scoped registries, ExecutionContext, parallel execution |
| [**DISCOVERY.md**](docs/DISCOVERY.md) | Dynamic tool discovery, intelligent search, synonym expansion |
| [**GUARDS.md**](docs/GUARDS.md) | Runtime guards for safety, validation, and resource management |
| [**MCP_INTEGRATION.md**](docs/MCP_INTEGRATION.md) | HTTP Streamable, STDIO, SSE, OAuth, Middleware Stack |
| [**ADVANCED_TOPICS.md**](docs/ADVANCED_TOPICS.md) | Deferred loading, code sandbox, isolated strategy, testing |
| [**isolated_execution.md**](docs/isolated_execution.md) | Running untrusted/LLM code behind a real boundary (`IsolatedCodeRunner`, backends) |
| [**security.md**](docs/security.md) | Security model: `CodeSandbox` vs real isolation, running untrusted code safely |
| [**CONFIGURATION.md**](docs/CONFIGURATION.md) | All config options and environment variables |
| [**OBSERVABILITY.md**](docs/OBSERVABILITY.md) | OpenTelemetry, Prometheus, metrics reference |
| [**ERRORS.md**](docs/ERRORS.md) | Error codes and handling patterns |

---

## Examples

```bash
# Getting started
python examples/01_getting_started/hello_tool.py

# Dynamic tool discovery (search, synonyms, fuzzy matching)
python examples/07_discovery/dynamic_tools_demo.py

# Hero demo: 8 tools, 5-second deadline, 3 pools (DAG + bulkheads + context)
python examples/02_production_features/hero_runtime_demo.py

# Production patterns (bulkheads, context, scoped registries)
python examples/02_production_features/production_patterns_demo.py

# Runtime features (return order, pattern bulkheads, scheduling)
python examples/02_production_features/runtime_features_demo.py

# Structured error handling for planners
python examples/02_production_features/structured_errors_demo.py

# Runtime guards (validation, security, resource limits)
python examples/guards_demo.py

# Redis registry for distributed deployments
python examples/02_production_features/redis_registry_demo.py

# Distributed configuration (Redis registry + resilience)
python examples/02_production_features/distributed_config_demo.py

# Observability demo
python examples/02_production_features/observability_demo.py

# MCP integration
python examples/04_mcp_integration/stdio_echo.py
python examples/04_mcp_integration/notion_oauth.py
python examples/04_mcp_integration/middleware_demo.py
```

See [examples/](examples/) for 45+ working examples.

---

## Compatibility

| Component | Supported |
|-----------|-----------|
| **Python** | 3.11, 3.12, 3.13 |
| **Platforms** | macOS, Linux, Windows |
| **LLM Providers** | OpenAI, Anthropic, Local models (Ollama, MLX, vLLM) |
| **MCP Transports** | HTTP Streamable, STDIO, SSE |
| **MCP Spec** | 2025-11-25, 2025-06-18, 2025-03-26 |

---

## Installation Options

```bash
# Core package
pip install chuk-tool-processor

# With observability (OpenTelemetry + Prometheus)
pip install chuk-tool-processor[observability]

# With MCP support
pip install chuk-tool-processor[mcp]

# With Redis registry (distributed deployments)
pip install chuk-tool-processor[redis]

# With fast JSON (2-3x faster with orjson)
pip install chuk-tool-processor[fast-json]

# All extras
pip install chuk-tool-processor[all]
```

---

## When to Use This

**Use CHUK Tool Processor when:**
- Your LLM calls tools or APIs
- You need retries, timeouts, caching, or rate limits
- You need crash isolation for flaky tools, or a real boundary for untrusted/LLM code ([`IsolatedCodeRunner`](docs/isolated_execution.md))
- Your tools are local or remote (MCP)
- You need multi-tenant isolation
- You want production-grade observability

**Don't use this if:**
- You want an agent framework (this is the execution runtime, not the agent)
- You want conversation flow/memory orchestration
- You need a planner to decide *which* tools to call

### The Seam: Runtime vs Planner

CHUK Tool Processor deliberately does not plan workflows or decide which tools to call. It executes tool calls reliably, under constraints, as directed by higher-level planners.

```
┌─────────────────────────────────────────────────────┐
│  Your Agent / LangChain / LlamaIndex / Custom       │  ← Decides WHICH tools
└─────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────┐
│            CHUK Tool Processor                      │  ← Executes tools RELIABLY
│  (timeouts, retries, caching, rate limits, etc.)   │
└─────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────┐
│          Local Tools / MCP Servers                  │  ← Does the actual work
└─────────────────────────────────────────────────────┘
```

This separation means you can swap planners without changing execution infrastructure, and vice versa.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.

```bash
# Development setup
git clone <repository-url>
cd chuk-tool-processor
uv pip install -e ".[dev]"

# Run tests
make check
```

---

## License

Apache 2.0 License - see [LICENSE](LICENSE) for details.

---

## Related Projects

- [chuk-mcp](https://pypi.org/project/chuk-mcp/) - Low-level MCP protocol client
- [Model Context Protocol](https://modelcontextprotocol.io) - MCP specification

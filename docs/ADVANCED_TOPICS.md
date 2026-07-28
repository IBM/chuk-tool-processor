# Advanced Topics

This guide covers advanced features for scaling tool systems and handling complex use cases.

## Table of Contents

- [Advanced Tool Use](#advanced-tool-use)
  - [Deferred Loading](#deferred-loading-99-token-reduction)
  - [Tool Use Examples](#tool-use-examples-25-accuracy)
  - [Programmatic Execution](#programmatic-execution-37-token-savings)
- [Isolated Strategy](#using-isolated-strategy)
- [Testing Tools](#testing-tools)

---

## Advanced Tool Use

**Three powerful features for scaling to large tool sets with ANY LLM:**

1. **Deferred Loading** - Load 1000s of tools, expose only a few
2. **Tool Use Examples** - Improve accuracy from 72% to 90%
3. **Programmatic Execution** - LLMs orchestrate tools via Python code

### Deferred Loading (99% Token Reduction)

Traditional approach: Send all 393 tools → Exceeds limits! (~196K tokens)

**Advanced approach**: Send only 4 core tools initially (~2K tokens) = **99% reduction**

```python
from chuk_tool_processor.mcp import register_mcp_tools, setup_mcp_stdio
from chuk_tool_processor import get_default_registry

# Connect to math server with 393 tools
processor, manager = await setup_mcp_stdio(
    config_file="mcp_config.json",
    servers=["math"],
    namespace="math"
)

# Register with deferred loading
await register_mcp_tools(
    stream_manager=manager,
    namespace="math",
    defer_loading=True,  # Only load on-demand
    defer_all_except=["add", "subtract", "multiply", "divide"],  # Core 4
)

# LLM only sees 4 tools initially
# If it needs "power", search and load dynamically
registry = await get_default_registry()
results = await registry.search_deferred_tools(query="power exponent")
await registry.load_deferred_tool(results[0].name, "math")
```

**Benefits**: Works with OpenAI (128 tool limit), Claude, and any LLM

### Tool Use Examples (+25% Accuracy)

Add concrete usage examples to improve LLM tool calling accuracy:

```python
from chuk_tool_processor.models.tool_spec import ToolSpec

spec = ToolSpec(
    name="create_event",
    description="Create calendar event",
    parameters={...},
    examples=[
        {
            "input": {
                "title": "Team Standup",
                "start_date": "2024-01-15T09:00:00Z",
                "attendees": ["alice@company.com"],
                "recurrence": "daily"
            },
            "description": "Daily recurring meeting"
        },
        {
            "input": {
                "title": "Project Deadline",
                "start_date": "2024-03-01T23:59:59Z",
                "attendees": []
            },
            "description": "Single event with no attendees"
        }
    ]
)

# Export to any provider (all support examples now)
openai_format = spec.to_openai()
anthropic_format = spec.to_anthropic()
mcp_format = spec.to_mcp()
```

**Research**: Anthropic found examples improve accuracy from 72% → 90% on complex parameters

### Programmatic Execution (37% Token Savings)

The built-in **code executor** (`CodeSandbox`) works with ANY LLM (OpenAI, Claude, Llama, etc.):

> [!WARNING]
> `CodeSandbox` runs code in-process via `exec()` and is **not** a security
> boundary — the restricted builtins are trivially escapable, so untrusted code
> is not contained. Execution is disabled unless you pass
> `allow_unsafe_execution=True`, which you should do **only for code you fully
> trust**. Never feed it untrusted or LLM-generated code expecting containment.
> See [security.md](./security.md).

```python
from chuk_tool_processor.execution import CodeSandbox

# allow_unsafe_execution asserts the code is trusted (in-process, no isolation).
sandbox = CodeSandbox(timeout=30.0, allow_unsafe_execution=True)

# Trusted Python code
code = """
# Process data using tools in a loop
results = []
for i in range(1, 6):
    result = await add(a=str(i), b=str(i))
    results.append(result)
return results
"""

# Runs in-process (no isolation) -- trusted code only
result = await sandbox.execute(code, namespace="math")
# All 5 tool calls happen in single execution context!
```

**Traditional vs Programmatic Approach:**

| Approach | API Calls | Tokens | Time |
|----------|-----------|--------|------|
| Traditional (sequential) | 3 | 25K | ~10s |
| Programmatic (code) | 1 | 3K | ~2s |

**Benefits**:
- **Works with ANY LLM** (OpenAI, Claude, Llama, Mistral, etc.)
- **37% token reduction** on complex workflows
- **Faster execution** (no API round-trips for intermediate values)
- **Convenience controls** (reduced builtins, timeouts, tool allowlist) — these are *not* a security boundary; see [security.md](./security.md)

**Complete Example:**

```python
from chuk_tool_processor.execution import CodeSandbox
from chuk_tool_processor.mcp import setup_mcp_stdio

# Setup tools
processor, manager = await setup_mcp_stdio(
    config_file="mcp_config.json",
    servers=["math"],
    namespace="math"
)

# Create code executor.
# WARNING: LLM-generated code is untrusted. CodeSandbox provides no isolation,
# so running it as shown here is unsafe -- this illustrates the API only. For
# untrusted code, run it inside real OS/process-level isolation. See security.md.
sandbox = CodeSandbox(allow_unsafe_execution=True)

# LLM generates this code
llm_generated_code = """
# Complex workflow with loops and conditionals
results = []
for i in range(1, 6):
    if i < 3:
        result = await add(a=str(i), b="100")
    else:
        result = await add(a=str(i), b="200")
    results.append(result)
return results
"""

# Execute (in-process, no isolation)
result = await sandbox.execute(llm_generated_code, namespace="math")
print(result)  # All tool calls executed!
```

See `examples/code_sandbox_demo.py` and `examples/advanced_tool_use_math_server.py` for complete working examples.

---

## Using Isolated Strategy

Use `IsolatedStrategy` for **crash/fault isolation** of tool execution — it runs each registered **tool call** in a separate worker process so a hanging or crashing tool can't take down your app.

> [!WARNING]
> `IsolatedStrategy` is **not a security boundary**. Workers run as the same OS
> user with no seccomp/namespace/rlimit confinement, and tool arguments/results
> cross the boundary via **pickle**. It also only governs how *registered tool
> calls* are dispatched — it never executes an orchestration **code string**. For
> running untrusted or LLM-generated *code*, use
> [`IsolatedCodeRunner`](./isolated_execution.md) (real OS/container/WASM
> isolation with brokered tool access), not this strategy and not `CodeSandbox`.

```python
import asyncio
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
        results = await processor.process(tool_calls)

asyncio.run(main())
```

### What IsolatedStrategy actually protects against

| Aspect | Protection |
|--------|------------|
| **Crash Blast Radius** | Zero — a crashing/hanging tool doesn't bring down your app |
| **Process separation** | Each tool call runs in a separate worker process (fault, not security, isolation) |
| **Timeouts** | Per-call deadlines terminate stuck workers |
| **Secrets** | Never injected by default — pass explicitly |

This is **fault isolation, not a security sandbox** — see the warning above. For a
real security boundary around untrusted code, use
[`IsolatedCodeRunner`](./isolated_execution.md), optionally with a container/gVisor
runtime for `--cpus`/`--memory`/egress limits.

### When to Use Each Strategy

| Scenario | Use |
|----------|-----|
| Trusted internal tools | `InProcessStrategy` |
| Performance-critical path | `InProcessStrategy` |
| Tools that might crash or hang | `IsolatedStrategy` (crash isolation) |
| Untrusted / third-party / LLM-generated **code** | [`IsolatedCodeRunner`](./isolated_execution.md) (real isolation) — *not* `IsolatedStrategy`, *not* `CodeSandbox` |

---

## Testing Tools

### Basic Test Pattern

```python
import pytest
from chuk_tool_processor import ToolProcessor, initialize

@pytest.mark.asyncio
async def test_calculator():
    await initialize()

    async with ToolProcessor() as processor:
        results = await processor.process(
            '<tool name="calculator" args=\'{"operation": "add", "a": 5, "b": 3}\'/>'
        )

        assert results[0].result["result"] == 8
```

### Fake Tool Pattern

```python
import pytest
from chuk_tool_processor import ToolProcessor, register_tool, initialize

@register_tool(name="fake_tool")
class FakeTool:
    """No-op tool for testing processor behavior."""
    call_count = 0

    async def execute(self, **kwargs) -> dict:
        FakeTool.call_count += 1
        return {"called": True, "args": kwargs}

@pytest.mark.asyncio
async def test_processor_with_fake_tool():
    await initialize()

    async with ToolProcessor() as processor:
        # Reset counter
        FakeTool.call_count = 0

        results = await processor.process(
            '<tool name="fake_tool" args=\'{"test": "value"}\'/>'
        )

        assert FakeTool.call_count == 1
        assert results[0].result["called"] is True
```

### Testing with Isolated Registries

Use scoped registries to prevent test pollution:

```python
import pytest
from chuk_tool_processor import ToolProcessor, create_registry, tool

@pytest.mark.asyncio
async def test_with_isolated_registry():
    # Create isolated registry for this test
    registry = create_registry()

    @tool(name="test_tool")
    class TestTool:
        async def execute(self, value: str) -> dict:
            return {"received": value}

    await registry.register_tool(TestTool)

    processor = ToolProcessor(registry=registry)
    async with processor:
        results = await processor.process(
            '<tool name="test_tool" args=\'{"value": "test"}\'/>'
        )

        assert results[0].result["received"] == "test"

    # Registry is completely isolated — no cleanup needed
```

### Testing Error Handling

```python
import pytest
from chuk_tool_processor import ToolProcessor, initialize
from chuk_tool_processor.core.exceptions import ToolNotFoundError

@pytest.mark.asyncio
async def test_tool_not_found():
    await initialize()

    async with ToolProcessor() as processor:
        results = await processor.process(
            '<tool name="nonexistent_tool" args=\'{}\'/>'
        )

        assert results[0].error is not None
        assert "not found" in results[0].error.lower()
```

### Testing with Mocked External Services

```python
import pytest
from unittest.mock import AsyncMock, patch
from chuk_tool_processor import ToolProcessor, register_tool, initialize

@register_tool(name="api_tool")
class ApiTool:
    async def execute(self, query: str) -> dict:
        # Real implementation calls external API
        response = await self._call_api(query)
        return {"result": response}

    async def _call_api(self, query: str) -> str:
        # External API call
        ...

@pytest.mark.asyncio
async def test_api_tool_with_mock():
    await initialize()

    with patch.object(ApiTool, '_call_api', new_callable=AsyncMock) as mock_api:
        mock_api.return_value = "mocked response"

        async with ToolProcessor() as processor:
            results = await processor.process(
                '<tool name="api_tool" args=\'{"query": "test"}\'/>'
            )

            assert results[0].result["result"] == "mocked response"
            mock_api.assert_called_once_with("test")
```

---

## Related Documentation

- [CORE_CONCEPTS.md](CORE_CONCEPTS.md) - Fundamental architecture
- [GETTING_STARTED.md](GETTING_STARTED.md) - Basic tutorials
- [PRODUCTION_PATTERNS.md](PRODUCTION_PATTERNS.md) - Production patterns
- [MCP_INTEGRATION.md](MCP_INTEGRATION.md) - MCP server integration
- [OBSERVABILITY.md](OBSERVABILITY.md) - Metrics and tracing

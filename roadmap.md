# CHUK Tool Processor — Roadmap

> Last updated: 2026-07-29

---

## Shipped (through v0.25)

Everything below is production-ready on `main`.

### Isolated Execution & Security (v0.23–v0.25)
- [x] **Fail-closed `CodeSandbox`** (v0.23) — the in-process `exec()` sandbox no
      longer pretends restricted builtins are a security boundary; it refuses to
      run untrusted code unless `allow_unsafe_execution=True` and is documented
      as trusted-code-only
- [x] **`IsolatedCodeRunner`** (v0.24) — runs untrusted / LLM-generated code
      behind a real OS/runtime boundary, with tool access brokered back to the
      host over a single audited JSON channel (never pickle)
- [x] Isolation backends behind one protocol — `SeatbeltBackend` (macOS),
      `DockerBackend` (throwaway container), `BubblewrapBackend` (Linux
      namespaces), `LocalProcessBackend` (dev/testing; refused without opt-in)
- [x] **Experimental Windows AppContainer backend** + pluggable broker transport
      (unix socket / Windows named pipe) (v0.25)
- [x] **Broker policy hardening** (v0.25) — host-authoritative namespace,
      namespace-qualified allowlists, calls routed through the canonical executor,
      and bounded lifecycle (in-flight calls cancelled on close; none honoured
      after the run's result)

### Core Runtime
- [x] Async-native tool execution (Python 3.11+)
- [x] Multi-format parsing — Anthropic XML, OpenAI `tool_calls`, JSON
- [x] Timeouts, retries (exponential backoff + jitter), caching (TTL + SHA256 keys)
- [x] Rate limiting (global + per-tool sliding windows)
- [x] Circuit breakers with automatic recovery
- [x] Structured error categories with retry hints for planners
- [x] All config classes are Pydantic `BaseModel` (no dataclasses)

### Execution Strategies
- [x] InProcess — fast, trusted tools
- [x] Subprocess/Isolated — untrusted code with zero crash blast radius
- [x] Remote via MCP — distributed tool execution

### MCP Integration
- [x] STDIO transport (local processes)
- [x] SSE transport (legacy servers) with headers support
- [x] HTTP Streamable transport (modern MCP spec 2025-11-25)
- [x] StreamManager with multi-server lifecycle management
- [x] Middleware stack — retry, circuit breaker, rate limiting for MCP calls
- [x] OAuth token refresh callbacks
- [x] Robust shutdown handling (shield + fallback strategies)
- [x] Health checks and diagnostics
- [x] Shared auth header + OAuth error logic extracted to `base_transport.py`

### Multi-Tenant & Isolation
- [x] Bulkheads — per-tool/namespace concurrency limits
- [x] Pattern bulkheads — glob patterns (`"db.*": 3`)
- [x] Scoped registries for multi-tenant apps
- [x] ExecutionContext — request-scoped metadata propagation
- [x] Redis registry for distributed deployments
- [x] Redis-backed circuit breaker and rate limiting

### Scheduling
- [x] Return order — completion (fast first) or submission (deterministic)
- [x] DAG-based greedy scheduler with topological sort
- [x] Deadline-aware skipping of low-priority calls
- [x] Pool-based concurrency constraints

### Guards (Constitution Layer)
- [x] SchemaStrictnessGuard — JSON schema validation + type coercion
- [x] SensitiveDataGuard — detect/block/redact secrets
- [x] NetworkPolicyGuard — SSRF defense
- [x] SideEffectGuard — read_only/write/destructive classification
- [x] ConcurrencyGuard — global/per-tool/per-namespace limits
- [x] TimeoutBudgetGuard — wall-clock budgets with soft/hard limits
- [x] OutputSizeGuard — payload size/depth/array limits
- [x] RetrySafetyGuard — backoff, idempotency, non-retryable classification
- [x] ProvenanceGuard — output attribution and lineage
- [x] PlanShapeGuard — detect fan-out explosions, long chains
- [x] SaturationGuard — degenerate statistical outputs
- [x] RunawayGuard, BudgetGuard, PerToolGuard, PreconditionGuard, UnresolvedReferenceGuard

### Discovery
- [x] Natural language tool search with synonym expansion
- [x] Fuzzy matching with typo tolerance
- [x] Session boosting — recent tools rank higher
- [x] BaseDynamicToolProvider for LLM-driven discovery

### Observability
- [x] OpenTelemetry distributed tracing
- [x] Prometheus metrics (latency, error rate, cache hits, circuit state)
- [x] Structured logging with context propagation

### Architecture Compliance (v0.22)

Comprehensive audit and fix of architecture principle violations:

- [x] **Enums everywhere** — `MCPTransport`, `ProviderType`, `TraceSinkType`, `DifferenceSeverity`, `GuardVerdict` replace all magic strings; all enums migrated to `StrEnum`
- [x] **Pydantic-native** — `config.py` migrated from dataclasses to `BaseModel`; `StreamManager` collections typed with Pydantic models
- [x] **Async-clean** — `FileTraceSink` uses `asyncio.to_thread()` for file I/O; `time.monotonic()` for all duration measurements
- [x] **MCP DRY** — shared OAuth error detection and auth header construction in `base_transport.py`
- [x] **Deprecation-clean** — `datetime.utcnow()` → `datetime.now(UTC)`, `asyncio.iscoroutinefunction` → `inspect.iscoroutinefunction`, `athrow()` 3-arg → 1-arg form
- [x] **Duplicate removal** — `CircuitState` enum consolidated (removed duplicate from `redis_circuit_breaker.py`)

### Developer Experience
- [x] `@tool` and `@register_tool` decorators
- [x] `register_fn_tool` for plain functions
- [x] PEP 561 type stubs (`py.typed`)
- [x] 120+ test files, 3000+ tests, 97% coverage (every file ≥ 90%)
- [x] 45+ working examples
- [x] CI on Python 3.11, 3.12, 3.13 (macOS + Linux)
- [x] Zero test warnings (pytest warning filters for known third-party issues)
- [x] Apache 2.0 license
- [x] Architecture Principles document (`ARCHITECTURE_PRINCIPLES.md`)

---

## Next — Features

### Guard Integration with Processor
Guards are already built as a standalone `GuardChain` system with pre/post-execution hooks. The integration into the processor pipeline should be clean since the API was designed for exactly this wiring.

- [ ] `guards=` parameter on `ToolProcessor`
- [ ] Pre-execution guard chain (before tool call) — schema validation, sensitive data, network policy
- [ ] Post-execution guard chain (validate outputs) — output size, provenance, saturation
- [ ] Guard metrics in observability layer (block rate, warn rate, latency overhead)
- [ ] Default guard presets (`"strict"`, `"permissive"`) for common configurations

### Distributed Caching (Redis)
Redis-backed circuit breakers and rate limiting are already shipped. Redis caching completes the distributed story — after this, the entire resilience stack (cache + rate limit + circuit breaker) works across multi-process and multi-machine deployments.

- [ ] Redis cache provider implementing existing `CacheInterface`
- [ ] TTL and eviction policy configuration
- [ ] Cache key prefixing for multi-tenant isolation
- [ ] Cache invalidation via pub/sub for cross-instance consistency

### Batch/Bulk Execution API
DAG scheduling and bulkheads handle concurrency, but planner-driven workflows (chuk-ai-planner, chuk-acp-agent) often emit large plans in a single shot. A dedicated bulk API with automatic chunking and backpressure serves this directly.

- [ ] `processor.process_batch()` accepting hundreds of calls
- [ ] Automatic chunking with configurable batch size
- [ ] Backpressure — pause accepting new calls when downstream is saturated
- [ ] Progress callbacks (`on_batch_progress(completed, total, failures)`)
- [ ] Partial result streaming — return results as batches complete

### Dry Run / Simulation Mode
Run the full pipeline (parsing, guards, middleware) without actually executing tools. Useful for planner validation — "would this plan pass all guards and fit within budget?" The guard chain and PlanShapeGuard are already there; dry run is a natural extension.

- [ ] `processor.process(calls, dry_run=True)` — returns guard verdicts, budget estimates, schema validation
- [ ] Simulation of middleware stack (would circuit breaker trip? would rate limit block?)
- [ ] Plan cost estimation (time budget, monetary cost, concurrency slots)
- [ ] Integration with PlanShapeGuard for full plan validation before execution

### Tool Versioning
As the MCP ecosystem grows, remote servers will upgrade tool schemas. A versioning layer on the registry prevents silent breakage when schemas change under you.

- [ ] Schema hash tracking on tool registration (detect changes)
- [ ] Compatibility checks — warn on breaking schema changes (removed fields, type changes)
- [ ] Deprecation warnings for tools marked as sunset
- [ ] Version pinning — `registry.get_tool("calc", version="1.x")`

### Cost Tracking Guard
TimeoutBudgetGuard handles wall-clock time. A parallel CostBudgetGuard tracks estimated monetary cost per tool call — especially valuable for paid APIs and MCP services in multi-tenant deployments where you're billing back to tenants.

- [ ] `CostBudgetGuard` with per-tool cost estimates and soft/hard limits
- [ ] Per-tenant cost tracking and enforcement
- [ ] Cost attribution in ExecutionContext (roll up per-request costs)
- [ ] Integration with observability layer (Prometheus cost metrics)

### Cache Key Normalization
Improve cache hit rates for tools with volatile arguments.

- [ ] `cache_key_fn` parameter on tool registration
- [ ] Strip timestamps, random IDs, and other volatile fields before hashing
- [ ] Documentation and examples

---

## Near-Term

### WebAssembly Sandbox
Infrastructure is already scaffolded (`IsolationLevel.WASM`). The subprocess/isolated strategy works but has noticeable per-call overhead. WASM gives near-native speed with proper sandboxing — especially relevant for MCP tool execution where you don't control what's running on remote servers.

- [ ] WASM runtime integration (wasmtime or similar)
- [ ] Lightweight, portable tool isolation with near-native performance
- [ ] Resource limits (memory, CPU cycles, wall-clock time) within WASM boundary
- [ ] MCP tool execution in WASM sandbox for untrusted remote tools

### Type Safety Hardening
Systematic module-by-module tightening of mypy strictness.

- [ ] Enable `disallow_untyped_defs` for public APIs (`core.processor`)
- [ ] Enable `disallow_untyped_calls` gradually
- [ ] Replace `any` type hints with proper `Callable` signatures (e.g., `stream_manager.py` oauth callbacks)
- [ ] Reduce mypy `ignore_errors` overrides for:
  - `mcp.*`
  - `plugins.*`
  - `execution.wrappers.*`
  - `execution.strategies.*`
  - `registry.decorators`

### Expanded Observability
- [ ] Custom metric exporters (StatsD, Datadog)
- [ ] Trace sampling configuration
- [ ] Custom span attributes via tool metadata
- [ ] Baggage propagation for distributed tracing
- [ ] Health check endpoint integration

### MCP Resources & Prompts
StreamManager already has `list_resources()`, `read_resource()`, `list_prompts()`. Expose these more fully.

- [ ] Resource subscriptions (watch for changes)
- [ ] Prompt template execution through processor
- [ ] Resource caching with invalidation

### Streaming Tool Execution
`StreamingTool` model exists. Extend runtime support.

- [ ] First-class streaming in `ToolProcessor.process()`
- [ ] Streaming results via async generators
- [ ] Backpressure handling for slow consumers
- [ ] Streaming through MCP transports

### Error Handling Consistency
MCP subsystem mixes two error patterns — standardize.

- [ ] Replace `{"isError": True, "error": ...}` dicts with `ToolResult.create_error()`
- [ ] Replace broad `except Exception` with specific exception types
- [ ] Distinguish "no results" from "error" in MCP calls (currently both return empty collections)

---

## Longer-Term

### Tool Call Replay / Audit Log
Persistent execution log (tool name, args, result hash, latency, guard verdicts, context) that enables replay for debugging and compliance. ProvenanceGuard already tracks lineage — this extends it to a full audit trail.

- [ ] Append-only execution log (pluggable backends — file, database, cloud storage)
- [ ] Replay API — re-execute a recorded sequence with optional argument overrides
- [ ] Diff mode — compare replay results against original for regression detection
- [ ] Compliance export (JSON-lines, CSV) for audit requirements

### Adaptive Middleware Tuning
Circuit breaker thresholds and rate limits are currently static config. An adaptive layer that adjusts based on observed error rates and latency distributions reduces operational toil. The Prometheus metrics already feed the data needed.

- [ ] Adaptive circuit breaker — adjust failure threshold based on rolling error rate
- [ ] Adaptive rate limiting — scale limits based on observed latency P95/P99
- [ ] Anomaly detection — alert on sudden latency shifts or error rate spikes
- [ ] Feedback loop from metrics → middleware config (control plane pattern)

### MCP Tool Schema Caching & Diffing
On connection to an MCP server, cache tool schemas and diff on reconnect. Alert when schemas change unexpectedly. Ties into tool versioning but specifically for the remote/MCP case where you don't control the server.

- [ ] Schema snapshot on first connect (persisted to registry)
- [ ] Diff on reconnect — detect added/removed/changed tools
- [ ] Breaking change alerts (removed required fields, type changes)
- [ ] Optional auto-quarantine of tools with unexpected schema changes

### Federation / Tool Routing Table
As MCP servers proliferate, a routing table that maps tool name patterns to backends with fallback chains simplifies configuration. Think DNS for tools.

- [ ] Pattern-based routing — `"notion.*" → mcp.notion.com`, `"db.*" → local-stdio`
- [ ] Fallback chains — primary → secondary → local stub
- [ ] Health-aware failover — route away from unhealthy backends automatically
- [ ] Routing table hot-reload without processor restart

### A2A (Agent-to-Agent) Protocol
As the chuk-acp ecosystem matures, tool processor becomes the execution substrate for agent-to-agent communication.

- [ ] A2A task cards as tool calls
- [ ] Cross-agent tool delegation
- [ ] Agent capability advertisement via tool schemas

### Multi-Model Tool Routing
Route tool calls to different execution backends based on tool characteristics.

- [ ] Cost-aware routing (cheap tools local, expensive tools remote)
- [ ] Latency-aware routing with automatic fallback
- [ ] Geographic routing for data sovereignty

### Plugin Ecosystem
- [ ] Third-party tool packs (pip-installable)
- [ ] Auto-discovery of installed tool packages
- [ ] Tool marketplace / registry service integration

---

## Ecosystem Context

**chuk-tool-processor** is a foundational layer in the chuk-ai ecosystem. These packages depend on it:

| Package | Min Version | Role |
|---------|-------------|------|
| **chuk-acp-agent** | >=0.9.7 | ACP agent framework |
| **chuk-ai-session-manager** | >=0.18 | Session management |
| **chuk-ai-planner** | >=0.11 | Planning agent |
| **chuk-mcp-server** | >=0.11.3 | MCP proxy server |
| **chuk-agent-experimental** | latest | Experimental agents |

Breaking changes require coordinated updates across these packages.

---

## Version Housekeeping

- [x] ~~Sync `__init__.py` version with `pyproject.toml`~~ — resolved: `__init__.py` now reads from `importlib.metadata` at runtime
- [ ] Establish semantic versioning policy for 1.0

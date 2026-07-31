# Changelog

All notable changes to this project are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed

- **MCP core is now [chuk-mcp-rs](https://github.com/IBM/chuk-mcp-rs) directly.**
  `chuk-tool-processor` depends on `chuk-mcp-rs` (the Rust-powered `chuk_mcp_rs`
  extension) instead of the `chuk-mcp` Python facade. The public API and tool-result
  behaviour are unchanged; result objects returned by the Rust core (`.to_dict()`)
  are normalised to plain dicts transparently. Requires `chuk-mcp-rs` to be
  installable (it must be published to PyPI before a release cut).

### Added

- **Dual-era STDIO connect.** STDIO servers are connected with era-aware
  negotiation, so `chuk-tool-processor` works transparently with both legacy
  (`initialize` handshake) and modern (`2026-07-28` `server/discover`) MCP servers.

### Fixed

- HTTP-streamable `read_resource` now normalises Rust-backed result objects
  (`.to_dict()`) instead of only Pydantic `.model_dump()`, so it no longer returns
  an empty dict against a modern server.

### Internal

- `StreamManager` (previously a 1185-line module) split into focused mixins
  (init / resources / lifecycle); the duplicate resource/prompt methods shared by
  the STDIO and HTTP-streamable transports extracted into a common mixin. No
  behaviour change; per-file MCP coverage is ≥96% (module total 99%).

## [0.26.0]

### Security

- **MCP tool-name shadowing fixed.** When multiple MCP servers advertised the
  same tool name, `StreamManager` used last-writer-wins with no warning, so a
  later server (malicious, compromised, or merely reusing a common name like
  `read_file`) silently captured every future unpinned `call_tool` for that name.
  Registration is now **first-wins**: the first server to advertise a name owns
  default routing, a colliding later server is ignored for routing and logged
  with a prominent warning, and the shadowed tool remains reachable only by
  passing `server_name=` explicitly.

### Added

- `StreamManager.get_servers_for_tool(name)` and
  `StreamManager.get_tool_collisions()` to inspect which servers advertise a tool
  name and surface cross-server name collisions.

### Fixed

- `SubprocessStrategy.shutdown()` now shuts the process pool down directly instead
  of offloading `pool.shutdown(wait=False)` (already non-blocking) to a thread with
  a 1s timeout. On a busy event loop the executor could miss the timeout and skip
  the shutdown entirely — an intermittent test failure and a real pool-leak risk.

## [0.25.0]

### Security

- **Broker namespace policy is now authoritative.** The isolated guest runs
  untrusted code and holds the broker token, so it can craft raw protocol frames.
  `call_tool` previously let a guest-supplied namespace override the host's, so a
  run pinned to one namespace could reach tools in another. The host namespace is
  now enforced: a mismatching guest-supplied namespace is rejected, and tool
  resolution is pinned to that namespace (no cross-namespace fuzzy fallback).
- **Namespace-qualified allowlists.** `allowed_tools` entries may be bare
  (`"name"`) or qualified (`"namespace.name"`); qualified entries pin a tool to a
  single namespace so an allowed name can't select a same-named tool elsewhere.
- **Brokered tool calls run through the canonical executor.** Instead of invoking
  `tool.execute()` directly, the broker routes calls through the normal
  `ToolExecutor` path, so sandboxed calls get the same wrappers, guards, and
  observability as any other call. Callers may inject a fully-wrapped executor.
- **Bounded broker lifecycle.** In-flight host tool calls are tracked and
  cancelled when the run ends (`aclose`), and no tool calls are honoured once the
  guest has reported its result — a departing or timed-out guest can no longer
  leave privileged host work running.

### Changed

- `IsolationLimits` and `IsolatedResult` are now Pydantic models (frozen +
  `extra="forbid"` for limits) with field-level validation, replacing dataclasses.

### Added

- **Experimental Windows AppContainer backend** (`WindowsBackend`) — the Windows
  analogue of the macOS Seatbelt backend. Launches the guest as a
  capability-restricted, low-integrity AppContainer process inside a Job Object
  (memory / active-process caps, whole-tree kill on close). With no capabilities
  granted, the guest has no network and no access to the user's files; it reaches
  the host only through the broker. Verified via the dedicated Windows isolation
  CI. Install with the `isolation-windows` extra (pulls in `pywin32`).
- **Pluggable broker transport.** The host↔guest broker channel is now an
  abstraction with two implementations: a unix-domain socket on POSIX and a
  **named pipe** on Windows (overlapped I/O; ACL grants `ALL APPLICATION
  PACKAGES` at low integrity so an AppContainer guest can connect over local IPC,
  not the network). The job payload carries both `endpoint` and `transport`.

### Notes

- The Windows backend is experimental and exercised only through Windows CI; its
  capability set and output capture may still change. On non-Windows hosts it is
  import-safe and reports `is_available() == False`.

## [0.24.0]

### Added

- **`IsolatedCodeRunner`** — runs untrusted / LLM-generated code behind a real
  OS/runtime boundary, with tool access brokered back to the host over a single
  audited channel (JSON, never pickle). This is the safe counterpart to
  `CodeSandbox`; see `docs/isolated_execution.md`.
- Isolation backends behind a common `IsolationBackend` protocol:
  `SeatbeltBackend` (macOS `sandbox-exec`), `DockerBackend` (throwaway
  container), `BubblewrapBackend` (Linux namespaces), and `LocalProcessBackend`
  (no isolation; dev/testing only — the runner refuses it unless
  `allow_no_isolation=True`).
- `IsolationLimits`, `IsolatedResult`, and the `IsolationBackend` protocol,
  exported from `chuk_tool_processor.execution.isolation`.

### Changed

- Documentation now routes untrusted / LLM-generated code to
  `IsolatedCodeRunner`, and no longer presents the subprocess `IsolatedStrategy`
  as a security boundary — it provides crash/fault isolation for tool dispatch,
  not isolation of an orchestration code string.

### Notes

- Experimental Windows (AppContainer) and WASM backends live on separate
  branches and are not part of this release.

## [0.23.0]

### Security

- **`CodeSandbox` is now fail-closed.** `CodeSandbox.execute()` raises
  `UnsafeExecutionError` unless the sandbox is constructed with
  `CodeSandbox(allow_unsafe_execution=True)`. Restricting `__builtins__` never
  provided isolation — it is trivially escapable via attribute access — so it
  must not be used for untrusted or LLM-generated code. Enable it only for code
  you fully trust (code you authored).
- Removed the inaccurate "safe" / "isolated execution" / "no file I/O or network
  access" claims from the `CodeSandbox` docstring, docs, and demo, and added
  `docs/security.md` documenting the actual security model.

### Added

- `UnsafeExecutionError` and `SandboxSecurityWarning`, exported from
  `chuk_tool_processor.execution`.

### Breaking changes

- Callers of `CodeSandbox.execute()` must now pass
  `allow_unsafe_execution=True` at construction time. Code that relied on
  `execute()` running by default will raise `UnsafeExecutionError`. This affects
  only direct users of `CodeSandbox`; all other public APIs (`ToolProcessor`,
  `StreamManager`, strategies, registry, MCP, discovery, guards) are unchanged.

### Notes

- A real isolation mechanism for running untrusted / LLM-generated code
  (`IsolatedCodeRunner`, with OS/container backends) is planned for a follow-up
  release.

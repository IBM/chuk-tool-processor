# Changelog

All notable changes to this project are documented here. This project follows
[Semantic Versioning](https://semver.org/).

## [0.25.0]

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

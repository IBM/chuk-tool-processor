# Security Guide

This document covers the security properties of `chuk-tool-processor`'s code
execution features and how to run untrusted code safely.

## TL;DR

- **`CodeSandbox` is not a security sandbox.** It runs Python in the host
  process via `exec()` with the host's privileges. The restricted `__builtins__`
  it installs only limits *name resolution* — it does **not** contain code.
- Because of this, `CodeSandbox.execute()` is **disabled by default** and raises
  `UnsafeExecutionError` unless you construct it with
  `CodeSandbox(allow_unsafe_execution=True)`.
- Enable it **only for code you fully trust** (e.g. code you authored). **Never**
  pass untrusted or LLM-generated code to it and expect containment.
- To run untrusted / LLM-generated code, use real OS/process-level isolation (a
  locked-down subprocess or container, or a WASM interpreter). See
  [Running untrusted code safely](#running-untrusted-code-safely).

## Why `CodeSandbox` is not a boundary

`CodeSandbox._build_safe_globals()` builds a small allow-listed `__builtins__`
dict (no `__import__`, `open`, `eval`, `exec`) and runs the caller's code with
`exec(compile(code, ...), safe_globals, ...)`.

Restricting `__builtins__` only affects *bare name lookups* in the global
namespace. It does nothing to stop attribute (`.`) access on objects the code
can already construct — which Python's object model always permits. Through
ordinary attribute access, code can walk the object/class graph to reach other
classes and modules already loaded in the process (including ones that can spawn
OS processes), using no `import` and none of the blocked builtins.

This is a well-known limitation of "restricted `exec`" sandboxes in CPython. It
is **not** fixable by:

- adding more names to the builtins allow-list, or
- denylisting specific introspection attributes (`__class__`, `__bases__`,
  `__subclasses__`, `__mro__`, `__globals__`, and friends) — these have further
  documented bypasses and give a false sense of safety.

The only credible mitigation is an isolation boundary outside the interpreter.

## What `CodeSandbox` *is* for

It is a convenience layer for orchestrating **trusted** tool-calling code in a
single execution context (fewer LLM round-trips for control flow). Treat it like
`exec()` with a curated namespace: fine for code you wrote, unsafe for anything
you didn't.

```python
from chuk_tool_processor.execution import CodeSandbox

sandbox = CodeSandbox(timeout=30.0, allow_unsafe_execution=True)  # trusted only
result = await sandbox.execute(my_trusted_code, namespace="math")
```

If you call `execute()` without `allow_unsafe_execution=True`, you get:

```
UnsafeExecutionError: CodeSandbox.execute() is disabled by default because it is
NOT a security boundary ...
```

## Running untrusted code safely

If executing untrusted or LLM-generated code is a genuine requirement, use
[`IsolatedCodeRunner`](./isolated_execution.md) instead of `CodeSandbox`. It runs
the code behind a real OS/runtime boundary and brokers tool access back to the
host over a single audited channel (JSON, never pickle) — exactly the "narrow,
audited RPC surface" pattern below, already built:

```python
from chuk_tool_processor.execution.isolation import IsolatedCodeRunner, DockerBackend

runner = IsolatedCodeRunner(DockerBackend(), namespace="math")
result = await runner.run(untrusted_code)   # no network, no host fs, tools brokered
```

Backends, in roughly increasing isolation strength:

1. **`SeatbeltBackend`** (macOS) / **`BubblewrapBackend`** (Linux) — OS sandbox:
   resource limits, no network, filesystem confined, only the broker channel open.
2. **`DockerBackend`** — one throwaway container per run (`--network none`,
   read-only root, dropped caps, memory/pids limits); works anywhere Docker/Podman
   runs. Combine with a gVisor/Firecracker runtime for microVM-grade isolation.

A WASM backend (guest cannot reach host syscalls at all) is in development on a
separate branch.

In every case tools are exposed through the host-side broker (allowlist + call
ceiling + per-run token), never by handing the guest live Python objects. See
[isolated_execution.md](./isolated_execution.md) for the full model, limits, and
how to add a custom backend.

## Reporting security issues

Please report suspected vulnerabilities privately to the maintainers rather than
opening a public issue.

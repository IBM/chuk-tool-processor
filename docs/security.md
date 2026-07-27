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

If executing untrusted or LLM-generated code is a genuine requirement, put a real
boundary between that code and your process. Standard options, roughly in order
of increasing isolation:

1. **Separate subprocess as a locked-down user**, with `seccomp`, resource
   limits (`RLIMIT_*`), no network namespace, and a read-only / ephemeral
   filesystem.
2. **Container / microVM isolation** — gVisor, Firecracker, or an equivalent
   sandbox runtime, one throwaway instance per execution.
3. **WebAssembly interpreter** (e.g. a WASM-compiled Python) so the guest code
   cannot reach host syscalls at all.

In every case, expose tools to the guest through a narrow, audited RPC surface
rather than by handing it live Python objects.

## Reporting security issues

Please report suspected vulnerabilities privately to the maintainers rather than
opening a public issue.

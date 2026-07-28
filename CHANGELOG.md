# Changelog

All notable changes to this project are documented here. This project follows
[Semantic Versioning](https://semver.org/).

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

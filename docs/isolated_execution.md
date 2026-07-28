# Isolated Code Execution

`IsolatedCodeRunner` runs **untrusted or LLM-generated** Python behind a real
OS/runtime boundary, while still letting that code call your registered tools.
It is the safe counterpart to
[`CodeSandbox`](./programmatic_execution.md), which runs code in-process with no
isolation and is **trusted-code-only** (see [security.md](./security.md)).

- **`CodeSandbox`** — in-process `exec()`, no boundary. Only for code you wrote.
- **`IsolatedCodeRunner`** — code runs inside a container / macOS Seatbelt /
  Linux bubblewrap / Windows AppContainer sandbox; tools are brokered back to the
  host over one audited channel. For code you did **not** write. (A WASM backend
  is in development on a separate branch.)

## Why not just reuse the subprocess strategy?

The existing `IsolatedStrategy` (`subprocess_strategy.py`) runs *registered tool
calls* in a `ProcessPoolExecutor` using **pickle**. That is fault isolation, not
security isolation: same OS user, no seccomp/rlimits/namespaces, and unpickling
data across the boundary is itself unsafe. It never executes the orchestration
code string. `IsolatedCodeRunner` is a separate mechanism built for untrusted
*code*.

## Architecture

The hard, backend-independent part is the **tool bridge**: untrusted code must
reach host tools (which hold real credentials) without any other host access.

```
host process (trusted)                     isolated guest (untrusted)
┌───────────────────────────┐              ┌──────────────────────────┐
│ IsolatedCodeRunner        │              │ guest_bootstrap.py       │
│  ├─ owns the registry     │              │  ├─ exec(user code)      │
│  ├─ ToolBroker (RPC srv)  │◄──JSON RPC───┤  └─ async tool proxies ──┼─┐
│  │   • list_tools()       │  1 unix fd   │      call_tool(name,kw)  │ │
│  │   • call_tool() ───────┼─ runs REAL   └──────────────────────────┘ │
│  │     (token+allowlist)  │  tool here          ▲                      │
│  └─ IsolationBackend ─────┼─ spawns guest ──────┘                      │
└───────────────────────────┘   with limits; nothing else crosses ◄─────┘
```

Invariants:

- **The broker channel is the only hole.** Network, filesystem, and host
  processes are denied by the backend; the guest can only reach the socket.
- **JSON on the wire, never pickle.** The guest is untrusted; unpickling
  guest-controlled bytes on the host would defeat the whole exercise.
- **Policy is enforced host-side.** The per-run token, the tool allowlist, and
  the `max_tool_calls` ceiling live in `ToolBroker`, not in the guest.
- **The return value is untrusted data.** `IsolatedResult.value` is JSON
  produced by untrusted code — validate before acting on it.

## Quick start

```python
from chuk_tool_processor.execution.isolation import (
    IsolatedCodeRunner, DockerBackend, IsolationLimits,
)

runner = IsolatedCodeRunner(
    DockerBackend(),                       # or SeatbeltBackend(), BubblewrapBackend()
    namespace="math",                      # tools the guest may call
    limits=IsolationLimits(wall_timeout=30.0, allow_network=False),
)

result = await runner.run("""
total = 0
for i in range(1, 6):
    r = await add(a=str(total), b=str(i))   # 'add' is a brokered host tool
    total = r["sum"]
return total
""")

print(result.ok, result.value, result.tool_calls)   # True 15 5
```

Pick the backend that matches where you deploy; the runner refuses a
non-isolating backend unless you pass `allow_no_isolation=True`.

## Backends

| Backend | Isolation | Platform | Needs | Notes |
|---|---|---|---|---|
| `DockerBackend` | Strong§ | Linux Docker host | `docker`/`podman` CLI + daemon | throwaway container, `--network none`, read-only root, dropped caps, runs as host uid |
| `SeatbeltBackend` | Strong* | macOS | `sandbox-exec` (built in) | no inet, no fs-writes outside work/tmp, secret dirs unreadable |
| `BubblewrapBackend` | Strong¶ | Linux | `bwrap` binary | user/mount/pid/net namespaces |
| `WindowsBackend` | Strong‡ | Windows | `pywin32` | AppContainer + Job Object (+ low integrity) |
| `LocalProcessBackend` | **None** | any | — | dev/testing only; runner refuses it without `allow_no_isolation=True` |

§ `DockerBackend` runs each guest in a throwaway `docker run --rm` container
(pre-pulled image + `--pull never`, `--network none`, read-only root, `--cap-drop
ALL`, memory/pids limits) **as the host uid**, and is CI-verified end-to-end on
native Linux. The host↔guest broker uses a bind-mounted unix socket, which the
VM-based file sharing in **Docker Desktop / podman-machine (macOS, Windows)**
does not support (`connect()` returns `ENOTSUP`) — run it on a native Linux
Docker host (servers, CI, WSL2).

¶ `BubblewrapBackend` requires a Linux host with **unprivileged user namespaces**
enabled. It is not exercised in GitHub CI because the runners block the netlink
call `bwrap` uses to bring up loopback in a fresh network namespace
(`RTM_NEWADDR: Operation not permitted`); verify it on a real Linux host.

\* Seatbelt reliably blocks network and filesystem *writes*; read confinement is
best-effort (broad reads with known secret dirs denied) because a strict read
allowlist aborts CPython. The denied secret paths are configurable —
`SeatbeltBackend(deny_read_paths=..., add_deny_read_paths=...)` — defaulting to
`DEFAULT_DENY_READ_PATHS` (`~/.ssh`, `~/.aws`, cloud creds, keychains, …).
`sandbox-exec` is deprecated by Apple but functional.

‡ `WindowsBackend` is **experimental** — the AppContainer + Job Object launch and
the named-pipe broker transport are verified via Windows CI, not on the author's
machine; see below.

A **WASM backend** (wasmtime/WASI — the strongest boundary by construction) is in
development on a separate branch; it is not part of this release.

Install notes: the Docker, Seatbelt, and bubblewrap backends need no Python
dependencies (they shell out to the respective binary). Windows needs
`pip install chuk-tool-processor[isolation-windows]` (pywin32).

## Windows backend (experimental)

`WindowsBackend` is the Windows analogue of Seatbelt:

    macOS Seatbelt profile  ≈  Windows AppContainer + Job Object (+ low integrity)

- The guest runs as an **AppContainer** with **no capabilities** — so no network
  and no access to the user's files by construction — at low integrity.
- It is placed in a **Job Object** that caps memory and active processes and
  kills the whole tree on close.
- Because AppContainers can't use unix sockets and loopback is blocked for them
  without a network-capability hole, the broker channel is a **named pipe** whose
  security descriptor grants `ALL APPLICATION PACKAGES` (a local IPC object, not
  the network). The staging dir is granted to the same SID (via `icacls`) so the
  guest can read the bootstrap and write its output.

It is availability-gated on Windows + `pywin32`, so `is_available()` is `False`
elsewhere and the runner won't select it. The implementation is a first draft
verified through the `isolation` GitHub Actions workflow (which installs pywin32
and sets `CTP_TEST_ISOLATION_WINDOWS=1`); details may change as CI exercises it.

## Resource limits

`IsolationLimits` (all enforced as far as the backend allows):

| Field | Default | Meaning |
|---|---|---|
| `wall_timeout` | 30s | hard wall-clock kill (always enforced) |
| `cpu_timeout` | 15s | CPU-seconds ceiling (RLIMIT_CPU / container) |
| `memory_bytes` | 256 MiB | memory ceiling (RLIMIT_AS / `--memory`) |
| `max_output_bytes` | 64 KiB | captured stdout/stderr cap |
| `max_tool_calls` | 100 | broker rejects calls beyond this |
| `max_processes` | 64 | RLIMIT_NPROC / `--pids-limit` |
| `allow_network` | `False` | deny all guest network except the broker channel |

## Security model

What the boundary is expected to stop, and where enforced:

- **Arbitrary host code execution / sandbox escape** → the backend (container,
  namespace, Seatbelt, or AppContainer). Even a full `CodeSandbox`-style
  `__subclasses__()` escape only reaches the *guest's* interpreter, which has no
  host access beyond the broker channel.
- **Reaching tools you didn't expose** → `ToolBroker` allowlist + namespace.
- **Tool-call flooding** → `max_tool_calls`.
- **Network exfiltration** → `allow_network=False` (default).
- **Reading host secrets / writing host files** → backend filesystem policy.
- **Runaway CPU/memory/fork bombs** → limits (`wall_timeout`, `cpu_timeout`,
  `memory_bytes`, `max_processes`).

Residual risks: the broker still runs *your* tools with their real privileges on
the guest's behalf — expose only tools that are safe to call with
attacker-chosen arguments. Seatbelt read-confinement is best-effort. The guest's
return value is untrusted.

## Writing a custom backend

Implement the `IsolationBackend` protocol:

```python
class MyBackend:
    name = "mybackend"
    provides_isolation = True            # False => runner requires allow_no_isolation

    def is_available(self) -> bool: ...
    async def run_guest(self, job, *, host_socket_path) -> GuestOutcome: ...
```

Most OS-level backends should subclass `SubprocessBackend` and override just
`_wrapper_argv()` (the sandbox launcher prefix) and, if paths are remapped,
`_guest_ctx()` — see `DockerBackend` for the remapping pattern.
```

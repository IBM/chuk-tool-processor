# tests/execution/isolation/test_backends.py
"""
Backend-specific tests.

Argv/profile construction is pure and tested everywhere. Full integration for
Docker and bubblewrap is gated on the runtime actually being present and usable
(daemon up / correct platform), so these are skipped in environments without it.
"""

from __future__ import annotations

import os
import shutil
import subprocess

import pytest

from chuk_tool_processor.execution.isolation import (
    BubblewrapBackend,
    DockerBackend,
    IsolatedCodeRunner,
    IsolationLimits,
    SeatbeltBackend,
)
from chuk_tool_processor.execution.isolation.backend import GuestJob
from tests.execution.isolation.test_runner import ADD_LOOP, StubRegistry


def _job(**limit_kw) -> GuestJob:
    return GuestJob(code="x", token="tok" + "0" * 32, limits=IsolationLimits(**limit_kw))


# --------------------------------------------------------------------------- #
# Docker argv construction
# --------------------------------------------------------------------------- #
class TestDockerArgv:
    def test_guest_paths_are_container_mounts(self):
        ctx = DockerBackend()._guest_ctx("/work", "/host/sock/broker.sock")
        assert ctx.bootstrap_guest == "/ctguest/guest_bootstrap.py"
        assert ctx.job_guest == "/ctguest/job.json"
        assert ctx.socket_guest == "/ctsock/broker.sock"

    def test_wrapper_argv_hardening(self):
        b = DockerBackend(image="python:3.12-slim")
        ctx = b._guest_ctx("/work", "/host/sock/broker.sock")
        argv = b._wrapper_argv(ctx, _job(memory_bytes=1_000_000, max_processes=7))
        assert argv[:3] == ["docker", "run", "--rm"]
        assert "--network" in argv and "none" in argv
        assert "--cap-drop" in argv and "ALL" in argv
        assert "no-new-privileges" in argv
        assert "--read-only" in argv
        assert "1000000" in argv  # --memory
        assert "7" in argv  # --pids-limit
        assert "/work:/ctguest:ro" in argv
        assert "/host/sock:/ctsock" in argv
        assert argv[-1] == "python:3.12-slim"
        assert f"ctiso-{_job().token[:24]}" in argv

    def test_allow_network_omits_none(self):
        b = DockerBackend()
        ctx = b._guest_ctx("/work", "/host/sock/broker.sock")
        argv = b._wrapper_argv(ctx, _job(allow_network=True))
        # No "--network none" pairing when network is allowed.
        assert not ("--network" in argv and "none" in argv)


# --------------------------------------------------------------------------- #
# Bubblewrap argv construction (pure; runs on any OS)
# --------------------------------------------------------------------------- #
class TestBubblewrapArgv:
    def test_wrapper_argv(self):
        b = BubblewrapBackend()
        ctx = b._guest_ctx("/work", "/host/sock/broker.sock")
        argv = b._wrapper_argv(ctx, _job())
        assert argv[0] == "bwrap"
        assert "--die-with-parent" in argv
        assert "--unshare-net" in argv
        assert "--tmpfs" in argv
        assert "--ro-bind" in argv
        assert argv[-1] == "--"

    def test_allow_network_keeps_net(self):
        b = BubblewrapBackend()
        ctx = b._guest_ctx("/work", "/host/sock/broker.sock")
        argv = b._wrapper_argv(ctx, _job(allow_network=True))
        assert "--unshare-net" not in argv


# --------------------------------------------------------------------------- #
# Seatbelt denylist configuration (pure profile construction; runs on any OS)
# --------------------------------------------------------------------------- #
class TestSeatbeltDenyReadPaths:
    def _profile(self, backend: SeatbeltBackend) -> str:
        ctx = backend._guest_ctx("/work", "/host/sock/broker.sock")
        return backend._profile(ctx, _job())

    def test_defaults_include_ssh_and_aws(self):
        prof = self._profile(SeatbeltBackend())
        assert os.path.realpath(os.path.expanduser("~/.ssh")) in prof
        assert os.path.realpath(os.path.expanduser("~/.aws")) in prof

    def test_add_extends_defaults(self):
        prof = self._profile(SeatbeltBackend(add_deny_read_paths=["/data/secrets"]))
        assert "/data/secrets" in prof
        assert os.path.realpath(os.path.expanduser("~/.ssh")) in prof  # defaults still present

    def test_deny_read_paths_overrides_defaults(self):
        prof = self._profile(SeatbeltBackend(deny_read_paths=["~/only-this"]))
        assert os.path.realpath(os.path.expanduser("~/only-this")) in prof
        assert os.path.realpath(os.path.expanduser("~/.ssh")) not in prof  # defaults replaced


# --------------------------------------------------------------------------- #
# Integration (gated) — Docker
# --------------------------------------------------------------------------- #
def _docker_up() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=10).returncode == 0
    except Exception:
        return False


@pytest.mark.skipif(not _docker_up(), reason="requires a running Docker daemon")
class TestDockerIntegration:
    @pytest.mark.asyncio
    async def test_add_loop(self):
        runner = IsolatedCodeRunner(
            DockerBackend(),
            registry=StubRegistry(),
            namespace="math",
            limits=IsolationLimits(wall_timeout=60.0),
        )
        r = await runner.run(ADD_LOOP)
        assert r.ok is True and r.value == 15 and r.tool_calls == 5

    @pytest.mark.asyncio
    async def test_network_blocked(self):
        runner = IsolatedCodeRunner(
            DockerBackend(),
            registry=StubRegistry(),
            namespace="math",
            limits=IsolationLimits(wall_timeout=60.0),
        )
        code = "import socket\nsocket.create_connection(('1.1.1.1', 53), timeout=3)\nreturn 'NET_OK'"
        r = await runner.run(code)
        assert r.ok is False


@pytest.mark.skipif(not BubblewrapBackend().is_available(), reason="requires Linux bwrap")
class TestBubblewrapIntegration:
    @pytest.mark.asyncio
    async def test_add_loop(self):
        runner = IsolatedCodeRunner(
            BubblewrapBackend(),
            registry=StubRegistry(),
            namespace="math",
            limits=IsolationLimits(wall_timeout=30.0),
        )
        r = await runner.run(ADD_LOOP)
        assert r.ok is True and r.value == 15

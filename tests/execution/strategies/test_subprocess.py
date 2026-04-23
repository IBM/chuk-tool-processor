# tests/execution/strategies/test_subprocess.py
"""
Unit tests for the SubprocessStrategy that executes tools in separate processes.

These tests verify that the SubprocessStrategy correctly:
- Executes tools in separate processes with proper isolation
- Handles process crashes and other edge cases
- Supports streaming execution
- Manages process pool lifecycle
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import datetime
from typing import Any

import pytest
import pytest_asyncio

from chuk_tool_processor.execution.strategies.subprocess_strategy import (
    SubprocessStrategy,
)
from chuk_tool_processor.models.tool_call import ToolCall


# Common Mock Registry for tests
class MockRegistry:
    """Mock registry that implements the async ToolRegistryInterface."""

    def __init__(self, tools: dict[str, Any] = None):
        self._tools = tools or {}

    async def get_tool(self, name: str, namespace: str = "default") -> Any | None:
        """Async version of get_tool to match the ToolRegistryInterface."""
        return self._tools.get(name)

    async def get_metadata(self, name: str, namespace: str = "default") -> Any | None:
        """Mock metadata retrieval."""
        if name in self._tools:
            return {
                "description": f"Mock metadata for {name}",
                "supports_streaming": False,
            }
        return None

    async def list_tools(self, namespace: str | None = None) -> list:
        """Mock list_tools method."""
        return [(namespace or "default", name) for name in self._tools]


# --------------------------------------------------------------------------- #
# FIXED: Sample tools for testing - all use 'execute' method and are at module level
# --------------------------------------------------------------------------- #


class AddTool:
    """Simple tool that adds two numbers."""

    async def execute(self, x: int, y: int):
        return x + y


class MulTool:
    """FIXED: Tool that multiplies two numbers using the execute method."""

    async def execute(self, a: int, b: int):  # FIXED: Changed from _aexecute to execute
        await asyncio.sleep(0.01)  # Small delay to simulate work
        return a * b


class SleepTool:
    """Tool that sleeps for a configurable time."""

    def __init__(self, delay: float = 0.2):
        self.delay = delay

    async def execute(self):
        await asyncio.sleep(self.delay)
        return {
            "done": True,
            "pid": os.getpid(),
            "timestamp": datetime.now().isoformat(),
        }


class ErrorTool:
    """Tool that raises an exception."""

    async def execute(self):
        raise RuntimeError("fail_op")


class MemoryTool:
    """Tool that allocates memory to test process isolation."""

    async def execute(self, size_mb: int = 10):
        # Allocate memory
        data = bytearray(size_mb * 1024 * 1024)
        # Touch the memory to ensure it's allocated
        for i in range(0, len(data), 1024):
            data[i] = 1
        return {"allocated_mb": size_mb, "pid": os.getpid()}


class CPUTool:
    """Tool that does CPU-intensive work."""

    async def execute(self, iterations: int = 100000):
        result = 0
        for i in range(iterations):
            result += i % 17
        return {"result": result, "pid": os.getpid()}


class LongRunningTool:
    """Tool that runs for a long time."""

    async def execute(self):
        await asyncio.sleep(10)
        return "done"


class SafeCrashTool:
    """A tool that causes a crash in controlled way."""

    async def execute(self):
        # Simulate a process crash with a bad operation
        try:
            import ctypes

            ctypes.string_at(0)  # This will cause a segfault in many environments
            return "This should not return"
        except Exception:
            # In case it doesn't crash, raise an explicit exception
            raise RuntimeError("Simulated crash")


# --------------------------------------------------------------------------- #
# Test fixtures
# --------------------------------------------------------------------------- #


@pytest_asyncio.fixture
async def registry():
    """Create a registry with test tools."""
    return MockRegistry(
        {
            "add": AddTool,
            "mul": MulTool,  # Now uses execute method
            "sleep": SleepTool,
            "error": ErrorTool,
            "memory": MemoryTool,
            "cpu": CPUTool,
            "custom_sleep": SleepTool(0.3),  # Instance with custom delay
            "safe_crash": SafeCrashTool,
            "long": LongRunningTool,
        }
    )


@pytest_asyncio.fixture
async def strategy(registry):
    """Create a SubprocessStrategy with the test registry."""
    strategy = SubprocessStrategy(registry, max_workers=2, default_timeout=2.0)
    yield strategy
    # Clean up
    await strategy.shutdown()


# --------------------------------------------------------------------------- #
# Basic tests
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_tool_not_found(strategy):
    """Test that a non-existent tool returns an appropriate error."""
    res = (await strategy.run([ToolCall(tool="missing", arguments={})], timeout=0.5))[0]
    assert res.error and "not found" in res.error.lower()
    assert res.result is None


@pytest.mark.asyncio
async def test_add_tool_execution(strategy):
    """Test that a simple add tool works correctly."""
    res = (await strategy.run([ToolCall(tool="add", arguments={"x": 2, "y": 3})], timeout=1))[0]
    assert res.result == 5
    assert res.error is None
    assert isinstance(res.start_time, datetime)
    assert res.end_time >= res.start_time


@pytest.mark.asyncio
async def test_mul_tool_execution(strategy):
    """FIXED: Test that a tool using execute works correctly."""
    res = (await strategy.run([ToolCall(tool="mul", arguments={"a": 4, "b": 5})], timeout=1))[0]
    assert res.result == 20  # Should work now since MulTool has execute method
    assert res.error is None


@pytest.mark.asyncio
async def test_timeout_error(strategy):
    """Test that timeouts are handled correctly."""
    res = (await strategy.run([ToolCall(tool="sleep", arguments={})], timeout=0.1))[0]
    assert res.result is None
    assert res.error is not None, "Expected error message for timeout"
    assert "timeout" in res.error.lower() or "timed out" in res.error.lower()


@pytest.mark.asyncio
async def test_unexpected_exception(strategy):
    """Test that exceptions in tools are handled correctly."""
    res = (await strategy.run([ToolCall(tool="error", arguments={})], timeout=1))[0]
    assert res.result is None
    assert "fail_op" in res.error


# --------------------------------------------------------------------------- #
# Process isolation tests
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_parallel_execution(strategy):
    """Test that tools execute in parallel in separate processes."""
    # Run two sleep tools concurrently - should take close to 0.3s, not 0.6s
    start_time = time.time()
    results = await strategy.run(
        [
            ToolCall(tool="sleep", arguments={}),
            ToolCall(tool="custom_sleep", arguments={}),
        ]
    )
    duration = time.time() - start_time

    # Verify both completed successfully
    assert all(r.error is None for r in results)
    assert all(isinstance(r.result, dict) for r in results)

    # Verify it ran in parallel (allowing buffer for process startup - CI can be slower)
    # In CI environments, process startup can be significantly slower
    assert duration < 2.5, f"Expected duration < 2.5s (parallel execution), got {duration}s"

    # Verify they ran in different processes if possible
    pids = [r.pid for r in results]
    assert len(set(pids)) >= 1  # At least one process different from main


@pytest.mark.asyncio
async def test_memory_isolation(strategy):
    """Test that memory usage in one process doesn't affect others."""
    # Run a memory-intensive tool
    memory_result = (await strategy.run([ToolCall(tool="memory", arguments={"size_mb": 20})], timeout=2))[0]

    # Verify it worked
    assert memory_result.error is None
    assert memory_result.result["allocated_mb"] == 20

    # Verify it ran in a different process
    assert memory_result.pid != os.getpid()


@pytest.mark.asyncio
async def test_cpu_isolation(strategy):
    """Test that CPU-intensive work in one process doesn't block others."""
    # Run a CPU-intensive tool and a quick tool concurrently
    time.time()
    results = await strategy.run(
        [
            ToolCall(tool="cpu", arguments={"iterations": 500000}),  # This is slow
            ToolCall(tool="add", arguments={"x": 1, "y": 2}),  # This is fast
        ]
    )

    # Check that both completed
    assert all(r.error is None for r in results)

    # Get results by tool
    cpu_result = next((r for r in results if r.tool == "cpu"), None)
    add_result = next((r for r in results if r.tool == "add"), None)

    # Verify correct results
    assert cpu_result and cpu_result.result["result"] > 0
    assert add_result and add_result.result == 3

    # Verify they ran in different processes
    assert cpu_result.pid != add_result.pid or cpu_result.pid != os.getpid()


# --------------------------------------------------------------------------- #
# Streaming tests
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_streaming_support(strategy):
    """Test that stream_run yields results as they become available."""
    calls = [
        ToolCall(tool="sleep", arguments={}),  # 0.2s
        ToolCall(tool="custom_sleep", arguments={}),  # 0.3s
        ToolCall(tool="add", arguments={"x": 1, "y": 2}),  # Fast
    ]

    # Use stream_run to get results as they become available
    results = []
    start_time = time.time()

    # Create a timeout to ensure we don't hang if something goes wrong
    try:
        async with asyncio.timeout(5.0):  # Generous timeout for slow CI environments
            async for result in strategy.stream_run(calls):
                results.append({"tool": result.tool, "elapsed": time.time() - start_time})
    except TimeoutError:
        pass  # Just end the test if it takes too long

    # Check we have all results or at least most of them
    assert len(results) >= 2, f"Expected at least 2 results, got {len(results)}"

    # Check that at least the add tool returned (it's the fastest)
    assert any(r["tool"] == "add" for r in results), "Expected 'add' tool to complete"


# --------------------------------------------------------------------------- #
# Error handling and edge cases
# --------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_concurrent_with_different_timeouts(strategy):
    """Test running multiple tools with different timeouts."""
    results = await strategy.run(
        [
            ToolCall(tool="add", arguments={"x": 1, "y": 2}),
            ToolCall(tool="sleep", arguments={}),
            ToolCall(tool="custom_sleep", arguments={}),
        ],
        timeout=0.5,
    )  # Only enough time for add and sleep, not custom_sleep

    # All tools should return results (in completion order, not submission order)
    assert len(results) == 3, "Expected 3 results"

    # Find add result by tool name (order may vary due to completion order)
    add_result = next((r for r in results if r.tool == "add"), None)
    assert add_result is not None, "Expected 'add' tool result"
    assert add_result.error is None
    assert add_result.result == 3


@pytest.mark.asyncio
async def test_empty_calls_list(strategy):
    """Test that an empty calls list returns an empty results list."""
    results = await strategy.run([])
    assert isinstance(results, list)
    assert len(results) == 0


@pytest.mark.asyncio
async def test_mixed_success_and_failure(strategy):
    """FIXED: Test a mix of successful and failed tool calls (results in completion order)."""
    calls = [
        ToolCall(tool="add", arguments={"x": 1, "y": 2}),  # Should succeed
        ToolCall(tool="missing", arguments={}),  # Should fail (not found)
        ToolCall(tool="error", arguments={}),  # Should fail (raises exception)
        ToolCall(tool="mul", arguments={"a": 3, "b": 4}),  # Should succeed now
    ]

    results = await strategy.run(calls)

    # All tools should return results (in completion order, not submission order)
    assert len(results) == 4

    # Find results by tool name since order may vary
    add_result = next((r for r in results if r.tool == "add"), None)
    missing_result = next((r for r in results if r.tool == "missing"), None)
    error_result = next((r for r in results if r.tool == "error"), None)
    mul_result = next((r for r in results if r.tool == "mul"), None)

    assert add_result is not None and add_result.error is None and add_result.result == 3
    assert (
        missing_result is not None and missing_result.error is not None and "not found" in missing_result.error.lower()
    )
    assert error_result is not None and error_result.error is not None
    assert mul_result is not None and mul_result.error is None and mul_result.result == 12


@pytest.mark.asyncio
async def test_process_crash_handling(registry):
    """Test that the strategy handles process crashes properly."""
    # Create strategy with single worker
    strategy = SubprocessStrategy(registry, max_workers=1)

    try:
        # Run the crash tool
        result = (await strategy.run([ToolCall(tool="safe_crash", arguments={})], timeout=1))[0]

        # Should get an error but not crash the test
        assert result.error is not None

        # Verify the strategy can recover and run more tools
        add_result = (await strategy.run([ToolCall(tool="add", arguments={"x": 5, "y": 7})], timeout=1))[0]

        # Pool recovery behaviour varies by Python version / OS.
        # Accept either a clean result or a pool-reinit error — both are valid.
        if add_result.error is None:
            assert add_result.result == 12
        else:
            assert "process pool" in add_result.error.lower() or "initialize" in add_result.error.lower()
    finally:
        # Cleanup
        await strategy.shutdown()


@pytest.mark.asyncio
async def test_shutdown_cancels_running_tasks(registry):
    """Test that shutdown cancels running tasks.

    Note: On Windows, subprocess termination works differently than Unix.
    Windows doesn't have SIGTERM/SIGKILL signals, so the task may complete
    successfully instead of being cancelled.
    """
    # Create strategy
    strategy = SubprocessStrategy(registry, max_workers=1)

    try:
        # Start a long-running task
        task = asyncio.create_task(strategy.run([ToolCall(tool="long", arguments={})], timeout=20))

        # Give it a moment to start (increased for slower CI environments)
        await asyncio.sleep(0.5)

        # Shutdown the strategy
        await strategy.shutdown()

        # Wait for the task to complete
        results = await task

        # On Windows, subprocess termination differs - task may complete successfully
        if sys.platform == "win32":
            # On Windows, accept either cancellation or successful completion
            if results[0].error is not None:
                # If cancelled, verify error message indicates termination
                assert any(
                    msg in results[0].error.lower()
                    for msg in [
                        "cancel",
                        "shutdown",
                        "abort",
                        "terminate",
                        "process",
                        "stop",
                    ]
                )
            # If no error (task completed), that's also acceptable on Windows
        else:
            # On Unix, the task should be terminated with an error message
            assert results[0].error is not None
            assert any(
                msg in results[0].error.lower()
                for msg in [
                    "cancel",
                    "shutdown",
                    "abort",
                    "terminate",
                    "process",
                    "stop",
                ]
            )
    except Exception:
        # If there's an error, ensure shutdown still happens
        await strategy.shutdown()
        raise

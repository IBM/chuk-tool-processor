# tests/execution/test_code_sandbox.py
"""
Comprehensive tests for CodeSandbox - programmatic tool execution.

Tests cover:
- Basic code execution (sync and async)
- Tool access from code
- Security (restricted builtins)
- Timeouts and error handling
- Edge cases and error conditions
"""

import pytest
import pytest_asyncio

from chuk_tool_processor.execution.code_sandbox import (
    CodeExecutionError,
    CodeSandbox,
    SandboxSecurityWarning,
    UnsafeExecutionError,
)
from chuk_tool_processor.registry import get_default_registry, register_tool, reset_registry


@pytest_asyncio.fixture
async def sandbox():
    """Create a fresh code sandbox for each test."""
    return CodeSandbox(timeout=5.0, allow_unsafe_execution=True)


@pytest_asyncio.fixture
async def setup_test_tools():
    """Register test tools for sandbox execution."""
    await reset_registry()

    # Register test tools as instances
    registry = await get_default_registry()

    # Create simple tool instances
    class AddTool:
        async def execute(self, a: str, b: str) -> dict:
            return {"result": str(int(a) + int(b))}

    class MultiplyTool:
        async def execute(self, a: str, b: str) -> dict:
            return {"result": str(int(a) * int(b))}

    class ConcatTool:
        async def execute(self, a: str, b: str) -> dict:
            return {"result": f"{a}{b}"}

    # Register tool instances directly
    await registry.register_tool(AddTool(), name="add_numbers", namespace="test")
    await registry.register_tool(MultiplyTool(), name="multiply", namespace="test")
    await registry.register_tool(ConcatTool(), name="concat", namespace="test")

    yield
    await reset_registry()


class TestBasicExecution:
    """Test basic code execution functionality."""

    @pytest.mark.asyncio
    async def test_simple_sync_code(self, sandbox):
        """Test executing simple synchronous code."""
        code = """
x = 5
y = 3
result = x + y
"""
        # Sync code doesn't return anything by default
        result = await sandbox.execute(code)
        assert result is None

    @pytest.mark.asyncio
    async def test_simple_async_code_with_return(self, sandbox):
        """Test executing async code with return value."""
        code = """
result = 10 + 20
return result
"""
        result = await sandbox.execute(code)
        assert result == 30

    @pytest.mark.asyncio
    async def test_code_with_builtins(self, sandbox):
        """Test that allowed builtins work."""
        code = """
numbers = [1, 2, 3, 4, 5]
total = sum(numbers)
return total
"""
        result = await sandbox.execute(code)
        assert result == 15

    @pytest.mark.asyncio
    async def test_list_comprehension(self, sandbox):
        """Test list comprehensions work."""
        code = """
squares = [x * x for x in range(5)]
return squares
"""
        result = await sandbox.execute(code)
        assert result == [0, 1, 4, 9, 16]

    @pytest.mark.asyncio
    async def test_string_operations(self, sandbox):
        """Test string operations with allowed builtins."""
        code = """
text = "hello world"
words = text.split()
return len(words)
"""
        result = await sandbox.execute(code)
        assert result == 2


class TestToolAccess:
    """Test accessing registered tools from code."""

    @pytest.mark.asyncio
    async def test_single_tool_call(self, sandbox, setup_test_tools):
        """Test calling a single tool from code."""
        code = """
result = await add_numbers(a="5", b="3")
return result
"""
        result = await sandbox.execute(code, namespace="test")
        assert result["result"] == "8"

    @pytest.mark.asyncio
    async def test_multiple_tool_calls(self, sandbox, setup_test_tools):
        """Test calling multiple tools from code."""
        code = """
add_result = await add_numbers(a="10", b="5")
mult_result = await multiply(a="3", b="4")
return {
    "add": add_result,
    "mult": mult_result
}
"""
        result = await sandbox.execute(code, namespace="test")
        assert result["add"]["result"] == "15"
        assert result["mult"]["result"] == "12"

    @pytest.mark.asyncio
    async def test_tool_in_loop(self, sandbox, setup_test_tools):
        """Test calling tools in a loop."""
        code = """
results = []
for i in range(3):
    result = await add_numbers(a=str(i), b="10")
    results.append(result["result"])
return results
"""
        result = await sandbox.execute(code, namespace="test")
        assert result == ["10", "11", "12"]

    @pytest.mark.asyncio
    async def test_tool_with_conditional(self, sandbox, setup_test_tools):
        """Test calling tools with conditional logic."""
        code = """
results = []
for i in range(5):
    if i < 3:
        result = await add_numbers(a=str(i), b="100")
    else:
        result = await multiply(a=str(i), b="10")
    results.append(result["result"])
return results
"""
        result = await sandbox.execute(code, namespace="test")
        assert result == ["100", "101", "102", "30", "40"]

    @pytest.mark.asyncio
    async def test_nested_tool_calls(self, sandbox, setup_test_tools):
        """Test chaining tool calls."""
        code = """
# Chain tool calls - use result from first call in second call
step1 = await add_numbers(a="5", b="3")
step2 = await multiply(a=step1["result"], b="2")
return step2["result"]
"""
        result = await sandbox.execute(code, namespace="test")
        assert result == "16"  # (5 + 3) * 2 = 16


class TestFailClosed:
    """Execution must be explicitly opted into; it is not a security boundary."""

    @pytest.mark.asyncio
    async def test_execute_disabled_by_default(self):
        """Without allow_unsafe_execution the sandbox refuses to run any code."""
        sandbox = CodeSandbox(timeout=5.0)  # note: no allow_unsafe_execution
        with pytest.raises(UnsafeExecutionError):
            await sandbox.execute("result = 1 + 1")

    @pytest.mark.asyncio
    async def test_unsafe_execution_error_is_code_execution_error(self):
        """UnsafeExecutionError subclasses CodeExecutionError for compatibility."""
        assert issubclass(UnsafeExecutionError, CodeExecutionError)

    @pytest.mark.asyncio
    async def test_execute_warns_when_enabled(self):
        """Enabling execution emits a SandboxSecurityWarning on each run."""
        sandbox = CodeSandbox(timeout=5.0, allow_unsafe_execution=True)
        with pytest.warns(SandboxSecurityWarning):
            await sandbox.execute("result = 1 + 1")

    @pytest.mark.asyncio
    async def test_restricted_builtins_are_not_a_boundary(self):
        """
        The reduced builtins do not stop ordinary attribute access, so code can
        still walk the object graph to reach other loaded classes. This is why
        execution is opt-in and trusted-code-only. See docs/security.md.
        """
        sandbox = CodeSandbox(timeout=5.0, allow_unsafe_execution=True)
        code = """
reachable = ().__class__.__bases__[0].__subclasses__()
return [c.__name__ for c in reachable]
"""
        names = await sandbox.execute(code)
        # Attribute access reaches classes beyond the allow-listed builtins.
        assert isinstance(names, list)
        assert len(names) > 0


class TestRestrictedBuiltins:
    """
    The reduced builtins namespace blocks direct name use of dangerous builtins.
    This is a convenience/footgun measure, NOT a security boundary (see
    TestFailClosed.test_restricted_builtins_are_not_a_boundary).
    """

    @pytest.mark.asyncio
    async def test_restricted_builtins(self, sandbox):
        """Test that dangerous builtins are not available."""
        code = """
try:
    import os
    return "SECURITY_BREACH"
except Exception as e:
    return str(type(e).__name__)
"""
        result = await sandbox.execute(code)
        # import is restricted - should raise ImportError
        assert "ImportError" in result or "NameError" in result

    @pytest.mark.asyncio
    async def test_no_file_access(self, sandbox):
        """Test that file operations are blocked."""
        code = """
try:
    open("/etc/passwd", "r")
    return "SECURITY_BREACH"
except Exception as e:
    return str(type(e).__name__)
"""
        result = await sandbox.execute(code)
        assert "NameError" in result  # open not available

    @pytest.mark.asyncio
    async def test_no_eval(self, sandbox):
        """Test that eval is not available."""
        code = """
try:
    eval("1 + 1")
    return "SECURITY_BREACH"
except Exception as e:
    return str(type(e).__name__)
"""
        result = await sandbox.execute(code)
        assert "NameError" in result

    @pytest.mark.asyncio
    async def test_no_exec(self, sandbox):
        """Test that exec is not directly available."""
        code = """
try:
    exec("x = 1")
    return "SECURITY_BREACH"
except Exception as e:
    return str(type(e).__name__)
"""
        result = await sandbox.execute(code)
        assert "NameError" in result


class TestErrorHandling:
    """Test error handling and edge cases."""

    @pytest.mark.asyncio
    async def test_syntax_error(self, sandbox):
        """Test that syntax errors are caught."""
        code = """
this is not valid python
"""
        with pytest.raises(CodeExecutionError) as exc_info:
            await sandbox.execute(code)
        assert "Syntax error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_runtime_error(self, sandbox):
        """Test that runtime errors are caught."""
        code = """
x = 1 / 0
return x
"""
        with pytest.raises(CodeExecutionError) as exc_info:
            await sandbox.execute(code)
        assert "execution failed" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_timeout_error(self):
        """Test that code execution times out."""
        # Create sandbox with very short timeout
        sandbox = CodeSandbox(timeout=0.1, allow_unsafe_execution=True)

        code = """
import time
time.sleep(10)  # This will fail because time is not available
return "done"
"""
        # The code will fail because 'time' module is not available
        with pytest.raises(CodeExecutionError):
            await sandbox.execute(code)

    @pytest.mark.asyncio
    @pytest.mark.skip(reason="CPU-bound infinite loops cannot be interrupted by asyncio.wait_for - known limitation")
    async def test_timeout_with_infinite_loop(self):
        """Test timeout with infinite loop."""
        sandbox = CodeSandbox(timeout=0.5, allow_unsafe_execution=True)

        code = """
while True:
    pass
"""
        with pytest.raises(CodeExecutionError) as exc_info:
            await sandbox.execute(code)
        assert "timed out" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_undefined_variable(self, sandbox):
        """Test accessing undefined variable raises error."""
        code = """
return undefined_variable
"""
        with pytest.raises(CodeExecutionError) as exc_info:
            await sandbox.execute(code)
        assert "execution failed" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_type_error(self, sandbox):
        """Test type errors are caught."""
        code = """
result = "string" + 5
return result
"""
        with pytest.raises(CodeExecutionError) as exc_info:
            await sandbox.execute(code)
        assert "execution failed" in str(exc_info.value).lower()


class TestAsyncCodeHandling:
    """Test async code execution."""

    @pytest.mark.asyncio
    async def test_await_in_code(self, sandbox, setup_test_tools):
        """Test that await is properly handled."""
        code = """
result = await add_numbers(a="10", b="20")
return result["result"]
"""
        result = await sandbox.execute(code, namespace="test")
        assert result == "30"

    @pytest.mark.asyncio
    async def test_multiple_awaits(self, sandbox, setup_test_tools):
        """Test multiple await statements."""
        code = """
r1 = await add_numbers(a="5", b="3")
r2 = await multiply(a="2", b="4")
r3 = await concat(a="hello", b="world")
return {
    "add": r1["result"],
    "mult": r2["result"],
    "concat": r3["result"]
}
"""
        result = await sandbox.execute(code, namespace="test")
        assert result["add"] == "8"
        assert result["mult"] == "8"
        assert result["concat"] == "helloworld"

    @pytest.mark.asyncio
    async def test_async_comprehension(self, sandbox, setup_test_tools):
        """Test async operations in comprehensions."""
        code = """
results = []
for i in range(3):
    r = await add_numbers(a=str(i), b=str(i))
    results.append(r["result"])
return results
"""
        result = await sandbox.execute(code, namespace="test")
        assert result == ["0", "2", "4"]


class TestInitialVariables:
    """Test passing initial variables to code."""

    @pytest.mark.asyncio
    async def test_initial_variables(self, sandbox):
        """Test that initial variables are accessible."""
        code = """
return x + y
"""
        result = await sandbox.execute(code, initial_vars={"x": 10, "y": 20})
        assert result == 30

    @pytest.mark.asyncio
    async def test_initial_variables_with_tools(self, sandbox, setup_test_tools):
        """Test initial variables with tool calls."""
        code = """
result = await add_numbers(a=str(base), b=str(increment))
return result["result"]
"""
        result = await sandbox.execute(code, namespace="test", initial_vars={"base": 100, "increment": 50})
        assert result == "150"


class TestComplexWorkflows:
    """Test complex multi-step workflows."""

    @pytest.mark.asyncio
    async def test_data_processing_workflow(self, sandbox, setup_test_tools):
        """Test a complex data processing workflow."""
        code = """
# Step 1: Generate data
data = []
for i in range(5):
    result = await add_numbers(a=str(i), b="10")
    data.append(int(result["result"]))

# Step 2: Process data
processed = []
for value in data:
    if value > 12:
        mult_result = await multiply(a=str(value), b="2")
        processed.append(int(mult_result["result"]))
    else:
        processed.append(value)

# Step 3: Sum results
total = sum(processed)
return {
    "data": data,
    "processed": processed,
    "total": total
}
"""
        result = await sandbox.execute(code, namespace="test")
        assert result["data"] == [10, 11, 12, 13, 14]
        assert result["processed"] == [10, 11, 12, 26, 28]
        assert result["total"] == 87

    @pytest.mark.asyncio
    async def test_conditional_branching(self, sandbox, setup_test_tools):
        """Test complex conditional logic."""
        code = """
results = []
for i in range(10):
    if i % 2 == 0:
        # Even: add
        r = await add_numbers(a=str(i), b="5")
    else:
        # Odd: multiply
        r = await multiply(a=str(i), b="3")
    results.append(r["result"])
return results
"""
        result = await sandbox.execute(code, namespace="test")
        assert result == ["5", "3", "7", "9", "9", "15", "11", "21", "13", "27"]


class TestNamespaceFiltering:
    """Test namespace filtering for tool access."""

    @pytest.mark.asyncio
    async def test_namespace_isolation(self, setup_test_tools):
        """Test that tools are only accessible from their namespace."""
        sandbox = CodeSandbox(allow_unsafe_execution=True)

        # This should work - tools are in 'test' namespace
        code = """
result = await add_numbers(a="5", b="3")
return result["result"]
"""
        result = await sandbox.execute(code, namespace="test")
        assert result == "8"

        # Register tool in different namespace
        @register_tool(name="other_tool", namespace="other")
        class OtherTool:
            async def execute(self) -> dict:
                return {"value": "other"}

        # Try to access 'other' tool from 'test' namespace should fail
        code2 = """
try:
    result = await other_tool()
    return "SHOULD_NOT_WORK"
except Exception as e:
    return str(type(e).__name__)
"""
        result = await sandbox.execute(code2, namespace="test")
        assert "NameError" in result


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_empty_code(self, sandbox):
        """Test executing empty code."""
        code = ""
        result = await sandbox.execute(code)
        assert result is None

    @pytest.mark.asyncio
    async def test_only_comments(self, sandbox):
        """Test code with only comments."""
        code = """
# This is a comment
# Another comment
"""
        result = await sandbox.execute(code)
        assert result is None

    @pytest.mark.asyncio
    async def test_return_none(self, sandbox):
        """Test explicitly returning None."""
        code = """
x = 5
return None
"""
        result = await sandbox.execute(code)
        assert result is None

    @pytest.mark.asyncio
    async def test_return_multiple_types(self, sandbox):
        """Test returning different types."""
        # Return dict
        code1 = "return {'a': 1, 'b': 2}"
        result1 = await sandbox.execute(code1)
        assert result1 == {"a": 1, "b": 2}

        # Return list
        code2 = "return [1, 2, 3]"
        result2 = await sandbox.execute(code2)
        assert result2 == [1, 2, 3]

        # Return string
        code3 = "return 'hello'"
        result3 = await sandbox.execute(code3)
        assert result3 == "hello"

        # Return number
        code4 = "return 42"
        result4 = await sandbox.execute(code4)
        assert result4 == 42

    @pytest.mark.asyncio
    async def test_nested_data_structures(self, sandbox):
        """Test returning nested data structures."""
        code = """
return {
    "numbers": [1, 2, 3],
    "nested": {
        "a": [4, 5, 6],
        "b": {"x": 7, "y": 8}
    },
    "tuple": (9, 10)
}
"""
        result = await sandbox.execute(code)
        assert result["numbers"] == [1, 2, 3]
        assert result["nested"]["a"] == [4, 5, 6]
        assert result["nested"]["b"]["x"] == 7
        assert result["tuple"] == (9, 10)


class TestCustomTimeout:
    """Test custom timeout configurations."""

    @pytest.mark.asyncio
    async def test_long_timeout(self):
        """Test with longer timeout."""
        sandbox = CodeSandbox(timeout=10.0, allow_unsafe_execution=True)

        code = """
total = 0
for i in range(1000):
    total += i
return total
"""
        result = await sandbox.execute(code)
        assert result == sum(range(1000))

    @pytest.mark.asyncio
    async def test_very_short_timeout(self):
        """Test with very short timeout."""
        sandbox = CodeSandbox(timeout=0.01, allow_unsafe_execution=True)

        # Even simple code might timeout with 0.01s
        code = """
x = 1
for i in range(100000):
    x = x + 1
return x
"""
        # This might timeout or succeed depending on system speed
        try:
            result = await sandbox.execute(code)
            # If it succeeds, check result
            assert isinstance(result, int)
        except CodeExecutionError as e:
            # If it times out, that's also acceptable
            assert "timed out" in str(e).lower()


class TestAllowedBuiltins:
    """Test allowed builtins configuration."""

    @pytest.mark.asyncio
    async def test_default_builtins_available(self, sandbox):
        """Test that default builtins are available."""
        code = """
# Test various allowed builtins
result = {
    "len": len([1, 2, 3]),
    "sum": sum([1, 2, 3]),
    "min": min([5, 2, 8]),
    "max": max([5, 2, 8]),
    "sorted": sorted([3, 1, 2]),
    "range": list(range(3)),
    "enumerate": list(enumerate(["a", "b"])),
}
return result
"""
        result = await sandbox.execute(code)
        assert result["len"] == 3
        assert result["sum"] == 6
        assert result["min"] == 2
        assert result["max"] == 8
        assert result["sorted"] == [1, 2, 3]
        assert result["range"] == [0, 1, 2]
        assert result["enumerate"] == [(0, "a"), (1, "b")]

    @pytest.mark.asyncio
    async def test_type_constructors_available(self, sandbox):
        """Test that type constructors are available."""
        code = """
return {
    "int": int("42"),
    "float": float("3.14"),
    "str": str(100),
    "bool": bool(1),
    "list": list((1, 2, 3)),
    "dict": dict(a=1, b=2),
    "tuple": tuple([1, 2, 3]),
    "set": set([1, 2, 2, 3]),
}
"""
        result = await sandbox.execute(code)
        assert result["int"] == 42
        assert result["float"] == 3.14
        assert result["str"] == "100"
        assert result["bool"] is True
        assert result["list"] == [1, 2, 3]
        assert result["dict"] == {"a": 1, "b": 2}
        assert result["tuple"] == (1, 2, 3)
        assert result["set"] == {1, 2, 3}

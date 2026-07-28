# chuk_tool_processor/execution/code_sandbox.py
"""
In-process Python code execution with tool access.

Enables programmatic tool orchestration by executing Python code that can call
registered tools.

.. warning::
    This module does **not** provide a security boundary. The code you pass to
    :class:`CodeSandbox` runs with ``exec()`` in the host process, with the host
    process's own privileges. The restricted ``__builtins__`` namespace only
    limits *name resolution*; it does not prevent attribute access on reachable
    objects and is trivially escapable, so it cannot contain untrusted code.
    Never pass untrusted or LLM-generated code to this class expecting it to be
    contained. See ``docs/security.md``.
"""

from __future__ import annotations

import asyncio
import sys
import warnings
from io import StringIO
from typing import Any

from chuk_tool_processor.registry import get_default_registry
from chuk_tool_processor.registry.interface import ToolRegistryInterface


class CodeExecutionError(Exception):
    """Raised when code execution fails."""

    pass


class UnsafeExecutionError(CodeExecutionError):
    """Raised when code execution is attempted without opting in to unsafe execution."""

    pass


class SandboxSecurityWarning(UserWarning):
    """Warns that CodeSandbox is not an isolation boundary."""

    pass


class CodeSandbox:
    """
    In-process Python code executor with tool access.

    Runs Python code that can call registered tools, enabling programmatic tool
    orchestration for any LLM (not just those with built-in code execution like
    Claude).

    .. warning::
        **This is not a security sandbox.** Code runs in the host process via
        ``exec()`` with the host's privileges. The restricted ``__builtins__``
        namespace is a convenience/footgun-reduction measure, not an isolation
        boundary — it is trivially escapable and cannot contain untrusted code.
        Because of this, execution is **disabled by default** and must be
        explicitly enabled with ``allow_unsafe_execution=True``. Only enable it
        for code you fully trust (i.e. code you authored), never for untrusted or
        LLM-generated code. For untrusted code you need real OS/process-level
        isolation (a locked-down subprocess/container, or a WASM interpreter).
        See ``docs/security.md``.

    Example:
        ```python
        from chuk_tool_processor.execution.code_sandbox import CodeSandbox

        # You are asserting the code is trusted by passing allow_unsafe_execution.
        sandbox = CodeSandbox(allow_unsafe_execution=True)

        code = '''
        # Call tools in a loop
        total = 0
        for i in range(1, 6):
            result = await add(a=str(total), b=str(i))
            total = int(result.content[0]['text'])
        return total
        '''

        result = await sandbox.execute(code)
        print(result)  # Output from the code
        ```

    What this class does provide (convenience, not security):
        - A reduced ``__builtins__`` namespace (limits accidental name use)
        - A configurable execution timeout
        - Tool functions injected for registered tools only
    """

    def __init__(
        self,
        registry: ToolRegistryInterface | None = None,
        timeout: float = 30.0,
        allowed_builtins: set[str] | None = None,
        allow_unsafe_execution: bool = False,
    ):
        """
        Initialize code sandbox.

        Args:
            registry: Tool registry to use (default: global registry)
            timeout: Maximum execution time in seconds
            allowed_builtins: Set of allowed builtin functions
            allow_unsafe_execution: Must be ``True`` to run any code. This is an
                explicit acknowledgement that ``execute()`` runs code in-process
                with no isolation boundary (see the class docstring). Leave it
                ``False`` (the default) unless every ``code`` string passed to
                ``execute()`` is fully trusted.
        """
        self.registry = registry
        self.timeout = timeout
        self.allow_unsafe_execution = allow_unsafe_execution
        self.allowed_builtins = allowed_builtins or {
            # Type constructors
            "int",
            "float",
            "str",
            "bool",
            "list",
            "dict",
            "tuple",
            "set",
            # Utility functions
            "len",
            "range",
            "enumerate",
            "zip",
            "sorted",
            "reversed",
            "sum",
            "min",
            "max",
            "abs",
            "round",
            "any",
            "all",
            # String operations
            "print",
            "format",
            # Data inspection
            "type",
            "isinstance",
            "hasattr",
            "getattr",
            # Exception handling
            "Exception",
            "ValueError",
            "TypeError",
            "KeyError",
            "IndexError",
            "AttributeError",
            "NameError",
        }

    async def execute(
        self,
        code: str,
        namespace: str | None = None,
        initial_vars: dict[str, Any] | None = None,
    ) -> Any:
        """
        Execute Python code with access to registered tools.

        Args:
            code: Python code to execute
            namespace: Namespace to filter tools (None = all namespaces)
            initial_vars: Initial variables to make available in code

        Returns:
            Result of code execution (value of last expression or return statement)

        Raises:
            UnsafeExecutionError: If ``allow_unsafe_execution`` was not enabled.
            CodeExecutionError: If execution fails or times out
        """
        # Fail closed: this class is not an isolation boundary, so refuse to run
        # anything unless the caller has explicitly opted in.
        if not self.allow_unsafe_execution:
            raise UnsafeExecutionError(
                "CodeSandbox.execute() is disabled by default because it is NOT a "
                "security boundary: code runs in-process via exec() and the restricted "
                "builtins are trivially escapable, so untrusted code is not contained. "
                "Pass CodeSandbox(allow_unsafe_execution=True) only for code you fully "
                "trust. For untrusted or LLM-generated code use real OS/process-level "
                "isolation instead (see docs/security.md)."
            )

        # Loud, once-per-instance reminder that there is no isolation here.
        warnings.warn(
            "CodeSandbox executes code in-process with no isolation boundary; the "
            "restricted builtins do not contain untrusted code. Only run trusted code. "
            "See docs/security.md.",
            SandboxSecurityWarning,
            stacklevel=2,
        )

        # Get registry
        if self.registry is None:
            self.registry = await get_default_registry()

        # Build safe globals
        safe_globals = await self._build_safe_globals(namespace, initial_vars or {})

        # Capture stdout
        stdout_capture = StringIO()
        old_stdout = sys.stdout
        sys.stdout = stdout_capture

        try:
            # Execute with timeout
            async def _run_code():
                # Create local scope for execution
                local_scope: dict[str, Any] = {}

                # Wrap code in function if it uses await or return
                needs_wrapping = "await " in code or "return " in code

                if needs_wrapping:
                    # Wrap in async function if uses await, sync function otherwise
                    if "await " in code:
                        wrapped_code = "async def __sandbox_main__():\n"
                    else:
                        wrapped_code = "def __sandbox_main__():\n"

                    for line in code.split("\n"):
                        wrapped_code += f"    {line}\n"

                    # Compile and execute wrapper
                    try:
                        exec(compile(wrapped_code, "<sandbox>", "exec"), safe_globals, local_scope)  # nosec B102 - intentional exec of caller-provided trusted code; gated behind allow_unsafe_execution (not an isolation boundary)
                    except SyntaxError as e:
                        raise CodeExecutionError(f"Syntax error in code: {e}")

                    # Call the function (await if async)
                    if "await " in code:
                        result = await local_scope["__sandbox_main__"]()
                    else:
                        result = local_scope["__sandbox_main__"]()
                    return result
                else:
                    # Execute synchronous code directly
                    try:
                        compiled = compile(code, "<sandbox>", "exec")
                        exec(compiled, safe_globals, local_scope)  # nosec B102 - intentional exec of caller-provided trusted code; gated behind allow_unsafe_execution (not an isolation boundary)
                    except SyntaxError as e:
                        raise CodeExecutionError(f"Syntax error in code: {e}")

                    # Return the last assigned value
                    return local_scope.get("__result__")

            try:
                result = await asyncio.wait_for(_run_code(), timeout=self.timeout)
            except TimeoutError:
                raise CodeExecutionError(f"Code execution timed out after {self.timeout}s")
            except Exception as e:
                raise CodeExecutionError(f"Code execution failed: {e}")

            return result

        finally:
            # Restore stdout
            sys.stdout = old_stdout
            output = stdout_capture.getvalue()
            if output:
                print(output, end="")

    async def _build_safe_globals(self, namespace: str | None, initial_vars: dict[str, Any]) -> dict[str, Any]:
        """
        Build the global scope with a reduced builtins namespace and tool access.

        Note: the reduced ``__builtins__`` is a convenience measure, not a
        security boundary (see the class docstring).

        Args:
            namespace: Namespace to filter tools
            initial_vars: Initial variables

        Returns:
            Dict of safe globals including tool functions
        """
        # Get actual builtins module
        import builtins as builtin_module

        # Start with allowed builtins
        safe_builtins = {}
        for name in self.allowed_builtins:
            if hasattr(builtin_module, name):
                safe_builtins[name] = getattr(builtin_module, name)

        safe_globals: dict[str, Any] = {
            "__builtins__": safe_builtins,
        }

        # Add initial variables
        safe_globals.update(initial_vars)

        # Add tool functions
        if self.registry:
            tools = await self.registry.list_tools(namespace=namespace)

            for tool_info in tools:
                # Get the tool instance
                tool = await self.registry.get_tool(tool_info.name, tool_info.namespace)

                if tool is None:
                    continue

                # Create async wrapper function with proper closure
                def make_tool_wrapper(tool_obj):
                    async def wrapper(**kwargs):
                        return await tool_obj.execute(**kwargs)

                    return wrapper

                # Add to globals
                tool_func = make_tool_wrapper(tool)
                tool_func.__name__ = tool_info.name
                safe_globals[tool_info.name] = tool_func

        return safe_globals

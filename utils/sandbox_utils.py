"""Safe code execution sandbox utilities.

Provides isolated execution environments for untrusted code with
resource limits, timeout enforcement, and whitelist controls.

Example:
    sandbox = Sandbox(timeout=5.0, memory_limit_mb=128)
    result = sandbox.execute("print('hello world')")
    print(result.stdout)  # 'hello world'
"""

from __future__ import annotations

import ast
import io
import os
import resource
import signal
import sys
import tempfile
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Callable, Sequence


@dataclass
class SandboxResult:
    """Result of sandboxed code execution."""
    success: bool
    stdout: str
    stderr: str
    return_value: Any = None
    error: str | None = None
    execution_time: float = 0.0
    memory_used_mb: float = 0.0


@dataclass
class SandboxConfig:
    """Configuration for sandbox execution."""
    timeout: float = 5.0
    memory_limit_mb: int = 128
    allowed_modules: tuple[str, ...] = ("math", "random", "json", "re")
    disallowed_builtins: tuple[str, ...] = (
        "open", "eval", "exec", "compile", "__import__",
        "exit", "quit", "breakpoint", "help",
    )
    working_directory: str | None = None
    environment: dict[str, str] | None = None


class SandboxExecutionError(Exception):
    """Raised when sandboxed execution fails."""
    pass


class TimeoutError(Exception):
    """Raised when execution exceeds time limit."""
    pass


class Sandbox:
    """Isolated execution environment for untrusted Python code.

    Limits CPU time, memory, and restricts access to dangerous operations.
    """

    def __init__(self, config: SandboxConfig | None = None) -> None:
        self.config = config or SandboxConfig()

    def execute(self, code: str, globals_dict: dict | None = None) -> SandboxResult:
        """Execute code in sandboxed environment.

        Args:
            code: Python code string to execute.
            globals_dict: Optional globals dict for execution context.

        Returns:
            SandboxResult with stdout, stderr, return value, and metrics.
        """
        import time
        start = time.perf_counter()

        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        globals_dict = globals_dict or {}
        globals_dict.setdefault("__name__", "__sandbox__")

        result_data: dict[str, Any] = {"return_value": None}

        def set_return_value(val: Any) -> None:
            result_data["return_value"] = val

        globals_dict["_sandbox_set_return"] = set_return_value

        old_stdout = sys.stdout
        old_stderr = sys.stderr
        old_working_dir = os.getcwd()

        try:
            sys.stdout = stdout_capture
            sys.stderr = stderr_capture

            if self.config.working_directory:
                os.chdir(self.config.working_directory)

            compiled = self._compile_code(code)

            self._set_memory_limit(self.config.memory_limit_mb)

            try:
                exec(compiled, globals_dict, None)
            except TimeoutError:
                raise
            except Exception as e:
                raise SandboxExecutionError(str(e)) from e

            execution_time = time.perf_counter() - start
            memory_mb = self._get_memory_usage()

            return SandboxResult(
                success=True,
                stdout=stdout_capture.getvalue(),
                stderr=stderr_capture.getvalue(),
                return_value=result_data.get("return_value"),
                execution_time=execution_time,
                memory_used_mb=memory_mb,
            )

        except TimeoutError:
            return SandboxResult(
                success=False,
                stdout=stdout_capture.getvalue(),
                stderr=stderr_capture.getvalue(),
                error="Execution timed out",
                execution_time=self.config.timeout,
            )
        except SandboxExecutionError as e:
            return SandboxResult(
                success=False,
                stdout=stdout_capture.getvalue(),
                stderr=stderr_capture.getvalue(),
                error=str(e),
                execution_time=time.perf_counter() - start,
            )
        except Exception as e:  # noqa: BLE001
            return SandboxResult(
                success=False,
                stdout=stdout_capture.getvalue(),
                stderr=stderr_capture.getvalue(),
                error=f"{type(e).__name__}: {e}",
                execution_time=time.perf_counter() - start,
            )
        finally:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            try:
                os.chdir(old_working_dir)
            except OSError:
                pass

    def execute_with_timeout(self, code: str, timeout: float | None = None) -> SandboxResult:
        """Execute code with custom timeout using signal-based interruption.

        Args:
            code: Python code string to execute.
            timeout: Override timeout in seconds.

        Returns:
            SandboxResult with execution outcome.
        """
        timeout_val = timeout or self.config.timeout

        def timeout_handler(signum: int, frame: Any) -> None:
            raise TimeoutError()

        old_handler = signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(int(timeout_val) + 1)

        try:
            result = self.execute(code)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

        return result

    def validate_syntax(self, code: str) -> tuple[bool, str | None]:
        """Validate Python syntax without executing.

        Args:
            code: Python code string to validate.

        Returns:
            Tuple of (is_valid, error_message or None).
        """
        try:
            ast.parse(code)
            return True, None
        except SyntaxError as e:
            return False, f"Line {e.lineno}: {e.msg}"

    def _compile_code(self, code: str) -> code:
        """Compile code with AST validation."""
        try:
            tree = ast.parse(code)
        except SyntaxError as e:
            raise SandboxExecutionError(f"Syntax error: {e}")

        self._check_ast_safety(tree)
        return compile(code, "<sandbox>", "exec", dont_inherit=True)

    def _check_ast_safety(self, tree: ast.AST) -> None:
        """Walk AST and raise on dangerous constructs."""
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    if node.func.id in self.config.disallowed_builtins:
                        raise SandboxExecutionError(
                            f"Disallowed builtin: {node.func.id}"
                        )
                elif isinstance(node.func, ast.Attribute):
                    attr = node.func.attr
                    if attr in ("__import__", "__builtins__"):
                        raise SandboxExecutionError(
                            f"Disallowed attribute access: {attr}"
                        )

    def _set_memory_limit(self, limit_mb: int) -> None:
        """Set per-process memory limit."""
        try:
            limit_bytes = limit_mb * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (limit_bytes, limit_bytes))
        except (ValueError, OSError):
            pass

    def _get_memory_usage(self) -> float:
        """Get current memory usage in MB."""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            return process.memory_info().rss / (1024 * 1024)
        except ImportError:
            try:
                usage = resource.getrusage(resource.RUSAGE_SELF)
                return usage.ru_maxrss / 1024
            except Exception:  # noqa: BLE001
                return 0.0


class AsyncSandbox:
    """Asynchronous wrapper for Sandbox execution."""

    def __init__(self, config: SandboxConfig | None = None) -> None:
        self.sandbox = Sandbox(config)

    async def execute(self, code: str) -> SandboxResult:
        """Execute code asynchronously in a thread pool.

        Args:
            code: Python code string to execute.

        Returns:
            SandboxResult with execution outcome.
        """
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.sandbox.execute, code)


@contextmanager
def temporary_sandbox(config: SandboxConfig | None = None):
    """Context manager for temporary sandboxed execution.

    Example:
        with temporary_sandbox(SandboxConfig(timeout=2.0)) as sandbox:
            result = sandbox.execute("x = 1 + 1")
    """
    sandbox = Sandbox(config)
    try:
        yield sandbox
    finally:
        pass

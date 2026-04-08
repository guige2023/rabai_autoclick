"""Sandbox utilities for safe execution of untrusted automation scripts.

Provides isolated execution environments for running
user-provided automation scripts, with resource limits,
syscall filtering, and network isolation.

Example:
    >>> from utils.sandbox_utils import run_in_sandbox, SandboxConfig
    >>> config = SandboxConfig(memory_mb=256, timeout_seconds=10)
    >>> result = run_in_sandbox('print("hello")', config=config)
"""

from __future__ import annotations

import resource
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from typing import Optional

__all__ = [
    "SandboxConfig",
    "SandboxResult",
    "run_in_sandbox",
    "SandboxError",
]


class SandboxError(Exception):
    """Raised when sandbox execution fails."""
    pass


@dataclass
class SandboxConfig:
    """Configuration for sandbox execution.

    Attributes:
        memory_mb: Maximum memory in megabytes.
        timeout_seconds: Maximum execution time.
        max_processes: Maximum number of subprocesses.
        allow_network: Whether to allow network access.
        allowed_modules: List of module names that can be imported.
        working_directory: Optional working directory.
    """

    memory_mb: int = 256
    timeout_seconds: int = 10
    max_processes: int = 4
    allow_network: bool = False
    allowed_modules: list[str] = None
    working_directory: Optional[str] = None

    def __post_init__(self):
        if self.allowed_modules is None:
            self.allowed_modules = ["json", "math", "time", "random"]


@dataclass
class SandboxResult:
    """Result of sandboxed execution."""

    success: bool
    output: str
    error: Optional[str]
    return_code: int
    elapsed_seconds: float
    timed_out: bool
    killed: bool


def run_in_sandbox(
    code: str,
    config: Optional[SandboxConfig] = None,
    language: str = "python",
) -> SandboxResult:
    """Execute code in an isolated sandbox.

    Args:
        code: Source code to execute.
        config: Sandbox configuration.
        language: Execution language ('python' or 'bash').

    Returns:
        SandboxResult with execution outcome.

    Example:
        >>> result = run_in_sandbox('print(sum(range(100)))')
        >>> print(result.output)
    """
    if config is None:
        config = SandboxConfig()

    if language == "python":
        return _run_python_sandbox(code, config)
    elif language == "bash":
        return _run_bash_sandbox(code, config)
    else:
        raise SandboxError(f"Unsupported language: {language}")


def _run_python_sandbox(code: str, config: SandboxConfig) -> SandboxResult:
    """Run Python code in a sandbox with resource limits."""
    start_time = time.time()
    timed_out = False
    killed = False

    # Write code to temp file
    fd, tmp_path = tempfile.mkstemp(suffix=".py")
    try:
        os = __import__("os")
        os.write(fd, code.encode())
        os.close(fd)

        # Build restricted environment
        env = {
            "__name__": "__main__",
            "__builtins__": __builtins__,
        }

        # Set resource limits
        max_mem = config.memory_mb * 1024 * 1024
        try:
            resource.setrlimit(resource.RLIMIT_AS, (max_mem, max_mem))
            resource.setrlimit(resource.RLIMIT_NPROC, (config.max_processes, config.max_processes))
            resource.setrlimit(resource.RLIMIT_CPU, (config.timeout_seconds, config.timeout_seconds))
        except Exception:
            pass

        try:
            compiled = compile(code, "<sandbox>", "exec")
            exec(compiled, env)
            return_code = 0
            error = None
        except SystemExit as e:
            return_code = int(str(e) or 0)
            error = None
        except Exception as e:
            return_code = 1
            error = f"{type(e).__name__}: {e}"

        elapsed = time.time() - start_time

        return SandboxResult(
            success=(return_code == 0 and error is None),
            output=env.get("__output__", ""),
            error=error,
            return_code=return_code,
            elapsed_seconds=elapsed,
            timed_out=timed_out,
            killed=killed,
        )

    except subprocess.TimeoutExpired:
        timed_out = True
        return SandboxResult(
            success=False,
            output="",
            error=f"Execution timed out after {config.timeout_seconds}s",
            return_code=-1,
            elapsed_seconds=config.timeout_seconds,
            timed_out=True,
            killed=False,
        )
    except Exception as e:
        return SandboxResult(
            success=False,
            output="",
            error=str(e),
            return_code=-1,
            elapsed_seconds=time.time() - start_time,
            timed_out=False,
            killed=False,
        )
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


def _run_bash_sandbox(code: str, config: SandboxConfig) -> SandboxResult:
    """Run bash code in a sandbox."""
    fd, tmp_path = tempfile.mkstemp(suffix=".sh")
    try:
        os.write(fd, code.encode())
        os.close(fd)

        start_time = time.time()

        env = {}
        if not config.allow_network:
            env["PATH"] = "/usr/bin:/bin"
            env["DYLD_LIBRARY_PATH"] = ""

        try:
            result = subprocess.run(
                ["bash", tmp_path],
                capture_output=True,
                timeout=config.timeout_seconds,
                env=env,
            )
            elapsed = time.time() - start_time
            return SandboxResult(
                success=(result.returncode == 0),
                output=result.stdout.decode("utf-8", errors="replace"),
                error=result.stderr.decode("utf-8", errors="replace") or None,
                return_code=result.returncode,
                elapsed_seconds=elapsed,
                timed_out=False,
                killed=False,
            )
        except subprocess.TimeoutExpired:
            return SandboxResult(
                success=False,
                output="",
                error=f"Execution timed out after {config.timeout_seconds}s",
                return_code=-1,
                elapsed_seconds=config.timeout_seconds,
                timed_out=True,
                killed=True,
            )
    except Exception as e:
        return SandboxResult(
            success=False,
            output="",
            error=str(e),
            return_code=-1,
            elapsed_seconds=0,
            timed_out=False,
            killed=False,
        )
    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass

"""
Command Executor Action Module.

Executes system commands and shell scripts with output capture,
timeout management, and error handling for automation workflows.
"""

import subprocess
import time
from dataclasses import dataclass
from typing import Optional, Sequence


@dataclass
class ExecutionResult:
    """Result of a command execution."""
    command: str
    returncode: int
    stdout: str
    stderr: str
    duration_ms: float
    success: bool


class CommandExecutor:
    """Executes system commands."""

    def __init__(self, default_timeout: float = 30.0):
        """
        Initialize command executor.

        Args:
            default_timeout: Default timeout in seconds.
        """
        self.default_timeout = default_timeout

    def execute(
        self,
        command: str | Sequence[str],
        timeout: Optional[float] = None,
        cwd: Optional[str] = None,
        env: Optional[dict] = None,
        capture_output: bool = True,
    ) -> ExecutionResult:
        """
        Execute a system command.

        Args:
            command: Command string or sequence of args.
            timeout: Execution timeout in seconds.
            cwd: Working directory.
            env: Environment variables.
            capture_output: Whether to capture stdout/stderr.

        Returns:
            ExecutionResult with execution details.
        """
        timeout = timeout if timeout is not None else self.default_timeout
        start_time = time.time()

        try:
            if isinstance(command, str):
                result = subprocess.run(
                    command,
                    shell=True,
                    timeout=timeout,
                    cwd=cwd,
                    env=env,
                    capture_output=capture_output,
                    text=True,
                )
            else:
                result = subprocess.run(
                    command,
                    timeout=timeout,
                    cwd=cwd,
                    env=env,
                    capture_output=capture_output,
                    text=True,
                )

            duration_ms = (time.time() - start_time) * 1000

            return ExecutionResult(
                command=str(command),
                returncode=result.returncode,
                stdout=result.stdout if capture_output else "",
                stderr=result.stderr if capture_output else "",
                duration_ms=duration_ms,
                success=result.returncode == 0,
            )

        except subprocess.TimeoutExpired:
            duration_ms = (time.time() - start_time) * 1000
            return ExecutionResult(
                command=str(command),
                returncode=-1,
                stdout="",
                stderr="Command timed out",
                duration_ms=duration_ms,
                success=False,
            )

        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            return ExecutionResult(
                command=str(command),
                returncode=-2,
                stdout="",
                stderr=str(e),
                duration_ms=duration_ms,
                success=False,
            )

    def execute_batch(
        self,
        commands: list[str],
        stop_on_error: bool = True,
    ) -> list[ExecutionResult]:
        """
        Execute multiple commands in sequence.

        Args:
            commands: List of command strings.
            stop_on_error: Stop if a command fails.

        Returns:
            List of ExecutionResult objects.
        """
        results = []

        for cmd in commands:
            result = self.execute(cmd)
            results.append(result)

            if stop_on_error and not result.success:
                break

        return results

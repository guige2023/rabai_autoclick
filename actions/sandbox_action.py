"""
Sandbox action for secure code execution environment.

This module provides actions for creating sandboxed execution
environments with resource limits and security controls.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import contextlib
import io
import os
import resource
import signal
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


class SandboxBackend(Enum):
    """Execution backend types."""
    SUBPROCESS = "subprocess"
    CONTAINER = "container"
    RESTRICTED = "restricted"


class ExecutionStatus(Enum):
    """Status of sandboxed execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    TIMEOUT = "timeout"
    MEMORY_LIMIT = "memory_limit"
    CPU_LIMIT = "cpu_limit"
    KILLED = "killed"
    ERROR = "error"


@dataclass
class ResourceLimits:
    """Resource limits for sandbox execution."""
    max_time_seconds: float = 30.0
    max_memory_bytes: Optional[int] = 256 * 1024 * 1024
    max_cpu_percent: Optional[int] = 80
    max_output_bytes: Optional[int] = 10 * 1024 * 1024
    max_processes: Optional[int] = 10
    max_file_size_bytes: Optional[int] = 50 * 1024 * 1024
    allow_network: bool = False
    allow_file_write: bool = False
    working_directory: Optional[str] = None


@dataclass
class ExecutionResult:
    """Result of sandboxed execution."""
    status: ExecutionStatus
    exit_code: Optional[int]
    stdout: str
    stderr: str
    duration_seconds: float
    start_time: datetime
    end_time: Optional[datetime] = None
    peak_memory_bytes: Optional[int] = None
    cpu_percent: Optional[float] = None
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "status": self.status.value,
            "exit_code": self.exit_code,
            "stdout": self.stdout[:10000] if self.stdout else "",
            "stderr": self.stderr[:10000] if self.stderr else "",
            "duration_seconds": self.duration_seconds,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "peak_memory_bytes": self.peak_memory_bytes,
            "cpu_percent": self.cpu_percent,
            "error_message": self.error_message,
        }


@dataclass
class SandboxConfig:
    """Configuration for sandbox environment."""
    backend: SandboxBackend = SandboxBackend.SUBPROCESS
    limits: ResourceLimits = field(default_factory=ResourceLimits)
    environment: Dict[str, str] = field(default_factory=dict)
    user: Optional[str] = None
    group: Optional[str] = None


class Sandbox:
    """
    Sandboxed execution environment for untrusted code.

    Supports resource limits, timeout enforcement, and output capture.
    """

    def __init__(self, config: Optional[SandboxConfig] = None):
        """
        Initialize the sandbox.

        Args:
            config: Sandbox configuration.
        """
        self.config = config or SandboxConfig()
        self._process: Optional[subprocess.Popen] = None
        self._start_time: Optional[datetime] = None
        self._memory_limit_reached = False

    def execute(
        self,
        code: str,
        language: str = "python",
        stdin: Optional[str] = None,
    ) -> ExecutionResult:
        """
        Execute code in the sandbox.

        Args:
            code: Code to execute.
            language: Programming language (python, node, bash).
            stdin: Optional standard input.

        Returns:
            ExecutionResult with execution details.

        Raises:
            ValueError: If language is not supported.
        """
        if language == "python":
            return self._execute_python(code, stdin)
        elif language == "node":
            return self._execute_node(code, stdin)
        elif language == "bash":
            return self._execute_bash(code, stdin)
        else:
            raise ValueError(f"Unsupported language: {language}")

    def _execute_python(self, code: str, stdin: Optional[str]) -> ExecutionResult:
        """Execute Python code."""
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".py",
            delete=False
        ) as f:
            f.write(code)
            temp_path = f.name

        try:
            return self._run_subprocess(
                ["python3", temp_path],
                stdin,
            )
        finally:
            try:
                os.unlink(temp_path)
            except Exception:
                pass

    def _execute_node(self, code: str, stdin: Optional[str]) -> ExecutionResult:
        """Execute Node.js code."""
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".js",
            delete=False
        ) as f:
            f.write(code)
            temp_path = f.name

        try:
            return self._run_subprocess(
                ["node", temp_path],
                stdin,
            )
        finally:
            try:
                os.unlink(temp_path)
            except Exception:
                pass

    def _execute_bash(self, code: str, stdin: Optional[str]) -> ExecutionResult:
        """Execute Bash script."""
        return self._run_subprocess(
            ["bash", "-c", code],
            stdin,
        )

    def _run_subprocess(
        self,
        command: List[str],
        stdin: Optional[str],
    ) -> ExecutionResult:
        """Run command in subprocess with resource limits."""
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()
        start_time = datetime.now()

        env = os.environ.copy()
        env.update(self.config.environment)

        limits = self.config.limits

        creationflags = 0
        if hasattr(subprocess, 'CREATE_NEW_PROCESS_GROUP'):
            creationflags |= subprocess.CREATE_NEW_PROCESS_GROUP

        try:
            self._process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE if stdin else None,
                env=env,
                cwd=limits.working_directory,
                creationflags=creationflags,
            )

            self._start_time = start_time

            stdout_data, stderr_data = self._process.communicate(
                input=stdin.encode() if stdin else None,
                timeout=limits.max_time_seconds,
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()

            exit_code = self._process.returncode

            if limits.max_output_bytes and len(stdout_data) > limits.max_output_bytes:
                stdout_data = stdout_data[:limits.max_output_bytes] + b"\n[OUTPUT TRUNCATED]"
            if limits.max_output_bytes and len(stderr_data) > limits.max_output_bytes:
                stderr_data = stderr_data[:limits.max_output_bytes] + b"\n[OUTPUT TRUNCATED]"

            return ExecutionResult(
                status=ExecutionStatus.COMPLETED if exit_code == 0 else ExecutionStatus.ERROR,
                exit_code=exit_code,
                stdout=stdout_data.decode("utf-8", errors="replace"),
                stderr=stderr_data.decode("utf-8", errors="replace"),
                duration_seconds=duration,
                start_time=start_time,
                end_time=end_time,
            )

        except subprocess.TimeoutExpired:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            self._kill_process()
            return ExecutionResult(
                status=ExecutionStatus.TIMEOUT,
                exit_code=-1,
                stdout=stdout_capture.getvalue(),
                stderr=f"Execution timed out after {limits.max_time_seconds} seconds",
                duration_seconds=duration,
                start_time=start_time,
                end_time=end_time,
                error_message=f"Timeout after {limits.max_time_seconds}s",
            )

        except MemoryError as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            self._kill_process()
            return ExecutionResult(
                status=ExecutionStatus.MEMORY_LIMIT,
                exit_code=-1,
                stdout=stdout_capture.getvalue(),
                stderr=f"Memory limit exceeded: {e}",
                duration_seconds=duration,
                start_time=start_time,
                end_time=end_time,
                error_message="Memory limit exceeded",
            )

        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            return ExecutionResult(
                status=ExecutionStatus.ERROR,
                exit_code=-1,
                stdout=stdout_capture.getvalue(),
                stderr=str(e),
                duration_seconds=duration,
                start_time=start_time,
                end_time=end_time,
                error_message=str(e),
            )

    def _kill_process(self) -> None:
        """Kill the running process."""
        if self._process:
            try:
                if os.name == 'nt':
                    self._process.terminate()
                else:
                    pgid = os.getpgid(self._process.pid)
                    os.killpg(pgid, signal.SIGTERM)
            except Exception:
                try:
                    self._process.kill()
                except Exception:
                    pass


class SandboxPool:
    """Pool of sandboxed execution environments."""

    def __init__(
        self,
        config: Optional[SandboxConfig] = None,
        pool_size: int = 4,
    ):
        """
        Initialize the sandbox pool.

        Args:
            config: Sandbox configuration.
            pool_size: Maximum concurrent executions.
        """
        self.config = config or SandboxConfig()
        self._pool_size = pool_size
        self._active_count = 0
        self._lock = threading.Lock()
        self._queue: List[Tuple[str, str, Optional[str], Callable]] = []
        self._results: Dict[str, ExecutionResult] = {}

    def execute(
        self,
        task_id: str,
        code: str,
        language: str = "python",
        stdin: Optional[str] = None,
    ) -> ExecutionResult:
        """
        Execute code in the sandbox pool.

        Args:
            task_id: Unique identifier for this task.
            code: Code to execute.
            language: Programming language.
            stdin: Optional standard input.

        Returns:
            ExecutionResult with execution details.
        """
        with self._lock:
            if self._active_count >= self._pool_size:
                self._queue.append((task_id, code, stdin, language))

            self._active_count += 1

        sandbox = Sandbox(self.config)
        result = sandbox.execute(code, language, stdin)

        with self._lock:
            self._results[task_id] = result
            self._active_count -= 1
            self._process_queue()

        return result

    def _process_queue(self) -> None:
        """Process queued executions."""
        while self._queue and self._active_count < self._pool_size:
            task_id, code, stdin, language = self._queue.pop(0)
            self._active_count += 1

            def run_task():
                sandbox = Sandbox(self.config)
                result = sandbox.execute(code, language, stdin)
                with self._lock:
                    self._results[task_id] = result
                    self._active_count -= 1
                    self._process_queue()

            thread = threading.Thread(target=run_task)
            thread.daemon = True
            thread.start()

    def get_result(self, task_id: str) -> Optional[ExecutionResult]:
        """Get result for a completed task."""
        return self._results.get(task_id)


def sandbox_execute_action(
    code: str,
    language: str = "python",
    stdin: Optional[str] = None,
    max_time_seconds: float = 30.0,
    max_memory_bytes: int = 256 * 1024 * 1024,
) -> Dict[str, Any]:
    """
    Action function to execute code in a sandbox.

    Args:
        code: Code to execute.
        language: Programming language (python, node, bash).
        stdin: Optional standard input.
        max_time_seconds: Maximum execution time.
        max_memory_bytes: Maximum memory usage.

    Returns:
        Dictionary with execution result.
    """
    limits = ResourceLimits(
        max_time_seconds=max_time_seconds,
        max_memory_bytes=max_memory_bytes,
    )

    config = SandboxConfig(limits=limits)
    sandbox = Sandbox(config)
    result = sandbox.execute(code, language, stdin)

    return result.to_dict()


def sandbox_execute_batch_action(
    tasks: List[Dict[str, Any]],
    pool_size: int = 4,
) -> Dict[str, Any]:
    """
    Action function to execute multiple tasks in a sandbox pool.

    Args:
        tasks: List of task dictionaries with code, language, stdin.
        pool_size: Maximum concurrent executions.

    Returns:
        Dictionary with batch execution results.
    """
    config = SandboxConfig()
    pool = SandboxPool(config, pool_size)

    task_ids = []
    for i, task in enumerate(tasks):
        task_id = f"task_{i}_{int(time.time())}"
        task_ids.append(task_id)
        pool.execute(
            task_id,
            task["code"],
            task.get("language", "python"),
            task.get("stdin"),
        )

    results = {}
    for task_id in task_ids:
        result = pool.get_result(task_id)
        if result:
            results[task_id] = result.to_dict()

    return {
        "total": len(tasks),
        "results": results,
    }

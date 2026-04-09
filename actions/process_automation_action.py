"""
Process Automation Action Module

Automates external process execution, monitoring,
and IPC communication for workflow integration.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import subprocess
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class ProcessState(Enum):
    """Process state identifiers."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class ProcessResult:
    """Result of process execution."""

    returncode: int
    stdout: str
    stderr: str
    duration: float
    state: ProcessState
    timed_out: bool = False


@dataclass
class ProcessInfo:
    """Information about a running process."""

    pid: int
    name: str
    command: str
    started_at: float
    state: ProcessState = ProcessState.RUNNING
    cpu_percent: float = 0.0
    memory_mb: float = 0.0


@dataclass
class ProcessConfig:
    """Configuration for process automation."""

    default_timeout: float = 300.0
    default_shell: bool = True
    capture_output: bool = True
    working_directory: Optional[str] = None
    environment: Optional[Dict[str, str]] = None


class ProcessAutomation:
    """
    Automates external process execution and management.

    Supports running, monitoring, killing processes,
    with full stdout/stderr capture and timeout handling.
    """

    def __init__(
        self,
        config: Optional[ProcessConfig] = None,
        output_handler: Optional[Callable[[str, str], None]] = None,
    ):
        self.config = config or ProcessConfig()
        self.output_handler = output_handler or self._default_output_handler
        self._processes: Dict[int, subprocess.Popen] = {}
        self._process_info: Dict[int, ProcessInfo] = {}

    def _default_output_handler(self, stdout: str, stderr: str) -> None:
        """Default output handler."""
        if stdout:
            logger.debug(f"STDOUT: {stdout[:200]}")
        if stderr:
            logger.warning(f"STDERR: {stderr[:200]}")

    def run(
        self,
        command: Union[str, List[str]],
        timeout: Optional[float] = None,
        shell: Optional[bool] = None,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        capture: bool = True,
    ) -> ProcessResult:
        """
        Run a command and wait for completion.

        Args:
            command: Command to run
            timeout: Timeout in seconds
            shell: Use shell execution
            cwd: Working directory
            env: Environment variables
            capture: Capture stdout/stderr

        Returns:
            ProcessResult with execution details
        """
        timeout = timeout or self.config.default_timeout
        shell = shell if shell is not None else self.config.default_shell
        cwd = cwd or self.config.working_directory

        merged_env = None
        if env or self.config.environment:
            import os
            merged_env = {**os.environ, **(self.config.environment or {}), **(env or {})}

        start_time = time.time()

        try:
            if isinstance(command, str) and shell:
                process = subprocess.Popen(
                    command,
                    shell=True,
                    stdout=subprocess.PIPE if capture else None,
                    stderr=subprocess.PIPE if capture else None,
                    cwd=cwd,
                    env=merged_env,
                    text=True,
                )
            elif isinstance(command, list):
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE if capture else None,
                    stderr=subprocess.PIPE if capture else None,
                    cwd=cwd,
                    env=merged_env,
                    text=True,
                )
            else:
                process = subprocess.Popen(
                    command,
                    shell=True,
                    stdout=subprocess.PIPE if capture else None,
                    stderr=subprocess.PIPE if capture else None,
                    cwd=cwd,
                    env=merged_env,
                    text=True,
                )

            self._processes[process.pid] = process

            try:
                stdout, stderr = process.communicate(timeout=timeout)
                state = ProcessState.COMPLETED
                timed_out = False
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, stderr = process.communicate()
                state = ProcessState.TIMEOUT
                timed_out = True

        except Exception as e:
            duration = time.time() - start_time
            return ProcessResult(
                returncode=-1,
                stdout="",
                stderr=str(e),
                duration=duration,
                state=ProcessState.FAILED,
            )

        duration = time.time() - start_time

        if process.pid in self._processes:
            del self._processes[process.pid]

        result = ProcessResult(
            returncode=process.returncode or 0,
            stdout=stdout or "",
            stderr=stderr or "",
            duration=duration,
            state=state,
            timed_out=timed_out,
        )

        if result.returncode != 0 and state == ProcessState.COMPLETED:
            result.state = ProcessState.FAILED

        return result

    def start(
        self,
        command: Union[str, List[str]],
        shell: Optional[bool] = None,
        cwd: Optional[str] = None,
    ) -> int:
        """
        Start a process without waiting.

        Args:
            command: Command to run
            shell: Use shell execution
            cwd: Working directory

        Returns:
            Process ID
        """
        shell = shell if shell is not None else self.config.default_shell
        cwd = cwd or self.config.working_directory

        if isinstance(command, str) and shell:
            process = subprocess.Popen(
                command,
                shell=True,
                cwd=cwd,
            )
        elif isinstance(command, list):
            process = subprocess.Popen(
                command,
                cwd=cwd,
            )
        else:
            process = subprocess.Popen(
                command,
                shell=True,
                cwd=cwd,
            )

        self._processes[process.pid] = process

        import os
        name = command if isinstance(command, str) else command[0]
        self._process_info[process.pid] = ProcessInfo(
            pid=process.pid,
            name=name,
            command=str(command),
            started_at=time.time(),
        )

        return process.pid

    def wait(
        self,
        pid: int,
        timeout: Optional[float] = None,
    ) -> Optional[ProcessResult]:
        """
        Wait for a process to complete.

        Args:
            pid: Process ID
            timeout: Timeout in seconds

        Returns:
            ProcessResult or None if process not found
        """
        if pid not in self._processes:
            return None

        process = self._processes[pid]
        timeout = timeout or self.config.default_timeout

        start_time = time.time()

        try:
            returncode = process.wait(timeout=timeout)
            stdout, stderr = process.communicate() if hasattr(process, 'communicate') else ("", "")
            duration = time.time() - start_time

            del self._processes[pid]
            if pid in self._process_info:
                del self._process_info[pid]

            return ProcessResult(
                returncode=returncode,
                stdout=stdout or "",
                stderr=stderr or "",
                duration=duration,
                state=ProcessState.COMPLETED if returncode == 0 else ProcessState.FAILED,
            )
        except subprocess.TimeoutExpired:
            return ProcessResult(
                returncode=-1,
                stdout="",
                stderr="Process timed out",
                duration=timeout,
                state=ProcessState.TIMEOUT,
                timed_out=True,
            )
        except Exception as e:
            logger.error(f"Wait failed: {e}")
            return None

    def kill(self, pid: int) -> bool:
        """
        Kill a process.

        Args:
            pid: Process ID

        Returns:
            True if successful
        """
        if pid in self._processes:
            try:
                self._processes[pid].kill()
                self._processes[pid].wait()
                del self._processes[pid]
                if pid in self._process_info:
                    self._process_info[pid].state = ProcessState.CANCELLED
                    del self._process_info[pid]
                return True
            except Exception as e:
                logger.error(f"Kill failed: {e}")

        return False

    def is_running(self, pid: int) -> bool:
        """Check if process is running."""
        if pid in self._processes:
            return self._processes[pid].poll() is None
        return False

    def get_process_info(self, pid: int) -> Optional[ProcessInfo]:
        """Get process information."""
        return self._process_info.get(pid)

    def list_processes(self) -> List[ProcessInfo]:
        """List all tracked processes."""
        return list(self._process_info.values())

    def run_pipeline(
        self,
        commands: List[Union[str, List[str]]],
        timeout: Optional[float] = None,
    ) -> List[ProcessResult]:
        """
        Run a pipeline of commands.

        Args:
            commands: List of commands to run in sequence
            timeout: Timeout per command

        Returns:
            List of ProcessResult for each command
        """
        results = []

        for cmd in commands:
            result = self.run(cmd, timeout=timeout)
            results.append(result)

            if result.state == ProcessState.FAILED:
                break

        return results

    def run_parallel(
        self,
        commands: List[Union[str, List[str]]],
        timeout: Optional[float] = None,
    ) -> List[ProcessResult]:
        """
        Run multiple commands in parallel.

        Args:
            commands: List of commands to run
            timeout: Timeout per command

        Returns:
            List of ProcessResult for each command
        """
        pids = [self.start(cmd) for cmd in commands]

        results = []
        for pid in pids:
            result = self.wait(pid, timeout=timeout)
            if result:
                results.append(result)
            else:
                results.append(ProcessResult(
                    returncode=-1,
                    stdout="",
                    stderr="Process not found",
                    duration=0,
                    state=ProcessState.FAILED,
                ))

        return results

    def shell(self, command: str, timeout: Optional[float] = None) -> ProcessResult:
        """
        Run a shell command.

        Args:
            command: Shell command string
            timeout: Timeout in seconds

        Returns:
            ProcessResult
        """
        return self.run(command, shell=True, timeout=timeout)


def create_process_automation(
    config: Optional[ProcessConfig] = None,
) -> ProcessAutomation:
    """Factory function to create a ProcessAutomation instance."""
    return ProcessAutomation(config=config)

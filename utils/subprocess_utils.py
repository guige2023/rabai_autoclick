"""
Subprocess management utilities for running external commands.

Provides high-level subprocess execution with timeout, streaming output,
environment control, and process grouping for automation workflows.

Example:
    >>> from subprocess_utils import run_command, run_pipeline, ProcessRunner
    >>> result = run_command("ls -la", timeout=30)
    >>> print(result.stdout)
"""

from __future__ import annotations

import os
import shlex
import subprocess
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


# =============================================================================
# Types
# =============================================================================


class RunStatus(Enum):
    """Process execution status."""
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class CommandResult:
    """Result of a command execution."""
    command: str
    status: RunStatus
    returncode: int
    stdout: str
    stderr: str
    duration: float
    timed_out: bool = False


# =============================================================================
# Command Execution
# =============================================================================


def run_command(
    command: str,
    timeout: Optional[float] = None,
    cwd: Optional[str] = None,
    env: Optional[Dict[str, str]] = None,
    shell: bool = False,
    check: bool = False,
) -> CommandResult:
    """
    Execute a shell command and return the result.

    Args:
        command: Command to execute.
        timeout: Maximum seconds to wait.
        cwd: Working directory.
        env: Environment variables.
        shell: Use shell to execute.
        check: Raise exception on non-zero exit.

    Returns:
        CommandResult with output and status.
    """
    start_time = time.monotonic()

    try:
        if shell:
            proc = subprocess.Popen(
                command,
                shell=True,
                cwd=cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        else:
            args = shlex.split(command) if isinstance(command, str) else command
            proc = subprocess.Popen(
                args,
                cwd=cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

        try:
            stdout, stderr = proc.communicate(timeout=timeout)
            status = RunStatus.SUCCESS if proc.returncode == 0 else RunStatus.FAILED
            timed_out = False
        except subprocess.TimeoutExpired:
            proc.kill()
            stdout, stderr = proc.communicate()
            status = RunStatus.TIMEOUT
            timed_out = True

        duration = time.monotonic() - start_time

        if check and proc.returncode != 0:
            raise subprocess.CalledProcessError(
                proc.returncode, command, stdout, stderr
            )

        return CommandResult(
            command=command,
            status=status,
            returncode=proc.returncode or 0,
            stdout=stdout or "",
            stderr=stderr or "",
            duration=duration,
            timed_out=timed_out,
        )

    except Exception as e:
        duration = time.monotonic() - start_time
        return CommandResult(
            command=command,
            status=RunStatus.FAILED,
            returncode=-1,
            stdout="",
            stderr=str(e),
            duration=duration,
        )


def run_pipeline(
    commands: List[str],
    timeout: Optional[float] = None,
    cwd: Optional[str] = None,
) -> List[CommandResult]:
    """
    Run multiple commands in a pipeline.

    Args:
        commands: List of commands to pipe together.
        timeout: Maximum seconds total.

    Returns:
        List of CommandResult for each command.
    """
    if not commands:
        return []

    if len(commands) == 1:
        return [run_command(commands[0], timeout=timeout, cwd=cwd)]

    results = []
    remaining_time = timeout

    for i, cmd in enumerate(commands):
        cmd_timeout = remaining_time if remaining_time else None
        result = run_command(cmd, timeout=cmd_timeout, cwd=cwd)

        results.append(result)

        if result.returncode != 0:
            break

        if remaining_time:
            remaining_time = max(0, remaining_time - result.duration)

    return results


# =============================================================================
# Process Runner
# =============================================================================


class ProcessRunner:
    """
    Advanced process runner with streaming output and callbacks.

    Example:
        >>> runner = ProcessRunner()
        >>> runner.on_stdout(lambda line: print(f"OUT: {line}"))
        >>> runner.on_stderr(lambda line: print(f"ERR: {line}"))
        >>> runner.start("tail -f /var/log/syslog")
        >>> time.sleep(10)
        >>> runner.stop()
    """

    def __init__(self):
        self._process: Optional[subprocess.Popen] = None
        self._running = False
        self._cancel_requested = False
        self._stdout_callbacks: List[Callable[[str], None]] = []
        self._stderr_callbacks: List[Callable[[str], None]] = []
        self._exit_callbacks: List[Callable[[int], None]] = []
        self._lock = threading.Lock()
        self._stdout_thread: Optional[threading.Thread] = None
        self._stderr_thread: Optional[threading.Thread] = None

    def on_stdout(self, callback: Callable[[str], None]) -> None:
        """Register callback for stdout lines."""
        with self._lock:
            self._stdout_callbacks.append(callback)

    def on_stderr(self, callback: Callable[[str], None]) -> None:
        """Register callback for stderr lines."""
        with self._lock:
            self._stderr_callbacks.append(callback)

    def on_exit(self, callback: Callable[[int], None]) -> None:
        """Register callback for process exit."""
        with self._lock:
            self._exit_callbacks.append(callback)

    def start(
        self,
        command: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        shell: bool = True,
    ) -> None:
        """
        Start executing a command.

        Args:
            command: Command to execute.
            cwd: Working directory.
            env: Environment variables.
            shell: Use shell.
        """
        if self._running:
            raise RuntimeError("Process already running")

        self._cancel_requested = False
        self._running = True

        if shell:
            self._process = subprocess.Popen(
                command,
                shell=True,
                cwd=cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
        else:
            args = shlex.split(command) if isinstance(command, str) else command
            self._process = subprocess.Popen(
                args,
                cwd=cwd,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )

        self._stdout_thread = threading.Thread(
            target=self._stream_stdout, daemon=True
        )
        self._stderr_thread = threading.Thread(
            target=self._stream_stderr, daemon=True
        )

        self._stdout_thread.start()
        self._stderr_thread.start()

    def _stream_stdout(self) -> None:
        """Stream stdout to callbacks."""
        if not self._process or not self._process.stdout:
            return

        for line in self._process.stdout:
            with self._lock:
                for cb in self._stdout_callbacks:
                    try:
                        cb(line.rstrip("\n"))
                    except Exception:
                        pass

    def _stream_stderr(self) -> None:
        """Stream stderr to callbacks."""
        if not self._process or not self._process.stderr:
            return

        for line in self._process.stderr:
            with self._lock:
                for cb in self._stderr_callbacks:
                    try:
                        cb(line.rstrip("\n"))
                    except Exception:
                        pass

    def wait(self) -> int:
        """Wait for process to complete and return exit code."""
        if not self._process:
            return -1

        returncode = self._process.wait()

        if self._stdout_thread:
            self._stdout_thread.join(timeout=1.0)
        if self._stderr_thread:
            self._stderr_thread.join(timeout=1.0)

        with self._lock:
            for cb in self._exit_callbacks:
                try:
                    cb(returncode)
                except Exception:
                    pass

        self._running = False
        return returncode

    def stop(self, timeout: float = 5.0) -> bool:
        """
        Stop the running process.

        Args:
            timeout: Seconds to wait for graceful shutdown.

        Returns:
            True if process was stopped.
        """
        if not self._process:
            return True

        self._cancel_requested = True

        try:
            self._process.terminate()
            self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait()

        self._running = False
        return True

    def is_running(self) -> bool:
        """Check if process is running."""
        if not self._process:
            return False
        return self._process.poll() is None

    @property
    def pid(self) -> Optional[int]:
        """Get process ID."""
        if self._process:
            return self._process.pid
        return None


# =============================================================================
# Process Group Management
# =============================================================================


class ProcessGroup:
    """
    Manages a group of related processes.

    Example:
        >>> group = ProcessGroup()
        >>> group.spawn("python server.py")
        >>> group.spawn("python worker.py")
        >>> group.wait()  # Wait for all
        >>> group.terminate()  # Stop all
    """

    def __init__(self):
        self._processes: Dict[int, subprocess.Popen] = {}
        self._lock = threading.Lock()

    def spawn(
        self,
        command: str,
        cwd: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
    ) -> int:
        """
        Spawn a new process in the group.

        Returns:
            PID of spawned process.
        """
        proc = subprocess.Popen(
            command,
            shell=True,
            cwd=cwd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        with self._lock:
            self._processes[proc.pid] = proc

        return proc.pid

    def terminate(self, timeout: float = 5.0) -> None:
        """Terminate all processes in the group."""
        with self._lock:
            pids = list(self._processes.keys())

        for pid in pids:
            try:
                os.kill(pid, 15)  # SIGTERM
            except OSError:
                pass

        time.sleep(0.5)

        with self._lock:
            for pid in pids:
                proc = self._processes.get(pid)
                if proc and proc.poll() is None:
                    try:
                        os.kill(pid, 9)  # SIGKILL
                    except OSError:
                        pass

        with self._lock:
            self._processes.clear()

    def wait(self) -> Dict[int, int]:
        """
        Wait for all processes and return exit codes.

        Returns:
            Dict mapping PID to exit code.
        """
        exit_codes = {}

        with self._lock:
            pids = list(self._processes.keys())

        for pid in pids:
            try:
                pid2, status = os.waitpid(pid, 0)
                exit_codes[pid2] = os.WEXITSTATUS(status)
            except ChildProcessError:
                exit_codes[pid] = -1

        return exit_codes

    def is_alive(self) -> bool:
        """Check if any processes are still alive."""
        with self._lock:
            for proc in self._processes.values():
                if proc.poll() is None:
                    return True
        return False

    def get_pids(self) -> List[int]:
        """Get list of process IDs in the group."""
        with self._lock:
            return list(self._processes.keys())


# =============================================================================
# Utility Functions
# =============================================================================


def get_command_output(command: str) -> str:
    """
    Execute command and return stdout.

    Args:
        command: Command to execute.

    Returns:
        Command stdout as string.
    """
    result = run_command(command)
    return result.stdout


def is_command_available(command: str) -> bool:
    """
    Check if a command is available in PATH.

    Args:
        command: Command name.

    Returns:
        True if command exists.
    """
    result = run_command(f"which {shlex.quote(command)}", check=False)
    return result.returncode == 0


def get_shell_info() -> Dict[str, str]:
    """Get information about the current shell."""
    shell = os.environ.get("SHELL", "/bin/sh")
    home = os.environ.get("HOME", "")

    result = run_command(f"{shell} -c 'echo $PATH'")
    path = result.stdout.strip()

    return {
        "shell": shell,
        "home": home,
        "path": path,
    }

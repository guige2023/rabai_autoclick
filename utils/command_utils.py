"""Command execution utilities: shell commands, subprocess management, and process control."""

from __future__ import annotations

import asyncio
import os
import shlex
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "CommandResult",
    "CommandRunner",
    "run_command",
    "run_command_async",
    "background_command",
]


@dataclass
class CommandResult:
    """Result of a command execution."""
    command: str
    returncode: int
    stdout: str
    stderr: str
    elapsed_ms: float
    timed_out: bool = False

    @property
    def success(self) -> bool:
        return self.returncode == 0

    @property
    def output(self) -> str:
        return self.stdout


class CommandRunner:
    """Configurable command execution runner."""

    def __init__(
        self,
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        shell: bool = False,
        timeout: float = 30.0,
    ) -> None:
        self.default_cwd = cwd or os.getcwd()
        self.default_env = env or {}
        self.default_shell = shell
        self.default_timeout = timeout

    def run(
        self,
        command: str | list[str],
        cwd: str | None = None,
        env: dict[str, str] | None = None,
        shell: bool | None = None,
        timeout: float | None = None,
        capture_output: bool = True,
        check: bool = False,
    ) -> CommandResult:
        cwd = cwd or self.default_cwd
        env = {**os.environ, **self.default_env, **(env or {})}
        shell = shell if shell is not None else self.default_shell
        timeout = timeout or self.default_timeout

        if isinstance(command, list):
            cmd = command
        elif shell:
            cmd = command
        else:
            cmd = shlex.split(command)

        start = time.monotonic()
        try:
            proc = subprocess.Popen(
                cmd,
                cwd=cwd,
                env=env,
                shell=shell,
                stdout=subprocess.PIPE if capture_output else None,
                stderr=subprocess.PIPE if capture_output else None,
            )
            stdout_b, stderr_b = proc.communicate(timeout=timeout)
            stdout = stdout_b.decode("utf-8", errors="replace") if stdout_b else ""
            stderr = stderr_b.decode("utf-8", errors="replace") if stderr_b else ""
            elapsed = (time.monotonic() - start) * 1000
            returncode = proc.returncode
            timed_out = False
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate()
            elapsed = (time.monotonic() - start) * 1000
            return CommandResult(
                command=str(command),
                returncode=-1,
                stdout="",
                stderr="Command timed out",
                elapsed_ms=elapsed,
                timed_out=True,
            )
        except Exception as e:
            elapsed = (time.monotonic() - start) * 1000
            return CommandResult(
                command=str(command),
                returncode=-1,
                stdout="",
                stderr=str(e),
                elapsed_ms=elapsed,
            )

        if check and returncode != 0:
            raise RuntimeError(f"Command failed with {returncode}: {stderr}")

        return CommandResult(
            command=str(command),
            returncode=returncode,
            stdout=stdout,
            stderr=stderr,
            elapsed_ms=elapsed,
        )


def run_command(
    command: str,
    cwd: str | None = None,
    timeout: float = 30.0,
    check: bool = False,
) -> CommandResult:
    """Run a shell command and return the result."""
    runner = CommandRunner(cwd=cwd, timeout=timeout)
    return runner.run(command, timeout=timeout, check=check, shell=True)


async def run_command_async(
    command: str,
    cwd: str | None = None,
    env: dict[str, str] | None = None,
) -> CommandResult:
    """Run a shell command asynchronously."""
    start = time.monotonic()
    proc = await asyncio.create_subprocess_shell(
        command,
        cwd=cwd,
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_b, stderr_b = await proc.communicate()
    elapsed = (time.monotonic() - start) * 1000
    return CommandResult(
        command=command,
        returncode=proc.returncode,
        stdout=stdout_b.decode("utf-8", errors="replace"),
        stderr=stderr_b.decode("utf-8", errors="replace"),
        elapsed_ms=elapsed,
    )


def background_command(
    command: str,
    on_output: Callable[[str], None] | None = None,
    cwd: str | None = None,
) -> subprocess.Popen:
    """Start a command in the background, optionally calling on_output for each line."""
    proc = subprocess.Popen(
        command,
        shell=True,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    def read_output():
        for line in proc.stdout or []:
            if on_output:
                on_output(line.rstrip())

    thread = threading.Thread(target=read_output, daemon=True)
    thread.start()
    return proc

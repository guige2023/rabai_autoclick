"""
Console Utilities

Provides utilities for console/terminal operations
in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import subprocess
import sys


@dataclass
class ConsoleOutput:
    """Result of a console command execution."""
    stdout: str
    stderr: str
    returncode: int
    success: bool


class ConsoleManager:
    """
    Manages console and terminal operations.
    
    Provides methods for running commands and
    capturing console output.
    """

    def __init__(self) -> None:
        self._last_output: ConsoleOutput | None = None

    def run(
        self,
        command: str | list[str],
        capture: bool = True,
        timeout: float = 30.0,
        shell: bool = False,
    ) -> ConsoleOutput:
        """
        Run a console command.
        
        Args:
            command: Command to run.
            capture: Whether to capture output.
            timeout: Command timeout in seconds.
            shell: Run through shell.
            
        Returns:
            ConsoleOutput with results.
        """
        try:
            if isinstance(command, str) and not shell:
                cmd_list = command.split()
            else:
                cmd_list = command

            result = subprocess.run(
                cmd_list,
                capture_output=capture,
                text=True,
                shell=shell,
                timeout=timeout,
            )
            output = ConsoleOutput(
                stdout=result.stdout or "",
                stderr=result.stderr or "",
                returncode=result.returncode,
                success=result.returncode == 0,
            )
            self._last_output = output
            return output
        except subprocess.TimeoutExpired:
            return ConsoleOutput(
                stdout="",
                stderr=f"Command timed out after {timeout}s",
                returncode=-1,
                success=False,
            )
        except Exception as e:
            return ConsoleOutput(
                stdout="",
                stderr=str(e),
                returncode=-1,
                success=False,
            )

    def get_last_output(self) -> ConsoleOutput | None:
        """Get the last command output."""
        return self._last_output

    def clear(self) -> None:
        """Clear last output."""
        self._last_output = None


def print_formatted(
    message: str,
    level: str = "INFO",
    color: bool = True
) -> None:
    """
    Print a formatted message to console.
    
    Args:
        message: Message to print.
        level: Log level (INFO, WARN, ERROR).
        color: Use ANSI colors.
    """
    colors = {
        "INFO": "\033[94m",
        "WARN": "\033[93m",
        "ERROR": "\033[91m",
        "SUCCESS": "\033[92m",
    }
    reset = "\033[0m" if color else ""

    prefix = f"{colors.get(level.upper(), '')}[{level}]{reset}"
    print(f"{prefix} {message}")


def print_table(headers: list[str], rows: list[list[str]]) -> None:
    """Print data as a formatted table."""
    if not rows:
        return

    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))

    header_line = " | ".join(
        h.ljust(col_widths[i]) for i, h in enumerate(headers)
    )
    print(header_line)
    print("-" * len(header_line))

    for row in rows:
        print(" | ".join(
            str(cell).ljust(col_widths[i]) for i, cell in enumerate(row)
        ))

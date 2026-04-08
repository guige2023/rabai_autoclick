"""
Command Builder Utilities

Provides utilities for building and executing
system commands in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, list
import shlex


@dataclass
class CommandPart:
    """Represents a single part of a command."""
    value: str
    quoted: bool = False
    escaped: bool = False


@dataclass
class Command:
    """Represents a complete command."""
    program: str
    args: list[str] = field(default_factory=list)
    env: dict[str, str] | None = None
    cwd: str | None = None
    timeout: float | None = None

    def add_arg(self, arg: str, escape: bool = True) -> Command:
        """Add an argument to the command."""
        if escape:
            self.args.append(shlex.quote(arg))
        else:
            self.args.append(arg)
        return self

    def add_flag(self, flag: str, value: str | None = None) -> Command:
        """Add a flag (option) to the command."""
        if value is not None:
            self.args.append(f"{flag}={shlex.quote(value)}")
        else:
            self.args.append(flag)
        return self

    def build(self) -> str:
        """Build command string."""
        parts = [self.program] + self.args
        return " ".join(parts)

    def build_list(self) -> list[str]:
        """Build command as list for subprocess."""
        result = [self.program]
        for arg in self.args:
            unquoted = arg.strip("'\"")
            result.append(unquoted)
        return result


class CommandBuilder:
    """
    Builder for constructing commands.
    
    Provides fluent interface for building
    complex commands with arguments and options.
    """

    def __init__(self, program: str) -> None:
        self._command = Command(program=program)

    def arg(self, value: str, escape: bool = True) -> CommandBuilder:
        """Add an argument."""
        self._command.add_arg(value, escape)
        return self

    def flag(self, flag: str, value: str | None = None) -> CommandBuilder:
        """Add a flag."""
        self._command.add_flag(flag, value)
        return self

    def env(self, key: str, value: str) -> CommandBuilder:
        """Set environment variable."""
        if self._command.env is None:
            self._command.env = {}
        self._command.env[key] = value
        return self

    def cwd(self, directory: str) -> CommandBuilder:
        """Set working directory."""
        self._command.cwd = directory
        return self

    def timeout(self, seconds: float) -> CommandBuilder:
        """Set command timeout."""
        self._command.timeout = seconds
        return self

    def build(self) -> Command:
        """Build the command."""
        return self._command

    def build_string(self) -> str:
        """Build command as string."""
        return self._command.build()


def build_command(program: str) -> CommandBuilder:
    """
    Create a command builder.
    
    Args:
        program: Program name to run.
        
    Returns:
        CommandBuilder instance.
    """
    return CommandBuilder(program)

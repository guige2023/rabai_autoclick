"""
Option Parser Utilities

Provides utilities for parsing command-line options
and configurations in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class Option:
    """Represents a parsed command-line option."""
    name: str
    value: Any
    short_flag: str | None = None


@dataclass
class ParsedOptions:
    """Container for parsed options."""
    options: dict[str, Option] = field(default_factory=dict)
    positional: list[str] = field(default_factory=list)


class OptionParser:
    """
    Parses command-line options and arguments.
    
    Supports short flags, long flags, and positional arguments.
    """

    def __init__(self) -> None:
        self._option_definitions: dict[str, dict[str, Any]] = {}
        self._short_to_long: dict[str, str] = {}

    def add_option(
        self,
        name: str,
        short: str | None = None,
        value_type: type = str,
        default: Any = None,
        required: bool = False,
    ) -> None:
        """Define an option."""
        self._option_definitions[name] = {
            "type": value_type,
            "default": default,
            "required": required,
        }
        if short:
            self._short_to_long[short] = name

    def parse(self, args: list[str]) -> ParsedOptions:
        """
        Parse command-line arguments.
        
        Args:
            args: List of argument strings.
            
        Returns:
            ParsedOptions with results.
        """
        result = ParsedOptions()
        i = 0
        while i < len(args):
            arg = args[i]
            if arg.startswith("--"):
                name = arg[2:]
                value = self._option_definitions.get(name, {}).get("default")
                if i + 1 < len(args) and not args[i + 1].startswith("-"):
                    value = args[i + 1]
                    i += 1
                result.options[name] = Option(name=name, value=value)
            elif arg.startswith("-"):
                short = arg[1:]
                name = self._short_to_long.get(short, short)
                value = self._option_definitions.get(name, {}).get("default")
                if i + 1 < len(args) and not args[i + 1].startswith("-"):
                    value = args[i + 1]
                    i += 1
                result.options[name] = Option(name=name, value=value)
            else:
                result.positional.append(arg)
            i += 1
        return result

    def get(
        self,
        options: ParsedOptions,
        name: str,
        default: Any = None,
    ) -> Any:
        """Get option value with default."""
        if name in options.options:
            return options.options[name].value
        return default

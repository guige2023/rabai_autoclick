"""CLI utilities for building command-line interfaces to automation tools.

Provides argument parsing, command registration, and
interactive prompt helpers for creating CLI tools around
automation workflows.

Example:
    >>> from utils.cli_utils import CLI, command, argument
    >>> cli = CLI('autoclick')
    >>> @cli.command()
    ... @argument('--x', type=int)
    ... def click(args):
    ...     click_at(args.x, args.y)
    >>> cli.run()
"""

from __future__ import annotations

import argparse
import sys
from typing import Callable, Optional

__all__ = [
    "CLI",
    "command",
    "argument",
    "prompt",
    "confirm",
    "choose",
    "CLIError",
]


class CLIError(Exception):
    """Raised when CLI operations fail."""
    pass


# Global argument parser for decorators
_argument_stack: list = []


def argument(*args, **kwargs) -> Callable:
    """Decorator to add arguments to a CLI command.

    Example:
        >>> @command()
        ... @argument('--name', default='World')
        ... def hello(args):
        ...     print(f"Hello, {args.name}!")
    """
    def decorator(func: Callable) -> Callable:
        if not hasattr(func, "_cli_args"):
            func._cli_args = []
        func._cli_args.insert(0, (args, kwargs))
        return func
    return decorator


def prompt(message: str, default: Optional[str] = None, password: bool = False) -> str:
    """Display a prompt and get user input.

    Args:
        message: Prompt message.
        default: Default value if user presses Enter.
        password: If True, use getpass-style input.

    Returns:
        User input string.
    """
    if default:
        prompt_str = f"{message} [{default}]: "
    else:
        prompt_str = f"{message}: "

    try:
        if password:
            import getpass
            return getpass.getpass(prompt_str) or (default or "")
        else:
            return input(prompt_str) or (default or "")
    except (EOFError, KeyboardInterrupt):
        return default or ""


def confirm(message: str, default: bool = False) -> bool:
    """Ask a yes/no confirmation question.

    Args:
        message: Question text.
        default: Default answer if user presses Enter.

    Returns:
        True if user answered yes.
    """
    suffix = " [Y/n]: " if default else " [y/N]: "
    while True:
        response = input(message + suffix).strip().lower()
        if not response:
            return default
        if response in ("y", "yes"):
            return True
        if response in ("n", "no"):
            return False
        print("Please answer 'y' or 'n'")


def choose(message: str, options: list[str], default: Optional[int] = None) -> str:
    """Ask user to choose from a list of options.

    Args:
        message: Prompt message.
        options: List of option strings.
        default: Default option index if user presses Enter.

    Returns:
        Selected option string.
    """
    if not options:
        raise CLIError("No options provided")

    print(message)
    for i, opt in enumerate(options, 1):
        marker = "*" if default is not None and default == i - 1 else " "
        print(f"  {marker} {i}. {opt}")

    while True:
        try:
            response = input("Choice: ").strip()
            if not response and default is not None:
                return options[default]
            idx = int(response) - 1
            if 0 <= idx < len(options):
                return options[idx]
            print(f"Please enter a number between 1 and {len(options)}")
        except ValueError:
            print("Please enter a valid number")


class CLI:
    """A command-line interface builder.

    Example:
        >>> cli = CLI('mytool', description='My automation tool')
        >>> @cli.command('greet')
        ... @argument('--name', default='World')
        ... def greet(args):
        ...     print(f"Hello, {args.name}!")
        >>> cli.run()
    """

    def __init__(
        self,
        name: str = "cli",
        description: str = "",
        version: Optional[str] = None,
    ):
        self.name = name
        self.description = description
        self.version = version
        self._commands: dict[str, Callable] = {}
        self._parser = argparse.ArgumentParser(
            description=description,
            prog=name,
        )

        if version:
            self._parser.add_argument("--version", action="version", version=version)

        self._subparsers = self._parser.add_subparsers(dest="command")

    def command(self, name: Optional[str] = None) -> Callable:
        """Decorator to register a command.

        Args:
            name: Command name (uses function name if None).

        Returns:
            Decorator function.
        """
        def decorator(fn: Callable) -> Callable:
            cmd_name = name or fn.__name__
            self._commands[cmd_name] = fn

            cmd_parser = self._subparsers.add_parser(
                cmd_name,
                help=fn.__doc__ or "",
            )

            # Add arguments from decorators
            args = getattr(fn, "_cli_args", [])
            for arg_args, arg_kwargs in args:
                cmd_parser.add_argument(*arg_args, **arg_kwargs)

            return fn
        return decorator

    def run(self, argv: Optional[list[str]] = None) -> int:
        """Run the CLI with the given arguments.

        Args:
            argv: Command line arguments (uses sys.argv if None).

        Returns:
            Exit code.
        """
        args = self._parser.parse_args(argv)

        if args.command is None:
            self._parser.print_help()
            return 0

        if args.command not in self._commands:
            self._parser.print_help()
            return 1

        try:
            result = self._commands[args.command](args)
            return result if isinstance(result, int) else 0
        except SystemExit as e:
            return e.code if e.code is not None else 0
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return 1

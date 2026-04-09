"""
Automation script utilities for parsing and executing scripts.

This module provides utilities for parsing, validating,
and executing automation scripts from various formats.
"""

from __future__ import annotations

import re
import time
from typing import List, Dict, Any, Optional, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path


class ScriptTokenType(Enum):
    """Token types in automation scripts."""
    COMMAND = auto()
    ARGUMENT = auto()
    LABEL = auto()
    COMMENT = auto()
    NEWLINE = auto()
    CONDITIONAL = auto()
    LOOP = auto()
    GOTO = auto()
    WAIT = auto()
    VARIABLE = auto()
    EOF = auto()


@dataclass
class ScriptToken:
    """A token in an automation script."""
    type: ScriptTokenType
    value: str
    line: int
    column: int


@dataclass
class ScriptCommand:
    """
    A parsed command from an automation script.

    Attributes:
        command: Command name.
        args: Positional arguments.
        kwargs: Keyword arguments.
        raw: Raw command string.
        line: Line number in source.
    """
    command: str
    args: List[Any] = field(default_factory=list)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    raw: str = ""
    line: int = 0

    def __repr__(self) -> str:
        return f"Command({self.command}, args={self.args}, kwargs={self.kwargs})"


@dataclass
class AutomationScript:
    """
    A parsed automation script.

    Attributes:
        name: Script name.
        commands: List of parsed commands.
        variables: Dictionary of variables.
        labels: Dictionary of label -> command index.
        metadata: Script metadata.
    """
    name: str = "unnamed"
    commands: List[ScriptCommand] = field(default_factory=list)
    variables: Dict[str, Any] = field(default_factory=dict)
    labels: Dict[str, int] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_command(self, cmd: ScriptCommand) -> None:
        """Add a command to the script."""
        self.commands.append(cmd)

    def get_command_at_label(self, label: str) -> Optional[ScriptCommand]:
        """Get the command at a specific label."""
        idx = self.labels.get(label)
        if idx is not None and idx < len(self.commands):
            return self.commands[idx]
        return None


class ScriptLexer:
    """
    Lexer for automation scripts.

    Supports simple command syntax:
        command arg1 arg2 key=value
        # comment
        label:
    """

    COMMAND_PATTERN = re.compile(r'^(\w+)\s*(.*)$')
    LABEL_PATTERN = re.compile(r'^(\w+):\s*$')
    ASSIGN_PATTERN = re.compile(r'^(\w+)\s*=\s*(.+)$')
    COMMENT_PATTERN = re.compile(r'^#.*$')

    def tokenize(self, source: str) -> List[ScriptToken]:
        """Tokenize script source into tokens."""
        tokens = []
        lines = source.split('\n')

        for line_num, line in enumerate(lines):
            stripped = line.strip()
            if not stripped or self.COMMENT_PATTERN.match(stripped):
                continue

            # Check for label
            label_match = self.LABEL_PATTERN.match(stripped)
            if label_match:
                tokens.append(ScriptToken(
                    ScriptTokenType.LABEL,
                    label_match.group(1),
                    line_num + 1, 0
                ))
                continue

            # Check for variable assignment
            assign_match = self.ASSIGN_PATTERN.match(stripped)
            if assign_match:
                tokens.append(ScriptToken(
                    ScriptTokenType.VARIABLE,
                    f"{assign_match.group(1)}={assign_match.group(2).strip()}",
                    line_num + 1, 0
                ))
                continue

            # Regular command
            cmd_match = self.COMMAND_PATTERN.match(stripped)
            if cmd_match:
                tokens.append(ScriptToken(
                    ScriptTokenType.COMMAND,
                    cmd_match.group(1),
                    line_num + 1, 0
                ))
                # Parse arguments
                args_str = cmd_match.group(2).strip()
                if args_str:
                    tokens.append(ScriptToken(
                        ScriptTokenType.ARGUMENT,
                        args_str,
                        line_num + 1, len(cmd_match.group(1)) + 1
                    ))

        tokens.append(ScriptToken(ScriptTokenType.EOF, '', -1, -1))
        return tokens


class ScriptParser:
    """
    Parser for automation scripts.

    Converts tokens into structured ScriptCommand objects.
    """

    def __init__(self):
        """Initialize the parser."""
        self._lexer = ScriptLexer()

    def parse(self, source: str, name: str = "script") -> AutomationScript:
        """
        Parse script source into an AutomationScript.

        Args:
            source: Script source code.
            name: Script name.

        Returns:
            Parsed AutomationScript object.
        """
        script = AutomationScript(name=name)
        tokens = self._lexer.tokenize(source)

        i = 0
        while i < len(tokens) - 1:  # Skip EOF
            token = tokens[i]

            if token.type == ScriptTokenType.LABEL:
                script.labels[token.value] = len(script.commands)
                i += 1
                continue

            if token.type == ScriptTokenType.VARIABLE:
                self._parse_variable(token.value, script)
                i += 1
                continue

            if token.type == ScriptTokenType.COMMAND:
                # Look ahead for arguments
                args_str = ""
                if i + 1 < len(tokens) and tokens[i + 1].type == ScriptTokenType.ARGUMENT:
                    args_str = tokens[i + 1].value
                    i += 1

                cmd = self._parse_command(token.value, args_str, token.line)
                script.add_command(cmd)

            i += 1

        return script

    def _parse_variable(self, assignment: str, script: AutomationScript) -> None:
        """Parse a variable assignment."""
        if '=' in assignment:
            name, value = assignment.split('=', 1)
            script.variables[name.strip()] = self._parse_value(value.strip())

    def _parse_command(
        self, command: str, args_str: str, line: int
    ) -> ScriptCommand:
        """Parse a command with its arguments."""
        cmd = ScriptCommand(command=command, raw=f"{command} {args_str}".strip(), line=line)

        # Parse positional and keyword arguments
        parts = self._split_args(args_str)
        for part in parts:
            if '=' in part:
                key, value = part.split('=', 1)
                cmd.kwargs[key.strip()] = self._parse_value(value.strip())
            else:
                cmd.args.append(self._parse_value(part.strip()))

        return cmd

    def _split_args(self, args_str: str) -> List[str]:
        """Split argument string into parts."""
        if not args_str:
            return []
        # Simple split by whitespace, respecting quotes
        parts = []
        current = []
        in_quote = False
        quote_char = None
        for char in args_str:
            if char in '"\'' and not in_quote:
                in_quote = True
                quote_char = char
            elif char == quote_char and in_quote:
                in_quote = False
                quote_char = None
            elif char == ' ' and not in_quote:
                if current:
                    parts.append(''.join(current))
                    current = []
            else:
                current.append(char)
        if current:
            parts.append(''.join(current))
        return parts

    def _parse_value(self, value: str) -> Any:
        """Parse a value string into a Python value."""
        value = value.strip()

        # Boolean
        if value.lower() == 'true':
            return True
        if value.lower() == 'false':
            return False
        if value.lower() == 'null' or value.lower() == 'none':
            return None

        # Number
        try:
            if '.' in value:
                return float(value)
            return int(value)
        except ValueError:
            pass

        # String (remove quotes)
        if (value.startswith('"') and value.endswith('"')) or \
           (value.startswith("'") and value.endswith("'")):
            return value[1:-1]

        return value


class ScriptExecutor:
    """
    Executes parsed automation scripts.

    Supports command registry and variable expansion.
    """

    def __init__(self):
        """Initialize the executor."""
        self._commands: Dict[str, Callable] = {}
        self._variables: Dict[str, Any] = {}
        self._is_running = False
        self._is_paused = False
        self._log: List[str] = []

    def register_command(
        self, name: str, handler: Callable[..., Any]
    ) -> None:
        """
        Register a command handler.

        Args:
            name: Command name.
            handler: Callable that takes ScriptCommand.
        """
        self._commands[name] = handler

    def execute(self, script: AutomationScript) -> None:
        """
        Execute a parsed script.

        Args:
            script: The AutomationScript to execute.
        """
        self._is_running = True
        self._variables.update(script.variables)

        idx = 0
        while idx < len(script.commands) and self._is_running:
            if self._is_paused:
                time.sleep(0.05)
                continue

            cmd = script.commands[idx]
            self._log_command(cmd)

            if cmd.command == 'goto':
                label = cmd.args[0] if cmd.args else ''
                idx = script.labels.get(label, idx + 1)
            elif cmd.command == 'wait':
                time.sleep(float(cmd.args[0]) if cmd.args else 1.0)
                idx += 1
            elif cmd.command == 'if':
                condition = cmd.args[0] if cmd.args else ''
                if self._evaluate_condition(condition):
                    label = cmd.kwargs.get('goto', '')
                    if label:
                        idx = script.labels.get(label, idx + 1)
                    else:
                        idx += 1
                else:
                    idx += 1
            else:
                self._execute_command(cmd)
                idx += 1

        self._is_running = False

    def _execute_command(self, cmd: ScriptCommand) -> None:
        """Execute a single command."""
        # Expand variables in arguments
        expanded_args = [self._expand_vars(str(a)) for a in cmd.args]
        expanded_kwargs = {
            k: self._expand_vars(str(v))
            for k, v in cmd.kwargs.items()
        }

        if cmd.command in self._commands:
            handler = self._commands[cmd.command]
            try:
                handler(cmd, *expanded_args, **expanded_kwargs)
            except Exception as e:
                self._log.append(f"Error in {cmd.command}: {e}")
        else:
            self._log.append(f"Unknown command: {cmd.command}")

    def _expand_vars(self, value: str) -> str:
        """Expand variable references in a string."""
        for name, val in self._variables.items():
            value = value.replace(f'${name}', str(val))
        return value

    def _evaluate_condition(self, condition: str) -> bool:
        """Evaluate a simple condition."""
        if '=' in condition:
            parts = condition.split('=')
            var_name = parts[0].strip()
            expected = self._expand_vars(parts[1].strip())
            actual = str(self._variables.get(var_name, ''))
            return actual == expected
        return bool(self._variables.get(condition, False))

    def _log_command(self, cmd: ScriptCommand) -> None:
        """Log a command execution."""
        self._log.append(f"Line {cmd.line}: {cmd.command} {cmd.args}")

    def pause(self) -> None:
        """Pause execution."""
        self._is_paused = True

    def resume(self) -> None:
        """Resume execution."""
        self._is_paused = False

    def stop(self) -> None:
        """Stop execution."""
        self._is_running = False
        self._is_paused = False

    def set_variable(self, name: str, value: Any) -> None:
        """Set a runtime variable."""
        self._variables[name] = value

    def get_variable(self, name: str) -> Any:
        """Get a runtime variable."""
        return self._variables.get(name)

    def get_log(self) -> List[str]:
        """Get execution log."""
        return self._log.copy()


def parse_script_file(path: str, name: Optional[str] = None) -> AutomationScript:
    """
    Parse a script file.

    Args:
        path: Path to the script file.
        name: Optional script name.

    Returns:
        Parsed AutomationScript.
    """
    p = Path(path)
    source = p.read_text()
    script_name = name or p.stem
    parser = ScriptParser()
    return parser.parse(source, script_name)


def create_basic_script(
    commands: List[Tuple[str, List, Dict]]
) -> AutomationScript:
    """
    Create a script from command tuples.

    Args:
        commands: List of (command_name, args, kwargs) tuples.

    Returns:
        AutomationScript object.
    """
    script = AutomationScript()
    for i, (cmd, args, kwargs) in enumerate(commands):
        script.add_command(ScriptCommand(
            command=cmd,
            args=list(args),
            kwargs=dict(kwargs),
            line=i + 1
        ))
    return script

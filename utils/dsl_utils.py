"""
Domain-specific language builder utilities.

Provides building blocks for creating DSLs: fluent builders,
command chains, and declarative rule systems.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Optional, Union
from dataclasses import dataclass, field


class DSLNode:
    """Base class for DSL expression nodes."""

    def __init__(self, name: str, **attrs: Any):
        self.name = name
        self.attrs = attrs

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r}, {self.attrs})"


@dataclass
class Command(DSLNode):
    """A named command with optional arguments and children."""
    args: dict[str, Any] = field(default_factory=dict)
    children: list[DSLNode] = field(default_factory=list)

    def add(self, *children: DSLNode) -> Command:
        self.children.extend(children)
        return self

    def render(self, indent: int = 0) -> str:
        args_str = " ".join(f'{k}={v!r}' for k, v in self.args.items())
        prefix = "  " * indent
        if self.children:
            return f"{prefix}{self.name}({args_str}):\n" + "\n".join(
                c.render(indent + 1) if isinstance(c, Command) else f"{prefix}  {c}"
                for c in self.children
            )
        return f"{prefix}{self.name}({args_str})"


class DSLBuilder:
    """Builder for constructing DSL expressions fluently."""

    def __init__(self):
        self._commands: list[Command] = []
        self._context: dict[str, Any] = {}

    def cmd(self, name: str, **kwargs: Any) -> Command:
        cmd = Command(name=name, args=kwargs)
        self._commands.append(cmd)
        return cmd

    def context(self, **kwargs: Any) -> DSLBuilder:
        self._context.update(kwargs)
        return self

    def build(self) -> list[Command]:
        return self._commands

    def reset(self) -> DSLBuilder:
        self._commands.clear()
        self._context.clear()
        return self


class CommandChain:
    """Chains multiple operations into a single executable command."""

    def __init__(self, name: str):
        self.name = name
        self._ops: list[tuple[str, tuple, dict]] = []
        self._result: Any = None

    def op(self, name: str, *args: Any, **kwargs: Any) -> CommandChain:
        self._ops.append((name, args, kwargs))
        return self

    def then(self, name: str, *args: Any, **kwargs: Any) -> CommandChain:
        return self.op(name, *args, **kwargs)

    def execute(self, executor: Callable[[str, tuple, dict], Any]) -> Any:
        for op_name, op_args, op_kwargs in self._ops:
            self._result = executor(op_name, op_args, op_kwargs)
        return self._result

    def __repr__(self) -> str:
        ops_str = " -> ".join(f"{n}({a})" for n, a, _ in self._ops)
        return f"CommandChain({self.name}): {ops_str}"


class Rule:
    """A declarative rule with a condition and action."""

    def __init__(
        self,
        name: str,
        condition: Callable[[dict[str, Any]], bool],
        action: Callable[[dict[str, Any]], Any],
        priority: int = 0,
    ):
        self.name = name
        self.condition = condition
        self.action = action
        self.priority = priority

    def evaluate(self, context: dict[str, Any]) -> tuple[bool, Any]:
        """Evaluate the rule's condition and execute action if true."""
        if self.condition(context):
            result = self.action(context)
            return True, result
        return False, None


class RuleSet:
    """A collection of rules that can be evaluated against a context."""

    def __init__(self, name: str = ""):
        self.name = name
        self.rules: list[Rule] = []
        self._cache: dict[str, Any] = {}

    def add(self, rule: Rule) -> RuleSet:
        self.rules.append(rule)
        self.rules.sort(key=lambda r: r.priority, reverse=True)
        return self

    def add_rule(
        self,
        name: str,
        condition: Callable[[dict[str, Any]], bool],
        action: Callable[[dict[str, Any]], Any],
        priority: int = 0,
    ) -> RuleSet:
        return self.add(Rule(name, condition, action, priority))

    def evaluate(self, context: dict[str, Any], stop_on_first: bool = False) -> list[tuple[Rule, Any]]:
        """Evaluate all matching rules against context."""
        results = []
        for rule in self.rules:
            matched, result = rule.evaluate(context)
            if matched:
                results.append((rule, result))
                if stop_on_first:
                    break
        return results

    def clear(self) -> None:
        self.rules.clear()
        self._cache.clear()


class TemplateRenderer:
    """Simple template renderer with variable substitution."""

    VAR_PATTERN = re.compile(r"\{\{(\w+)(?::([^}]*))?\}\}")

    def __init__(self, template: str):
        self.template = template

    def render(self, **variables: Any) -> str:
        """Render the template with provided variables."""

        def replacer(m: re.Match) -> str:
            var_name = m.group(1)
            default = m.group(2) or ""
            value = variables.get(var_name, default)
            return str(value)

        return self.VAR_PATTERN.sub(replacer, self.template)


class DSLParser:
    """Parses simple DSL syntax into structured commands."""

    BLOCK_RE = re.compile(r"^(\w+)(?:\(([^)]*)\))?:$")
    LINE_RE = re.compile(r"^(\w+)(?:\(([^)]*)\))?(?:\s+(.*))?$")

    def __init__(self):
        self._handlers: dict[str, Callable[..., Any]] = {}

    def register(self, keyword: str, handler: Callable[..., Any]) -> DSLParser:
        self._handlers[keyword.lower()] = handler
        return self

    def parse(self, source: str) -> list[Command]:
        """Parse DSL source text into command tree."""
        lines = [l.rstrip() for l in source.strip().split("\n")]
        return self._parse_block(lines, 0)[0]

    def _parse_block(
        self, lines: list[str], start: int
    ) -> tuple[list[Command], int]:
        commands: list[Command] = []
        i = start
        while i < len(lines):
            line = lines[i]
            if not line or line.startswith("#"):
                i += 1
                continue
            match = self.LINE_RE.match(line)
            if not match:
                i += 1
                continue
            keyword, args_str, body = match.groups()
            args = self._parse_args(args_str or "")
            cmd = Command(name=keyword.lower(), args=args)
            # Check for block (next lines are indented)
            if i + 1 < len(lines) and lines[i + 1].startswith("  "):
                sub_commands, i = self._parse_block(lines, i + 1)
                cmd.children = sub_commands
            commands.append(cmd)
            i += 1
        return commands, i

    def _parse_args(self, args_str: str) -> dict[str, Any]:
        args: dict[str, Any] = {}
        if not args_str:
            return args
        parts = args_str.split(",")
        for part in parts:
            part = part.strip()
            if "=" in part:
                k, v = part.split("=", 1)
                args[k.strip()] = self._parse_value(v.strip())
            else:
                args[part] = True
        return args

    def _parse_value(self, value: str) -> Any:
        if value.startswith('"') and value.endswith('"'):
            return value[1:-1]
        if value.startswith("'") and value.endswith("'"):
            return value[1:-1]
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            pass
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        if value.lower() == "none":
            return None
        return value


def fluent(name: str) -> CommandChain:
    """Create a new fluent command chain."""
    return CommandChain(name)


def rule(
    name: str,
    condition: Callable[[dict[str, Any]], bool],
    action: Callable[[dict[str, Any]], Any],
) -> Rule:
    return Rule(name, condition, action)

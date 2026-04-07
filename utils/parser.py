"""Parsing utilities for RabAI AutoClick.

Provides:
- String parsing
- Expression evaluation
- Template parsing
"""

import ast
import os
import re
from typing import Any, Callable, Dict, List, Optional, Tuple


class StringParser:
    """Parse strings with patterns."""

    def __init__(self, text: str) -> None:
        """Initialize parser.

        Args:
            text: Text to parse.
        """
        self._text = text
        self._pos = 0

    @property
    def pos(self) -> int:
        """Get current position."""
        return self._pos

    @property
    def remaining(self) -> str:
        """Get remaining text."""
        return self._text[self._pos:]

    @property
    def at_end(self) -> bool:
        """Check if at end."""
        return self._pos >= len(self._text)

    def peek(self, length: int = 1) -> str:
        """Peek at text without advancing.

        Args:
            length: Length to peek.

        Returns:
            Peeked text.
        """
        return self._text[self._pos:self._pos + length]

    def read(self, length: int = 1) -> str:
        """Read text and advance.

        Args:
            length: Length to read.

        Returns:
            Read text.
        """
        result = self._text[self._pos:self._pos + length]
        self._pos += length
        return result

    def read_while(self, predicate: Callable[[str], bool]) -> str:
        """Read while predicate is true.

        Args:
            predicate: Function that returns True for valid chars.

        Returns:
            Read text.
        """
        start = self._pos
        while not self.at_end and predicate(self.peek()):
            self._pos += 1
        return self._text[start:self._pos]

    def skip_whitespace(self) -> None:
        """Skip whitespace characters."""
        while not self.at_end and self.peek() in " \t\n\r":
            self._pos += 1

    def match(self, pattern: str) -> bool:
        """Match pattern at current position.

        Args:
            pattern: Regex pattern.

        Returns:
            True if matched.
        """
        return bool(re.match(pattern, self.remaining))

    def search(self, pattern: str) -> Optional[str]:
        """Search for pattern.

        Args:
            pattern: Regex pattern.

        Returns:
            Matched text or None.
        """
        match = re.search(pattern, self.remaining)
        if match:
            return match.group()
        return None

    def extract(self, pattern: str) -> List[str]:
        """Extract all matches.

        Args:
            pattern: Regex pattern.

        Returns:
            List of matches.
        """
        return re.findall(pattern, self.remaining)


class ExpressionParser:
    """Parse and evaluate simple expressions."""

    def __init__(self) -> None:
        """Initialize parser."""
        self._operators = {
            "+": lambda a, b: a + b,
            "-": lambda a, b: a - b,
            "*": lambda a, b: a * b,
            "/": lambda a, b: a / b if b != 0 else 0,
            "//": lambda a, b: a // b if b != 0 else 0,
            "%": lambda a, b: a % b if b != 0 else 0,
            "**": lambda a, b: a ** b,
        }

    def evaluate(self, expression: str, context: Optional[Dict[str, Any]] = None) -> Any:
        """Evaluate expression.

        Args:
            expression: Expression string.
            context: Variable context.

        Returns:
            Result.
        """
        context = context or {}

        try:
            # Safe eval using AST
            return self._safe_eval(expression, context)
        except Exception:
            return None

    def _safe_eval(self, expression: str, context: Dict[str, Any]) -> Any:
        """Safely evaluate expression using AST.

        Args:
            expression: Expression string.
            context: Variable context.

        Returns:
            Result.
        """
        # Parse into AST
        tree = ast.parse(expression, mode="eval")

        # Evaluate
        return self._eval_node(tree.body, context)

    def _eval_node(self, node: ast.AST, context: Dict[str, Any]) -> Any:
        """Evaluate AST node.

        Args:
            node: AST node.
            context: Variable context.

        Returns:
            Result.
        """
        if isinstance(node, ast.Constant):
            return node.value

        elif isinstance(node, ast.Name):
            if node.id in context:
                return context[node.id]
            raise NameError(f"Unknown variable: {node.id}")

        elif isinstance(node, ast.BinOp):
            left = self._eval_node(node.left, context)
            right = self._eval_node(node.right, context)
            op_type = type(node.op).__name__
            op_map = {
                "Add": "+",
                "Sub": "-",
                "Mult": "*",
                "Div": "/",
                "FloorDiv": "//",
                "Mod": "%",
                "Pow": "**",
            }
            op = op_map.get(op_type)
            if op and op in self._operators:
                return self._operators[op](left, right)
            raise ValueError(f"Unsupported operator: {op_type}")

        elif isinstance(node, ast.UnaryOp):
            operand = self._eval_node(node.operand, context)
            if isinstance(node.op, ast.USub):
                return -operand
            elif isinstance(node.op, ast.UAdd):
                return +operand
            elif isinstance(node.op, ast.Not):
                return not operand

        elif isinstance(node, ast.BoolOp):
            left = self._eval_node(node.values[0], context)
            for op, right in zip(node.ops, node.values[1:]):
                right = self._eval_node(right, context)
                if isinstance(op, ast.And):
                    left = left and right
                elif isinstance(op, ast.Or):
                    left = left or right
            return left

        elif isinstance(node, ast.Compare):
            left = self._eval_node(node.left, context)
            for op, comparator in zip(node.ops, node.comparators):
                right = self._eval_node(comparator, context)
                if isinstance(op, ast.Eq):
                    left = left == right
                elif isinstance(op, ast.NotEq):
                    left = left != right
                elif isinstance(op, ast.Lt):
                    left = left < right
                elif isinstance(op, ast.LtE):
                    left = left <= right
                elif isinstance(op, ast.Gt):
                    left = left > right
                elif isinstance(op, ast.GtE):
                    left = left >= right
            return left

        raise ValueError(f"Unsupported node type: {type(node).__name__}")


class TemplateParser:
    """Parse templates with placeholders."""

    def __init__(self, template: str) -> None:
        """Initialize parser.

        Args:
            template: Template string.
        """
        self._template = template
        self._pattern = re.compile(r"\$\{([^}]+)\}")

    def render(self, context: Dict[str, Any]) -> str:
        """Render template with context.

        Args:
            context: Variables for placeholders.

        Returns:
            Rendered string.
        """
        def replace(match):
            expr = match.group(1)
            return self._evaluate(expr, context)

        return self._pattern.sub(replace, self._template)

    def _evaluate(self, expr: str, context: Dict[str, Any]) -> str:
        """Evaluate expression.

        Args:
            expr: Expression string.
            context: Variable context.

        Returns:
            Result as string.
        """
        # Handle filters
        parts = expr.split("|")
        value = parts[0].strip()

        # Get value from context
        if "." in value:
            # Object attribute access
            parts_name = value.split(".")
            obj = context
            for p in parts_name:
                if obj is None:
                    return ""
                obj = obj.get(p) if isinstance(obj, dict) else getattr(obj, p, None)
            result = obj
        else:
            result = context.get(value, "")

        # Apply filters
        for filter_name in parts[1:]:
            result = self._apply_filter(filter_name.strip(), result)

        return str(result) if result is not None else ""

    def _apply_filter(self, name: str, value: Any) -> Any:
        """Apply filter to value.

        Args:
            name: Filter name.
            value: Value to filter.

        Returns:
            Filtered value.
        """
        filters = {
            "upper": lambda v: str(v).upper(),
            "lower": lambda v: str(v).lower(),
            "trim": lambda v: str(v).strip(),
            "capitalize": lambda v: str(v).capitalize(),
            "default": lambda v, d="": str(v) if v else d,
        }

        if name in filters:
            return filters[name](value)
        return value


class KeyValueParser:
    """Parse key-value pairs."""

    def __init__(self, delimiter: str = "=", separator: str = ";") -> None:
        """Initialize parser.

        Args:
            delimiter: Key-value delimiter.
            separator: Pair separator.
        """
        self._delimiter = delimiter
        self._separator = separator

    def parse(self, text: str) -> Dict[str, str]:
        """Parse text into key-value pairs.

        Args:
            text: Text to parse.

        Returns:
            Dict of key-value pairs.
        """
        result = {}
        pairs = text.split(self._separator)

        for pair in pairs:
            if self._delimiter in pair:
                key, value = pair.split(self._delimiter, 1)
                result[key.strip()] = value.strip()

        return result

    def to_string(self, data: Dict[str, str]) -> str:
        """Convert dict to string.

        Args:
            data: Dict to convert.

        Returns:
            String representation.
        """
        pairs = [f"{k}{self._delimiter}{v}" for k, v in data.items()]
        return self._separator.join(pairs)


class INIParser:
    """Parse INI-style config files."""

    def __init__(self) -> None:
        """Initialize parser."""
        self._sections: Dict[str, Dict[str, str]] = {}
        self._current_section: Optional[str] = None

    def parse(self, text: str) -> Dict[str, Dict[str, str]]:
        """Parse INI text.

        Args:
            text: INI text to parse.

        Returns:
            Dict of sections.
        """
        self._sections = {}
        self._current_section = None

        for line in text.split("\n"):
            line = line.strip()

            if not line or line.startswith("#") or line.startswith(";"):
                continue

            if line.startswith("[") and line.endswith("]"):
                self._current_section = line[1:-1]
                self._sections[self._current_section] = {}
                continue

            if "=" in line:
                key, value = line.split("=", 1)
                if self._current_section:
                    self._sections[self._current_section][key.strip()] = value.strip()
                else:
                    # Global section
                    if "" not in self._sections:
                        self._sections[""] = {}
                    self._sections[""][key.strip()] = value.strip()

        return self._sections

    def get(self, section: str, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get value from section.

        Args:
            section: Section name.
            key: Key name.
            default: Default if not found.

        Returns:
            Value or default.
        """
        return self._sections.get(section, {}).get(key, default)

    def sections(self) -> List[str]:
        """Get section names.

        Returns:
            List of section names.
        """
        return list(self._sections.keys())

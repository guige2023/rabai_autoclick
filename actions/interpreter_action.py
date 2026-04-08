"""Interpreter action module for RabAI AutoClick.

Provides interpreter pattern implementation:
- Expression: Abstract expression interface
- Context: Context for interpretation
- TerminalExpression: Terminal symbol interpretation
- NonTerminalExpression: Non-terminal symbol interpretation
- Parser: Parse and build expression trees
"""

from typing import Any, Callable, Dict, List, Optional, Union
from abc import ABC, abstractmethod
import re

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Context:
    """Context for interpretation."""

    def __init__(self, data: Optional[Dict[str, Any]] = None):
        self._data = data or {}

    def get(self, key: str) -> Any:
        """Get value."""
        return self._data.get(key)

    def set(self, key: str, value: Any) -> None:
        """Set value."""
        self._data[key] = value

    def has(self, key: str) -> bool:
        """Check if key exists."""
        return key in self._data

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return self._data.copy()


class Expression(ABC):
    """Abstract expression interface."""

    @abstractmethod
    def interpret(self, context: Context) -> Any:
        """Interpret the expression."""
        pass

    def __and__(self, other: "Expression") -> "AndExpression":
        return AndExpression(self, other)

    def __or__(self, other: "Expression") -> "OrExpression":
        return OrExpression(self, other)

    def __invert__(self) -> "NotExpression":
        return NotExpression(self)


class TerminalExpression(Expression):
    """Terminal expression for literal values."""

    def __init__(self, value: Any):
        self.value = value

    def interpret(self, context: Context) -> Any:
        """Interpret literal value."""
        return self.value


class VariableExpression(Expression):
    """Variable expression referencing context."""

    def __init__(self, name: str):
        self.name = name

    def interpret(self, context: Context) -> Any:
        """Interpret variable from context."""
        return context.get(self.name)


class ContextAccessorExpression(Expression):
    """Access nested context values."""

    def __init__(self, path: str, delimiter: str = "."):
        self.path = path
        self.delimiter = delimiter

    def interpret(self, context: Context) -> Any:
        """Interpret path in context."""
        parts = self.path.split(self.delimiter)
        current = context._data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current


class EqualsExpression(Expression):
    """Equality comparison expression."""

    def __init__(self, left: Expression, right: Expression):
        self.left = left
        self.right = right

    def interpret(self, context: Context) -> bool:
        """Interpret equality check."""
        return self.left.interpret(context) == self.right.interpret(context)


class NotEqualsExpression(Expression):
    """Not equal comparison expression."""

    def __init__(self, left: Expression, right: Expression):
        self.left = left
        self.right = right

    def interpret(self, context: Context) -> bool:
        """Interpret not equal check."""
        return self.left.interpret(context) != self.right.interpret(context)


class GreaterThanExpression(Expression):
    """Greater than comparison expression."""

    def __init__(self, left: Expression, right: Expression):
        self.left = left
        self.right = right

    def interpret(self, context: Context) -> bool:
        """Interpret greater than check."""
        lv = self.left.interpret(context)
        rv = self.right.interpret(context)
        try:
            return float(lv) > float(rv)
        except (TypeError, ValueError):
            return lv > rv


class LessThanExpression(Expression):
    """Less than comparison expression."""

    def __init__(self, left: Expression, right: Expression):
        self.left = left
        self.right = right

    def interpret(self, context: Context) -> bool:
        """Interpret less than check."""
        lv = self.left.interpret(context)
        rv = self.right.interpret(context)
        try:
            return float(lv) < float(rv)
        except (TypeError, ValueError):
            return lv < rv


class AndExpression(Expression):
    """Logical AND expression."""

    def __init__(self, left: Expression, right: Expression):
        self.left = left
        self.right = right

    def interpret(self, context: Context) -> bool:
        """Interpret logical AND."""
        return bool(self.left.interpret(context)) and bool(self.right.interpret(context))


class OrExpression(Expression):
    """Logical OR expression."""

    def __init__(self, left: Expression, right: Expression):
        self.left = left
        self.right = right

    def interpret(self, context: Context) -> bool:
        """Interpret logical OR."""
        return bool(self.left.interpret(context)) or bool(self.right.interpret(context))


class NotExpression(Expression):
    """Logical NOT expression."""

    def __init__(self, expression: Expression):
        self.expression = expression

    def interpret(self, context: Context) -> bool:
        """Interpret logical NOT."""
        return not bool(self.expression.interpret(context))


class ContainsExpression(Expression):
    """Contains check expression."""

    def __init__(self, left: Expression, right: Expression):
        self.left = left
        self.right = right

    def interpret(self, context: Context) -> bool:
        """Interpret contains check."""
        lv = self.left.interpret(context)
        rv = self.right.interpret(context)
        if isinstance(lv, (list, tuple, str)):
            return rv in lv
        if isinstance(lv, dict):
            return rv in lv
        return False


class MatchesExpression(Expression):
    """Regex matches expression."""

    def __init__(self, left: Expression, pattern: str):
        self.left = left
        self.pattern = re.compile(pattern)

    def interpret(self, context: Context) -> bool:
        """Interpret regex match."""
        value = self.left.interpret(context)
        if value is None:
            return False
        return bool(self.pattern.match(str(value)))


class Parser:
    """Simple expression parser."""

    def __init__(self):
        self._variables: Dict[str, Any] = {}

    def parse(self, expression_str: str, context: Context) -> Expression:
        """Parse expression string."""
        expression_str = expression_str.strip()

        if expression_str.startswith("("") and expression_str.endswith("""):
            return TerminalExpression(expression_str[2:-1])

        if expression_str.startswith("(") and expression_str.endswith(")"):
            inner = expression_str[1:-1]
            return self._parse_logical(inner, context)

        if " and " in expression_str.lower():
            parts = re.split(r"\s+and\s+", expression_str, flags=re.IGNORECASE)
            left = self.parse(parts[0], context)
            right = self.parse(parts[1], context)
            return AndExpression(left, right)

        if " or " in expression_str.lower():
            parts = re.split(r"\s+or\s+", expression_str, flags=re.IGNORECASE)
            left = self.parse(parts[0], context)
            right = self.parse(parts[1], context)
            return OrExpression(left, right)

        if " not " in expression_str.lower():
            match = re.match(r"not\s+(.+)", expression_str, re.IGNORECASE)
            if match:
                inner = self.parse(match.group(1), context)
                return NotExpression(inner)

        if " == " in expression_str:
            left_str, right_str = expression_str.split(" == ", 1)
            return EqualsExpression(
                VariableExpression(left_str.strip()),
                self._parse_value(right_str.strip(), context),
            )

        if " != " in expression_str:
            left_str, right_str = expression_str.split(" != ", 1)
            return NotEqualsExpression(
                VariableExpression(left_str.strip()),
                self._parse_value(right_str.strip(), context),
            )

        if " > " in expression_str:
            left_str, right_str = expression_str.split(" > ", 1)
            return GreaterThanExpression(
                VariableExpression(left_str.strip()),
                self._parse_value(right_str.strip(), context),
            )

        if " < " in expression_str:
            left_str, right_str = expression_str.split(" < ", 1)
            return LessThanExpression(
                VariableExpression(left_str.strip()),
                self._parse_value(right_str.strip(), context),
            )

        if " matches " in expression_str:
            left_str, pattern = expression_str.split(" matches ", 1)
            return MatchesExpression(
                VariableExpression(left_str.strip()),
                pattern.strip().strip('"').strip("'"),
            )

        return VariableExpression(expression_str)

    def _parse_value(self, value_str: str, context: Context) -> Expression:
        """Parse a value expression."""
        value_str = value_str.strip()

        if value_str.startswith('"') and value_str.endswith('"'):
            return TerminalExpression(value_str[1:-1])

        if value_str.startswith("'") and value_str.endswith("'"):
            return TerminalExpression(value_str[1:-1])

        if value_str.lower() == "true":
            return TerminalExpression(True)
        if value_str.lower() == "false":
            return TerminalExpression(False)
        if value_str.lower() == "null" or value_str.lower() == "none":
            return TerminalExpression(None)

        try:
            num = float(value_str)
            if "." in value_str:
                return TerminalExpression(num)
            return TerminalExpression(int(num))
        except ValueError:
            pass

        return VariableExpression(value_str)

    def _parse_logical(self, expression_str: str, context: Context) -> Expression:
        """Parse logical expression."""
        return self.parse(expression_str, context)


class InterpreterAction(BaseAction):
    """Interpreter pattern action."""
    action_type = "interpreter"
    display_name = "解释器模式"
    description = "表达式解释器"

    def __init__(self):
        super().__init__()
        self._parser = Parser()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "evaluate")

            if operation == "evaluate":
                return self._evaluate(params)
            elif operation == "parse":
                return self._parse(params)
            elif operation == "build":
                return self._build_expression(params)
            elif operation == "batch":
                return self._batch_evaluate(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Interpreter error: {str(e)}")

    def _evaluate(self, params: Dict[str, Any]) -> ActionResult:
        """Evaluate an expression."""
        expression_str = params.get("expression", "")
        data = params.get("data", {})

        if not expression_str:
            return ActionResult(success=False, message="expression is required")

        ctx = Context(data)
        expression = self._parser.parse(expression_str, ctx)

        try:
            result = expression.interpret(ctx)
            return ActionResult(success=True, message="Expression evaluated", data={"result": result})
        except Exception as e:
            return ActionResult(success=False, message=f"Evaluation failed: {e}")

    def _parse(self, params: Dict[str, Any]) -> ActionResult:
        """Parse an expression."""
        expression_str = params.get("expression", "")
        data = params.get("data", {})

        if not expression_str:
            return ActionResult(success=False, message="expression is required")

        ctx = Context(data)
        expression = self._parser.parse(expression_str, ctx)

        return ActionResult(success=True, message="Expression parsed", data={"type": type(expression).__name__})

    def _build_expression(self, params: Dict[str, Any]) -> ActionResult:
        """Build expression programmatically."""
        expression_type = params.get("type")
        left_var = params.get("left_var")
        right_val = params.get("right_val")

        if not expression_type or not left_var:
            return ActionResult(success=False, message="type and left_var are required")

        left = VariableExpression(left_var)
        right = self._parser._parse_value(str(right_val), Context())

        expr_map = {
            "equals": EqualsExpression,
            "not_equals": NotEqualsExpression,
            "greater_than": GreaterThanExpression,
            "less_than": LessThanExpression,
            "contains": ContainsExpression,
        }

        if expression_type not in expr_map:
            return ActionResult(success=False, message=f"Unknown type: {expression_type}")

        expr = expr_map[expression_type](left, right)
        data = params.get("data", {})
        ctx = Context(data)
        result = expr.interpret(ctx)

        return ActionResult(success=True, message=f"Expression built and evaluated", data={"result": result})

    def _batch_evaluate(self, params: Dict[str, Any]) -> ActionResult:
        """Batch evaluate expressions."""
        expressions = params.get("expressions", [])
        data = params.get("data", {})

        if not expressions:
            return ActionResult(success=False, message="expressions is required")

        ctx = Context(data)
        results = []

        for expr_item in expressions:
            expr_str = expr_item.get("expression", "")
            expr_name = expr_item.get("name", expr_str[:20])

            try:
                expression = self._parser.parse(expr_str, ctx)
                result = expression.interpret(ctx)
                results.append({"name": expr_name, "result": result, "success": True})
            except Exception as e:
                results.append({"name": expr_name, "error": str(e), "success": False})

        successful = sum(1 for r in results if r["success"])

        return ActionResult(
            success=successful == len(results),
            message=f"Evaluated {successful}/{len(results)} expressions",
            data={"results": results},
        )

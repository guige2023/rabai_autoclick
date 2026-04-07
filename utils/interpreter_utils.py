"""
Interpreter Pattern Implementation

Defines a representation for a grammar and an interpreter
that uses the representation to interpret sentences.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class Context(ABC):
    """
    Context for the interpreter.
    Contains information that's global to the interpreter.
    """

    def __init__(self):
        self._variables: dict[str, Any] = {}
        self._results: list[Any] = []

    def set_variable(self, name: str, value: Any) -> None:
        """Set a variable value."""
        self._variables[name] = value

    def get_variable(self, name: str) -> Any:
        """Get a variable value."""
        return self._variables.get(name)

    def has_variable(self, name: str) -> bool:
        """Check if a variable exists."""
        return name in self._variables

    def add_result(self, result: Any) -> None:
        """Add an interpretation result."""
        self._results.append(result)

    def get_results(self) -> list[Any]:
        """Get all results."""
        return list(self._results)

    def clear_results(self) -> None:
        """Clear all results."""
        self._results.clear()


class Expression(ABC):
    """
    Abstract expression interface.
    """

    @abstractmethod
    def interpret(self, context: Context) -> Any:
        """Interpret this expression in the given context."""
        pass

    @abstractmethod
    def __str__(self) -> str:
        """String representation of the expression."""
        pass


class LiteralExpression(Expression):
    """
    Literal value expression.
    """

    def __init__(self, value: Any):
        self.value = value

    def interpret(self, context: Context) -> Any:
        return self.value

    def __str__(self) -> str:
        return str(self.value)


class VariableExpression(Expression):
    """
    Variable reference expression.
    """

    def __init__(self, name: str):
        self.name = name

    def interpret(self, context: Context) -> Any:
        return context.get_variable(self.name)

    def __str__(self) -> str:
        return f"${self.name}"


class BinaryExpression(Expression):
    """
    Base class for binary expressions.
    """

    def __init__(self, left: Expression, right: Expression):
        self.left = left
        self.right = right

    @abstractmethod
    def get_operator(self) -> str:
        """Return the operator symbol."""
        pass

    def __str__(self) -> str:
        return f"({self.left} {self.get_operator()} {self.right})"


class AddExpression(BinaryExpression):
    """Addition expression."""

    def interpret(self, context: Context) -> Any:
        left_val = self.left.interpret(context)
        right_val = self.right.interpret(context)
        if isinstance(left_val, str) or isinstance(right_val, str):
            return str(left_val) + str(right_val)
        return left_val + right_val

    def get_operator(self) -> str:
        return "+"


class SubtractExpression(BinaryExpression):
    """Subtraction expression."""

    def interpret(self, context: Context) -> Any:
        return self.left.interpret(context) - self.right.interpret(context)

    def get_operator(self) -> str:
        return "-"


class MultiplyExpression(BinaryExpression):
    """Multiplication expression."""

    def interpret(self, context: Context) -> Any:
        return self.left.interpret(context) * self.right.interpret(context)

    def get_operator(self) -> str:
        return "*"


class DivideExpression(BinaryExpression):
    """Division expression."""

    def interpret(self, context: Context) -> Any:
        return self.left.interpret(context) / self.right.interpret(context)

    def get_operator(self) -> str:
        return "/"


class UnaryExpression(Expression):
    """Base class for unary expressions."""

    def __init__(self, operand: Expression):
        self.operand = operand


class NegateExpression(UnaryExpression):
    """Negate expression."""

    def interpret(self, context: Context) -> Any:
        return -self.operand.interpret(context)

    def __str__(self) -> str:
        return f"-{self.operand}"


class FunctionExpression(Expression):
    """
    Function call expression.
    """

    def __init__(
        self,
        name: str,
        args: list[Expression] | None = None,
        func: Callable[..., Any] | None = None,
    ):
        self.name = name
        self.args = args or []
        self._func = func

    def interpret(self, context: Context) -> Any:
        arg_values = [arg.interpret(context) for arg in self.args]

        if self._func:
            return self._func(*arg_values)

        # Built-in functions
        if self.name == "len":
            return len(arg_values[0]) if arg_values else 0
        elif self.name == "str":
            return str(arg_values[0]) if arg_values else ""
        elif self.name == "int":
            return int(arg_values[0]) if arg_values else 0
        elif self.name == "float":
            return float(arg_values[0]) if arg_values else 0.0

        return None

    def __str__(self) -> str:
        args_str = ", ".join(str(a) for a in self.args)
        return f"{self.name}({args_str})"


class InterpreterBuilder:
    """
    Builder for constructing expressions programmatically.
    """

    def __init__(self):
        self._expressions: dict[str, Expression] = {}

    def literal(self, value: Any) -> LiteralExpression:
        """Create a literal expression."""
        return LiteralExpression(value)

    def variable(self, name: str) -> VariableExpression:
        """Create a variable expression."""
        return VariableExpression(name)

    def add(self, left: Expression, right: Expression) -> AddExpression:
        """Create an addition expression."""
        return AddExpression(left, right)

    def subtract(self, left: Expression, right: Expression) -> SubtractExpression:
        """Create a subtraction expression."""
        return SubtractExpression(left, right)

    def multiply(self, left: Expression, right: Expression) -> MultiplyExpression:
        """Create a multiplication expression."""
        return MultiplyExpression(left, right)

    def divide(self, left: Expression, right: Expression) -> DivideExpression:
        """Create a division expression."""
        return DivideExpression(left, right)

    def negate(self, expr: Expression) -> NegateExpression:
        """Create a negate expression."""
        return NegateExpression(expr)

    def func(self, name: str, args: list[Expression] | None = None) -> FunctionExpression:
        """Create a function expression."""
        return FunctionExpression(name, args)


class SimpleRegexInterpreter:
    """
    Simple regex-based expression interpreter.
    """

    def __init__(self):
        self._pattern_cache: dict[str, re.Pattern] = {}

    def compile(self, pattern: str) -> re.Pattern:
        """Compile a regex pattern with caching."""
        if pattern not in self._pattern_cache:
            self._pattern_cache[pattern] = re.compile(pattern)
        return self._pattern_cache[pattern]

    def match(self, pattern: str, text: str) -> bool:
        """Check if pattern matches text."""
        return self.compile(pattern).match(text) is not None

    def find_all(self, pattern: str, text: str) -> list[str]:
        """Find all matches in text."""
        return self.compile(pattern).findall(text)

    def replace(self, pattern: str, text: str, replacement: str) -> str:
        """Replace matches in text."""
        return self.compile(pattern).sub(replacement, text)

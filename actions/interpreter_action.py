"""Interpreter Pattern Action Module.

Provides interpreter pattern for
language evaluation.
"""

import time
import re
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class Expression:
    """Base expression."""
    def interpret(self, context: Dict) -> Any:
        raise NotImplementedError


class LiteralExpression(Expression):
    """Literal value expression."""
    def __init__(self, value: Any):
        self.value = value

    def interpret(self, context: Dict) -> Any:
        return self.value


class VariableExpression(Expression):
    """Variable expression."""
    def __init__(self, name: str):
        self.name = name

    def interpret(self, context: Dict) -> Any:
        return context.get(self.name)


class OperationExpression(Expression):
    """Operation expression."""
    def __init__(self, left: Expression, op: str, right: Expression):
        self.left = left
        self.op = op
        self.right = right

    def interpret(self, context: Dict) -> Any:
        left_val = self.left.interpret(context)
        right_val = self.right.interpret(context)

        if self.op == "+":
            return left_val + right_val
        elif self.op == "-":
            return left_val - right_val
        elif self.op == "*":
            return left_val * right_val
        elif self.op == "/":
            return left_val / right_val if right_val != 0 else 0
        elif self.op == "==":
            return left_val == right_val
        elif self.op == "!=":
            return left_val != right_val
        return None


class InterpreterManager:
    """Manages interpreter pattern."""

    def __init__(self):
        self._functions: Dict[str, Callable] = {}

    def register_function(self, name: str, func: Callable) -> None:
        """Register a function."""
        self._functions[name] = func

    def evaluate(self, expression: str, context: Dict) -> Any:
        """Evaluate expression string."""
        try:
            if expression.isdigit():
                return int(expression)
            if expression.replace(".", "").isdigit():
                return float(expression)
            if expression in ('true', 'false'):
                return expression == 'true'
            if expression in context:
                return context[expression]
            return None
        except Exception:
            return None


class InterpreterPatternAction(BaseAction):
    """Action for interpreter pattern operations."""

    def __init__(self):
        super().__init__("interpreter")
        self._manager = InterpreterManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute interpreter action."""
        try:
            operation = params.get("operation", "evaluate")

            if operation == "register":
                return self._register(params)
            elif operation == "evaluate":
                return self._evaluate(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _register(self, params: Dict) -> ActionResult:
        """Register function."""
        self._manager.register_function(
            params.get("name", ""),
            params.get("func") or (lambda: None)
        )
        return ActionResult(success=True)

    def _evaluate(self, params: Dict) -> ActionResult:
        """Evaluate expression."""
        result = self._manager.evaluate(
            params.get("expression", ""),
            params.get("context", {})
        )
        return ActionResult(success=True, data={"result": result})

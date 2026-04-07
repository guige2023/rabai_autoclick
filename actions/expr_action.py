"""expr_action module for rabai_autoclick.

Provides expression evaluation: safe expression parser,
mathematical expression evaluator, and custom function support.
"""

from __future__ import annotations

import ast
import math
import operator
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Union

__all__ = [
    "Expression",
    "ExpressionEvaluator",
    "SafeEvaluator",
    "MathEvaluator",
    "evaluate",
    "eval_expression",
    "compile_expression",
    "ExpressionError",
]


class ExpressionError(Exception):
    """Raised when expression evaluation fails."""
    pass


class Expression:
    """Compiled expression that can be evaluated repeatedly."""

    def __init__(self, expression: str, func: Callable) -> None:
        self.expression = expression
        self._func = func

    def evaluate(self, context: Optional[Dict[str, Any]] = None) -> Any:
        """Evaluate expression with context."""
        return self._func(context or {})

    def __call__(self, context: Optional[Dict[str, Any]] = None) -> Any:
        return self.evaluate(context)


class SafeEvaluator:
    """Safe expression evaluator with limited operations."""

    def __init__(
        self,
        allowed_functions: Optional[Dict[str, Callable]] = None,
        allowed_modules: Optional[List[str]] = None,
    ) -> None:
        self._allowed_functions = allowed_functions or self._default_functions()
        self._allowed_modules = allowed_modules or []

    def _default_functions(self) -> Dict[str, Callable]:
        """Default allowed functions."""
        return {
            "abs": abs,
            "min": min,
            "max": max,
            "round": round,
            "len": len,
            "sum": sum,
            "pow": pow,
            "sqrt": math.sqrt,
            "sin": math.sin,
            "cos": math.cos,
            "tan": math.tan,
            "log": math.log,
            "log10": math.log10,
            "exp": math.exp,
            "floor": math.floor,
            "ceil": math.ceil,
        }

    def evaluate(self, expr: str, context: Optional[Dict[str, Any]] = None) -> Any:
        """Evaluate expression safely.

        Args:
            expr: Expression string.
            context: Variables for evaluation.

        Returns:
            Result of expression.

        Raises:
            ExpressionError: If evaluation fails.
        """
        context = context or {}
        ops = {
            ast.Add: operator.add,
            ast.Sub: operator.sub,
            ast.Mult: operator.mul,
            ast.Div: operator.truediv,
            ast.FloorDiv: operator.floordiv,
            ast.Mod: operator.mod,
            ast.Pow: operator.pow,
            ast.USub: operator.neg,
            ast.UAdd: operator.pos,
            ast.Eq: operator.eq,
            ast.NotEq: operator.ne,
            ast.Lt: operator.lt,
            ast.LtE: operator.le,
            ast.Gt: operator.gt,
            ast.GtE: operator.ge,
            ast.Not: operator.not_,
            ast.And: operator.and_,
            ast.Or: operator.or_,
        }

        def eval_node(node: ast.AST) -> Any:
            if isinstance(node, ast.Constant):
                return node.value
            elif isinstance(node, ast.Num):
                return node.n
            elif isinstance(node, ast.Name):
                if node.id in context:
                    return context[node.id]
                if node.id in self._allowed_functions:
                    return self._allowed_functions[node.id]
                raise ExpressionError(f"Unknown variable: {node.id}")
            elif isinstance(node, ast.BinOp):
                left = eval_node(node.left)
                right = eval_node(node.right)
                op_func = ops.get(type(node.op))
                if op_func is None:
                    raise ExpressionError(f"Unsupported binary operator: {type(node.op)}")
                return op_func(left, right)
            elif isinstance(node, ast.UnaryOp):
                operand = eval_node(node.operand)
                op_func = ops.get(type(node.op))
                if op_func is None:
                    raise ExpressionError(f"Unsupported unary operator: {type(node.op)}")
                return op_func(operand)
            elif isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    func_name = node.func.id
                    if func_name in self._allowed_functions:
                        args = [eval_node(arg) for arg in node.args]
                        kwargs = {kw.arg: eval_node(kw.value) for kw in node.keywords}
                        return self._allowed_functions[func_name](*args, **kwargs)
                    raise ExpressionError(f"Function not allowed: {func_name}")
                raise ExpressionError("Only simple function calls allowed")
            elif isinstance(node, ast.Compare):
                left = eval_node(node.left)
                for op, comparator in zip(node.ops, node.comparators):
                    right = eval_node(comparator)
                    op_func = ops.get(type(op))
                    if op_func is None:
                        raise ExpressionError(f"Unsupported comparison: {type(op)}")
                    if not op_func(left, right):
                        return False
                    left = right
                return True
            elif isinstance(node, ast.IfExp):
                test = eval_node(node.body)
                if test:
                    return eval_node(node.body)
                return eval_node(node.orelse)
            elif isinstance(node, ast.Attribute):
                raise ExpressionError("Attribute access not allowed")
            else:
                raise ExpressionError(f"Unsupported AST node: {type(node).__name__}")

        try:
            tree = ast.parse(expr, mode="eval")
            return eval_node(tree.body)
        except Exception as e:
            raise ExpressionError(f"Evaluation error: {e}")


class MathEvaluator(SafeEvaluator):
    """Math-focused expression evaluator."""

    def __init__(self) -> None:
        functions = {
            "abs": abs,
            "min": min,
            "max": max,
            "round": round,
            "pow": pow,
            "sqrt": math.sqrt,
            "sin": math.sin,
            "cos": math.cos,
            "tan": math.tan,
            "asin": math.asin,
            "acos": math.acos,
            "atan": math.atan,
            "atan2": math.atan2,
            "sinh": math.sinh,
            "cosh": math.cosh,
            "tanh": math.tanh,
            "log": math.log,
            "log10": math.log10,
            "log2": math.log2,
            "exp": math.exp,
            "floor": math.floor,
            "ceil": math.ceil,
            "trunc": math.trunc,
            "degrees": math.degrees,
            "radians": math.radians,
            "pi": math.pi,
            "e": math.e,
            "tau": math.tau,
            "inf": float("inf"),
            "nan": float("nan"),
        }
        super().__init__(allowed_functions=functions)


class ExpressionEvaluator(SafeEvaluator):
    """Extended expression evaluator with custom operators."""

    def __init__(
        self,
        custom_ops: Optional[Dict[str, Callable]] = None,
        custom_functions: Optional[Dict[str, Callable]] = None,
    ) -> None:
        functions = dict(self._default_functions())
        if custom_functions:
            functions.update(custom_functions)
        super().__init__(allowed_functions=functions)
        self._custom_ops = custom_ops or {}

    def compile(self, expr: str) -> Expression:
        """Compile expression into reusable Expression object."""
        def func(context: Dict[str, Any]) -> Any:
            return self.evaluate(expr, context)
        return Expression(expr, func)


def evaluate(expr: str, context: Optional[Dict[str, Any]] = None) -> Any:
    """Quick expression evaluation.

    Args:
        expr: Expression string.
        context: Variables.

    Returns:
        Result.
    """
    evaluator = SafeEvaluator()
    return evaluator.evaluate(expr, context)


def eval_expression(expr: str, context: Optional[Dict[str, Any]] = None) -> Any:
    """Alias for evaluate."""
    return evaluate(expr, context)


def compile_expression(expr: str) -> Expression:
    """Compile expression for reuse.

    Args:
        expr: Expression string.

    Returns:
        Compiled Expression object.
    """
    evaluator = ExpressionEvaluator()
    return evaluator.compile(expr)

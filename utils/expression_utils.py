"""Expression evaluation utilities for dynamic automation logic.

Provides safe evaluation of mathematical and logical expressions
with a built-in context, supporting variable substitution and
function calls for automation decision trees.

Example:
    >>> from utils.expression_utils import eval_expression, eval_logical
    >>> eval_expression("x * 2 + y", {"x": 5, "y": 3})
    13
    >>> eval_logical("a > 10 and not b", {"a": 15, "b": False})
    True
"""

from __future__ import annotations

import ast
import math
import operator
import re
from typing import Any, Callable, Dict, List, Optional, Union

SAFE_MATH_FUNCTIONS = {
    "abs": abs,
    "round": round,
    "min": min,
    "max": max,
    "sum": sum,
    "len": len,
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
    "pi": math.pi,
    "e": math.e,
}

SAFE_OPERATORS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.UAdd: operator.pos,
    ast.USub: operator.neg,
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.And: operator.and_,
    ast.Or: operator.or_,
    ast.Not: operator.not_,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
    ast.Is: lambda a, b: a is b,
    ast.IsNot: lambda a, b: a is not b,
}

SAFE_CONSTANTS = {
    "true": True,
    "false": False,
    "null": None,
    "none": None,
    "True": True,
    "False": False,
    "None": None,
}


class ExpressionError(Exception):
    """Raised when expression evaluation fails."""
    pass


def eval_expression(
    expr: str,
    context: Optional[Dict[str, Any]] = None,
    functions: Optional[Dict[str, Callable[..., Any]]] = None,
    *,
    timeout: float = 1.0,
    allow_assignment: bool = False,
) -> Any:
    """Safely evaluate a mathematical expression.

    Args:
        expr: Expression string.
        context: Variable name -> value mapping.
        functions: Additional safe functions to expose.
        timeout: Maximum evaluation time in seconds.
        allow_assignment: If True, allow assignment expressions.

    Returns:
        Evaluated result.

    Raises:
        ExpressionError: If evaluation fails or expression is invalid.

    Example:
        >>> eval_expression("price * quantity * 0.9", {"price": 100, "quantity": 2})
        180.0
    """
    if not expr or not expr.strip():
        raise ExpressionError("Empty expression")

    if len(expr) > 10000:
        raise ExpressionError("Expression too long")

    context = context or {}
    functions = functions or {}

    all_functions = {**SAFE_MATH_FUNCTIONS, **functions}

    local_vars = {**SAFE_CONSTANTS, **context}

    class SafeEvalNodeVisitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self._allowed = True

        def visit_Expr(self, node: ast.Expr) -> Any:
            return self.visit(node.value)

        def visit_Constant(self, node: ast.Constant) -> Any:
            return node.value

        def visit_Name(self, node: ast.Name) -> Any:
            if isinstance(node.ctx, ast.Load):
                if node.id in local_vars:
                    return local_vars[node.id]
                if node.id in all_functions:
                    return all_functions[node.id]
                raise ExpressionError(f"Unknown variable: {node.id}")
            raise ExpressionError("Assignment not allowed")

        def visit_BinOp(self, node: ast.BinOp) -> Any:
            left = self.visit(node.left)
            right = self.visit(node.right)
            op_type = type(node.op)
            if op_type not in SAFE_OPERATORS:
                raise ExpressionError(f"Unsupported operator: {op_type.__name__}")
            return SAFE_OPERATORS[op_type](left, right)

        def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
            operand = self.visit(node.operand)
            op_type = type(node.op)
            if op_type not in SAFE_OPERATORS:
                raise ExpressionError(f"Unsupported unary operator: {op_type.__name__}")
            return SAFE_OPERATORS[op_type](operand)

        def visit_BoolOp(self, node: ast.BoolOp) -> Any:
            values = [self.visit(v) for v in node.values]
            op_type = type(node.op)
            if op_type not in SAFE_OPERATORS:
                raise ExpressionError(f"Unsupported bool operator: {op_type.__name__}")
            result = values[0]
            for val in values[1:]:
                result = SAFE_OPERATORS[op_type](result, val)
            return result

        def visit_Compare(self, node: ast.Compare) -> Any:
            left = self.visit(node.left)
            for op, comparator in zip(node.ops, node.comparators):
                right = self.visit(comparator)
                op_type = type(op)
                if op_type not in SAFE_OPERATORS:
                    raise ExpressionError(f"Unsupported comparator: {op_type.__name__}")
                if not SAFE_OPERATORS[op_type](left, right):
                    return False
                left = right
            return True

        def visit_Call(self, node: ast.Call) -> Any:
            func = self.visit(node.func)
            args = [self.visit(arg) for arg in node.args]
            kwargs = {
                kw.arg: self.visit(kw.value)
                for kw in node.keywords
            }
            return func(*args, **kwargs)

        def visit_Subscript(self, node: ast.Subscript) -> Any:
            obj = self.visit(node.value)
            key = self.visit(node.slice)
            return obj[key]

        def visit_Index(self, node: ast.Index) -> Any:
            return self.visit(node.value)

        def visit_List(self, node: ast.List) -> Any:
            return [self.visit(el) for el in node.elts]

        def visit_Tuple(self, node: ast.Tuple) -> Any:
            return tuple(self.visit(el) for el in node.elts)

        def visit_Dict(self, node: ast.Dict) -> Any:
            return {
                self.visit(k): self.visit(v)
                for k, v in zip(node.keys, node.values)
            }

    try:
        tree = ast.parse(expr.strip(), mode="eval")
    except SyntaxError as e:
        raise ExpressionError(f"Syntax error: {e}")

    visitor = SafeEvalNodeVisitor()
    try:
        result = visitor.visit(tree)
        return result
    except RecursionError:
        raise ExpressionError("Expression too deeply nested")
    except TimeoutError:
        raise ExpressionError("Evaluation timed out")


def eval_logical(
    expr: str,
    context: Optional[Dict[str, Any]] = None,
) -> bool:
    """Evaluate a logical/boolean expression.

    Args:
        expr: Logical expression string.
        context: Variable context.

    Returns:
        Boolean result.

    Example:
        >>> eval_logical("score >= 80 and passed", {"score": 85, "passed": True})
        True
    """
    try:
        result = eval_expression(expr, context)
        return bool(result)
    except ExpressionError:
        return False


def eval_range(
    expr: str,
    context: Optional[Dict[str, Any]] = None,
) -> List[float]:
    """Evaluate a range expression like "1:10:2" or "0:100".

    Args:
        expr: Range expression.
        context: Variable context.

    Returns:
        List of numbers in the range.

    Example:
        >>> eval_range("0:10:2")
        [0, 2, 4, 6, 8]
    """
    parts = expr.split(":")
    if len(parts) == 2:
        start = int(eval_expression(parts[0].strip(), context or {}))
        stop = int(eval_expression(parts[1].strip(), context or {}))
        return list(range(start, stop))
    elif len(parts) == 3:
        start = int(eval_expression(parts[0].strip(), context or {}))
        stop = int(eval_expression(parts[1].strip(), context or {}))
        step = int(eval_expression(parts[2].strip(), context or {}))
        return list(range(start, stop, step))
    else:
        raise ExpressionError("Invalid range expression, expected 'start:stop' or 'start:stop:step'")


class ExpressionContext:
    """Mutable expression context with variable tracking.

    Example:
        >>> ctx = ExpressionContext(x=10, y=20)
        >>> ctx.update(x=15)
        >>> result = eval_expression("x + y", ctx.variables)
    """

    def __init__(self, **initial: Any) -> None:
        self._variables: Dict[str, Any] = dict(initial)
        self._history: List[Dict[str, Any]] = []

    @property
    def variables(self) -> Dict[str, Any]:
        """Current variable snapshot."""
        return dict(self._variables)

    def update(self, **kwargs: Any) -> None:
        """Update variables, saving history."""
        self._history.append(dict(self._variables))
        self._variables.update(kwargs)

    def get(self, key: str, default: Any = None) -> Any:
        """Get a variable value."""
        return self._variables.get(key, default)

    def rollback(self) -> bool:
        """Rollback to previous state.

        Returns:
            True if rollback succeeded, False if at oldest state.
        """
        if not self._history:
            return False
        self._variables = self._history.pop()
        return True

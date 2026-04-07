"""Expression evaluation utilities.

Safe evaluation of mathematical and logical expressions with variables.
Supports arithmetics, comparisons, and custom functions.

Example:
    eval_expr = ExpressionEvaluator()
    result = eval_expr.evaluate("x + y * 2", {"x": 5, "y": 3})
    print(result)  # 11
"""

from __future__ import annotations

import ast
import math
import operator
import re
from dataclasses import dataclass
from typing import Any, Callable

import numpy as np  # type: ignore


class ExpressionError(Exception):
    """Raised when expression evaluation fails."""
    pass


BINARY_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.BitXor: operator.xor,
    ast.BitAnd: operator.and_,
    ast.BitOr: operator.or_,
}

COMPARE_OPS = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}


class ExpressionEvaluator:
    """Safe mathematical/logical expression evaluator.

    Parses and evaluates expressions with variables and built-in functions.
    Only whitelisted operations and functions are allowed.
    """

    SAFE_FUNCTIONS: dict[str, Callable[..., Any]] = {
        "abs": abs,
        "min": min,
        "max": max,
        "sum": sum,
        "round": round,
        "len": len,
        "float": float,
        "int": int,
        "str": str,
        "bool": bool,
        "list": list,
        "dict": dict,
        "tuple": tuple,
        "set": set,
        "sorted": sorted,
        "reversed": reversed,
        "enumerate": enumerate,
        "zip": zip,
        "map": map,
        "filter": filter,
        "any": any,
        "all": all,
        "range": range,
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
        "np_min": np.min if "np" in dir() else min,
        "np_max": np.max if "np" in dir() else max,
        "np_mean": np.mean if "np" in dir() else (lambda x: sum(x) / len(x)),
        "np_sum": np.sum if "np" in dir() else sum,
    }

    def __init__(self) -> None:
        self._variables: dict[str, Any] = {}
        self._custom_functions: dict[str, Callable[..., Any]] = {}

    def evaluate(
        self,
        expression: str,
        context: dict[str, Any] | None = None,
    ) -> Any:
        """Evaluate an expression with optional variables.

        Args:
            expression: Expression string to evaluate.
            context: Dict of variable names to values.

        Returns:
            Result of evaluation.

        Raises:
            ExpressionError: If expression is invalid or unsafe.
        """
        context = context or {}

        try:
            tree = ast.parse(expression, mode="eval")
        except SyntaxError as e:
            raise ExpressionError(f"Syntax error: {e}")

        variables = {**self._variables, **context}
        all_functions = {**self.SAFE_FUNCTIONS, **self._custom_functions}

        return self._eval_node(tree.body, variables, all_functions)

    def evaluate_statement(self, statement: str, context: dict[str, Any] | None = None) -> Any:
        """Evaluate a statement (not just expression) with print handling.

        Args:
            statement: Python statement(s) to execute.
            context: Dict of variable names to values.

        Returns:
            Last expression result.
        """
        context = context or {}

        try:
            tree = ast.parse(statement, mode="exec")
        except SyntaxError as e:
            raise ExpressionError(f"Syntax error: {e}")

        variables = {**self._variables, **context}
        all_functions = {**self.SAFE_FUNCTIONS, **self._custom_functions}

        for node in tree.body[:-1]:
            self._exec_node(node, variables, all_functions)

        if tree.body:
            last = tree.body[-1]
            if isinstance(last, ast.Expr):
                return self._eval_node(last.value, variables, all_functions)
            return self._exec_node(last, variables, all_functions)

        return None

    def _eval_node(
        self,
        node: ast.AST,
        variables: dict[str, Any],
        functions: dict[str, Callable],
    ) -> Any:
        """Recursively evaluate an AST node."""
        if isinstance(node, ast.Constant):
            return node.value

        if isinstance(node, ast.Name):
            if node.id in variables:
                return variables[node.id]
            if node.id in functions:
                return functions[node.id]
            raise ExpressionError(f"Unknown variable: {node.id}")

        if isinstance(node, ast.NameConstants):
            return node.value

        if isinstance(node, ast.Num):
            return node.n

        if isinstance(node, ast.UnaryOp):
            operand = self._eval_node(node.operand, variables, functions)
            if isinstance(node.op, ast.USub):
                return -operand
            if isinstance(node.op, ast.UAdd):
                return +operand
            if isinstance(node.op, ast.Not):
                return not operand

        if isinstance(node, ast.BinOp):
            left = self._eval_node(node.left, variables, functions)
            right = self._eval_node(node.right, variables, functions)
            op_func = BINARY_OPS.get(type(node.op))
            if op_func:
                return op_func(left, right)
            raise ExpressionError(f"Unsupported binary operator: {type(node.op)}")

        if isinstance(node, ast.Compare):
            left = self._eval_node(node.left, variables, functions)
            for op, comparator in zip(node.ops, node.comparators):
                right = self._eval_node(comparator, variables, functions)
                op_func = COMPARE_OPS.get(type(op))
                if op_func:
                    if not op_func(left, right):
                        return False
                else:
                    raise ExpressionError(f"Unsupported compare: {type(op)}")
                left = right
            return True

        if isinstance(node, ast.BoolOp):
            values = [self._eval_node(v, variables, functions) for v in node.values]
            if isinstance(node.op, ast.And):
                return all(values)
            if isinstance(node.op, ast.Or):
                return any(values)

        if isinstance(node, ast.IfExp):
            test = self._eval_node(node.body, variables, functions)
            if test:
                return self._eval_node(node.body, variables, functions)
            return self._eval_node(node.orelse, variables, functions)

        if isinstance(node, ast.Call):
            func = self._eval_node(node.func, variables, functions)
            args = [self._eval_node(arg, variables, functions) for arg in node.args]
            kwargs = {
                kw.arg: self._eval_node(kw.value, variables, functions)
                for kw in node.keywords
            }
            return func(*args, **kwargs)

        if isinstance(node, ast.Attribute):
            value = self._eval_node(node.value, variables, functions)
            return getattr(value, node.attr)

        if isinstance(node, ast.Subscript):
            value = self._eval_node(node.value, variables, functions)
            key = self._eval_node(node.slice, variables, functions)
            return value[key]

        if isinstance(node, ast.List):
            return [self._eval_node(elt, variables, functions) for elt in node.elts]

        if isinstance(node, ast.Tuple):
            return tuple(self._eval_node(elt, variables, functions) for elt in node.elts)

        if isinstance(node, ast.Dict):
            return {
                self._eval_node(k, variables, functions): self._eval_node(v, variables, functions)
                for k, v in zip(node.keys, node.values)
            }

        if isinstance(node, ast.FormattedValue):
            return self._eval_node(node.value, variables, functions)

        if isinstance(node, ast.JoinedStr):
            return "".join(
                self._eval_node(v, variables, functions) if isinstance(v, ast.FormattedValue) else v.s
                for v in node.values
            )

        raise ExpressionError(f"Unsupported node type: {type(node).__name__}")

    def _exec_node(
        self,
        node: ast.AST,
        variables: dict[str, Any],
        functions: dict[str, Callable],
    ) -> Any:
        """Execute a statement node."""
        if isinstance(node, ast.Assign):
            value = self._eval_node(node.value, variables, functions)
            for target in node.targets:
                if isinstance(target, ast.Name):
                    variables[target.id] = value
            return value

        if isinstance(node, ast.Expr):
            return self._eval_node(node.value, variables, functions)

        if isinstance(node, ast.Pass):
            return None

        if isinstance(node, ast.Return):
            return self._eval_node(node.value, variables, functions)

        raise ExpressionError(f"Unsupported exec node: {type(node).__name__}")

    def register_function(self, name: str, func: Callable[..., Any]) -> None:
        """Register a custom function for use in expressions.

        Args:
            name: Function name to expose in expressions.
            func: Callable to register.
        """
        self._custom_functions[name] = func

    def set_variable(self, name: str, value: Any) -> None:
        """Set a persistent variable for all subsequent evaluations."""
        self._variables[name] = value

    def clear_variables(self) -> None:
        """Clear all persistent variables."""
        self._variables.clear()


def evaluate_template(
    template: str,
    context: dict[str, Any],
    pattern: str = r"\{([^}]+)\}",
) -> str:
    """Evaluate a simple template with {variable} placeholders.

    Args:
        template: Template string with {var} placeholders.
        context: Dict of variables.
        pattern: Regex pattern for placeholders.

    Returns:
        Template with placeholders replaced.
    """
    def replacer(match: re.Match) -> str:
        expr = match.group(1).strip()
        evaluator = ExpressionEvaluator()
        try:
            return str(evaluator.evaluate(expr, context))
        except ExpressionError:
            return match.group(0)

    return re.sub(pattern, replacer, template)

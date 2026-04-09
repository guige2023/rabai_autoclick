"""Automation Condition Evaluator.

This module provides conditional execution logic:
- Multiple condition types
- Logical operators (AND, OR, NOT)
- Condition chaining
- Dynamic condition evaluation

Example:
    >>> from actions.automation_condition_action import ConditionEvaluator
    >>> evaluator = ConditionEvaluator()
    >>> evaluator.add_condition("is_peak_hours", lambda ctx: ctx["hour"] > 9)
    >>> result = evaluator.evaluate("is_peak_hours", {"hour": 15})
"""

from __future__ import annotations

import logging
import threading
import operator
from typing import Any, Callable, Optional
from enum import Enum
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class ConditionType(Enum):
    """Types of conditions."""
    BOOLEAN = "boolean"
    COMPARISON = "comparison"
    RANGE = "range"
    PATTERN = "pattern"
    CUSTOM = "custom"


@dataclass
class Condition:
    """A named condition with evaluation logic."""
    name: str
    condition_type: ConditionType
    evaluate: Callable[[Any], bool]
    description: str = ""


@dataclass
class ConditionResult:
    """Result of condition evaluation."""
    condition_name: str
    passed: bool
    context: dict[str, Any]
    message: str = ""


class ConditionEvaluator:
    """Evaluates conditions for automation workflows."""

    COMPARISON_OPS = {
        "==": operator.eq,
        "!=": operator.ne,
        ">": operator.gt,
        ">=": operator.ge,
        "<": operator.lt,
        "<=": operator.le,
        "in": lambda a, b: a in b,
        "not in": lambda a, b: a not in b,
        "contains": lambda a, b: b in a,
    }

    def __init__(self) -> None:
        """Initialize the condition evaluator."""
        self._conditions: dict[str, Condition] = {}
        self._lock = threading.RLock()
        self._stats = {"evaluations": 0, "passed": 0, "failed": 0}

    def add_condition(
        self,
        name: str,
        condition_type: ConditionType,
        evaluate: Callable[[Any], bool],
        description: str = "",
    ) -> None:
        """Add a named condition.

        Args:
            name: Condition name.
            condition_type: Type of condition.
            evaluate: Evaluation function.
            description: Human-readable description.
        """
        with self._lock:
            self._conditions[name] = Condition(
                name=name,
                condition_type=condition_type,
                evaluate=evaluate,
                description=description,
            )
            logger.info("Added condition: %s", name)

    def add_boolean_condition(
        self,
        name: str,
        expression: Callable[[dict[str, Any]], bool],
        description: str = "",
    ) -> None:
        """Add a boolean expression condition.

        Args:
            name: Condition name.
            expression: Lambda that takes context and returns bool.
            description: Human-readable description.
        """
        self.add_condition(
            name=name,
            condition_type=ConditionType.BOOLEAN,
            evaluate=expression,
            description=description,
        )

    def add_comparison_condition(
        self,
        name: str,
        field: str,
        op: str,
        value: Any,
        description: str = "",
    ) -> None:
        """Add a comparison condition.

        Args:
            name: Condition name.
            field: Field name in context.
            op: Comparison operator (==, !=, >, <, >=, <=, in, not in, contains).
            value: Value to compare against.
            description: Human-readable description.
        """
        op_func = self.COMPARISON_OPS.get(op)
        if op_func is None:
            raise ValueError(f"Unknown operator: {op}")

        def evaluate(ctx: dict[str, Any]) -> bool:
            field_value = ctx.get(field)
            try:
                return op_func(field_value, value)
            except (TypeError, ValueError):
                return False

        self.add_condition(
            name=name,
            condition_type=ConditionType.COMPARISON,
            evaluate=evaluate,
            description=description or f"{field} {op} {value}",
        )

    def add_range_condition(
        self,
        name: str,
        field: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        description: str = "",
    ) -> None:
        """Add a range condition.

        Args:
            name: Condition name.
            field: Field name in context.
            min_val: Minimum value (inclusive).
            max_val: Maximum value (inclusive).
            description: Human-readable description.
        """
        def evaluate(ctx: dict[str, Any]) -> bool:
            try:
                val = float(ctx.get(field, 0))
                if min_val is not None and val < min_val:
                    return False
                if max_val is not None and val > max_val:
                    return False
                return True
            except (ValueError, TypeError):
                return False

        self.add_condition(
            name=name,
            condition_type=ConditionType.RANGE,
            evaluate=evaluate,
            description=description or f"{field} in [{min_val}, {max_val}]",
        )

    def evaluate(
        self,
        name: str,
        context: Optional[dict[str, Any]] = None,
    ) -> ConditionResult:
        """Evaluate a condition.

        Args:
            name: Condition name.
            context: Evaluation context.

        Returns:
            ConditionResult with pass/fail status.
        """
        with self._lock:
            condition = self._conditions.get(name)
            ctx = context or {}

        if condition is None:
            return ConditionResult(
                condition_name=name,
                passed=False,
                context=ctx,
                message=f"Condition not found: {name}",
            )

        try:
            passed = condition.evaluate(ctx)
        except Exception as e:
            passed = False
            logger.error("Condition %s evaluation error: %s", name, e)

        with self._lock:
            self._stats["evaluations"] += 1
            if passed:
                self._stats["passed"] += 1
            else:
                self._stats["failed"] += 1

        return ConditionResult(
            condition_name=name,
            passed=passed,
            context=ctx,
            message="Passed" if passed else "Failed",
        )

    def evaluate_all(
        self,
        condition_names: list[str],
        context: Optional[dict[str, Any]] = None,
        mode: str = "AND",
    ) -> tuple[bool, list[ConditionResult]]:
        """Evaluate multiple conditions.

        Args:
            condition_names: List of condition names.
            context: Evaluation context.
            mode: "AND", "OR", or "ALL".

        Returns:
            Tuple of (overall_result, list of individual results).
        """
        results = [self.evaluate(name, context) for name in condition_names]

        if mode == "AND":
            overall = all(r.passed for r in results)
        elif mode == "OR":
            overall = any(r.passed for r in results)
        else:
            overall = all(r.passed for r in results)

        return overall, results

    def evaluate_and(
        self,
        condition_names: list[str],
        context: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Evaluate conditions with AND logic.

        Args:
            condition_names: List of condition names.
            context: Evaluation context.

        Returns:
            True if all conditions pass.
        """
        overall, _ = self.evaluate_all(condition_names, context, mode="AND")
        return overall

    def evaluate_or(
        self,
        condition_names: list[str],
        context: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Evaluate conditions with OR logic.

        Args:
            condition_names: List of condition names.
            context: Evaluation context.

        Returns:
            True if any condition passes.
        """
        overall, _ = self.evaluate_all(condition_names, context, mode="OR")
        return overall

    def remove_condition(self, name: str) -> bool:
        """Remove a condition.

        Args:
            name: Condition name.

        Returns:
            True if removed, False if not found.
        """
        with self._lock:
            if name in self._conditions:
                del self._conditions[name]
                return True
            return False

    def list_conditions(self) -> list[Condition]:
        """List all registered conditions."""
        with self._lock:
            return list(self._conditions.values())

    def get_stats(self) -> dict[str, int]:
        """Get evaluation statistics."""
        with self._lock:
            return dict(self._stats)

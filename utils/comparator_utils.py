"""
Comparator Utilities

Provides utilities for comparing UI elements,
values, and states in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable
from enum import Enum, auto


class ComparisonOperator(Enum):
    """Comparison operators."""
    EQUAL = auto()
    NOT_EQUAL = auto()
    GREATER = auto()
    LESS = auto()
    GREATER_EQUAL = auto()
    LESS_EQUAL = auto()
    CONTAINS = auto()
    STARTS_WITH = auto()
    ENDS_WITH = auto()
    MATCHES = auto()


@dataclass
class ComparisonResult:
    """Result of a comparison operation."""
    operator: ComparisonOperator
    left: Any
    right: Any
    passed: bool
    message: str = ""


class ElementComparator:
    """
    Compares UI elements and values.
    
    Supports various comparison operators and
    provides detailed comparison results.
    """

    def __init__(self) -> None:
        self._custom_comparators: dict[str, Callable[[Any, Any], bool]] = {}

    def register_comparator(
        self,
        name: str,
        func: Callable[[Any, Any], bool]
    ) -> None:
        """Register a custom comparator function."""
        self._custom_comparators[name] = func

    def compare(
        self,
        operator: ComparisonOperator,
        left: Any,
        right: Any,
    ) -> ComparisonResult:
        """
        Perform a comparison.
        
        Args:
            operator: Comparison operator.
            left: Left operand.
            right: Right operand.
            
        Returns:
            ComparisonResult with details.
        """
        passed = self._apply_operator(operator, left, right)
        return ComparisonResult(
            operator=operator,
            left=left,
            right=right,
            passed=passed,
            message=self._format_message(operator, left, right, passed),
        )

    def compare_elements(
        self,
        elem1: dict[str, Any],
        elem2: dict[str, Any],
        keys: list[str] | None = None,
    ) -> list[ComparisonResult]:
        """
        Compare two element dictionaries.
        
        Args:
            elem1: First element.
            elem2: Second element.
            keys: Specific keys to compare (None = all).
            
        Returns:
            List of comparison results.
        """
        results = []
        compare_keys = keys or set(elem1.keys()) | set(elem2.keys())
        for key in compare_keys:
            v1 = elem1.get(key)
            v2 = elem2.get(key)
            results.append(self.compare(
                ComparisonOperator.EQUAL,
                v1,
                v2,
            ))
        return results

    def _apply_operator(
        self,
        operator: ComparisonOperator,
        left: Any,
        right: Any,
    ) -> bool:
        """Apply comparison operator."""
        if operator == ComparisonOperator.EQUAL:
            return left == right
        elif operator == ComparisonOperator.NOT_EQUAL:
            return left != right
        elif operator == ComparisonOperator.GREATER:
            return left > right
        elif operator == ComparisonOperator.LESS:
            return left < right
        elif operator == ComparisonOperator.GREATER_EQUAL:
            return left >= right
        elif operator == ComparisonOperator.LESS_EQUAL:
            return left <= right
        elif operator == ComparisonOperator.CONTAINS:
            return right in left
        elif operator == ComparisonOperator.STARTS_WITH:
            return str(left).startswith(str(right))
        elif operator == ComparisonOperator.ENDS_WITH:
            return str(left).endswith(str(right))
        elif operator == ComparisonOperator.MATCHES:
            import re
            return bool(re.match(str(right), str(left)))
        return False

    def _format_message(
        self,
        operator: ComparisonOperator,
        left: Any,
        right: Any,
        passed: bool,
    ) -> str:
        """Format comparison message."""
        op_str = {
            ComparisonOperator.EQUAL: "==",
            ComparisonOperator.NOT_EQUAL: "!=",
            ComparisonOperator.GREATER: ">",
            ComparisonOperator.LESS: "<",
            ComparisonOperator.GREATER_EQUAL: ">=",
            ComparisonOperator.LESS_EQUAL: "<=",
            ComparisonOperator.CONTAINS: "contains",
            ComparisonOperator.STARTS_WITH: "starts with",
            ComparisonOperator.ENDS_WITH: "ends with",
            ComparisonOperator.MATCHES: "matches",
        }.get(operator, "?")
        status = "PASS" if passed else "FAIL"
        return f"{status}: {left!r} {op_str} {right!r}"

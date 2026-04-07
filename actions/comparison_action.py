"""Comparison action for comparing values and data structures.

This module provides comparison operations for
values, files, and data structures.

Example:
    >>> action = ComparisonAction()
    >>> result = action.execute(operation="equal", a=1, b=1)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class ComparisonResult:
    """Result from comparison."""
    equal: bool
    identical: bool
    difference: Optional[str] = None


class ComparisonAction:
    """Value and data comparison action.

    Provides comparison operations for values,
    files, and data structures.

    Example:
        >>> action = ComparisonAction()
        >>> result = action.execute(
        ...     operation="compare",
        ...     a={"x": 1},
        ...     b={"x": 2}
        ... )
    """

    def __init__(self) -> None:
        """Initialize comparison action."""
        pass

    def execute(
        self,
        operation: str,
        a: Any = None,
        b: Any = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute comparison operation.

        Args:
            operation: Operation (equal, compare, diff, etc.).
            a: First value.
            b: Second value.
            **kwargs: Additional parameters.

        Returns:
            Comparison result dictionary.

        Raises:
            ValueError: If operation is invalid.
        """
        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        if op == "equal":
            if a is None or b is None:
                raise ValueError("a and b required for 'equal'")
            result["equal"] = a == b

        elif op == "identical":
            if a is None or b is None:
                raise ValueError("a and b required for 'identical'")
            result["identical"] = a is b

        elif op == "compare":
            if a is None or b is None:
                raise ValueError("a and b required for 'compare'")
            result.update(self._compare_values(a, b))

        elif op == "diff":
            if a is None or b is None:
                raise ValueError("a and b required for 'diff'")
            result.update(self._diff_values(a, b))

        elif op == "greater":
            if a is None or b is None:
                raise ValueError("a and b required for 'greater'")
            result["greater"] = a > b

        elif op == "less":
            if a is None or b is None:
                raise ValueError("a and b required for 'less'")
            result["less"] = a < b

        elif op == "greater_equal":
            if a is None or b is None:
                raise ValueError("a and b required for 'greater_equal'")
            result["greater_equal"] = a >= b

        elif op == "less_equal":
            if a is None or b is None:
                raise ValueError("a and b required for 'less_equal'")
            result["less_equal"] = a <= b

        elif op == "is_null":
            result["is_null"] = a is None

        elif op == "is_type":
            expected_type = kwargs.get("type")
            if expected_type is None:
                raise ValueError("type required for 'is_type'")
            result["is_type"] = type(a).__name__ == expected_type

        elif op == "in":
            if a is None or b is None:
                raise ValueError("a and b required for 'in'")
            result["in"] = a in b

        elif op == "not_in":
            if a is None or b is None:
                raise ValueError("a and b required for 'not_in'")
            result["not_in"] = a not in b

        elif op == "contains":
            if a is None or b is None:
                raise ValueError("a and b required for 'contains'")
            result["contains"] = b in a

        else:
            raise ValueError(f"Unknown operation: {operation}")

        return result

    def _compare_values(self, a: Any, b: Any) -> dict[str, Any]:
        """Compare two values.

        Args:
            a: First value.
            b: Second value.

        Returns:
            Comparison result.
        """
        if a == b:
            return {
                "equal": True,
                "relation": "equal",
            }
        elif a > b:
            return {
                "equal": False,
                "relation": "greater",
                "difference": f"{a} > {b}",
            }
        elif a < b:
            return {
                "equal": False,
                "relation": "less",
                "difference": f"{a} < {b}",
            }
        else:
            return {
                "equal": False,
                "relation": "incomparable",
            }

    def _diff_values(self, a: Any, b: Any) -> dict[str, Any]:
        """Get differences between values.

        Args:
            a: First value.
            b: Second value.

        Returns:
            Difference result.
        """
        if isinstance(a, dict) and isinstance(b, dict):
            diff = {}
            all_keys = set(a.keys()) | set(b.keys())
            for key in all_keys:
                if key in a and key not in b:
                    diff[key] = {"status": "removed", "value": a[key]}
                elif key not in a and key in b:
                    diff[key] = {"status": "added", "value": b[key]}
                elif a[key] != b[key]:
                    diff[key] = {"status": "changed", "a": a[key], "b": b[key]}
            return {"diff": diff, "type": "dict"}

        elif isinstance(a, list) and isinstance(b, list):
            diff = []
            max_len = max(len(a), len(b))
            for i in range(max_len):
                if i >= len(a):
                    diff.append({"index": i, "status": "added", "value": b[i]})
                elif i >= len(b):
                    diff.append({"index": i, "status": "removed", "value": a[i]})
                elif a[i] != b[i]:
                    diff.append({"index": i, "status": "changed", "a": a[i], "b": b[i]})
            return {"diff": diff, "type": "list"}

        else:
            if a != b:
                return {
                    "diff": {"a": a, "b": b},
                    "type": "primitive",
                }
            return {"diff": None, "type": "primitive"}

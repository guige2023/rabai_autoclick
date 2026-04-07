"""Conditional action for logic flow control.

This module provides conditional operations including
if/else logic, switch-like behavior, and ternary operations.

Example:
    >>> action = ConditionalAction()
    >>> result = action.execute(operation="if", condition=True, then="yes", else_="no")
"""

from __future__ import annotations

from typing import Any, Callable, Optional


class ConditionalAction:
    """Conditional logic flow action.

    Provides conditional operations including
    if/else, switch, and ternary logic.

    Example:
        >>> action = ConditionalAction()
        >>> result = action.execute(
        ...     operation="ternary",
        ...     condition=5 > 3,
        ...     then_value="yes",
        ...     else_value="no"
        ... )
    """

    def __init__(self) -> None:
        """Initialize conditional action."""
        pass

    def execute(
        self,
        operation: str,
        condition: Any = None,
        then_value: Any = None,
        else_value: Any = None,
        value: Any = None,
        cases: Optional[dict] = None,
        default: Any = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute conditional operation.

        Args:
            operation: Operation (if, switch, ternary, etc.).
            condition: Condition to evaluate.
            then_value: Value if condition is true.
            else_value: Value if condition is false.
            value: Value to check in switch.
            cases: Cases for switch operation.
            default: Default value.
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If operation is invalid.
        """
        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        if op == "if":
            if condition:
                result["result"] = then_value
                result["branch"] = "then"
            else:
                result["result"] = else_value
                result["branch"] = "else"

        elif op == "ternary":
            result["result"] = then_value if condition else else_value

        elif op == "switch":
            if cases is None:
                raise ValueError("cases required for 'switch'")
            if value in cases:
                result["result"] = cases[value]
                result["matched"] = value
            else:
                result["result"] = default
                result["matched"] = None

        elif op == "coalesce":
            values = kwargs.get("values", [])
            for v in values:
                if v is not None and v != "":
                    result["result"] = v
                    result["matched"] = True
                    break
            else:
                result["result"] = default
                result["matched"] = False

        elif op == "and":
            a = kwargs.get("a")
            b = kwargs.get("b")
            result["result"] = bool(a and b)

        elif op == "or":
            a = kwargs.get("a")
            b = kwargs.get("b")
            result["result"] = bool(a or b)

        elif op == "not":
            result["result"] = not condition

        elif op == "is_none":
            result["result"] = value is None

        elif op == "is_truthy":
            result["result"] = bool(value) if value is not None else False

        elif op == "is_falsy":
            result["result"] = not value if value is not None else True

        elif op == "all":
            values = kwargs.get("values", [])
            result["result"] = all(values)

        elif op == "any":
            values = kwargs.get("values", [])
            result["result"] = any(values)

        elif op == "matches":
            pattern = kwargs.get("pattern")
            if pattern is None or value is None:
                raise ValueError("pattern and value required for 'matches'")
            import re
            result["result"] = bool(re.search(pattern, str(value)))

        elif op == "between":
            if value is None:
                raise ValueError("value required for 'between'")
            min_val = kwargs.get("min")
            max_val = kwargs.get("max")
            if min_val is None or max_val is None:
                raise ValueError("min and max required")
            result["result"] = min_val <= value <= max_val

        else:
            raise ValueError(f"Unknown operation: {operation}")

        return result

    def if_else(
        self,
        condition: bool,
        then_value: Any,
        else_value: Any,
    ) -> Any:
        """Simple if/else helper.

        Args:
            condition: Condition to check.
            then_value: Value if true.
            else_value: Value if false.

        Returns:
            Selected value.
        """
        return then_value if condition else else_value

    def switch_case(
        self,
        value: Any,
        cases: dict,
        default: Any = None,
    ) -> Any:
        """Simple switch/case helper.

        Args:
            value: Value to match.
            cases: Cases dictionary.
            default: Default value.

        Returns:
            Matched value or default.
        """
        return cases.get(value, default)

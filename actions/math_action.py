"""Math utilities action for mathematical operations.

This module provides mathematical operations including
arithmetic, trigonometry, statistics, and random number generation.

Example:
    >>> action = MathAction()
    >>> result = action.execute(operation="add", a=5, b=3)
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass
from typing import Any, Optional


class MathAction:
    """Mathematical operations action.

    Provides arithmetic, trigonometry, statistics,
    and random number generation.

    Example:
        >>> action = MathAction()
        >>> result = action.execute(
        ...     operation="sqrt",
        ...     value=16
        ... )
    """

    def __init__(self) -> None:
        """Initialize math action."""
        pass

    def execute(
        self,
        operation: str,
        a: Any = None,
        b: Any = None,
        value: Any = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute math operation.

        Args:
            operation: Operation (add, sqrt, sin, etc.).
            a: First operand.
            b: Second operand.
            value: Single value.
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If operation is invalid.
        """
        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        # Arithmetic
        if op == "add":
            if a is None or b is None:
                raise ValueError("a and b required")
            result["value"] = a + b

        elif op == "subtract":
            if a is None or b is None:
                raise ValueError("a and b required")
            result["value"] = a - b

        elif op == "multiply":
            if a is None or b is None:
                raise ValueError("a and b required")
            result["value"] = a * b

        elif op == "divide":
            if a is None or b is None:
                raise ValueError("a and b required")
            if b == 0:
                return {"success": False, "error": "Division by zero"}
            result["value"] = a / b

        elif op == "modulo":
            if a is None or b is None:
                raise ValueError("a and b required")
            result["value"] = a % b

        elif op == "power":
            if a is None or b is None:
                raise ValueError("a and b required")
            result["value"] = a ** b

        elif op == "floor":
            if value is None:
                raise ValueError("value required")
            result["value"] = math.floor(value)

        elif op == "ceil":
            if value is None:
                raise ValueError("value required")
            result["value"] = math.ceil(value)

        elif op == "round":
            if value is None:
                raise ValueError("value required")
            digits = kwargs.get("digits", 0)
            result["value"] = round(value, digits)

        # Trigonometry
        elif op == "sin":
            if value is None:
                raise ValueError("value required")
            result["value"] = math.sin(math.radians(value))

        elif op == "cos":
            if value is None:
                raise ValueError("value required")
            result["value"] = math.cos(math.radians(value))

        elif op == "tan":
            if value is None:
                raise ValueError("value required")
            result["value"] = math.tan(math.radians(value))

        elif op == "asin":
            if value is None:
                raise ValueError("value required")
            result["value"] = math.degrees(math.asin(value))

        elif op == "acos":
            if value is None:
                raise ValueError("value required")
            result["value"] = math.degrees(math.acos(value))

        elif op == "atan":
            if value is None:
                raise ValueError("value required")
            result["value"] = math.degrees(math.atan(value))

        # Powers and roots
        elif op == "sqrt":
            if value is None:
                raise ValueError("value required")
            result["value"] = math.sqrt(value)

        elif op == "log":
            if value is None:
                raise ValueError("value required")
            base = kwargs.get("base", math.e)
            result["value"] = math.log(value, base)

        elif op == "log10":
            if value is None:
                raise ValueError("value required")
            result["value"] = math.log10(value)

        elif op == "exp":
            if value is None:
                raise ValueError("value required")
            result["value"] = math.exp(value)

        # Constants
        elif op == "pi":
            result["value"] = math.pi

        elif op == "e":
            result["value"] = math.e

        # Statistics
        elif op == "sum":
            values = kwargs.get("values", [])
            if not values:
                raise ValueError("values required")
            result["value"] = sum(values)

        elif op == "avg" or op == "mean":
            values = kwargs.get("values", [])
            if not values:
                raise ValueError("values required")
            result["value"] = sum(values) / len(values)

        elif op == "median":
            values = kwargs.get("values", [])
            if not values:
                raise ValueError("values required")
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            if n % 2 == 0:
                result["value"] = (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
            else:
                result["value"] = sorted_vals[n // 2]

        elif op == "min":
            values = kwargs.get("values", [])
            if not values:
                raise ValueError("values required")
            result["value"] = min(values)

        elif op == "max":
            values = kwargs.get("values", [])
            if not values:
                raise ValueError("values required")
            result["value"] = max(values)

        elif op == "stddev":
            values = kwargs.get("values", [])
            if not values:
                raise ValueError("values required")
            mean = sum(values) / len(values)
            variance = sum((x - mean) ** 2 for x in values) / len(values)
            result["value"] = math.sqrt(variance)

        # Random
        elif op == "random":
            start = kwargs.get("start", 0)
            end = kwargs.get("end", 1)
            result["value"] = random.uniform(start, end)

        elif op == "randint":
            start = kwargs.get("start", 0)
            end = kwargs.get("end", 100)
            result["value"] = random.randint(start, end)

        elif op == "choice":
            choices = kwargs.get("choices", [])
            if not choices:
                raise ValueError("choices required")
            result["value"] = random.choice(choices)

        elif op == "shuffle":
            items = kwargs.get("items", [])
            shuffled = list(items)
            random.shuffle(shuffled)
            result["items"] = shuffled

        elif op == "sample":
            items = kwargs.get("items", [])
            count = kwargs.get("count", 1)
            if not items:
                raise ValueError("items required")
            result["items"] = random.sample(items, min(count, len(items)))

        # Utilities
        elif op == "abs":
            if value is None:
                raise ValueError("value required")
            result["value"] = abs(value)

        elif op == "factorial":
            if value is None:
                raise ValueError("value required")
            result["value"] = math.factorial(int(value))

        elif op == "gcd":
            if a is None or b is None:
                raise ValueError("a and b required")
            result["value"] = math.gcd(int(a), int(b))

        else:
            raise ValueError(f"Unknown operation: {operation}")

        return result

"""Equality and comparison utilities for RabAI AutoClick.

Provides:
- Deep equality comparison
- Type-aware comparison
- Approximate equality for floats
- Object comparison helpers
"""

from __future__ import annotations

from dataclasses import fields, is_dataclass
from typing import Any, Callable, Optional


def deep_equal(a: Any, b: Any) -> bool:
    """Check if two objects are deeply equal.

    Args:
        a: First object.
        b: Second object.

    Returns:
        True if objects are deeply equal.
    """
    if type(a) != type(b):
        return False

    if a is b:
        return True

    if isinstance(a, dict):
        if len(a) != len(b):
            return False
        for key in a:
            if key not in b:
                return False
            if not deep_equal(a[key], b[key]):
                return False
        return True

    if isinstance(a, (list, tuple)):
        if len(a) != len(b):
            return False
        return all(deep_equal(x, y) for x, y in zip(a, b))

    if isinstance(a, set):
        return a == b

    if is_dataclass(a):
        return dataclass_equal(a, b)

    return a == b


def dataclass_equal(a: Any, b: Any) -> bool:
    """Check if two dataclass instances are equal.

    Args:
        a: First dataclass instance.
        b: Second dataclass instance.

    Returns:
        True if equal.
    """
    if not is_dataclass(a) or not is_dataclass(b):
        return False

    a_fields = {f.name for f in fields(a)}
    b_fields = {f.name for f in fields(b)}

    if a_fields != b_fields:
        return False

    for field_name in a_fields:
        if not deep_equal(getattr(a, field_name), getattr(b, field_name)):
            return False

    return True


def approx_equal(
    a: float,
    b: float,
    rel_tol: float = 1e-9,
    abs_tol: float = 0.0,
) -> bool:
    """Check if two floats are approximately equal.

    Args:
        a: First value.
        b: Second value.
        rel_tol: Relative tolerance.
        abs_tol: Absolute tolerance.

    Returns:
        True if approximately equal.
    """
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


def compare(
    a: Any,
    b: Any,
    key: Optional[Callable[[Any], Any]] = None,
) -> int:
    """Compare two objects.

    Args:
        a: First object.
        b: Second object.
        key: Optional key function.

    Returns:
        -1 if a < b, 0 if a == b, 1 if a > b.
    """
    if key:
        a = key(a)
        b = key(b)

    if a < b:
        return -1
    elif a > b:
        return 1
    return 0


def min_by(data: list, key: Callable[[Any], Any]) -> Any:
    """Get minimum item by key function."""
    if not data:
        raise ValueError("Cannot get min of empty sequence")
    return min(data, key=key)


def max_by(data: list, key: Callable[[Any], Any]) -> Any:
    """Get maximum item by key function."""
    if not data:
        raise ValueError("Cannot get max of empty sequence")
    return max(data, key=key)


def clamp(value: float, min_val: float, max_val: float) -> float:
    """Clamp value to range.

    Args:
        value: Value to clamp.
        min_val: Minimum value.
        max_val: Maximum value.

    Returns:
        Clamped value.
    """
    if value < min_val:
        return min_val
    if value > max_val:
        return max_val
    return value

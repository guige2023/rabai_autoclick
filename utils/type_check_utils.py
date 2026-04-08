"""Type check utilities for RabAI AutoClick.

Provides:
- Type checking helpers
- Type validation
- Type casting utilities
"""

from __future__ import annotations

from typing import (
    Any,
    List,
    Optional,
    Type,
    Union,
)


def is_str(value: Any) -> bool:
    """Check if value is a string."""
    return isinstance(value, str)


def is_int(value: Any) -> bool:
    """Check if value is an integer."""
    return isinstance(value, int) and not isinstance(value, bool)


def is_float(value: Any) -> bool:
    """Check if value is a float."""
    return isinstance(value, float)


def is_bool(value: Any) -> bool:
    """Check if value is a boolean."""
    return isinstance(value, bool)


def is_list(value: Any) -> bool:
    """Check if value is a list."""
    return isinstance(value, list)


def is_dict(value: Any) -> bool:
    """Check if value is a dict."""
    return isinstance(value, dict)


def is_tuple(value: Any) -> bool:
    """Check if value is a tuple."""
    return isinstance(value, tuple)


def is_set(value: Any) -> bool:
    """Check if value is a set."""
    return isinstance(value, set)


def is_none(value: Any) -> bool:
    """Check if value is None."""
    return value is None


def is_numeric(value: Any) -> bool:
    """Check if value is numeric (int or float)."""
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def is_string_like(value: Any) -> bool:
    """Check if value is string-like."""
    return isinstance(value, (str, bytes))


def is_collection(value: Any) -> bool:
    """Check if value is a collection (list, tuple, set, dict)."""
    return isinstance(value, (list, tuple, set, dict))


def cast_int(value: Any, default: int = 0) -> int:
    """Safely cast to int."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def cast_float(value: Any, default: float = 0.0) -> float:
    """Safely cast to float."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def cast_str(value: Any, default: str = "") -> str:
    """Safely cast to string."""
    try:
        return str(value)
    except Exception:
        return default


__all__ = [
    "is_str",
    "is_int",
    "is_float",
    "is_bool",
    "is_list",
    "is_dict",
    "is_tuple",
    "is_set",
    "is_none",
    "is_numeric",
    "is_string_like",
    "is_collection",
    "cast_int",
    "cast_float",
    "cast_str",
]

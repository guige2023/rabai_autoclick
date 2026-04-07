"""Types utilities v4 - simple type utilities.

Simple type checking and coercion utilities.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "is_int",
    "is_float",
    "is_str",
    "is_bool",
    "is_list",
    "is_dict",
    "is_tuple",
    "is_set",
    "to_int",
    "to_float",
    "to_str",
    "to_bool",
]


def is_int(value: Any) -> bool:
    """Check if value is int."""
    return isinstance(value, int)


def is_float(value: Any) -> bool:
    """Check if value is float."""
    return isinstance(value, float)


def is_str(value: Any) -> bool:
    """Check if value is str."""
    return isinstance(value, str)


def is_bool(value: Any) -> bool:
    """Check if value is bool."""
    return isinstance(value, bool)


def is_list(value: Any) -> bool:
    """Check if value is list."""
    return isinstance(value, list)


def is_dict(value: Any) -> bool:
    """Check if value is dict."""
    return isinstance(value, dict)


def is_tuple(value: Any) -> bool:
    """Check if value is tuple."""
    return isinstance(value, tuple)


def is_set(value: Any) -> bool:
    """Check if value is set."""
    return isinstance(value, (set, frozenset))


def to_int(value: Any) -> int:
    """Convert to int."""
    return int(value)


def to_float(value: Any) -> float:
    """Convert to float."""
    return float(value)


def to_str(value: Any) -> str:
    """Convert to str."""
    return str(value)


def to_bool(value: Any) -> bool:
    """Convert to bool."""
    return bool(value)

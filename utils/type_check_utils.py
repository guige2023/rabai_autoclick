"""Type checking and casting utilities for RabAI AutoClick.

Provides:
- Safe type conversion
- Type guards
- Type hints utilities
- Union type handling
"""

from __future__ import annotations

from typing import (
    Any,
    get_args,
    get_origin,
    get_type_hints,
    Union,
)


def is_list_type(tp: type) -> bool:
    """Check if type is List[X]."""
    return get_origin(tp) is list


def is_dict_type(tp: type) -> bool:
    """Check if type is Dict[K, V]."""
    return get_origin(tp) is dict


def is_optional(tp: type) -> bool:
    """Check if type is Optional[X] (Union[X, None])."""
    return get_origin(tp) is Union and type(None) in get_args(tp)


def is_union(tp: type) -> bool:
    """Check if type is a Union."""
    return get_origin(tp) is Union


def get_base_type(tp: type) -> type:
    """Get base type from Optional or Union."""
    args = get_args(tp)
    if args:
        for arg in args:
            if arg is not type(None):
                return arg
    return tp


def safe_int(value: Any, default: int = 0) -> int:
    """Safely convert to int."""
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_float(value: Any, default: float = 0.0) -> float:
    """Safely convert to float."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_str(value: Any, default: str = "") -> str:
    """Safely convert to string."""
    try:
        return str(value)
    except Exception:
        return default


def safe_bool(value: Any) -> bool:
    """Safely convert to bool."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes", "on")
    return bool(value)


def isinstance_check(obj: Any, types: tuple[type, ...]) -> bool:
    """Check if object is instance of types."""
    return isinstance(obj, types)

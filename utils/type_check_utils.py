"""Type checking and coercion utilities for runtime type validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Type, get_origin, get_args

__all__ = [
    "TypeChecker",
    "coerce_type",
    "is_subclass",
    "is_instance",
    "safe_cast",
]


def is_subclass(obj: type, class_or_tuple: type | tuple[type, ...]) -> bool:
    """Check if obj is a class that's a subclass of class_or_tuple."""
    try:
        return isinstance(obj, type) and issubclass(obj, class_or_tuple)
    except TypeError:
        return False


def is_instance(obj: Any, class_or_tuple: type | tuple[type, ...]) -> bool:
    """Type-safe isinstance check."""
    try:
        return isinstance(obj, class_or_tuple)
    except TypeError:
        return False


@dataclass
class TypeConversionError(Exception):
    """Raised when type coercion fails."""
    value: Any
    target_type: type
    reason: str = ""


def coerce_type(
    value: Any,
    target: type,
    default: Any = None,
) -> Any:
    """Coerce a value to a target type with fallback."""
    if value is None:
        return default

    origin = get_origin(target)
    args = get_args(target)

    try:
        if origin is list and args:
            item_type = args[0]
            if isinstance(value, (list, tuple)):
                return [coerce_type(v, item_type) for v in value]
            return [coerce_type(value, item_type)]

        if origin is dict and len(args) >= 2:
            key_type, val_type = args[0], args[1]
            if isinstance(value, dict):
                return {coerce_type(k, key_type): coerce_type(v, val_type) for k, v in value.items()}

        if target is bool:
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes", "on")
            return bool(value)

        if target is int:
            if isinstance(value, float):
                return int(value)
            return int(value)

        if target is float:
            return float(value)

        if target is str:
            return str(value)

        return target(value)
    except (ValueError, TypeError) as e:
        return default


class TypeChecker:
    """Runtime type checker for complex type specifications."""

    @staticmethod
    def check_int(value: Any) -> bool:
        return isinstance(value, int) and not isinstance(value, bool)

    @staticmethod
    def check_str(value: Any) -> bool:
        return isinstance(value, str)

    @staticmethod
    def check_float(value: Any) -> bool:
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    @staticmethod
    def check_list(value: Any, item_type: type | None = None) -> bool:
        if not isinstance(value, list):
            return False
        if item_type:
            return all(isinstance(v, item_type) for v in value)
        return True

    @staticmethod
    def check_dict(value: Any, key_type: type | None = None, val_type: type | None = None) -> bool:
        if not isinstance(value, dict):
            return False
        if key_type:
            return all(isinstance(k, key_type) for k in value.keys())
        if val_type:
            return all(isinstance(v, val_type) for v in value.values())
        return True

    @staticmethod
    def check_optional(value: Any, inner_type: type) -> bool:
        return value is None or isinstance(value, inner_type)


def safe_cast(value: Any, target_type: type[T], default: T) -> T:
    """Safely cast a value to a type, returning default on failure."""
    try:
        result = coerce_type(value, target_type)
        return result if result is not None else default
    except Exception:
        return default

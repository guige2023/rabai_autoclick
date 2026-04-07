"""Typing utilities v4 - optional and union utilities.

Optional and union type utilities.
"""

from __future__ import annotations

from typing import Any, Union, get_args, get_origin

__all__ = [
    "unwrap_optional",
    "is_optional",
    "is_union",
    "union_members",
    "common_base",
    "narrow_type",
]


def unwrap_optional(tp: Any) -> Any:
    """Unwrap Optional type.

    Args:
        tp: Optional type.

    Returns:
        Inner type.
    """
    if not is_optional(tp):
        return tp
    args = get_args(tp)
    return next(arg for arg in args if arg is not type(None))


def is_optional(tp: Any) -> bool:
    """Check if type is Optional.

    Args:
        tp: Type to check.

    Returns:
        True if Optional.
    """
    if get_origin(tp) is Union:
        args = get_args(tp)
        return len(args) == 2 and type(None) in args
    return False


def is_union(tp: Any) -> bool:
    """Check if type is Union.

    Args:
        tp: Type to check.

    Returns:
        True if Union.
    """
    return get_origin(tp) is Union


def union_members(tp: Any) -> list[type]:
    """Get all members of Union type.

    Args:
        tp: Union type.

    Returns:
        List of member types.
    """
    if not is_union(tp):
        return [tp]
    return list(get_args(tp))


def common_base(*types: type) -> type | None:
    """Find common base class.

    Args:
        *types: Types to find common base of.

    Returns:
        Common base or None.
    """
    if not types:
        return None
    bases = set(types)
    for cls in types:
        for base in cls.__mro__:
            if base in bases:
                return base
    return None


def narrow_type(value: Any, tp: type) -> bool:
    """Narrow type by checking isinstance.

    Args:
        value: Value to check.
        tp: Type to narrow to.

    Returns:
        True if value is instance of type.
    """
    return isinstance(value, tp)

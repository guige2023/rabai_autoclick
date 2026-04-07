"""Enum utilities for RabAI AutoClick.

Provides:
- Enum helpers
- Enum value lookup
- Enum iteration
"""

from __future__ import annotations

from enum import Enum, auto
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    TypeVar,
)


T = TypeVar("T", bound=Enum)


def enum_values(enum_class: type[T]) -> list:
    """Get all values of an enum.

    Args:
        enum_class: Enum class.

    Returns:
        List of enum values.
    """
    return [e.value for e in enum_class]


def enum_names(enum_class: type[T]) -> list[str]:
    """Get all names of an enum.

    Args:
        enum_class: Enum class.

    Returns:
        List of enum names.
    """
    return [e.name for e in enum_class]


def enum_lookup(enum_class: type[T], value: Any) -> Optional[T]:
    """Look up enum by value.

    Args:
        enum_class: Enum class.
        value: Value to look up.

    Returns:
        Enum member or None.
    """
    try:
        return enum_class(value)
    except ValueError:
        return None


def enum_by_name(enum_class: type[T], name: str) -> Optional[T]:
    """Look up enum by name.

    Args:
        enum_class: Enum class.
        name: Name to look up.

    Returns:
        Enum member or None.
    """
    try:
        return enum_class[name]
    except KeyError:
        return None


def enum_choices(enum_class: type[T]) -> List[tuple[str, Any]]:
    """Get choices for Django-style choice field.

    Args:
        enum_class: Enum class.

    Returns:
        List of (name, value) tuples.
    """
    return [(e.name, e.value) for e in enum_class]


def auto_enum() -> Enum:
    """Create enum with auto values.

    Returns:
        New enum class.
    """
    return auto()

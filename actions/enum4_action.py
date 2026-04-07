"""Enum utilities v4 - enum value operations.

Enum value and member operations.
"""

from __future__ import annotations

from enum import Enum

__all__ = [
    "get_by_value",
    "get_by_name",
    "next_value",
    "prev_value",
    "values_list",
    "names_list",
]


def get_by_value(enum_cls: type[Enum], value: Any) -> Enum | None:
    """Get enum member by value.

    Args:
        enum_cls: Enum class.
        value: Value to find.

    Returns:
        Enum member or None.
    """
    return enum_cls._value2member_map_.get(value)


def get_by_name(enum_cls: type[Enum], name: str) -> Enum | None:
    """Get enum member by name.

    Args:
        enum_cls: Enum class.
        name: Name to find.

    Returns:
        Enum member or None.
    """
    return enum_cls._member_map_.get(name)


def next_value(member: Enum) -> Enum | None:
    """Get next enum value.

    Args:
        member: Current enum member.

    Returns:
        Next member or None.
    """
    members = list(member.__class__)
    idx = members.index(member)
    if idx + 1 < len(members):
        return members[idx + 1]
    return None


def prev_value(member: Enum) -> Enum | None:
    """Get previous enum value.

    Args:
        member: Current enum member.

    Returns:
        Previous member or None.
    """
    members = list(member.__class__)
    idx = members.index(member)
    if idx > 0:
        return members[idx - 1]
    return None


def values_list(enum_cls: type[Enum]) -> list[Any]:
    """Get list of all values.

    Args:
        enum_cls: Enum class.

    Returns:
        List of values.
    """
    return [m.value for m in enum_cls]


def names_list(enum_cls: type[Enum]) -> list[str]:
    """Get list of all names.

    Args:
        enum_cls: Enum class.

    Returns:
        List of names.
    """
    return [m.name for m in enum_cls]

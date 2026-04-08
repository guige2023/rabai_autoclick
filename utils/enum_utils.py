"""Enum utilities for RabAI AutoClick.

Provides:
- Enum value lookup by name or value
- Serialization/deserialization helpers
- Enum iteration and filtering
- Flag operations for IntFlag enums
"""

from __future__ import annotations

import enum
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)


T = TypeVar("T", bound=enum.Enum)


def get_enum_value(
    enum_cls: Type[enum.Enum],
    value: Any,
    default: Optional[Any] = None,
) -> Any:
    """Get an enum member by its value.

    Args:
        enum_cls: The enum class.
        value: Value to look up.
        default: Default if not found.

    Returns:
        Enum member or default.
    """
    try:
        return enum_cls(value)
    except ValueError:
        return default


def get_enum_name(
    enum_cls: Type[enum.Enum],
    name: str,
    default: Optional[enum.Enum] = None,
) -> Optional[enum.Enum]:
    """Get an enum member by its name.

    Args:
        enum_cls: The enum class.
        name: Member name.
        default: Default if not found.

    Returns:
        Enum member or default.
    """
    try:
        return enum_cls[name]
    except KeyError:
        return default


def enum_to_dict(
    enum_cls: Type[enum.Enum],
    value_key: str = "value",
    name_key: str = "name",
) -> List[Dict[str, Any]]:
    """Convert an enum to a list of dicts.

    Args:
        enum_cls: The enum class.
        value_key: Key name for the value.
        name_key: Key name for the name.

    Returns:
        List of {name_key: str, value_key: Any} dicts.
    """
    return [
        {name_key: member.name, value_key: member.value}
        for member in enum_cls
    ]


def enum_to_str_dict(
    enum_cls: Type[enum.Enum],
) -> Dict[str, Any]:
    """Convert an enum to a string-keyed dict.

    Args:
        enum_cls: The enum class.

    Returns:
        Dict mapping member names to values.
    """
    return {member.name: member.value for member in enum_cls}


def iterate_enums(
    enum_cls: Type[enum.Enum],
    filter_func: Optional[Callable[[enum.Enum], bool]] = None,
) -> Iterator[enum.Enum]:
    """Iterate over enum members with optional filtering.

    Args:
        enum_cls: The enum class.
        filter_func: Optional filter predicate.

    Yields:
        Enum members.
    """
    for member in enum_cls:
        if filter_func is None or filter_func(member):
            yield member


def enum_values(enum_cls: Type[enum.Enum]) -> List[Any]:
    """Get all values from an enum.

    Args:
        enum_cls: The enum class.

    Returns:
        List of enum values.
    """
    return [member.value for member in enum_cls]


def enum_names(enum_cls: Type[enum.Enum]) -> List[str]:
    """Get all member names from an enum.

    Args:
        enum_cls: The enum class.

    Returns:
        List of enum names.
    """
    return [member.name for member in enum_cls]


def is_flag(enum_cls: Type[Any]) -> bool:
    """Check if an enum is an IntFlag or Flag type.

    Args:
        enum_cls: The enum class to check.

    Returns:
        True if it's a Flag type.
    """
    return issubclass(enum_cls, enum.Flag)


def flag_add(flag_enum: Type[enum.Enum], *flags: enum.Enum) -> enum.Enum:
    """Combine multiple flag enum values.

    Args:
        flag_enum: The flag enum class.
        *flags: Flag values to combine.

    Returns:
        Combined flag value.
    """
    result = flag_enum(0)
    for f in flags:
        result = result | f
    return result


def flag_remove(flag_enum: Type[enum.Enum], flags: enum.Enum, *to_remove: enum.Enum) -> enum.Enum:
    """Remove specific flags from a flag value.

    Args:
        flag_enum: The flag enum class.
        flags: Current flag value.
        *to_remove: Flags to remove.

    Returns:
        New flag value with removals applied.
    """
    result = flags
    for r in to_remove:
        result = result & ~r
    return result


def flag_has(flags: enum.Enum, flag: enum.Enum) -> bool:
    """Check if a flag has a specific bit set.

    Args:
        flags: Current flag value.
        flag: Flag to check for.

    Returns:
        True if the flag is set.
    """
    return bool(flags & flag)


def deserialize_enum(
    enum_cls: Type[enum.Enum],
    value: Union[str, Any],
    default: Optional[enum.Enum] = None,
) -> Optional[enum.Enum]:
    """Deserialize an enum from a string or value.

    Args:
        enum_cls: Target enum class.
        value: String name or integer value.
        default: Default if deserialization fails.

    Returns:
        Enum member or default.
    """
    if isinstance(value, str):
        return get_enum_name(enum_cls, value.upper(), default)
    return get_enum_value(enum_cls, value, default)


def serialize_enum(member: enum.Enum) -> Union[str, Any]:
    """Serialize an enum member to a primitive.

    Args:
        member: Enum member.

    Returns:
        The value (or name if not hashable).
    """
    return member.value


def auto_enum(
    cls: Type[T],
    start: int = 1,
) -> Type[T]:
    """Create an auto-numbered enum class.

    Args:
        cls: The class being decorated.
        start: Starting number.

    Returns:
        Modified enum class with auto-incrementing values.
    """
    members = {name: i for i, name in enumerate(cls.__members__, start)}
    return enum.Enum(cls.__name__, members)  # type: ignore


__all__ = [
    "get_enum_value",
    "get_enum_name",
    "enum_to_dict",
    "enum_to_str_dict",
    "iterate_enums",
    "enum_values",
    "enum_names",
    "is_flag",
    "flag_add",
    "flag_remove",
    "flag_has",
    "deserialize_enum",
    "serialize_enum",
    "auto_enum",
]

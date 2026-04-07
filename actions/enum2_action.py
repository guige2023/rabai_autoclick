"""Enum utilities v2 - extended enum operations.

Extended enum utilities including flag operations,
 auto-numbering, and serialization.
"""

from __future__ import annotations

from enum import Enum, Flag, IntEnum, auto, member
from typing import Any, Iterator

__all__ = [
    "auto_name",
    "AutoName",
    "extend_enum",
    "remove_enum_value",
    "enum_by_name",
    "enum_by_value",
    "enum_values",
    "enum_members",
    "flag_set",
    "flag_unset",
    "flag_toggle",
    "flag_all",
    "flag_none",
    "serialize_enum",
    "deserialize_enum",
    "enum_to_dict",
    "dict_to_enum",
    "FlagSet",
    "EnumRange",
]


class AutoName(Enum):
    """Enum with automatic names from class variable name."""

    @staticmethod
    def _generate_next_value_(name: str, start: int, count: int, last_values: list) -> str:
        return name


def auto_name() -> str:
    """Return name for auto() in AutoName enum."""
    return auto()


def extend_enum(base_enum: type[Enum], name: str, value: Any) -> type[Enum]:
    """Extend an enum with a new value.

    Args:
        base_enum: Enum class to extend.
        name: New member name.
        value: New member value.

    Returns:
        Extended enum class (actually modifies in place).
    """
    if name in base_enum.__members__:
        raise ValueError(f"{name} already exists in {base_enum}")
    if value in [m.value for m in base_enum]:
        raise ValueError(f"{value} already exists in {base_enum}")
    new_member = dict.__new__(base_enum)
    new_member._name_ = name
    new_member._value_ = value
    setattr(base_enum, name, new_member)
    base_enum._member_map_[name] = new_member
    base_enum._member_names_.append(name)
    base_enum._value2member_map_[value] = new_member
    return base_enum


def remove_enum_value(base_enum: type[Enum], value: Any) -> bool:
    """Remove a value from enum.

    Args:
        base_enum: Enum class.
        value: Value to remove.

    Returns:
        True if removed.
    """
    member = base_enum._value2member_map_.get(value)
    if member is None:
        return False
    name = member.name
    del base_enum._member_map_[name]
    base_enum._member_names_.remove(name)
    del base_enum._value2member_map_[value]
    delattr(base_enum, name)
    return True


def enum_by_name(enum_cls: type[Enum], name: str) -> Enum | None:
    """Get enum member by name.

    Args:
        enum_cls: Enum class.
        name: Member name.

    Returns:
        Member or None.
    """
    try:
        return enum_cls[name]
    except KeyError:
        return None


def enum_by_value(enum_cls: type[Enum], value: Any) -> Enum | None:
    """Get enum member by value.

    Args:
        enum_cls: Enum class.
        value: Member value.

    Returns:
        Member or None.
    """
    return enum_cls._value2member_map_.get(value)


def enum_values(enum_cls: type[Enum]) -> list[Any]:
    """Get all enum values.

    Args:
        enum_cls: Enum class.

    Returns:
        List of values.
    """
    return [m.value for m in enum_cls]


def enum_members(enum_cls: type[Enum]) -> list[Enum]:
    """Get all enum members.

    Args:
        enum_cls: Enum class.

    Returns:
        List of members.
    """
    return list(enum_cls)


def flag_set(flag_enum: type[Flag], *values: Any) -> Flag:
    """Combine flag values.

    Args:
        flag_enum: Flag enum class.
        *values: Values to combine.

    Returns:
        Combined flag.
    """
    result = flag_enum(0)
    for v in values:
        result |= flag_enum(v)
    return result


def flag_unset(flag_enum: type[Flag], flags: Flag, *values: Any) -> Flag:
    """Unset specific flags.

    Args:
        flag_enum: Flag enum.
        flags: Current flags.
        *values: Flags to unset.

    Returns:
        New flag state.
    """
    result = flags
    for v in values:
        result &= ~flag_enum(v)
    return result


def flag_toggle(flag_enum: type[Flag], flags: Flag, *values: Any) -> Flag:
    """Toggle specific flags.

    Args:
        flag_enum: Flag enum.
        flags: Current flags.
        *values: Flags to toggle.

    Returns:
        New flag state.
    """
    result = flags
    for v in values:
        result ^= flag_enum(v)
    return result


def flag_all(flag_enum: type[Flag]) -> Flag:
    """Get all flags combined.

    Args:
        flag_enum: Flag enum.

    Returns:
        All flags set.
    """
    result = flag_enum(0)
    for m in flag_enum:
        result |= m
    return result


def flag_none(flag_enum: type[Flag]) -> Flag:
    """Get zero flag value.

    Args:
        flag_enum: Flag enum.

    Returns:
        Zero flag.
    """
    return flag_enum(0)


def serialize_enum(member: Enum) -> str:
    """Serialize enum to string.

    Args:
        member: Enum member.

    Returns:
        String like 'ClassName.member'.
    """
    return f"{member.__class__.__name__}.{member.name}"


def deserialize_enum(enum_cls: type[Enum], serialized: str) -> Enum | None:
    """Deserialize enum from string.

    Args:
        enum_cls: Enum class.
        serialized: String like 'ClassName.member'.

    Returns:
        Enum member or None.
    """
    try:
        _, name = serialized.rsplit(".", 1)
        return enum_cls[name]
    except (ValueError, KeyError, AttributeError):
        return None


def enum_to_dict(enum_cls: type[Enum]) -> dict[str, Any]:
    """Convert enum to dictionary.

    Args:
        enum_cls: Enum class.

    Returns:
        Dict mapping names to values.
    """
    return {m.name: m.value for m in enum_cls}


def dict_to_enum(enum_cls: type[Enum], data: dict[str, Any]) -> Enum | None:
    """Convert dict to enum member.

    Args:
        enum_cls: Enum class.
        data: Dict with 'name' or 'value' key.

    Returns:
        Enum member or None.
    """
    if "name" in data:
        return enum_by_name(enum_cls, data["name"])
    if "value" in data:
        return enum_by_value(enum_cls, data["value"])
    return None


class FlagSet:
    """Set operations on flags."""

    def __init__(self, flag_enum: type[Flag], initial: Flag | None = None) -> None:
        self._enum = flag_enum
        self._flags = initial or flag_enum(0)

    def add(self, *values: Any) -> FlagSet:
        """Add flags."""
        for v in values:
            self._flags |= self._enum(v)
        return self

    def remove(self, *values: Any) -> FlagSet:
        """Remove flags."""
        for v in values:
            self._flags &= ~self._enum(v)
        return self

    def toggle(self, *values: Any) -> FlagSet:
        """Toggle flags."""
        for v in values:
            self._flags ^= self._enum(v)
        return self

    def has(self, *values: Any) -> bool:
        """Check if flags are set."""
        for v in values:
            if not (self._flags & self._enum(v)):
                return False
        return True

    def any(self, *values: Any) -> bool:
        """Check if any flag is set."""
        for v in values:
            if self._flags & self._enum(v):
                return True
        return False

    def get(self) -> Flag:
        """Get current flags."""
        return self._flags

    def __repr__(self) -> str:
        return f"FlagSet({self._flags!r})"


class EnumRange:
    """Range of consecutive enum values."""

    def __init__(self, enum_cls: type[Enum], start: int | Enum, end: int | Enum) -> None:
        self._enum = enum_cls
        start_val = start.value if isinstance(start, Enum) else start
        end_val = end.value if isinstance(end, Enum) else end
        self._members = [enum_cls(v) for v in range(start_val, end_val + 1) if v in enum_cls._value2member_map_]

    def __iter__(self) -> Iterator[Enum]:
        return iter(self._members)

    def __len__(self) -> int:
        return len(self._members)

    def __getitem__(self, index: int) -> Enum:
        return self._members[index]

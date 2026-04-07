"""Enum utilities v3 - advanced enum patterns.

Advanced enum utilities including state machines,
 flags operations, and enum transformations.
"""

from __future__ import annotations

from enum import Enum, Flag, IntEnum, auto

__all__ = [
    "StateMachine",
    "Transition",
    "FlagOps",
    "EnumTransform",
    "EnumFilter",
    "enum_random",
    "enum_sequential",
]


class Transition:
    """Represents a state transition."""

    def __init__(self, from_state: str, to_state: str, event: str) -> None:
        self.from_state = from_state
        self.to_state = to_state
        self.event = event


class StateMachine:
    """Simple state machine using enums."""

    def __init__(self, initial_state: Enum) -> None:
        self._state = initial_state
        self._transitions: dict[tuple[Enum, str], Enum] = {}

    def add_transition(self, from_state: Enum, event: str, to_state: Enum) -> None:
        """Add a transition."""
        self._transitions[(from_state, event)] = to_state

    def trigger(self, event: str) -> bool:
        """Trigger an event and transition state.

        Returns:
            True if transition occurred.
        """
        key = (self._state, event)
        if key in self._transitions:
            self._state = self._transitions[key]
            return True
        return False

    @property
    def state(self) -> Enum:
        """Get current state."""
        return self._state


class FlagOps:
    """Operations on flag enums."""

    @staticmethod
    def is_set(flags: Flag, value: Flag) -> bool:
        """Check if flag is set."""
        return bool(flags & value)

    @staticmethod
    def set_flag(flags: Flag, value: Flag) -> Flag:
        """Set a flag."""
        return flags | value

    @staticmethod
    def clear_flag(flags: Flag, value: Flag) -> Flag:
        """Clear a flag."""
        return flags & ~value

    @staticmethod
    def toggle_flag(flags: Flag, value: Flag) -> Flag:
        """Toggle a flag."""
        return flags ^ value

    @staticmethod
    def all_flags(flag_class: type[Flag]) -> Flag:
        """Get all flags combined."""
        all_val = 0
        for m in flag_class:
            all_val |= m.value
        return flag_class(all_val)


class EnumTransform:
    """Transform enum values."""

    @staticmethod
    def to_dict(enum_cls: type[Enum]) -> dict[str, Any]:
        """Convert enum to dictionary."""
        return {m.name: m.value for m in enum_cls}

    @staticmethod
    def to_list(enum_cls: type[Enum]) -> list[Any]:
        """Convert enum to list of values."""
        return [m.value for m in enum_cls]

    @staticmethod
    def invert(enum_cls: type[Enum]) -> dict[Any, Enum]:
        """Create value-to-member mapping."""
        return {m.value: m for m in enum_cls}


class EnumFilter:
    """Filter enum members."""

    @staticmethod
    def by_prefix(enum_cls: type[Enum], prefix: str) -> list[Enum]:
        """Filter members by prefix."""
        return [m for m in enum_cls if m.name.startswith(prefix)]

    @staticmethod
    def by_value_range(enum_cls: type[Enum], min_val: int, max_val: int) -> list[Enum]:
        """Filter by value range."""
        return [m for m in enum_cls if min_val <= m.value <= max_val]


def enum_random(enum_cls: type[Enum]) -> Enum:
    """Get random enum member."""
    import random
    members = list(enum_cls)
    return random.choice(members)


def enum_sequential(enum_cls: type[Enum]) -> list[Enum]:
    """Get enum members in order."""
    return list(enum_cls)

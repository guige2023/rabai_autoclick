"""Automation context provider for UI automation workflows.

Provides shared context for running automation workflows including
configuration, state, and resource management.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class ContextLevel(Enum):
    """Context nesting levels."""
    GLOBAL = auto()
    SESSION = auto()
    WORKFLOW = auto()
    STEP = auto()


@dataclass
class ContextValue:
    """A value stored in the automation context."""
    key: str
    value: Any
    level: ContextLevel
    created_at: float = 0.0
    modified_at: float = 0.0


class AutomationContext:
    """Hierarchical context for automation workflows.

    Provides scoped storage with GLOBAL > SESSION > WORKFLOW > STEP
    precedence, similar to variable scoping rules.
    """

    def __init__(self) -> None:
        """Initialize empty context."""
        self._store: dict[ContextLevel, dict[str, Any]] = {
            level: {} for level in ContextLevel
        }
        self._listeners: dict[str, list[Callable[[str, Any], None]]] = {}

    def set(
        self,
        key: str,
        value: Any,
        level: ContextLevel = ContextLevel.STEP,
    ) -> None:
        """Set a value at a specific context level."""
        self._store[level][key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value, searching from lowest to highest level."""
        for level in [ContextLevel.STEP, ContextLevel.WORKFLOW,
                       ContextLevel.SESSION, ContextLevel.GLOBAL]:
            if key in self._store[level]:
                return self._store[level][key]
        return default

    def has(self, key: str) -> bool:
        """Check if a key exists at any level."""
        return any(key in self._store[l] for l in ContextLevel)

    def remove(self, key: str, level: ContextLevel) -> bool:
        """Remove a key at a specific level. Returns True if found."""
        if key in self._store[level]:
            del self._store[level][key]
            return True
        return False

    def clear_level(self, level: ContextLevel) -> None:
        """Clear all values at a level."""
        self._store[level].clear()

    def get_all_at(self, level: ContextLevel) -> dict[str, Any]:
        """Get all values at a level."""
        return dict(self._store[level])

    def on_change(
        self,
        key: str,
        callback: Callable[[str, Any], None],
    ) -> None:
        """Register a listener for key changes."""
        self._listeners.setdefault(key, []).append(callback)

    def notify_change(self, key: str, value: Any) -> None:
        """Notify listeners of a change."""
        for callback in self._listeners.get(key, []):
            try:
                callback(key, value)
            except Exception:
                pass

    def snapshot(self) -> dict[ContextLevel, dict[str, Any]]:
        """Return a deep copy of the entire context."""
        return {
            level: dict(values)
            for level, values in self._store.items()
        }


# Global singleton
_automation_context = AutomationContext()


def get_automation_context() -> AutomationContext:
    """Return the global automation context."""
    return _automation_context

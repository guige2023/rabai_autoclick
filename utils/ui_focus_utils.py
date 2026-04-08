"""UI focus management utilities.

This module provides utilities for managing UI focus,
tracking focus changes, and handling focus-related events.
"""

from __future__ import annotations

import time
from typing import Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum, auto


class FocusChangeReason(Enum):
    """Reasons for focus changes."""
    CLICK = auto()
    KEYBOARD = auto()
    PROGRAMMATIC = auto()
    WINDOW_SWITCH = auto()
    APPLICATION_SWITCH = auto()
    TIMEOUT = auto()
    UNKNOWN = auto()


@dataclass
class FocusState:
    """Current focus state."""
    window_id: Optional[int]
    element_id: Optional[str]
    timestamp: float
    reason: FocusChangeReason


@dataclass
class FocusHistory:
    """History of focus changes."""
    states: List[FocusState] = field(default_factory=list)

    def add(self, state: FocusState) -> None:
        self.states.append(state)

    def last(self) -> Optional[FocusState]:
        return self.states[-1] if self.states else None

    def clear(self) -> None:
        self.states.clear()


class FocusManager:
    """Manages UI focus state and changes."""

    def __init__(self) -> None:
        self._current: Optional[FocusState] = None
        self._history = FocusHistory()
        self._on_focus_change: Optional[Callable[[FocusState, FocusState], None]] = None
        self._on_focus_lost: Optional[Callable[[FocusState], None]] = None
        self._on_focus_gained: Optional[Callable[[FocusState], None]] = None

    def set_focus(
        self,
        window_id: Optional[int],
        element_id: Optional[str],
        reason: FocusChangeReason = FocusChangeReason.PROGRAMMATIC,
    ) -> None:
        old = self._current
        new = FocusState(
            window_id=window_id,
            element_id=element_id,
            timestamp=time.time(),
            reason=reason,
        )
        self._current = new
        self._history.add(new)

        if self._on_focus_change and old:
            self._on_focus_change(old, new)
        if self._on_focus_gained:
            self._on_focus_gained(new)
        if old and self._on_focus_lost:
            self._on_focus_lost(old)

    def clear_focus(self, reason: FocusChangeReason = FocusChangeReason.UNKNOWN) -> None:
        old = self._current
        self._current = FocusState(
            window_id=None,
            element_id=None,
            timestamp=time.time(),
            reason=reason,
        )
        self._history.add(self._current)
        if old and self._on_focus_lost:
            self._on_focus_lost(old)

    @property
    def current(self) -> Optional[FocusState]:
        return self._current

    @property
    def history(self) -> FocusHistory:
        return self._history

    @property
    def has_focus(self) -> bool:
        return self._current is not None and self._current.element_id is not None

    def is_focused(self, window_id: int, element_id: str) -> bool:
        return (
            self._current is not None
            and self._current.window_id == window_id
            and self._current.element_id == element_id
        )

    def on_focus_change(self, handler: Callable[[FocusState, FocusState], None]) -> None:
        self._on_focus_change = handler

    def on_focus_lost(self, handler: Callable[[FocusState], None]) -> None:
        self._on_focus_lost = handler

    def on_focus_gained(self, handler: Callable[[FocusState], None]) -> None:
        self._on_focus_gained = handler


__all__ = [
    "FocusChangeReason",
    "FocusState",
    "FocusHistory",
    "FocusManager",
]

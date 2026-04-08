"""Focus ring manager for UI automation.

Tracks and manages keyboard focus across UI elements,
providing focus ring visualization and focus-change notifications.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class FocusRingEntry:
    """An entry in the focus ring history."""
    element_id: str
    element_name: str
    timestamp: float
    is_handled: bool = False


class FocusRingManager:
    """Manages focus ring tracking and notifications.

    Tracks focus changes across UI elements, maintains a history
    ring, and fires callbacks on focus transitions.
    """

    def __init__(self, max_history: int = 50) -> None:
        """Initialize focus ring manager.

        Args:
            max_history: Maximum focus history entries to keep.
        """
        self._current_focus: Optional[str] = None
        self._current_name: str = ""
        self._history: list[FocusRingEntry] = []
        self._max_history = max_history
        self._on_focus_callbacks: list[Callable[[str, str], None]] = []
        self._on_blur_callbacks: list[Callable[[str], None]] = []
        self._element_names: dict[str, str] = {}

    def register_element(self, element_id: str, name: str) -> None:
        """Register an element with a readable name."""
        self._element_names[element_id] = name

    def set_focus(self, element_id: str) -> bool:
        """Set focus to an element.

        Returns True if focus was successfully set.
        """
        if self._current_focus == element_id:
            return True

        previous = self._current_focus
        self._current_focus = element_id
        self._current_name = self._element_names.get(element_id, element_id)

        entry = FocusRingEntry(
            element_id=element_id,
            element_name=self._current_name,
            timestamp=self._get_time(),
        )
        self._history.append(entry)
        if len(self._history) > self._max_history:
            self._history.pop(0)

        if previous:
            self._notify_blur(previous)
        self._notify_focus(element_id, self._current_name)
        return True

    def get_focus(self) -> Optional[str]:
        """Return the currently focused element ID."""
        return self._current_focus

    def clear_focus(self) -> None:
        """Clear current focus."""
        if self._current_focus:
            prev = self._current_focus
            self._current_focus = None
            self._current_name = ""
            self._notify_blur(prev)

    def is_focused(self, element_id: str) -> bool:
        """Return True if the given element has focus."""
        return self._current_focus == element_id

    def on_focus(
        self, callback: Callable[[str, str], None]
    ) -> None:
        """Register a callback for focus gain events."""
        self._on_focus_callbacks.append(callback)

    def on_blur(self, callback: Callable[[str], None]) -> None:
        """Register a callback for focus loss events."""
        self._on_blur_callbacks.append(callback)

    def get_history(
        self, limit: int = 10, handled: Optional[bool] = None
    ) -> list[FocusRingEntry]:
        """Get focus history, optionally filtered."""
        history = list(reversed(self._history))
        if limit > 0:
            history = history[:limit]
        if handled is not None:
            history = [e for e in history if e.is_handled == handled]
        return history

    def mark_handled(self, index: int) -> None:
        """Mark a history entry as handled."""
        if 0 <= index < len(self._history):
            self._history[index].is_handled = True

    def clear_history(self) -> None:
        """Clear focus history."""
        self._history.clear()

    def _notify_focus(self, element_id: str, name: str) -> None:
        """Notify all focus callbacks."""
        for cb in self._on_focus_callbacks:
            try:
                cb(element_id, name)
            except Exception:
                pass

    def _notify_blur(self, element_id: str) -> None:
        """Notify all blur callbacks."""
        for cb in self._on_blur_callbacks:
            try:
                cb(element_id)
            except Exception:
                pass

    def _get_time(self) -> float:
        """Get current time."""
        import time
        return time.time()

    @property
    def focus_name(self) -> str:
        """Return the name of the currently focused element."""
        return self._current_name

    @property
    def has_focus(self) -> bool:
        """Return True if any element has focus."""
        return self._current_focus is not None

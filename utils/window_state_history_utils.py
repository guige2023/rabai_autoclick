"""Window State History Utilities.

Tracks and replays window state history for debugging and testing.

Example:
    >>> from window_state_history_utils import WindowStateHistory
    >>> history = WindowStateHistory()
    >>> history.record_state(window_id="1", bounds=(0, 0, 800, 600))
    >>> states = history.get_states()
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class WindowState:
    """Snapshot of window state at a point in time."""
    window_id: str
    timestamp: float
    bounds: Tuple[int, int, int, int]
    is_visible: bool = True
    is_focused: bool = False
    title: str = ""
    extra: Dict[str, Any] = field(default_factory=dict)

    @property
    def x(self) -> int:
        return self.bounds[0]

    @property
    def y(self) -> int:
        return self.bounds[1]

    @property
    def width(self) -> int:
        return self.bounds[2]

    @property
    def height(self) -> int:
        return self.bounds[3]


class WindowStateHistory:
    """Tracks window state over time."""

    def __init__(self, max_entries: int = 1000):
        """Initialize history tracker.

        Args:
            max_entries: Maximum number of entries to keep.
        """
        self.max_entries = max_entries
        self._states: List[WindowState] = []

    def record_state(
        self,
        window_id: str,
        bounds: Tuple[int, int, int, int],
        is_visible: bool = True,
        is_focused: bool = False,
        title: str = "",
        **extra: Any,
    ) -> None:
        """Record a window state snapshot.

        Args:
            window_id: Window identifier.
            bounds: (x, y, width, height).
            is_visible: Window visibility.
            is_focused: Window focus state.
            title: Window title.
            **extra: Additional metadata.
        """
        state = WindowState(
            window_id=window_id,
            timestamp=time.time(),
            bounds=bounds,
            is_visible=is_visible,
            is_focused=is_focused,
            title=title,
            extra=extra,
        )
        self._states.append(state)
        if len(self._states) > self.max_entries:
            self._states.pop(0)

    def get_states(
        self,
        window_id: Optional[str] = None,
        since: Optional[float] = None,
    ) -> List[WindowState]:
        """Get recorded states.

        Args:
            window_id: Optional filter by window ID.
            since: Optional timestamp filter.

        Returns:
            List of WindowState objects.
        """
        result = self._states[:]
        if window_id:
            result = [s for s in result if s.window_id == window_id]
        if since is not None:
            result = [s for s in result if s.timestamp >= since]
        return result

    def get_last_state(self, window_id: str) -> Optional[WindowState]:
        """Get the most recent state for a window.

        Args:
            window_id: Window identifier.

        Returns:
            Latest WindowState or None.
        """
        for state in reversed(self._states):
            if state.window_id == window_id:
                return state
        return None

    def get_transitions(self, window_id: str) -> List[Dict[str, Any]]:
        """Get state transitions for a window.

        Args:
            window_id: Window identifier.

        Returns:
            List of transition records.
        """
        states = self.get_states(window_id=window_id)
        transitions = []
        for i in range(1, len(states)):
            prev = states[i - 1]
            curr = states[i]
            changes = {}
            if prev.bounds != curr.bounds:
                changes["bounds"] = {"from": prev.bounds, "to": curr.bounds}
            if prev.is_visible != curr.is_visible:
                changes["visible"] = {"from": prev.is_visible, "to": curr.is_visible}
            if prev.is_focused != curr.is_focused:
                changes["focused"] = {"from": prev.is_focused, "to": curr.is_focused}
            if changes:
                transitions.append({
                    "timestamp": curr.timestamp,
                    "window_id": window_id,
                    "changes": changes,
                })
        return transitions

    def clear(self) -> None:
        """Clear all recorded history."""
        self._states.clear()

"""Focus tracking action for UI automation.

Tracks keyboard focus and input routing:
- Focus element monitoring
- Focus change events
- Input routing detection
- Focus history
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable


class FocusEventType(Enum):
    """Focus event types."""
    ELEMENT_FOCUSED = auto()
    ELEMENT_UNFOCUSED = auto()
    WINDOW_ACTIVATED = auto()
    WINDOW_DEACTIVATED = auto()
    APPLICATION_ACTIVATED = auto()
    APPLICATION_DEACTIVATED = auto()


@dataclass
class FocusEvent:
    """Focus change event."""
    event_type: FocusEventType
    timestamp: float
    element_id: str | None = None
    element_role: str | None = None
    element_title: str | None = None
    window_id: str | None = None
    window_title: str | None = None
    app_bundle_id: str | None = None
    app_name: str | None = None


@dataclass
class FocusHistoryEntry:
    """Focus history entry."""
    element_id: str
    element_title: str
    window_id: str
    window_title: str
    app_bundle_id: str
    app_name: str
    timestamp: float
    duration: float = 0.0


class FocusTracker:
    """Tracks keyboard/input focus across applications.

    Features:
    - Focus element monitoring
    - Focus change callbacks
    - Focus history tracking
    - Input window detection
    - Focus restoration
    """

    def __init__(self, history_size: int = 100):
        self.history_size = history_size
        self._history: list[FocusHistoryEntry] = []
        self._callbacks: list[Callable[[FocusEvent], None]] = []
        self._current_focus: FocusHistoryEntry | None = None
        self._focus_func: Callable | None = None
        self._is_tracking = False

    def set_focus_query_func(self, func: Callable) -> None:
        """Set function to query current focus.

        Args:
            func: Function() -> FocusEvent
        """
        self._focus_func = func

    def start_tracking(self) -> None:
        """Start focus tracking."""
        self._is_tracking = True

    def stop_tracking(self) -> None:
        """Stop focus tracking."""
        self._is_tracking = False

    @property
    def is_tracking(self) -> bool:
        """Check if tracking is active."""
        return self._is_tracking

    def query_focus(self) -> FocusEvent | None:
        """Query current focus state.

        Returns:
            Current focus event or None
        """
        if self._focus_func:
            return self._focus_func()
        return None

    def get_current_focus(self) -> FocusHistoryEntry | None:
        """Get current focus element."""
        return self._current_focus

    def register_callback(self, callback: Callable[[FocusEvent], None]) -> None:
        """Register for focus change events.

        Args:
            callback: Function(FocusEvent) to call on changes
        """
        self._callbacks.append(callback)

    def unregister_callback(self, callback: Callable[[FocusEvent], None]) -> None:
        """Unregister focus callback."""
        if callback in self._callbacks:
            self._callbacks.remove(callback)

    def record_focus_change(self, event: FocusEvent) -> None:
        """Record a focus change event.

        Args:
            event: Focus event to record
        """
        # Update previous entry duration
        if self._current_focus:
            self._current_focus.duration = event.timestamp - self._current_focus.timestamp

        # Create new entry
        entry = FocusHistoryEntry(
            element_id=event.element_id or "",
            element_title=event.element_title or "",
            window_id=event.window_id or "",
            window_title=event.window_title or "",
            app_bundle_id=event.app_bundle_id or "",
            app_name=event.app_name or "",
            timestamp=event.timestamp,
        )

        self._history.append(entry)
        self._current_focus = entry

        # Prune history
        if len(self._history) > self.history_size:
            self._history = self._history[-self.history_size:]

        # Notify callbacks
        for cb in self._callbacks:
            try:
                cb(event)
            except Exception:
                pass

    def get_focus_history(
        self,
        limit: int = 10,
        app_bundle_id: str | None = None,
        window_id: str | None = None,
    ) -> list[FocusHistoryEntry]:
        """Get focus history.

        Args:
            limit: Maximum entries to return
            app_bundle_id: Filter by app (optional)
            window_id: Filter by window (optional)

        Returns:
            List of focus history entries
        """
        history = self._history

        if app_bundle_id:
            history = [e for e in history if e.app_bundle_id == app_bundle_id]
        if window_id:
            history = [e for e in history if e.window_id == window_id]

        return history[-limit:]

    def get_most_focused(
        self,
        limit: int = 5,
        since: float | None = None,
    ) -> list[tuple[str, float]]:
        """Get most focused elements.

        Args:
            limit: Number of top elements to return
            since: Only consider entries after this timestamp

        Returns:
            List of (element_id, total_duration) tuples
        """
        durations: dict[str, float] = {}

        for entry in self._history:
            if since and entry.timestamp < since:
                continue
            if entry.element_id:
                durations[entry.element_id] = durations.get(entry.element_id, 0) + entry.duration

        sorted_durations = sorted(durations.items(), key=lambda x: x[1], reverse=True)
        return sorted_durations[:limit]

    def get_focus_stats(self) -> dict:
        """Get focus statistics.

        Returns:
            Dict with focus stats
        """
        if not self._history:
            return {
                "total_events": 0,
                "unique_elements": 0,
                "unique_windows": 0,
                "unique_apps": 0,
                "total_focus_time": 0,
            }

        element_ids = {e.element_id for e in self._history if e.element_id}
        window_ids = {e.window_id for e in self._history if e.window_id}
        app_ids = {e.app_bundle_id for e in self._history if e.app_bundle_id}
        total_time = sum(e.duration for e in self._history)

        return {
            "total_events": len(self._history),
            "unique_elements": len(element_ids),
            "unique_windows": len(window_ids),
            "unique_apps": len(app_ids),
            "total_focus_time": total_time,
        }

    def find_element_at_time(
        self,
        timestamp: float,
    ) -> FocusHistoryEntry | None:
        """Find focused element at specific time.

        Args:
            timestamp: Time to query

        Returns:
            Element that was focused at time, or None
        """
        for entry in reversed(self._history):
            if entry.timestamp <= timestamp:
                return entry
        return None

    def clear_history(self) -> None:
        """Clear focus history."""
        self._history.clear()
        self._current_focus = None

    def get_recent_events(self, limit: int = 10) -> list[FocusEvent]:
        """Get recent focus events.

        Args:
            limit: Maximum events to return

        Returns:
            List of recent focus events
        """
        # This would need to track events separately from history entries
        # For now, construct from history
        return [
            FocusEvent(
                event_type=FocusEventType.ELEMENT_FOCUSED,
                timestamp=e.timestamp,
                element_id=e.element_id,
                element_title=e.element_title,
                window_id=e.window_id,
                window_title=e.window_title,
                app_bundle_id=e.app_bundle_id,
                app_name=e.app_name,
            )
            for e in self._history[-limit:]
        ]


def create_focus_tracker(history_size: int = 100) -> FocusTracker:
    """Create focus tracker."""
    return FocusTracker(history_size)

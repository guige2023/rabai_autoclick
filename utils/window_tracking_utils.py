"""Window tracking utilities for RabAI AutoClick.

Provides:
- Window change detection
- Window event callbacks
- Window history tracking
"""

from __future__ import annotations

import time
from typing import (
    Callable,
    Dict,
    List,
    Optional,
)


class WindowEvent:
    """A window lifecycle event."""

    CREATED = "created"
    CLOSED = "closed"
    MOVED = "moved"
    RESIZED = "resized"
    FOCUSED = "focused"
    UNFOCUSED = "unfocused"


class WindowTracker:
    """Track window lifecycle events."""

    def __init__(self) -> None:
        self._history: List[Dict] = []
        self._callbacks: Dict[str, List[Callable]] = {
            WindowEvent.CREATED: [],
            WindowEvent.CLOSED: [],
            WindowEvent.MOVED: [],
            WindowEvent.RESIZED: [],
            WindowEvent.FOCUSED: [],
            WindowEvent.UNFOCUSED: [],
        }
        self._last_focused: Optional[str] = None

    def on(
        self,
        event: str,
        callback: Callable[[Dict], None],
    ) -> None:
        """Register an event callback.

        Args:
            event: Event type.
            callback: Function to call.
        """
        if event in self._callbacks:
            self._callbacks[event].append(callback)

    def emit(self, event: str, data: Dict) -> None:
        """Emit a window event.

        Args:
            event: Event type.
            data: Event data.
        """
        self._history.append({"event": event, "data": data, "time": time.time()})
        for cb in self._callbacks.get(event, []):
            cb(data)

    def get_history(
        self,
        event_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict]:
        """Get window event history.

        Args:
            event_type: Filter by event type.
            limit: Maximum events to return.

        Returns:
            List of events.
        """
        history = self._history
        if event_type:
            history = [h for h in history if h["event"] == event_type]
        return history[-limit:]

    def get_last_focused(self) -> Optional[str]:
        """Get the last focused window ID."""
        return self._last_focused

    def clear_history(self) -> None:
        """Clear event history."""
        self._history.clear()


__all__ = [
    "WindowEvent",
    "WindowTracker",
]

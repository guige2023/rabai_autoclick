"""
Accessibility event watcher for monitoring UI changes.

Watches accessibility events and triggers callbacks
when specific element states change.

Author: AutoClick Team
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable


class EventType(Enum):
    """Accessibility event types."""

    ELEMENT_ADDED = auto()
    ELEMENT_REMOVED = auto()
    ELEMENT_UPDATED = auto()
    WINDOW_OPENED = auto()
    WINDOW_CLOSED = auto()
    WINDOW_MOVED = auto()
    WINDOW_RESIZED = auto()
    FOCUS_CHANGED = auto()
    VALUE_CHANGED = auto()
    MENU_OPENED = auto()
    MENU_CLOSED = auto()


@dataclass
class AccessibilityEvent:
    """
    Represents an accessibility event.

    Attributes:
        event_type: Type of event
        element: Element associated with event
        window_id: Window where event occurred
        timestamp: When event occurred
        metadata: Additional event data
    """

    event_type: EventType
    element: dict[str, Any] | None = None
    window_id: str | None = None
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


class AccessibilityEventWatcher:
    """
    Watches accessibility events and dispatches to handlers.

    Maintains a subscription model for different event types
    with filtering capabilities.

    Example:
        watcher = AccessibilityEventWatcher()

        watcher.subscribe(
            EventType.FOCUS_CHANGED,
            callback=lambda e: print(f"Focus: {e.element.get('title')}")
        )

        watcher.start()
    """

    def __init__(self) -> None:
        """Initialize the event watcher."""
        self._handlers: dict[EventType, list[Callable[[AccessibilityEvent], None]]] = {
            event_type: [] for event_type in EventType
        }
        self._history: list[AccessibilityEvent] = []
        self._max_history = 500
        self._running = False
        self._filters: list[Callable[[AccessibilityEvent], bool]] = []

    def subscribe(
        self,
        event_type: EventType,
        callback: Callable[[AccessibilityEvent], None],
    ) -> None:
        """
        Subscribe to specific event type.

        Args:
            event_type: Type of event to watch
            callback: Function to call when event occurs
        """
        self._handlers[event_type].append(callback)

    def unsubscribe(
        self,
        event_type: EventType,
        callback: Callable[[AccessibilityEvent], None],
    ) -> None:
        """Remove a subscription."""
        if callback in self._handlers[event_type]:
            self._handlers[event_type].remove(callback)

    def add_filter(
        self,
        filter_fn: Callable[[AccessibilityEvent], bool],
    ) -> None:
        """
        Add a global filter for all events.

        Args:
            filter_fn: Returns True to pass event, False to drop
        """
        self._filters.append(filter_fn)

    def emit(self, event: AccessibilityEvent) -> None:
        """
        Emit an event to all subscribers.

        Args:
            event: Event to dispatch
        """
        for filter_fn in self._filters:
            if not filter_fn(event):
                return

        self._history.append(event)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history :]

        for callback in self._handlers.get(event.event_type, []):
            try:
                callback(event)
            except Exception:
                pass

    def get_history(
        self,
        event_type: EventType | None = None,
        since: float | None = None,
    ) -> list[AccessibilityEvent]:
        """
        Get event history with optional filtering.

        Args:
            event_type: Filter by specific event type
            since: Return events after this timestamp

        Returns:
            List of matching events
        """
        events = self._history

        if event_type is not None:
            events = [e for e in events if e.event_type == event_type]

        if since is not None:
            events = [e for e in events if e.timestamp >= since]

        return events

    def get_recent(
        self,
        count: int = 10,
        event_type: EventType | None = None,
    ) -> list[AccessibilityEvent]:
        """Get N most recent events."""
        history = self.get_history(event_type=event_type)
        return history[-count:] if len(history) > count else history

    def start(self) -> None:
        """Start watching (placeholder for actual implementation)."""
        self._running = True

    def stop(self) -> None:
        """Stop watching."""
        self._running = False

    @property
    def is_running(self) -> bool:
        """Check if watcher is active."""
        return self._running

    def clear_history(self) -> None:
        """Clear all event history."""
        self._history.clear()

    def get_stats(self) -> dict[str, Any]:
        """Get event statistics."""
        return {
            "total_events": len(self._history),
            "by_type": {
                event_type.name: sum(
                    1 for e in self._history if e.event_type == event_type
                )
                for event_type in EventType
            },
            "running": self._running,
        }


def create_focus_watcher() -> AccessibilityEventWatcher:
    """Create a watcher pre-configured for focus events."""
    watcher = AccessibilityEventWatcher()
    return watcher


def create_window_watcher() -> AccessibilityEventWatcher:
    """Create a watcher pre-configured for window events."""
    watcher = AccessibilityEventWatcher()
    for event_type in [
        EventType.WINDOW_OPENED,
        EventType.WINDOW_CLOSED,
        EventType.WINDOW_MOVED,
        EventType.WINDOW_RESIZED,
    ]:
        pass
    return watcher

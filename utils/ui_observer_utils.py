"""
UI observer pattern utilities for reactive automation.

This module provides observer/subscriber utilities for monitoring
and responding to UI state changes.
"""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, Any
from enum import Enum, auto


class ObserverEvent(Enum):
    """Types of observer events."""
    ELEMENT_APPEARED = auto()
    ELEMENT_DISAPPEARED = auto()
    ELEMENT_CHANGED = auto()
    ELEMENT_CLICKED = auto()
    WINDOW_OPENED = auto()
    WINDOW_CLOSED = auto()
    WINDOW_RESIZED = auto()
    APPLICATION_LAUNCHED = auto()
    APPLICATION_TERMINATED = auto()


@dataclass
class ObserverNotification:
    """
    Notification sent to observers.

    Attributes:
        event: Type of event that occurred.
        timestamp: When the event occurred.
        source: Source element/app that triggered event.
        data: Additional event-specific data.
    """
    event: ObserverEvent
    timestamp: float = field(default_factory=time.time)
    source: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)


class Observer(Callable[[ObserverNotification], None]):
    """
    Base observer class for UI events.

    Subclass and override on_notify() to handle events.
    """

    def __init__(self, name: str = "") -> None:
        self._name = name
        self._subscriptions: Set[ObserverEvent] = set()

    @property
    def name(self) -> str:
        """Observer name for debugging."""
        return self._name

    def subscribe_to(self, event: ObserverEvent) -> None:
        """Subscribe to a specific event type."""
        self._subscriptions.add(event)

    def unsubscribe_from(self, event: ObserverEvent) -> None:
        """Unsubscribe from a specific event type."""
        self._subscriptions.discard(event)

    def is_subscribed_to(self, event: ObserverEvent) -> bool:
        """Check if subscribed to event type."""
        return event in self._subscriptions

    def __call__(self, notification: ObserverNotification) -> None:
        """Handle notification - delegates to on_notify()."""
        self.on_notify(notification)

    def on_notify(self, notification: ObserverNotification) -> None:
        """Override this method to handle notifications."""
        pass


class CallbackObserver(Observer):
    """
    Observer that wraps a callback function.
    """

    def __init__(
        self,
        callback: Callable[[ObserverNotification], None],
        name: str = "",
    ) -> None:
        super().__init__(name)
        self._callback = callback

    def on_notify(self, notification: ObserverNotification) -> None:
        """Invoke the wrapped callback."""
        self._callback(notification)


class UIObserverManager:
    """
    Manages observers and dispatches UI events.

    Provides thread-safe subscription, notification, and
    observer lifecycle management.
    """

    def __init__(self) -> None:
        self._observers: Dict[ObserverEvent, List[Observer]] = {
            event: [] for event in ObserverEvent
        }
        self._all_observers: List[Observer] = []
        self._lock = threading.RLock()
        self._dispatch_queue: List[ObserverNotification] = []
        self._processing: bool = False

    def add_observer(
        self,
        observer: Observer,
        events: Optional[List[ObserverEvent]] = None,
    ) -> UIObserverManager:
        """
        Add an observer for specific event types.

        If events is None, observer receives all events.
        """
        with self._lock:
            if events:
                for event in events:
                    if observer not in self._observers[event]:
                        self._observers[event].append(observer)
                    observer.subscribe_to(event)
            else:
                if observer not in self._all_observers:
                    self._all_observers.append(observer)

            return self

    def remove_observer(self, observer: Observer) -> bool:
        """Remove an observer from all subscriptions."""
        with self._lock:
            removed = False

            for event in ObserverEvent:
                if observer in self._observers[event]:
                    self._observers[event].remove(observer)
                    removed = True

            if observer in self._all_observers:
                self._all_observers.remove(observer)
                removed = True

            return removed

    def notify(self, notification: ObserverNotification) -> None:
        """
        Notify all relevant observers of an event.

        Thread-safe - notifications are queued.
        """
        with self._lock:
            self._dispatch_queue.append(notification)
            if not self._processing:
                self._process_queue()

    def _process_queue(self) -> None:
        """Process pending notifications."""
        self._processing = True

        while self._dispatch_queue:
            notification = self._dispatch_queue.pop(0)
            self._dispatch(notification)

        self._processing = False

    def _dispatch(self, notification: ObserverNotification) -> None:
        """Dispatch notification to matching observers."""
        event = notification.event

        # Notify event-specific subscribers
        for observer in self._observers.get(event, []):
            try:
                observer(notification)
            except Exception:
                pass

        # Notify global subscribers
        for observer in self._all_observers:
            if not observer.is_subscribed_to(event):
                try:
                    observer(notification)
                except Exception:
                    pass

    def get_observer_count(self, event: Optional[ObserverEvent] = None) -> int:
        """Get count of observers subscribed to event or total."""
        with self._lock:
            if event:
                return len(self._observers.get(event, []))
            return len(self._all_observers)


@dataclass
class ConditionalObserver(Observer):
    """
    Observer that filters notifications based on conditions.

    Only calls on_notify if all conditions return True.
    """

    conditions: List[Callable[[ObserverNotification], bool]] = field(default_factory=list)

    def add_condition(
        self,
        condition: Callable[[ObserverNotification], bool],
    ) -> ConditionalObserver:
        """Add a filter condition."""
        self.conditions.append(condition)
        return self

    def on_notify(self, notification: ObserverNotification) -> None:
        """Only notify if all conditions pass."""
        if all(c(notification) for c in self.conditions):
            self._notify_internal(notification)

    def _notify_internal(self, notification: ObserverNotification) -> None:
        """Override this in subclass to handle filtered notifications."""
        pass


class ThrottledObserver(Observer):
    """
    Observer that throttles notification delivery.

    Ensures minimum interval between notifications.
    """

    def __init__(self, min_interval: float = 0.1, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._min_interval = min_interval
        self._last_notification: Optional[ObserverNotification] = None
        self._last_notify_time: float = 0.0
        self._pending: bool = False

    def on_notify(self, notification: ObserverNotification) -> None:
        """Throttle notification delivery."""
        now = time.time()
        elapsed = now - self._last_notify_time

        if elapsed >= self._min_interval:
            self._deliver(notification)
            self._last_notify_time = now
        else:
            self._last_notification = notification
            if not self._pending:
                self._pending = True
                threading.Timer(
                    self._min_interval - elapsed,
                    self._deliver_pending
                ).start()

    def _deliver_pending(self) -> None:
        """Deliver pending notification after throttle period."""
        if self._last_notification:
            self._deliver(self._last_notification)
            self._last_notify_time = time.time()
        self._pending = False

    def _deliver(self, notification: ObserverNotification) -> None:
        """Override this in subclass to handle delivered notifications."""
        pass


class EventLoggerObserver(Observer):
    """
    Observer that logs all notifications.

    Useful for debugging and monitoring.
    """

    def __init__(self, name: str = "EventLogger") -> None:
        super().__init__(name)
        self._logs: List[ObserverNotification] = []

    @property
    def logs(self) -> List[ObserverNotification]:
        """Get all logged notifications."""
        return list(self._logs)

    def on_notify(self, notification: ObserverNotification) -> None:
        """Log the notification."""
        self._logs.append(notification)

    def get_logs_since(self, timestamp: float) -> List[ObserverNotification]:
        """Get logs after a specific time."""
        return [n for n in self._logs if n.timestamp >= timestamp]

    def get_logs_by_event(self, event: ObserverEvent) -> List[ObserverNotification]:
        """Get logs for a specific event type."""
        return [n for n in self._logs if n.event == event]

"""UI event bus for decoupled event handling in automation.

Provides a publish-subscribe event bus for UI automation events,
enabling loose coupling between components.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional
import time


class EventPriority(Enum):
    """Event listener priority (higher = called first)."""
    LOW = 1
    NORMAL = 5
    HIGH = 10


@dataclass
class EventSubscription:
    """A subscription to an event type."""
    event_type: str
    callback: Callable[[dict[str, Any]], None]
    priority: EventPriority = EventPriority.NORMAL
    subscriber_id: str = ""
    is_active: bool = True


@dataclass
class AutomationEvent:
    """An event in the automation system.

    Attributes:
        event_type: The type/kind of event.
        source: Source of the event (component name, etc.).
        data: Event payload data.
        timestamp: Event timestamp.
        event_id: Unique event identifier.
    """
    event_type: str
    source: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))


class UIEventBus:
    """Central event bus for automation events.

    Supports:
    - Subscribe to specific event types
    - Priority-based event handling
    - Wildcard subscriptions
    - Event filtering
    """

    def __init__(self) -> None:
        """Initialize empty event bus."""
        self._subscriptions: dict[str, list[EventSubscription]] = {}
        self._wildcard_subscriptions: list[EventSubscription] = []
        self._event_history: list[AutomationEvent] = []
        self._max_history: int = 100

    def subscribe(
        self,
        event_type: str,
        callback: Callable[[dict[str, Any]], None],
        priority: EventPriority = EventPriority.NORMAL,
        subscriber_id: str = "",
    ) -> EventSubscription:
        """Subscribe to an event type.

        Returns the subscription object.
        """
        subscription = EventSubscription(
            event_type=event_type,
            callback=callback,
            priority=priority,
            subscriber_id=subscriber_id or str(uuid.uuid4()),
        )

        if event_type == "*":
            self._wildcard_subscriptions.append(subscription)
            self._wildcard_subscriptions.sort(
                key=lambda s: s.priority.value, reverse=True
            )
        else:
            self._subscriptions.setdefault(event_type, []).append(subscription)
            self._subscriptions[event_type].sort(
                key=lambda s: s.priority.value, reverse=True
            )

        return subscription

    def unsubscribe(self, subscriber_id: str) -> int:
        """Remove all subscriptions for a subscriber.

        Returns number of subscriptions removed.
        """
        removed = 0

        for event_type in list(self._subscriptions.keys()):
            subs = self._subscriptions[event_type]
            new_subs = [s for s in subs if s.subscriber_id != subscriber_id]
            removed += len(subs) - len(new_subs)
            if new_subs:
                self._subscriptions[event_type] = new_subs
            else:
                del self._subscriptions[event_type]

        old_wildcard = self._wildcard_subscriptions
        self._wildcard_subscriptions = [
            s for s in old_wildcard if s.subscriber_id != subscriber_id
        ]
        removed += len(old_wildcard) - len(self._wildcard_subscriptions)

        return removed

    def publish(
        self,
        event_type: str,
        data: Optional[dict[str, Any]] = None,
        source: str = "",
    ) -> int:
        """Publish an event to all subscribers.

        Returns the number of subscribers that received the event.
        """
        event = AutomationEvent(
            event_type=event_type,
            source=source,
            data=data or {},
        )

        self._add_to_history(event)

        delivered = 0

        for sub in self._wildcard_subscriptions:
            if sub.is_active:
                try:
                    sub.callback(event.data)
                    delivered += 1
                except Exception:
                    pass

        for sub in self._subscriptions.get(event_type, []):
            if sub.is_active:
                try:
                    sub.callback(event.data)
                    delivered += 1
                except Exception:
                    pass

        return delivered

    def once(
        self,
        event_type: str,
        callback: Callable[[dict[str, Any]], None],
    ) -> EventSubscription:
        """Subscribe to an event for a single delivery."""
        wrapper_subscription: dict[str, EventSubscription] = {}

        def wrapper(data: dict[str, Any]) -> None:
            callback(data)
            if wrapper_subscription:
                wrapper_subscription["sub"].is_active = False
                self.unsubscribe(wrapper_subscription["sub"].subscriber_id)

        sub = self.subscribe(
            event_type,
            wrapper,
            subscriber_id=str(uuid.uuid4()),
        )
        wrapper_subscription["sub"] = sub
        return sub

    def clear(self) -> None:
        """Clear all subscriptions."""
        self._subscriptions.clear()
        self._wildcard_subscriptions.clear()

    def get_history(
        self,
        event_type: Optional[str] = None,
        limit: int = 0,
    ) -> list[AutomationEvent]:
        """Get event history, optionally filtered."""
        events = self._event_history
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        if limit > 0:
            events = events[-limit:]
        return list(events)

    def _add_to_history(self, event: AutomationEvent) -> None:
        """Add event to history with size limit."""
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history.pop(0)

    @property
    def subscription_count(self) -> int:
        """Return total number of subscriptions."""
        return (
            sum(len(subs) for subs in self._subscriptions.values())
            + len(self._wildcard_subscriptions)
        )


# Common event types
class UIEvents:
    """Common UI automation event types."""
    ELEMENT_APPEARED = "element.appeared"
    ELEMENT_DISAPPEARED = "element.disappeared"
    ELEMENT_CLICKED = "element.clicked"
    ELEMENT_CHANGED = "element.changed"
    WINDOW_OPENED = "window.opened"
    WINDOW_CLOSED = "window.closed"
    WINDOW_ACTIVATED = "window.activated"
    DIALOG_APPEARED = "dialog.appeared"
    DIALOG_DISMISSED = "dialog.dismissed"
    FOCUS_CHANGED = "focus.changed"
    ERROR = "automation.error"
    STEP_STARTED = "step.started"
    STEP_COMPLETED = "step.completed"
    STEP_FAILED = "step.failed"


# Global singleton
_event_bus = UIEventBus()


def get_event_bus() -> UIEventBus:
    """Return the global event bus."""
    return _event_bus

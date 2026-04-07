"""
Event Bus Utilities

Provides a centralized event system for pub/sub messaging
with filtering, priorities, and async support.
"""

from __future__ import annotations

import asyncio
import copy
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class EventHandler(ABC):
    """Abstract event handler interface."""

    @abstractmethod
    def handle(self, event: Event) -> None:
        """Handle an event."""
        pass


@dataclass
class Event:
    """Base event class."""
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    type: str = ""
    data: Any = None
    timestamp: float = field(default_factory=time.time)
    source: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"Event({self.type}, {self.timestamp:.2f})"


@dataclass
class Subscription:
    """Event subscription details."""
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    event_type: str = ""
    handler: Callable[[Event], Any] | None = None
    filter_func: Callable[[Event], bool] | None = None
    priority: int = 0
    once: bool = False
    active: bool = True

    def matches(self, event: Event) -> bool:
        """Check if subscription matches an event."""
        if not self.active:
            return False
        if self.event_type and self.event_type != event.type:
            return False
        if self.filter_func and not self.filter_func(event):
            return False
        return True


class EventBus:
    """
    Central event bus for publish/subscribe messaging.
    """

    def __init__(self):
        self._subscriptions: dict[str, list[Subscription]] = {}  # event_type -> subscriptions
        self._all_subscriptions: list[Subscription] = []  # Subscriptions for all events
        self._lock = threading.RLock()
        self._event_history: list[Event] = []
        self._max_history = 1000
        self._metrics: dict[str, int] = {}

    def subscribe(
        self,
        event_type: str,
        handler: Callable[[Event], Any],
        *,
        priority: int = 0,
        filter_func: Callable[[Event], bool] | None = None,
        once: bool = False,
    ) -> str:
        """
        Subscribe to events.

        Args:
            event_type: Type of event to subscribe to. Empty string for all events.
            handler: Function to call when event is published.
            priority: Higher priority handlers are called first.
            filter_func: Optional filter function.
            once: If True, unsubscribe after first event.

        Returns:
            Subscription ID.
        """
        subscription = Subscription(
            event_type=event_type,
            handler=handler,
            priority=priority,
            filter_func=filter_func,
            once=once,
        )

        with self._lock:
            if event_type:
                if event_type not in self._subscriptions:
                    self._subscriptions[event_type] = []
                self._subscriptions[event_type].append(subscription)
            else:
                self._all_subscriptions.append(subscription)

            # Sort by priority
            self._sort_subscriptions(event_type)

        return subscription.id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe by ID."""
        with self._lock:
            # Check event-specific subscriptions
            for event_type, subs in self._subscriptions.items():
                for sub in subs:
                    if sub.id == subscription_id:
                        sub.active = False
                        return True

            # Check all-event subscriptions
            for sub in self._all_subscriptions:
                if sub.id == subscription_id:
                    sub.active = False
                    return True

        return False

    def publish(self, event: Event) -> list[Any]:
        """
        Publish an event to all matching subscribers.

        Args:
            event: The event to publish.

        Returns:
            List of results from handlers.
        """
        results = []

        with self._lock:
            # Store in history
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history.pop(0)

            # Update metrics
            self._metrics[event.type] = self._metrics.get(event.type, 0) + 1

            # Get matching subscriptions
            matching = []

            if event.type in self._subscriptions:
                matching.extend(self._subscriptions[event.type])

            matching.extend(self._all_subscriptions)

            # Sort by priority
            matching.sort(key=lambda s: s.priority, reverse=True)

        # Execute handlers outside lock
        for sub in matching:
            if sub.matches(event):
                try:
                    if sub.handler:
                        result = sub.handler(event)
                        results.append(result)
                except Exception:
                    pass  # Swallow handler exceptions

                if sub.once:
                    self.unsubscribe(sub.id)

        return results

    def emit(
        self,
        event_type: str,
        data: Any = None,
        source: str = "",
        **metadata: Any,
    ) -> list[Any]:
        """
        Emit a new event.

        Args:
            event_type: Type of event.
            data: Event data.
            source: Event source.
            **metadata: Additional metadata.

        Returns:
            List of handler results.
        """
        event = Event(
            type=event_type,
            data=data,
            source=source,
            metadata=metadata,
        )
        return self.publish(event)

    def _sort_subscriptions(self, event_type: str) -> None:
        """Sort subscriptions by priority."""
        if event_type in self._subscriptions:
            self._subscriptions[event_type].sort(key=lambda s: s.priority, reverse=True)

    def get_history(
        self,
        event_type: str | None = None,
        limit: int = 100,
    ) -> list[Event]:
        """Get event history."""
        with self._lock:
            if event_type:
                events = [e for e in self._event_history if e.type == event_type]
            else:
                events = list(self._event_history)

        return events[-limit:]

    def get_metrics(self) -> dict[str, int]:
        """Get event metrics."""
        return copy.copy(self._metrics)

    def get_subscription_count(self) -> int:
        """Get total subscription count."""
        with self._lock:
            return sum(len(subs) for subs in self._subscriptions.values()) + len(self._all_subscriptions)


class AsyncEventBus(EventBus):
    """
    Async-compatible event bus.
    """

    async def publish_async(self, event: Event) -> list[Any]:
        """Publish an event asynchronously."""
        results = []

        with self._lock:
            matching = []

            if event.type in self._subscriptions:
                matching.extend(self._subscriptions[event.type])

            matching.extend(self._all_subscriptions)
            matching.sort(key=lambda s: s.priority, reverse=True)

        for sub in matching:
            if sub.matches(event):
                try:
                    if sub.handler:
                        if asyncio.iscoroutinefunction(sub.handler):
                            result = await sub.handler(event)
                        else:
                            result = sub.handler(event)
                        results.append(result)
                except Exception:
                    pass

                if sub.once:
                    self.unsubscribe(sub.id)

        return results

    async def emit_async(
        self,
        event_type: str,
        data: Any = None,
        source: str = "",
        **metadata: Any,
    ) -> list[Any]:
        """Emit an event asynchronously."""
        event = Event(
            type=event_type,
            data=data,
            source=source,
            metadata=metadata,
        )
        return await self.publish_async(event)


class EventBusBuilder:
    """Builder for creating configured event buses."""

    def __init__(self):
        self._handlers: list[tuple[str, Callable]] = []

    def on(self, event_type: str, handler: Callable[[Event], Any]) -> EventBusBuilder:
        """Add an event handler."""
        self._handlers.append((event_type, handler))
        return self

    def build(self) -> EventBus:
        """Build and return the event bus."""
        bus = EventBus()
        for event_type, handler in self._handlers:
            bus.subscribe(event_type, handler)
        return bus


# Global default event bus
_default_bus: EventBus | None = None


def get_event_bus() -> EventBus:
    """Get the default global event bus."""
    global _default_bus
    if _default_bus is None:
        _default_bus = EventBus()
    return _default_bus


def on(event_type: str, handler: Callable[[Event], Any]) -> str:
    """Subscribe to the default event bus."""
    return get_event_bus().subscribe(event_type, handler)


def off(subscription_id: str) -> bool:
    """Unsubscribe from the default event bus."""
    return get_event_bus().unsubscribe(subscription_id)


def emit(event_type: str, data: Any = None, **metadata: Any) -> list[Any]:
    """Emit an event on the default event bus."""
    return get_event_bus().emit(event_type, data, **metadata)

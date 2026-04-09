"""Event bus for publish-subscribe messaging.

This module provides an event bus for decoupled communication:
- Topic-based subscription
- Event filtering
- Async and sync handlers
- Dead letter queue for failed events

Example:
    >>> from actions.event_bus_action import EventBus
    >>> bus = EventBus()
    >>> bus.subscribe("user.created", handler_func)
    >>> bus.publish("user.created", {"user_id": 123})
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import json

logger = logging.getLogger(__name__)


class EventPriority(Enum):
    """Event handler priority."""
    LOW = 0
    NORMAL = 1
    HIGH = 2


@dataclass
class Event:
    """An event object."""
    topic: str
    data: Any
    timestamp: float = field(default_factory=time.time)
    headers: dict[str, str] = field(default_factory=dict)
    event_id: str = field(default_factory=lambda: str(time.time()))


@dataclass
class Subscription:
    """An event subscription."""
    topic: str
    handler: Callable[[Event], Any]
    priority: EventPriority = EventPriority.NORMAL
    filter_func: Optional[Callable[[Event], bool]] = None
    async_handler: bool = False


@dataclass
class DeadLetterEvent:
    """Event that failed processing."""
    event: Event
    error: str
    attempts: int = 0
    last_attempt: float = field(default_factory=time.time)


class EventBus:
    """Publish-subscribe event bus.

    Attributes:
        name: Bus name for logging.
    """

    def __init__(
        self,
        name: str = "event-bus",
        max_dead_letter_size: int = 1000,
    ) -> None:
        self.name = name
        self.max_dead_letter_size = max_dead_letter_size
        self._subscriptions: dict[str, list[Subscription]] = defaultdict(list)
        self._dead_letters: deque[DeadLetterEvent] = deque(maxlen=max_dead_letter_size)
        self._lock = threading.RLock()
        self._wildcard_subscriptions: list[Subscription] = []
        logger.info(f"EventBus '{name}' initialized")

    def subscribe(
        self,
        topic: str,
        handler: Callable[[Event], Any],
        priority: EventPriority = EventPriority.NORMAL,
        filter_func: Optional[Callable[[Event], bool]] = None,
    ) -> None:
        """Subscribe to an event topic.

        Args:
            topic: Topic pattern (supports * wildcard).
            handler: Handler function.
            priority: Handler priority.
            filter_func: Optional event filter.
        """
        subscription = Subscription(
            topic=topic,
            handler=handler,
            priority=priority,
            filter_func=filter_func,
        )
        with self._lock:
            if "*" in topic:
                self._wildcard_subscriptions.append(subscription)
            else:
                self._subscriptions[topic].append(subscription)
            self._subscriptions[topic] = sorted(
                self._subscriptions[topic],
                key=lambda s: s.priority.value,
                reverse=True,
            )
        logger.debug(f"Subscribed to topic: {topic}")

    def unsubscribe(self, topic: str, handler: Callable[[Event], Any]) -> bool:
        """Unsubscribe a handler from a topic.

        Args:
            topic: Topic to unsubscribe from.
            handler: Handler to remove.

        Returns:
            True if handler was found and removed.
        """
        with self._lock:
            if topic in self._subscriptions:
                for sub in self._subscriptions[topic]:
                    if sub.handler == handler:
                        self._subscriptions[topic].remove(sub)
                        logger.debug(f"Unsubscribed from: {topic}")
                        return True
        return False

    def publish(self, topic: str, data: Any, headers: Optional[dict[str, str]] = None) -> list[Any]:
        """Publish an event to a topic.

        Args:
            topic: Topic to publish to.
            data: Event data.
            headers: Optional event headers.

        Returns:
            List of handler results.
        """
        event = Event(
            topic=topic,
            data=data,
            headers=headers or {},
        )
        return self._dispatch_event(event)

    def _dispatch_event(self, event: Event) -> list[Any]:
        """Dispatch an event to all matching subscriptions."""
        results = []
        with self._lock:
            subs = list(self._subscriptions.get(event.topic, []))
            for sub in self._wildcard_subscriptions:
                if self._topic_matches(event.topic, sub.topic):
                    subs.append(sub)
        for sub in subs:
            if sub.filter_func and not sub.filter_func(event):
                continue
            try:
                result = sub.handler(event)
                results.append(result)
            except Exception as e:
                logger.error(f"Event handler error for {event.topic}: {e}")
                self._dead_letters.append(DeadLetterEvent(
                    event=event,
                    error=str(e),
                ))
        return results

    def _topic_matches(self, topic: str, pattern: str) -> bool:
        """Check if topic matches a pattern."""
        import fnmatch
        return fnmatch.fnmatch(topic, pattern)

    def get_dead_letters(self, limit: int = 100) -> list[DeadLetterEvent]:
        """Get dead letter events.

        Args:
            limit: Maximum number to return.

        Returns:
            List of dead letter events.
        """
        return list(self._dead_letters)[-limit:]

    def retry_dead_letter(self, index: int = -1) -> bool:
        """Retry a dead letter event.

        Args:
            index: Index in dead letter queue (default: last).

        Returns:
            True if retried successfully.
        """
        if not self._dead_letters:
            return False
        dle = self._dead_letters[index]
        dle.attempts += 1
        dle.last_attempt = time.time()
        try:
            self._dispatch_event(dle.event)
            self._dead_letters.remove(dle)
            return True
        except Exception as e:
            dle.error = str(e)
            return False

    def clear_dead_letters(self) -> int:
        """Clear all dead letter events.

        Returns:
            Number of dead letters cleared.
        """
        count = len(self._dead_letters)
        self._dead_letters.clear()
        return count

    def get_stats(self) -> dict[str, Any]:
        """Get event bus statistics."""
        with self._lock:
            return {
                "name": self.name,
                "topics": len(self._subscriptions),
                "subscriptions": sum(len(s) for s in self._subscriptions.values()),
                "dead_letters": len(self._dead_letters),
            }


class EventBusBuilder:
    """Builder for creating configured event buses."""

    def __init__(self, name: str = "event-bus") -> None:
        self._bus = EventBus(name=name)

    def with_subscription(
        self,
        topic: str,
        handler: Callable[[Event], Any],
        priority: EventPriority = EventPriority.NORMAL,
    ) -> EventBusBuilder:
        """Add a subscription."""
        self._bus.subscribe(topic, handler, priority)
        return self

    def with_dead_letter_queue(self, max_size: int) -> EventBusBuilder:
        """Configure dead letter queue."""
        self._bus.max_dead_letter_size = max_size
        return self

    def build(self) -> EventBus:
        """Build the event bus."""
        return self._bus

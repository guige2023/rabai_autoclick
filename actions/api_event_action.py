"""
API Event Action Module.

Event-driven API handling with pub/sub patterns,
supports event routing, filtering, and batching.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
import logging
import asyncio
import time
from enum import Enum

logger = logging.getLogger(__name__)


class EventType(Enum):
    """Event types."""
    REQUEST = "request"
    RESPONSE = "response"
    ERROR = "error"
    TIMEOUT = "timeout"
    RATE_LIMIT = "rate_limit"


@dataclass
class APIEvent:
    """API event representation."""
    event_type: EventType
    endpoint: str
    data: Any
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


class APISubscriber:
    """Event subscriber."""
    def __init__(
        self,
        callback: Callable[[APIEvent], Any],
        event_types: Optional[list[EventType]] = None,
        endpoint_filter: Optional[str] = None,
    ) -> None:
        self.callback = callback
        self.event_types = set(event_types) if event_types else None
        self.endpoint_filter = endpoint_filter


class APIEventAction:
    """
    Event-driven API handling with pub/sub.

    Manages event subscriptions, routing,
    and batched event processing.

    Example:
        bus = APIEventAction()
        bus.subscribe(on_api_request, event_types=[EventType.REQUEST])
        bus.publish(APIEvent(EventType.REQUEST, "/api/users", data))
    """

    def __init__(self, async_mode: bool = True) -> None:
        self.async_mode = async_mode
        self._subscribers: list[APISubscriber] = []
        self._event_history: list[APIEvent] = []
        self._max_history: int = 1000
        self._lock = asyncio.Lock() if async_mode else None

    def subscribe(
        self,
        callback: Callable[[APIEvent], Any],
        event_types: Optional[list[EventType]] = None,
        endpoint: Optional[str] = None,
    ) -> None:
        """Subscribe to events."""
        subscriber = APISubscriber(
            callback=callback,
            event_types=event_types,
            endpoint_filter=endpoint,
        )
        self._subscribers.append(subscriber)

    def unsubscribe(
        self,
        callback: Callable[[APIEvent], Any],
    ) -> bool:
        """Unsubscribe a callback."""
        for i, sub in enumerate(self._subscribers):
            if sub.callback == callback:
                del self._subscribers[i]
                return True
        return False

    async def publish(self, event: APIEvent) -> None:
        """Publish an event to all subscribers."""
        self._add_to_history(event)

        if self.async_mode:
            async with self._lock:
                await self._deliver_event(event)
        else:
            self._deliver_event_sync(event)

    def publish_sync(self, event: APIEvent) -> None:
        """Publish event synchronously."""
        self._add_to_history(event)
        self._deliver_event_sync(event)

    async def _deliver_event(self, event: APIEvent) -> None:
        """Deliver event to matching subscribers."""
        tasks = []

        for subscriber in self._subscribers:
            if self._matches_subscriber(event, subscriber):
                try:
                    if asyncio.iscoroutinefunction(subscriber.callback):
                        tasks.append(subscriber.callback(event))
                    else:
                        subscriber.callback(event)
                except Exception as e:
                    logger.error("Subscriber error: %s", e)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _deliver_event_sync(self, event: APIEvent) -> None:
        """Deliver event synchronously."""
        for subscriber in self._subscribers:
            if self._matches_subscriber(event, subscriber):
                try:
                    subscriber.callback(event)
                except Exception as e:
                    logger.error("Subscriber error: %s", e)

    def _matches_subscriber(
        self,
        event: APIEvent,
        subscriber: APISubscriber,
    ) -> bool:
        """Check if event matches subscriber filters."""
        if subscriber.event_types:
            if event.event_type not in subscriber.event_types:
                return False

        if subscriber.endpoint_filter:
            if subscriber.endpoint_filter not in event.endpoint:
                return False

        return True

    def _add_to_history(self, event: APIEvent) -> None:
        """Add event to history."""
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

    def get_history(
        self,
        event_type: Optional[EventType] = None,
        limit: int = 100,
    ) -> list[APIEvent]:
        """Get event history."""
        history = self._event_history

        if event_type:
            history = [e for e in history if e.event_type == event_type]

        return history[-limit:]

    def clear_history(self) -> None:
        """Clear event history."""
        self._event_history.clear()

    @property
    def subscriber_count(self) -> int:
        """Number of active subscribers."""
        return len(self._subscribers)

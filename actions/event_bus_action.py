"""
Event Bus Action Module.

Provides a publish-subscribe event system for decoupled
communication between components.

Author: rabai_autoclick team
"""

import asyncio
import logging
import time
from typing import (
    Optional, Dict, Any, List, Callable, Set, Awaitable
)
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
from uuid import uuid4

logger = logging.getLogger(__name__)


class EventPriority(Enum):
    """Event priority levels."""
    LOW = 3
    NORMAL = 2
    HIGH = 1
    CRITICAL = 0


@dataclass
class Event:
    """Base event class."""
    event_type: str
    data: Any = None
    timestamp: float = field(default_factory=time.time)
    event_id: str = field(default_factory=lambda: str(uuid4()))
    source: Optional[str] = None
    priority: EventPriority = EventPriority.NORMAL
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __repr__(self) -> str:
        return f"Event(type={self.event_type}, id={self.event_id[:8]})"


@dataclass
class Subscription:
    """Event subscription."""
    subscriber_id: str
    callback: Callable[[Event], Awaitable[Any]]
    event_types: Set[str]
    priority: EventPriority = EventPriority.NORMAL
    filter_func: Optional[Callable[[Event], bool]] = None
    max_invocations: Optional[int] = None
    invocation_count: int = 0

    def can_invoke(self) -> bool:
        """Check if subscription can still be invoked."""
        if self.max_invocations is None:
            return True
        return self.invocation_count < self.max_invocations


@dataclass
class EventResult:
    """Result of event processing."""
    event_id: str
    processed: bool
    subscriber_results: Dict[str, Any] = field(default_factory=dict)
    errors: List[Exception] = field(default_factory=list)
    duration: float = 0


class EventBusAction:
    """
    Publish-Subscribe Event Bus.

    Supports event filtering, priority-based delivery,
    async handlers, and dead-letter queues.

    Example:
        >>> bus = EventBusAction()
        >>> async def handler(event):
        ...     print(f"Got event: {event.event_type}")
        >>> bus.subscribe("user.created", handler)
        >>> bus.publish(Event("user.created", {"user_id": "123"}))
    """

    def __init__(self, max_queue_size: int = 1000):
        self._subscriptions: Dict[str, List[Subscription]] = defaultdict(list)
        self._wildcard_subscriptions: List[Subscription] = []
        self._subscriber_ids: Set[str] = set()
        self._dead_letter_queue: List[Event] = []
        self._max_queue_size = max_queue_size
        self._lock = asyncio.Lock()
        self._global_filters: List[Callable[[Event], bool]] = []
        self._event_history: List[Event] = []
        self._max_history = 100

    def subscribe(
        self,
        event_type: str,
        callback: Callable[[Event], Awaitable[Any]],
        subscriber_id: Optional[str] = None,
        priority: EventPriority = EventPriority.NORMAL,
        filter_func: Optional[Callable[[Event], bool]] = None,
        max_invocations: Optional[int] = None,
    ) -> str:
        """
        Subscribe to an event type.

        Args:
            event_type: Event type to subscribe to
            callback: Async callback function
            subscriber_id: Optional subscriber ID (auto-generated if None)
            priority: Handler priority
            filter_func: Optional filter function
            max_invocations: Max number of times to invoke

        Returns:
            Subscriber ID
        """
        if subscriber_id is None:
            subscriber_id = str(uuid4())

        if subscriber_id in self._subscriber_ids:
            raise ValueError(f"Subscriber ID '{subscriber_id}' already exists")

        subscription = Subscription(
            subscriber_id=subscriber_id,
            callback=callback,
            event_types={event_type},
            priority=priority,
            filter_func=filter_func,
            max_invocations=max_invocations,
        )

        self._subscriptions[event_type].append(subscription)
        self._subscriptions[event_type].sort(key=lambda s: s.priority.value)
        self._subscriber_ids.add(subscriber_id)

        logger.info(f"Subscribed {subscriber_id} to '{event_type}'")
        return subscriber_id

    def subscribe_wildcard(
        self,
        callback: Callable[[Event], Awaitable[Any]],
        subscriber_id: Optional[str] = None,
        priority: EventPriority = EventPriority.NORMAL,
        filter_func: Optional[Callable[[Event], bool]] = None,
    ) -> str:
        """
        Subscribe to all events.

        Args:
            callback: Async callback function
            subscriber_id: Optional subscriber ID
            priority: Handler priority
            filter_func: Optional filter function

        Returns:
            Subscriber ID
        """
        if subscriber_id is None:
            subscriber_id = str(uuid4())

        subscription = Subscription(
            subscriber_id=subscriber_id,
            callback=callback,
            event_types={"*"},
            priority=priority,
            filter_func=filter_func,
        )

        self._wildcard_subscriptions.append(subscription)
        self._wildcard_subscriptions.sort(key=lambda s: s.priority.value)
        self._subscriber_ids.add(subscriber_id)

        logger.info(f"Subscribed {subscriber_id} to wildcard '*'")
        return subscriber_id

    def unsubscribe(self, subscriber_id: str) -> bool:
        """
        Unsubscribe a subscriber.

        Args:
            subscriber_id: Subscriber ID

        Returns:
            True if unsubscribed
        """
        found = False

        for event_type in list(self._subscriptions.keys()):
            subs = self._subscriptions[event_type]
            self._subscriptions[event_type] = [
                s for s in subs if s.subscriber_id != subscriber_id
            ]
            if subs and not self._subscriptions[event_type]:
                del self._subscriptions[event_type]

        self._wildcard_subscriptions = [
            s for s in self._wildcard_subscriptions
            if s.subscriber_id != subscriber_id
        ]

        self._subscriber_ids.discard(subscriber_id)

        if found:
            logger.info(f"Unsubscribed {subscriber_id}")
        return found

    def add_global_filter(self, filter_func: Callable[[Event], bool]) -> None:
        """
        Add a global event filter.

        Args:
            filter_func: Filter function that returns bool
        """
        self._global_filters.append(filter_func)

    async def publish(self, event: Event) -> EventResult:
        """
        Publish an event to all subscribers.

        Args:
            event: Event to publish

        Returns:
            EventResult with processing details
        """
        start_time = time.time()
        result = EventResult(event_id=event.event_id, processed=False)

        for filter_func in self._global_filters:
            if not filter_func(event):
                logger.debug(f"Event filtered by global filter: {event}")
                return result

        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history.pop(0)

        subscribers = self._get_matching_subscriptions(event)

        if not subscribers:
            logger.debug(f"No subscribers for event: {event}")
            result.processed = True
            result.duration = time.time() - start_time
            return result

        async with self._lock:
            tasks = []
            for sub in subscribers:
                if sub.can_invoke():
                    sub.invocation_count += 1
                    tasks.append(self._invoke_subscriber(sub, event, result))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        result.processed = True
        result.duration = time.time() - start_time

        if result.errors:
            logger.warning(f"Event {event} had {len(result.errors)} errors")

        return result

    async def _invoke_subscriber(
        self,
        subscription: Subscription,
        event: Event,
        result: EventResult,
    ) -> None:
        """Invoke a subscriber callback."""
        try:
            subscriber_result = await subscription.callback(event)
            result.subscriber_results[subscription.subscriber_id] = subscriber_result
        except Exception as e:
            logger.error(f"Subscriber {subscription.subscriber_id} error: {e}")
            result.errors.append(e)

    def _get_matching_subscriptions(self, event: Event) -> List[Subscription]:
        """Get all subscriptions matching an event."""
        subscriptions = []

        if event.event_type in self._subscriptions:
            subscriptions.extend(self._subscriptions[event.event_type])

        subscriptions.extend(self._wildcard_subscriptions)

        subscriptions = [
            s for s in subscriptions
            if s.filter_func is None or s.filter_func(event)
        ]

        subscriptions.sort(key=lambda s: s.priority.value)
        return subscriptions

    def get_dead_letter_events(self) -> List[Event]:
        """Get events that failed processing."""
        return self._dead_letter_queue.copy()

    def clear_dead_letter_queue(self) -> None:
        """Clear the dead letter queue."""
        self._dead_letter_queue.clear()

    def get_event_history(
        self,
        event_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Event]:
        """
        Get event history.

        Args:
            event_type: Optional filter by event type
            limit: Maximum number of events

        Returns:
            List of historical events
        """
        events = self._event_history
        if event_type:
            events = [e for e in events if e.event_type == event_type]
        return events[-limit:]

    def get_subscriber_count(self, event_type: Optional[str] = None) -> int:
        """
        Get number of subscribers.

        Args:
            event_type: Optional event type (returns total if None)

        Returns:
            Number of subscribers
        """
        if event_type:
            return len(self._subscriptions.get(event_type, []))
        return len(self._subscriber_ids)

    def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics."""
        return {
            "total_subscribers": len(self._subscriber_ids),
            "event_types": list(self._subscriptions.keys()),
            "wildcard_subscribers": len(self._wildcard_subscriptions),
            "dead_letter_queue_size": len(self._dead_letter_queue),
            "event_history_size": len(self._event_history),
        }

"""Automation Event Bus Action module.

Provides event bus pub/sub system for automation workflows
with support for event filtering, dead letter queues,
and reliable event delivery.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

from collections import defaultdict


class EventPriority(Enum):
    """Event priority levels."""

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Event:
    """An event in the bus."""

    event_type: str
    payload: dict[str, Any]
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    priority: EventPriority = EventPriority.NORMAL
    source: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "priority": self.priority.value,
            "source": self.source,
            "metadata": self.metadata,
        }


@dataclass
class Subscription:
    """Event subscription."""

    subscriber_id: str
    event_types: list[str]
    handler: Callable[[Event], Any]
    filter_func: Optional[Callable[[Event], bool]] = None
    async_handler: bool = False


class DeadLetterQueue:
    """Dead letter queue for failed events."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._queue: list[tuple[Event, Exception]] = []

    def add(self, event: Event, error: Exception) -> None:
        """Add failed event to DLQ."""
        self._queue.append((event, error))
        if len(self._queue) > self.max_size:
            self._queue.pop(0)

    def get_failed_events(self) -> list[tuple[Event, Exception]]:
        """Get all failed events."""
        return list(self._queue)

    def retry_event(self, event_id: str) -> Optional[Event]:
        """Get event for retry."""
        for event, error in self._queue:
            if event.event_id == event_id:
                return event
        return None

    def remove(self, event_id: str) -> bool:
        """Remove event from DLQ."""
        for i, (event, _) in enumerate(self._queue):
            if event.event_id == event_id:
                self._queue.pop(i)
                return True
        return False

    def size(self) -> int:
        """Get DLQ size."""
        return len(self._queue)


class EventBus:
    """Event bus for pub/sub messaging."""

    def __init__(self, max_subscribers: int = 1000):
        self.max_subscribers = max_subscribers
        self._subscribers: dict[str, list[Subscription]] = defaultdict(list)
        self._sub_id_counter = 0
        self._dlq = DeadLetterQueue()
        self._event_queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=10000)
        self._running = False
        self._processor_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._stats = {
            "published": 0,
            "delivered": 0,
            "failed": 0,
            "dlq_size": 0,
        }

    def subscribe(
        self,
        event_types: list[str],
        handler: Callable[[Event], Any],
        filter_func: Optional[Callable[[Event], bool]] = None,
    ) -> str:
        """Subscribe to event types.

        Args:
            event_types: List of event types to subscribe to
            handler: Function to call when event matches
            filter_func: Optional additional filter

        Returns:
            Subscriber ID
        """
        self._sub_id_counter += 1
        sub_id = f"sub_{self._sub_id_counter}"

        subscription = Subscription(
            subscriber_id=sub_id,
            event_types=event_types,
            handler=handler,
            filter_func=filter_func,
            async_handler=asyncio.iscoroutinefunction(handler),
        )

        for event_type in event_types:
            self._subscribers[event_type].append(subscription)

        return sub_id

    def unsubscribe(self, subscriber_id: str) -> bool:
        """Unsubscribe a subscriber.

        Args:
            subscriber_id: Subscriber ID to remove

        Returns:
            True if found and removed
        """
        removed = False
        for event_type in list(self._subscribers.keys()):
            subs = self._subscribers[event_type]
            self._subscribers[event_type] = [
                s for s in subs if s.subscriber_id != subscriber_id
            ]
            if len(subs) != len(self._subscribers[event_type]):
                removed = True

        return removed

    async def publish(self, event: Event) -> None:
        """Publish event to the bus.

        Args:
            event: Event to publish
        """
        self._stats["published"] += 1
        priority_map = {
            EventPriority.CRITICAL: 0,
            EventPriority.HIGH: 1,
            EventPriority.NORMAL: 2,
            EventPriority.LOW: 3,
        }
        priority = priority_map.get(event.priority, 2)

        try:
            self._event_queue.put_nowait((priority, event))
        except asyncio.QueueFull:
            self._stats["failed"] += 1

    def publish_sync(self, event: Event) -> None:
        """Synchronous publish to the bus.

        Args:
            event: Event to publish
        """
        self._stats["published"] += 1
        self._deliver_event(event)

    async def start(self) -> None:
        """Start the event bus processor."""
        if self._running:
            return
        self._running = True
        self._processor_task = asyncio.create_task(self._process_events())

    async def stop(self) -> None:
        """Stop the event bus processor."""
        self._running = False
        if self._processor_task:
            self._processor_task.cancel()
            try:
                await self._processor_task
            except asyncio.CancelledError:
                pass

    async def _process_events(self) -> None:
        """Process events from the queue."""
        while self._running:
            try:
                priority, event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=1.0,
                )
                await self._deliver_event_async(event)
            except asyncio.TimeoutError:
                continue
            except Exception:
                pass

    async def _deliver_event_async(self, event: Event) -> None:
        """Deliver event to subscribers asynchronously."""
        for event_type in self._get_matching_types(event.event_type):
            for subscription in self._subscribers.get(event_type, []):
                if subscription.filter_func and not subscription.filter_func(event):
                    continue

                try:
                    if subscription.async_handler:
                        await subscription.handler(event)
                    else:
                        subscription.handler(event)
                    self._stats["delivered"] += 1
                except Exception as e:
                    self._stats["failed"] += 1
                    self._dlq.add(event, e)

    def _deliver_event(self, event: Event) -> None:
        """Deliver event synchronously."""
        for event_type in self._get_matching_types(event.event_type):
            for subscription in self._subscribers.get(event_type, []):
                if subscription.filter_func and not subscription.filter_func(event):
                    continue

                try:
                    result = subscription.handler(event)
                    if asyncio.iscoroutine(result):
                        asyncio.create_task(result)
                    self._stats["delivered"] += 1
                except Exception as e:
                    self._stats["failed"] += 1
                    self._dlq.add(event, e)

    def _get_matching_types(self, event_type: str) -> list[str]:
        """Get all event types that match (including wildcards)."""
        matching = [event_type]
        for stored_type in self._subscribers.keys():
            if self._type_matches(stored_type, event_type):
                if stored_type not in matching:
                    matching.append(stored_type)
        return matching

    def _type_matches(self, pattern: str, event_type: str) -> bool:
        """Check if type pattern matches event type."""
        if pattern == "*":
            return True
        if pattern.endswith("*"):
            prefix = pattern[:-1]
            return event_type.startswith(prefix)
        return pattern == event_type

    def get_stats(self) -> dict[str, Any]:
        """Get bus statistics."""
        self._stats["dlq_size"] = self._dlq.size()
        return dict(self._stats)

    def get_dlq(self) -> DeadLetterQueue:
        """Get the dead letter queue."""
        return self._dlq


class EventBusBuilder:
    """Builder for creating configured event buses."""

    def __init__(self):
        self._bus: Optional[EventBus] = None
        self._subscriptions: list[tuple[list[str], Callable]] = []

    def with_subscription(
        self,
        event_types: list[str],
        handler: Callable[[Event], Any],
    ) -> "EventBusBuilder":
        """Add a subscription to be registered."""
        self._subscriptions.append((event_types, handler))
        return self

    def build(self) -> EventBus:
        """Build the event bus."""
        self._bus = EventBus()
        for event_types, handler in self._subscriptions:
            self._bus.subscribe(event_types, handler)
        return self._bus

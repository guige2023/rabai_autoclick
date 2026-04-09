"""
Event Bus Action Module

Provides event bus and pub/sub messaging for UI automation workflows.
Supports topic-based routing, dead letter queues, and event filtering.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class EventPriority(Enum):
    """Event priority levels."""
    LOW = auto()
    NORMAL = auto()
    HIGH = auto()
    CRITICAL = auto()


class EventDeliveryMode(Enum):
    """Event delivery mode."""
    AT_MOST_ONCE = auto()
    AT_LEAST_ONCE = auto()
    EXACTLY_ONCE = auto()


@dataclass
class Event:
    """Represents an event."""
    event_type: str
    payload: Any = None
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=lambda: time.time())
    source: str = ""
    priority: EventPriority = EventPriority.NORMAL
    headers: dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None

    def __post_init__(self) -> None:
        if isinstance(self.timestamp, datetime):
            self.timestamp = self.timestamp.timestamp()


@dataclass
class Subscription:
    """Event subscription."""
    id: str
    event_type: str
    handler: Callable[[Event], Any]
    filter_fn: Optional[Callable[[Event], bool]] = None
    priority: EventPriority = EventPriority.NORMAL
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EventMetrics:
    """Event bus metrics."""
    total_events: int = 0
    delivered_events: int = 0
    failed_events: int = 0
    pending_events: int = 0
    subscribers: int = 0
    topics: int = 0


class DeadLetterQueue:
    """Dead letter queue for failed events."""

    def __init__(self, max_size: int = 1000) -> None:
        self.max_size = max_size
        self._queue: list[Event] = []
        self._lock = asyncio.Lock()

    async def add(self, event: Event, error: str) -> None:
        """Add failed event to DLQ."""
        async with self._lock:
            if len(self._queue) >= self.max_size:
                self._queue.pop(0)
            event.metadata["dlq_error"] = error
            event.metadata["dlq_timestamp"] = time.time()
            self._queue.append(event)

    async def get_all(self) -> list[Event]:
        """Get all dead letter events."""
        async with self._lock:
            return list(self._queue)

    async def size(self) -> int:
        """Get DLQ size."""
        async with self._lock:
            return len(self._queue)

    async def clear(self) -> None:
        """Clear DLQ."""
        async with self._lock:
            self._queue.clear()


class EventBus:
    """
    Event bus for pub/sub messaging.

    Example:
        >>> bus = EventBus()
        >>> await bus.subscribe("order.created", handler_func)
        >>> await bus.publish(Event("order.created", payload={"order_id": "123"}))
    """

    def __init__(
        self,
        max_queue_size: int = 10000,
        enable_dlq: bool = True,
        max_dlq_size: int = 1000,
    ) -> None:
        self.max_queue_size = max_queue_size
        self.enable_dlq = enable_dlq
        self._subscribers: dict[str, list[Subscription]] = defaultdict(list)
        self._event_queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=max_queue_size)
        self._dlq = DeadLetterQueue(max_size=max_dlq_size)
        self._running = False
        self._dispatcher_task: Optional[asyncio.Task] = None
        self._metrics = EventMetrics()
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Start event bus."""
        self._running = True
        self._dispatcher_task = asyncio.create_task(self._dispatch_loop())
        logger.info("Event bus started")

    async def stop(self) -> None:
        """Stop event bus."""
        self._running = False
        if self._dispatcher_task:
            self._dispatcher_task.cancel()
            try:
                await self._dispatcher_task
            except asyncio.CancelledError:
                pass
        logger.info("Event bus stopped")

    async def subscribe(
        self,
        event_type: str,
        handler: Callable[[Event], Any],
        filter_fn: Optional[Callable[[Event], bool]] = None,
        priority: EventPriority = EventPriority.NORMAL,
    ) -> str:
        """Subscribe to event type."""
        subscription_id = str(uuid.uuid4())
        subscription = Subscription(
            id=subscription_id,
            event_type=event_type,
            handler=handler,
            filter_fn=filter_fn,
            priority=priority,
        )

        async with self._lock:
            self._subscribers[event_type].append(subscription)
            self._subscribers[event_type].sort(
                key=lambda s: s.priority.value, reverse=True
            )
            self._metrics.subscribers = sum(
                len(subs) for subs in self._subscribers.values()
            )
            self._metrics.topics = len(self._subscribers)

        logger.debug(f"Subscribed to {event_type}: {subscription_id}")
        return subscription_id

    async def unsubscribe(self, event_type: str, subscription_id: str) -> bool:
        """Unsubscribe from event type."""
        async with self._lock:
            if event_type in self._subscribers:
                subs = self._subscribers[event_type]
                for i, s in enumerate(subs):
                    if s.id == subscription_id:
                        subs.pop(i)
                        return True
        return False

    async def publish(self, event: Event) -> None:
        """Publish event to bus."""
        self._metrics.total_events += 1
        self._metrics.pending_events += 1

        try:
            await asyncio.wait_for(
                self._event_queue.put(event),
                timeout=1.0,
            )
        except asyncio.queues.QueueFull:
            logger.warning(f"Event queue full, dropping event: {event.event_id}")
            self._metrics.failed_events += 1
            self._metrics.pending_events -= 1

    async def publish_sync(self, event: Event) -> list[Any]:
        """Publish event synchronously and wait for handlers."""
        self._metrics.total_events += 1
        results = await self._dispatch_event(event)
        self._metrics.delivered_events += len(results)
        return results

    async def _dispatch_loop(self) -> None:
        """Event dispatch loop."""
        while self._running:
            try:
                event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=1.0,
                )
                self._metrics.pending_events -= 1

                results = await self._dispatch_event(event)
                self._metrics.delivered_events += len(results)

            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Dispatch error: {e}")
                self._metrics.failed_events += 1

    async def _dispatch_event(self, event: Event) -> list[Any]:
        """Dispatch event to subscribers."""
        results = []
        event_type = event.event_type

        if event_type not in self._subscribers:
            return results

        subscribers = list(self._subscribers[event_type])

        for subscription in subscribers:
            if subscription.filter_fn and not subscription.filter_fn(event):
                continue

            try:
                result = subscription.handler(event)
                if asyncio.iscoroutine(result):
                    result = await result
                results.append(result)
            except Exception as e:
                logger.error(f"Handler error for {event_type}: {e}")
                self._metrics.failed_events += 1

                if self.enable_dlq:
                    await self._dlq.add(event, str(e))

        return results

    async def request_reply(
        self,
        event: Event,
        timeout: float = 5.0,
    ) -> Optional[Any]:
        """Send request and wait for reply."""
        reply_event = asyncio.Event()
        reply_value: dict = {}

        async def reply_handler(reply: Event) -> None:
            reply_value["event"] = reply
            reply_event.set()

        correlation_id = str(uuid.uuid4())
        event.correlation_id = correlation_id

        sub_id = await self.subscribe(
            f"{event.event_type}.reply",
            reply_handler,
        )

        await self.publish(event)

        try:
            await asyncio.wait_for(reply_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"Request reply timeout: {event.event_id}")
            return None
        finally:
            await self.unsubscribe(f"{event.event_type}.reply", sub_id)

        return reply_value.get("event")

    def get_metrics(self) -> EventMetrics:
        """Get event bus metrics."""
        return self._metrics

    def get_dlq_size(self) -> int:
        """Get dead letter queue size."""
        return self._dlq._queue.__len__()

    async def get_dlq_events(self) -> list[Event]:
        """Get dead letter queue events."""
        return await self._dlq.get_all()

    async def clear_dlq(self) -> None:
        """Clear dead letter queue."""
        await self._dlq.clear()

    def __repr__(self) -> str:
        return f"EventBus(topics={len(self._subscribers)}, subscribers={self._metrics.subscribers})"


class EventBusBuilder:
    """Builder for creating configured event bus instances."""

    @staticmethod
    def create_standard() -> EventBus:
        """Create standard event bus."""
        return EventBus(
            max_queue_size=10000,
            enable_dlq=True,
            max_dlq_size=1000,
        )

    @staticmethod
    def create_high_throughput() -> EventBus:
        """Create high throughput event bus."""
        return EventBus(
            max_queue_size=50000,
            enable_dlq=True,
            max_dlq_size=5000,
        )

    @staticmethod
    def create_low_latency() -> EventBus:
        """Create low latency event bus."""
        return EventBus(
            max_queue_size=1000,
            enable_dlq=False,
        )

"""
Automation Event Bus Action Module.

In-memory event bus for decoupling automation components with
topic subscriptions, wildcard matching, and event persistence.
"""

import asyncio
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

from uuid import uuid4


class EventPriority(Enum):
    """Event delivery priority."""

    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class Event:
    """An event in the bus."""

    event_id: str
    topic: str
    data: Any
    timestamp: float
    priority: EventPriority = EventPriority.NORMAL
    source: str = ""
    correlation_id: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Generate event_id if not provided."""
        if not self.event_id:
            self.event_id = str(uuid4())


@dataclass
class Subscription:
    """An event subscription."""

    subscription_id: str
    topic_pattern: str
    handler: Callable[[Event], Any]
    priority: EventPriority = EventPriority.NORMAL
    filter_func: Optional[Callable[[Event], bool]] = None
    async_handler: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)

    def matches(self, topic: str) -> bool:
        """Check if this subscription matches a topic."""
        if self.topic_pattern == topic:
            return True
        if self.topic_pattern == "*":
            return True
        if self.topic_pattern.endswith(".*"):
            prefix = self.topic_pattern[:-2]
            return topic.startswith(prefix + ".")
        if self.topic_pattern.endswith(".**"):
            prefix = self.topic_pattern[:-3]
            return topic.startswith(prefix + ".")
        pattern = self.topic_pattern.replace(".", r"\.").replace("**", ".*")
        pattern = pattern.replace("*", r"[^.]+")
        import re
        return bool(re.match(f"^{pattern}$", topic))


@dataclass
class EventBusStats:
    """Statistics for event bus."""

    events_published: int = 0
    events_delivered: int = 0
    events_dropped: int = 0
    subscriptions_created: int = 0
    subscriptions_removed: int = 0
    total_latency: float = 0.0

    @property
    def avg_latency(self) -> float:
        """Average event delivery latency."""
        if self.events_delivered == 0:
            return 0.0
        return self.total_latency / self.events_delivered

    def to_dict(self) -> dict[str, Any]:
        """Export as dictionary."""
        return {
            "events_published": self.events_published,
            "events_delivered": self.events_delivered,
            "events_dropped": self.events_dropped,
            "subscriptions_created": self.subscriptions_created,
            "subscriptions_removed": self.subscriptions_removed,
            "avg_latency_ms": round(self.avg_latency * 1000, 3),
        }


class EventBus:
    """
    In-memory event bus for publish-subscribe communication.

    Supports wildcard topics, priority ordering, async handlers,
    and optional event persistence.
    """

    def __init__(
        self,
        max_queue_per_subscriber: int = 1000,
        enable_persistence: bool = False,
        persistence_path: Optional[str] = None,
        drop_on_overflow: bool = True,
    ) -> None:
        """
        Initialize the event bus.

        Args:
            max_queue_per_subscriber: Max events queued per subscriber.
            enable_persistence: Enable event persistence.
            persistence_path: Path for event log.
            drop_on_overflow: Drop events when queue is full.
        """
        self._max_queue = max_queue_per_subscriber
        self._enable_persistence = enable_persistence
        self._persistence_path = persistence_path
        self._drop_on_overflow = drop_on_overflow
        self._subscriptions: dict[str, list[Subscription]] = defaultdict(list)
        self._subscriber_queues: dict[str, asyncio.Queue] = {}
        self._stats = EventBusStats()
        self._lock = asyncio.Lock()
        self._running = False
        self._dispatcher_task: Optional[asyncio.Task] = None

    def subscribe(
        self,
        topic_pattern: str,
        handler: Callable[[Event], Any],
        priority: EventPriority = EventPriority.NORMAL,
        filter_func: Optional[Callable[[Event], bool]] = None,
    ) -> str:
        """
        Subscribe to an event topic.

        Args:
            topic_pattern: Topic pattern (supports * and ** wildcards).
            handler: Function to call when event matches.
            priority: Handler priority (higher = first).
            filter_func: Optional filter to further refine matching.

        Returns:
            Subscription ID for later unsubscription.
        """
        subscription_id = str(uuid4())
        is_async = asyncio.iscoroutinefunction(handler)

        sub = Subscription(
            subscription_id=subscription_id,
            topic_pattern=topic_pattern,
            handler=handler,
            priority=priority,
            filter_func=filter_func,
            async_handler=is_async,
        )

        self._subscriptions[topic_pattern].append(sub)
        self._subscriptions[topic_pattern].sort(key=lambda s: s.priority.value, reverse=True)
        self._stats.subscriptions_created += 1
        return subscription_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from events.

        Args:
            subscription_id: ID returned from subscribe.

        Returns:
            True if subscription was found and removed.
        """
        for pattern, subs in self._subscriptions.items():
            for i, sub in enumerate(subs):
                if sub.subscription_id == subscription_id:
                    del subs[i]
                    self._stats.subscriptions_removed += 1
                    return True
        return False

    async def publish(
        self,
        topic: str,
        data: Any,
        priority: EventPriority = EventPriority.NORMAL,
        source: str = "",
        correlation_id: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> int:
        """
        Publish an event to the bus.

        Args:
            topic: Event topic name.
            data: Event payload.
            priority: Event priority.
            source: Source identifier.
            correlation_id: Optional correlation ID.
            metadata: Optional event metadata.

        Returns:
            Number of subscribers that received the event.
        """
        event = Event(
            event_id=str(uuid4()),
            topic=topic,
            data=data,
            timestamp=time.time(),
            priority=priority,
            source=source,
            correlation_id=correlation_id,
            metadata=metadata or {},
        )

        self._stats.events_published += 1

        if self._enable_persistence:
            await self._persist_event(event)

        matching_subs = self._get_matching_subscriptions(topic)
        if not matching_subs:
            return 0

        delivered = 0
        for sub in matching_subs:
            if sub.filter_func and not sub.filter_func(event):
                continue

            try:
                if sub.async_handler:
                    asyncio.create_task(self._safe_invoke_async(sub, event))
                else:
                    asyncio.create_task(self._safe_invoke(sub, event))
                delivered += 1
            except RuntimeError:
                pass

        return delivered

    async def _safe_invoke(self, sub: Subscription, event: Event) -> None:
        """Safely invoke a sync handler."""
        try:
            sub.handler(event)
            self._stats.events_delivered += 1
        except Exception:
            self._stats.events_dropped += 1

    async def _safe_invoke_async(self, sub: Subscription, event: Event) -> None:
        """Safely invoke an async handler."""
        try:
            await sub.handler(event)
            self._stats.events_delivered += 1
        except Exception:
            self._stats.events_dropped += 1

    def _get_matching_subscriptions(self, topic: str) -> list[Subscription]:
        """Get all subscriptions matching a topic."""
        matching: list[Subscription] = []
        for pattern, subs in self._subscriptions.items():
            for sub in subs:
                if sub.matches(topic):
                    matching.append(sub)
        matching.sort(key=lambda s: s.priority.value, reverse=True)
        return matching

    async def _persist_event(self, event: Event) -> None:
        """Persist an event to disk."""
        if not self._persistence_path:
            return
        try:
            with open(self._persistence_path, "a") as f:
                f.write(json.dumps({
                    "event_id": event.event_id,
                    "topic": event.topic,
                    "data": str(event.data),
                    "timestamp": event.timestamp,
                    "priority": event.priority.value,
                    "source": event.source,
                }) + "\n")
        except Exception:
            pass

    async def request_reply(
        self,
        topic: str,
        request_data: Any,
        reply_topic: str,
        timeout: float = 5.0,
        priority: EventPriority = EventPriority.NORMAL,
    ) -> Any:
        """
        Send a request and wait for a reply.

        Args:
            topic: Request topic.
            request_data: Request payload.
            reply_topic: Topic to listen for reply.
            timeout: Seconds to wait for reply.
            priority: Request priority.

        Returns:
            Reply data.

        Raises:
            asyncio.TimeoutError: If no reply received within timeout.
        """
        correlation_id = str(uuid4())
        reply_received = asyncio.Event()
        reply_data: dict[str, Any] = {}

        def reply_handler(event: Event) -> None:
            if event.correlation_id == correlation_id:
                reply_data["data"] = event.data
                reply_received.set()

        sub_id = self.subscribe(reply_topic, reply_handler)
        await self.publish(
            topic=topic,
            data=request_data,
            priority=priority,
            correlation_id=correlation_id,
        )

        try:
            await asyncio.wait_for(reply_received.wait(), timeout=timeout)
            return reply_data.get("data")
        except asyncio.TimeoutError:
            raise
        finally:
            self.unsubscribe(sub_id)

    def stats(self) -> EventBusStats:
        """Return current event bus statistics."""
        return self._stats

    def get_subscription_count(self) -> int:
        """Return total number of subscriptions."""
        return sum(len(subs) for subs in self._subscriptions.values())


def create_event_bus() -> EventBus:
    """
    Factory function to create an event bus.

    Returns:
        Configured EventBus instance.
    """
    return EventBus()

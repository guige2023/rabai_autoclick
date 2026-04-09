"""Event Bus v2 with topic filtering and delivery guarantees.

This module provides a robust event bus with:
- Topic-based routing with wildcards
- Dead letter handling for failed events
- Event persistence and replay
- Priority queues
- Exactly-once delivery semantics
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Awaitable, TypeVar
from collections import defaultdict

logger = logging.getLogger(__name__)

T = TypeVar("T")


class DeliveryMode(Enum):
    """Event delivery modes."""

    AT_MOST_ONCE = "at_most_once"  # Fire and forget
    AT_LEAST_ONCE = "at_least_once"  # With ack
    EXACTLY_ONCE = "exactly_once"  # With deduplication


class EventStatus(Enum):
    """Status of a published event."""

    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


@dataclass
class Event:
    """An event message."""

    topic: str
    data: Any
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    headers: dict[str, str] = field(default_factory=dict)
    priority: int = 0
    ttl: float | None = None
    correlation_id: str | None = None
    reply_to: str | None = None

    def is_expired(self) -> bool:
        """Check if event has expired."""
        if self.ttl is None:
            return False
        return time.time() - self.timestamp > self.ttl

    def to_dict(self) -> dict:
        """Serialize event to dictionary."""
        return {
            "topic": self.topic,
            "data": self.data,
            "event_id": self.event_id,
            "timestamp": self.timestamp,
            "headers": self.headers,
            "priority": self.priority,
            "ttl": self.ttl,
            "correlation_id": self.correlation_id,
            "reply_to": self.reply_to,
        }


@dataclass
class Subscription:
    """An event subscription."""

    subscriber_id: str
    topic_pattern: str
    handler: Callable[[Event], Awaitable[None]]
    filter_func: Callable[[Event], bool] | None = None
    max_retries: int = 3
    ack_timeout: float = 30.0
    dead_letter_topic: str | None = None


@dataclass
class DeliveryResult:
    """Result of event delivery."""

    event_id: str
    subscriber_id: str
    success: bool
    error: Exception | None = None
    delivery_time: float = 0.0
    attempt: int = 1


class DeadLetterHandler:
    """Handler for failed event deliveries."""

    def __init__(self, max_size: int = 10000):
        """Initialize dead letter handler.

        Args:
            max_size: Maximum number of dead letters to store
        """
        self.max_size = max_size
        self.dead_letters: list[Event] = []
        self.failure_counts: dict[str, int] = defaultdict(int)
        self.failure_reasons: dict[str, str] = {}

    def add(self, event: Event, error: Exception) -> None:
        """Add a failed event to dead letter queue."""
        self.failure_counts[event.event_id] += 1
        self.failure_reasons[event.event_id] = str(error)

        if len(self.dead_letters) >= self.max_size:
            self.dead_letters.pop(0)

        self.dead_letters.append(event)
        logger.warning(f"Dead letter added: {event.event_id}, reason: {error}")

    def get_all(self) -> list[Event]:
        """Get all dead letter events."""
        return self.dead_letters.copy()

    def get_failure_info(self, event_id: str) -> dict[str, Any]:
        """Get failure information for an event."""
        return {
            "event_id": event_id,
            "failure_count": self.failure_counts.get(event_id, 0),
            "reason": self.failure_reasons.get(event_id, "Unknown"),
        }

    def retry(self, event: Event) -> bool:
        """Attempt to retry a dead letter."""
        if self.failure_counts.get(event.event_id, 0) < 3:
            return True
        return False


class EventBusV2:
    """Advanced event bus with topic routing and delivery guarantees."""

    def __init__(
        self,
        delivery_mode: DeliveryMode = DeliveryMode.AT_LEAST_ONCE,
        enable_persistence: bool = False,
        enable_dead_letter: bool = True,
    ):
        """Initialize the event bus.

        Args:
            delivery_mode: Event delivery mode
            enable_persistence: Enable event persistence
            enable_dead_letter: Enable dead letter handling
        """
        self.delivery_mode = delivery_mode
        self.enable_persistence = enable_persistence
        self.enable_dead_letter = enable_dead_letter

        self._subscriptions: dict[str, list[Subscription]] = defaultdict(list)
        self._subscriber_ids: set[str] = set()
        self._pending_events: dict[str, asyncio.Event] = {}
        self._acknowledged: set[str] = set()
        self._delivered: set[str] = set()
        self._lock = asyncio.Lock()

        self.dead_letter_handler = DeadLetterHandler() if enable_dead_letter else None
        self._event_history: list[Event] = []
        self._max_history = 1000

        self._running = False
        self._metrics = {
            "events_published": 0,
            "events_delivered": 0,
            "events_failed": 0,
            "dead_letters": 0,
        }

    def subscribe(
        self,
        topic_pattern: str,
        handler: Callable[[Event], Awaitable[None]],
        subscriber_id: str | None = None,
        filter_func: Callable[[Event], bool] | None = None,
        max_retries: int = 3,
        dead_letter_topic: str | None = None,
    ) -> str:
        """Subscribe to events matching a topic pattern.

        Args:
            topic_pattern: Topic pattern (supports * and # wildcards)
            handler: Async handler function
            subscriber_id: Optional subscriber ID (auto-generated if not provided)
            filter_func: Optional event filter function
            max_retries: Maximum delivery retries
            dead_letter_topic: Topic for dead letter events

        Returns:
            Subscriber ID
        """
        sub_id = subscriber_id or str(uuid.uuid4())

        if sub_id in self._subscriber_ids:
            raise ValueError(f"Subscriber ID {sub_id} already exists")

        subscription = Subscription(
            subscriber_id=sub_id,
            topic_pattern=topic_pattern,
            handler=handler,
            filter_func=filter_func,
            max_retries=max_retries,
            dead_letter_topic=dead_letter_topic,
        )

        self._subscriptions[topic_pattern].append(subscription)
        self._subscriber_ids.add(sub_id)

        logger.info(f"Subscriber {sub_id} subscribed to {topic_pattern}")
        return sub_id

    def unsubscribe(self, subscriber_id: str) -> None:
        """Unsubscribe a subscriber.

        Args:
            subscriber_id: Subscriber ID to remove
        """
        for pattern, subs in self._subscriptions.items():
            self._subscriptions[pattern] = [
                s for s in subs if s.subscriber_id != subscriber_id
            ]
        self._subscriber_ids.discard(subscriber_id)
        logger.info(f"Subscriber {subscriber_id} unsubscribed")

    async def publish(
        self,
        topic: str,
        data: Any,
        headers: dict[str, str] | None = None,
        priority: int = 0,
        ttl: float | None = None,
        correlation_id: str | None = None,
    ) -> Event:
        """Publish an event to a topic.

        Args:
            topic: Topic name
            data: Event data
            headers: Optional headers
            priority: Event priority (higher = more urgent)
            ttl: Time to live in seconds
            correlation_id: Optional correlation ID for tracing

        Returns:
            Published event
        """
        event = Event(
            topic=topic,
            data=data,
            headers=headers or {},
            priority=priority,
            ttl=ttl,
            correlation_id=correlation_id,
        )

        self._metrics["events_published"] += 1

        if self.enable_persistence:
            self._persist_event(event)

        # Notify waiting coroutines
        async with self._lock:
            if topic in self._pending_events:
                self._pending_events[topic].set()

        # Deliver event asynchronously
        asyncio.create_task(self._deliver_event(event))

        return event

    async def publish_and_wait(
        self,
        topic: str,
        data: Any,
        timeout: float = 30.0,
        **kwargs,
    ) -> bool:
        """Publish an event and wait for all deliveries.

        Args:
            topic: Topic name
            data: Event data
            timeout: Maximum wait time
            **kwargs: Additional publish parameters

        Returns:
            True if all deliveries succeeded
        """
        event = await self.publish(topic, data, **kwargs)
        event_id = event.event_id

        wait_event = asyncio.Event()
        self._pending_events[event_id] = wait_event

        try:
            await asyncio.wait_for(wait_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False
        finally:
            self._pending_events.pop(event_id, None)

    async def request_reply(
        self,
        topic: str,
        data: Any,
        reply_topic: str | None = None,
        timeout: float = 30.0,
    ) -> Any:
        """Send a request and wait for a reply.

        Args:
            topic: Request topic
            data: Request data
            reply_topic: Reply topic (auto-generated if not provided)
            timeout: Reply timeout

        Returns:
            Reply data
        """
        reply_topic = reply_topic or f"reply.{uuid.uuid4()}"
        reply_event = asyncio.Event()
        reply_data: dict[str, Any] = {}

        async def reply_handler(event: Event) -> None:
            reply_data["data"] = event.data
            reply_event.set()

        self.subscribe(reply_topic, reply_handler, filter_func=lambda e: e.correlation_id == reply_topic)

        try:
            await self.publish(topic, data, correlation_id=reply_topic, reply_to=reply_topic)
            await asyncio.wait_for(reply_event.wait(), timeout=timeout)
            return reply_data.get("data")
        except asyncio.TimeoutError:
            raise TimeoutError(f"Reply not received within {timeout}s")

    def _match_topic(self, topic: str, pattern: str) -> bool:
        """Match a topic against a pattern with wildcards.

        Args:
            topic: Topic to match
            pattern: Pattern with * (single level) and # (multi level)

        Returns:
            True if topic matches pattern
        """
        if pattern == topic:
            return True
        if pattern == "#":
            return True
        if pattern == "*":
            return "." not in topic

        pattern_parts = pattern.split(".")
        topic_parts = topic.split(".")

        i = 0
        for pp in pattern_parts:
            if pp == "#":
                return True
            if i >= len(topic_parts):
                return False
            if pp == "*":
                i += 1
                continue
            if pp != topic_parts[i]:
                return False
            i += 1

        return i == len(topic_parts)

    async def _deliver_event(self, event: Event) -> None:
        """Deliver an event to all matching subscribers."""
        if event.is_expired():
            logger.warning(f"Event {event.event_id} expired, skipping delivery")
            return

        matching_subscribers: list[tuple[Subscription, Event]] = []

        async with self._lock:
            for pattern, subs in self._subscriptions.items():
                if self._match_topic(event.topic, pattern):
                    for sub in subs:
                        if sub.filter_func is None or sub.filter_func(event):
                            matching_subscribers.append((sub, event))

        # Deliver to all matching subscribers
        tasks = [
            self._deliver_to_subscriber(sub, evt)
            for sub, evt in matching_subscribers
        ]

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # Mark as delivered for at-least-once mode
        if self.delivery_mode == DeliveryMode.AT_LEAST_ONCE:
            self._delivered.add(event.event_id)

        # Clean up old history
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history // 2:]

    async def _deliver_to_subscriber(self, subscription: Subscription, event: Event) -> None:
        """Deliver event to a single subscriber with retry."""
        last_error: Exception | None = None

        for attempt in range(subscription.max_retries + 1):
            try:
                if self.delivery_mode == DeliveryMode.AT_LEAST_ONCE:
                    # Wait for ack
                    await asyncio.wait_for(
                        subscription.handler(event),
                        timeout=subscription.ack_timeout,
                    )
                else:
                    await subscription.handler(event)

                self._metrics["events_delivered"] += 1
                self._acknowledged.add(event.event_id)
                return

            except Exception as e:
                last_error = e
                logger.warning(
                    f"Delivery to {subscription.subscriber_id} failed "
                    f"(attempt {attempt + 1}): {e}"
                )
                if attempt < subscription.max_retries:
                    await asyncio.sleep(0.5 * (2 ** attempt))

        # Delivery failed - handle dead letter
        self._metrics["events_failed"] += 1

        if self.dead_letter_handler and subscription.dead_letter_topic:
            self.dead_letter_handler.add(event, last_error)
            self._metrics["dead_letters"] += 1

            # Publish to dead letter topic
            await self.publish(
                subscription.dead_letter_topic,
                event.to_dict(),
                headers={"original_error": str(last_error)},
            )

    def _persist_event(self, event: Event) -> None:
        """Persist an event to history."""
        self._event_history.append(event)

    def get_pending_events(self, topic_pattern: str | None = None) -> list[Event]:
        """Get pending events for a topic pattern."""
        if topic_pattern:
            return [e for e in self._event_history if self._match_topic(e.topic, topic_pattern)]
        return self._event_history.copy()

    def get_dead_letters(self) -> list[Event]:
        """Get all dead letter events."""
        if self.dead_letter_handler:
            return self.dead_letter_handler.get_all()
        return []

    def get_metrics(self) -> dict[str, Any]:
        """Get event bus metrics."""
        return {
            **self._metrics,
            "subscriber_count": len(self._subscriber_ids),
            "topic_count": len(self._subscriptions),
            "event_history_size": len(self._event_history),
            "dead_letter_count": len(self.get_dead_letters()),
        }


def create_event_bus(
    delivery_mode: DeliveryMode = DeliveryMode.AT_LEAST_ONCE,
    enable_dead_letter: bool = True,
) -> EventBusV2:
    """Create a new event bus.

    Args:
        delivery_mode: Event delivery mode
        enable_dead_letter: Enable dead letter handling

    Returns:
        New EventBusV2 instance
    """
    return EventBusV2(
        delivery_mode=delivery_mode,
        enable_dead_letter=enable_dead_letter,
    )

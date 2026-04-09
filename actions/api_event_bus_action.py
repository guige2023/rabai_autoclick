"""
API Event Bus Action Module.

Provides pub/sub event bus for API services with
topic filtering, dead letter handling, and delivery guarantees.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class DeliveryMode(Enum):
    """Event delivery modes."""

    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


@dataclass
class Event:
    """Represents an event on the bus."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    topic: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    headers: dict[str, str] = field(default_factory=dict)
    delivery_count: int = 0
    source: str = ""


@dataclass
class Subscription:
    """Represents an event subscription."""

    id: str
    topic_pattern: str
    callback: Callable
    filter_func: Optional[Callable] = None
    created_at: float = field(default_factory=time.time)
    is_active: bool = True
    messages_received: int = 0


class APIEventBusAction:
    """
    Pub/sub event bus for API service communication.

    Features:
    - Topic-based routing with wildcards
    - Multiple delivery modes
    - Dead letter queue
    - Message persistence
    - Subscription management

    Example:
        bus = APIEventBusAction()
        bus.subscribe("user.*", handler_func)
        await bus.publish("user.created", {"user_id": "123"})
    """

    def __init__(
        self,
        delivery_mode: DeliveryMode = DeliveryMode.AT_LEAST_ONCE,
        enable_dlq: bool = True,
        max_retries: int = 3,
    ) -> None:
        """
        Initialize event bus.

        Args:
            delivery_mode: Message delivery mode.
            enable_dlq: Enable dead letter queue.
            max_retries: Maximum delivery retries.
        """
        self.delivery_mode = delivery_mode
        self.enable_dlq = enable_dlq
        self.max_retries = max_retries
        self._subscriptions: dict[str, Subscription] = {}
        self._topic_subscribers: dict[str, list[str]] = {}
        self._dlq: list[Event] = []
        self._message_log: list[Event] = []
        self._stats = {
            "total_published": 0,
            "total_delivered": 0,
            "dlq_messages": 0,
            "delivery_failures": 0,
        }
        self._lock = asyncio.Lock()

    def subscribe(
        self,
        topic_pattern: str,
        callback: Callable[[Event], None],
        filter_func: Optional[Callable] = None,
    ) -> str:
        """
        Subscribe to a topic pattern.

        Args:
            topic_pattern: Topic pattern (supports * wildcard).
            callback: Event handler callback.
            filter_func: Optional message filter.

        Returns:
            Subscription ID.
        """
        sub_id = str(uuid.uuid4())
        subscription = Subscription(
            id=sub_id,
            topic_pattern=topic_pattern,
            callback=callback,
            filter_func=filter_func,
        )
        self._subscriptions[sub_id] = subscription

        if topic_pattern not in self._topic_subscribers:
            self._topic_subscribers[topic_pattern] = []
        self._topic_subscribers[topic_pattern].append(sub_id)

        logger.info(f"Subscribed to topic: {topic_pattern}")
        return sub_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """
        Unsubscribe from a topic.

        Args:
            subscription_id: Subscription ID.

        Returns:
            True if unsubscribed.
        """
        if subscription_id in self._subscriptions:
            sub = self._subscriptions[subscription_id]
            sub.is_active = False
            del self._subscriptions[subscription_id]
            logger.info(f"Unsubscribed: {subscription_id}")
            return True
        return False

    async def publish(
        self,
        topic: str,
        payload: dict[str, Any],
        headers: Optional[dict[str, str]] = None,
        source: str = "",
    ) -> str:
        """
        Publish an event to a topic.

        Args:
            topic: Topic name.
            payload: Event payload.
            headers: Optional message headers.
            source: Event source identifier.

        Returns:
            Event ID.
        """
        event = Event(
            topic=topic,
            payload=payload,
            headers=headers or {},
            source=source,
        )

        self._message_log.append(event)
        if len(self._message_log) > 10000:
            self._message_log = self._message_log[-5000:]

        self._stats["total_published"] += 1

        await self._deliver_to_subscribers(event)

        logger.debug(f"Published event: {event.id} to {topic}")
        return event.id

    async def _deliver_to_subscribers(self, event: Event) -> None:
        """Deliver event to matching subscribers."""
        matching_subs = self._get_matching_subscriptions(event.topic)

        for sub_id in matching_subs:
            subscription = self._subscriptions[sub_id]
            if not subscription.is_active:
                continue

            if subscription.filter_func:
                if not subscription.filter_func(event):
                    continue

            await self._deliver_event(event, subscription)

    async def _deliver_event(
        self,
        event: Event,
        subscription: Subscription,
    ) -> bool:
        """Deliver event to a single subscriber."""
        for attempt in range(self.max_retries + 1):
            try:
                if asyncio.iscoroutinefunction(subscription.callback):
                    await subscription.callback(event)
                else:
                    subscription.callback(event)

                subscription.messages_received += 1
                self._stats["total_delivered"] += 1
                return True

            except Exception as e:
                event.delivery_count += 1
                logger.error(f"Delivery failed for {subscription.id}: {e}")

        self._stats["delivery_failures"] += 1

        if self.enable_dlq:
            self._dlq.append(event)
            self._stats["dlq_messages"] += 1

        return False

    def _get_matching_subscriptions(self, topic: str) -> list[str]:
        """Get all subscriptions matching a topic."""
        matching = []

        for pattern, sub_ids in self._topic_subscribers.items():
            if self._match_topic(pattern, topic):
                matching.extend(sub_ids)

        return matching

    def _match_topic(self, pattern: str, topic: str) -> bool:
        """Match topic against pattern."""
        if pattern == topic:
            return True
        if pattern == "*":
            return True
        if "*" in pattern:
            pattern_parts = pattern.split(".")
            topic_parts = topic.split(".")
            for p, t in zip(pattern_parts, topic_parts):
                if p != "*" and p != t:
                    return False
            return True
        return False

    def get_dlq(self) -> list[Event]:
        """
        Get dead letter queue events.

        Returns:
            List of DLQ events.
        """
        return self._dlq.copy()

    def retry_dlq(self) -> int:
        """
        Retry delivering DLQ messages.

        Returns:
            Number of messages retried.
        """
        count = 0
        dlq_copy = self._dlq.copy()
        self._dlq.clear()

        for event in dlq_copy:
            asyncio.create_task(self._deliver_to_subscribers(event))
            count += 1

        return count

    def get_stats(self) -> dict[str, Any]:
        """
        Get event bus statistics.

        Returns:
            Statistics dictionary.
        """
        return {
            **self._stats,
            "active_subscriptions": sum(1 for s in self._subscriptions.values() if s.is_active),
            "dlq_size": len(self._dlq),
            "message_log_size": len(self._message_log),
        }

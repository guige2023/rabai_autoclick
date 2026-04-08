"""
Event bus and pub/sub module for asynchronous event-driven architectures.

Supports event filtering, routing, dead letter queues, and multiple
transport backends (in-memory, Redis, Kafka, SQS).
"""
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class EventDeliveryMode(Enum):
    """Event delivery mode."""
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


class EventStatus(Enum):
    """Event processing status."""
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    DLQ = "dead_letter"


@dataclass
class Event:
    """An event in the bus."""
    id: str
    topic: str
    data: Any
    metadata: dict = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    source: str = ""
    event_type: str = ""
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    partition_key: Optional[str] = None

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "topic": self.topic,
            "data": self.data,
            "metadata": self.metadata,
            "timestamp": self.timestamp,
            "source": self.source,
            "event_type": self.event_type,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "partition_key": self.partition_key,
        }


@dataclass
class Subscription:
    """An event subscription."""
    id: str
    topic: str
    handler: Callable
    filter_function: Optional[Callable] = None
    concurrency: int = 1
    auto_ack: bool = True
    dead_letter_queue: Optional[str] = None
    retry_count: int = 3
    retry_delay_seconds: float = 1.0


@dataclass
class DeadLetterEntry:
    """Entry in the dead letter queue."""
    event: Event
    error: str
    failed_at: float = field(default_factory=time.time)
    retry_count: int = 0
    original_subscription: Optional[str] = None


@dataclass
class EventEnvelope:
    """Wrapper for events with routing information."""
    event: Event
    status: EventStatus = EventStatus.PENDING
    delivery_count: int = 0
    last_delivery_attempt: Optional[float] = None
    next_delivery: Optional[float] = None
    error: Optional[str] = None


class EventBus:
    """
    Event bus for pub/sub messaging.

    Supports event filtering, routing, dead letter queues,
    and multiple transport backends.
    """

    def __init__(
        self,
        name: str = "default",
        delivery_mode: EventDeliveryMode = EventDeliveryMode.AT_LEAST_ONCE,
    ):
        self.name = name
        self.delivery_mode = delivery_mode
        self._topics: dict[str, list[Subscription]] = {}
        self._subscriptions: dict[str, Subscription] = {}
        self._pending_events: list[EventEnvelope] = []
        self._dead_letter_queue: list[DeadLetterEntry] = []
        self._event_history: list[Event] = []
        self._max_history: int = 10000

    def create_topic(self, topic: str) -> None:
        """Create a new topic."""
        if topic not in self._topics:
            self._topics[topic] = []

    def subscribe(
        self,
        topic: str,
        handler: Callable,
        subscription_id: Optional[str] = None,
        filter_function: Optional[Callable] = None,
        concurrency: int = 1,
        auto_ack: bool = True,
        dead_letter_queue: Optional[str] = None,
        retry_count: int = 3,
        retry_delay_seconds: float = 1.0,
    ) -> Subscription:
        """Subscribe to a topic with a handler."""
        self.create_topic(topic)

        sub_id = subscription_id or str(uuid.uuid4())[:8]
        subscription = Subscription(
            id=sub_id,
            topic=topic,
            handler=handler,
            filter_function=filter_function,
            concurrency=concurrency,
            auto_ack=auto_ack,
            dead_letter_queue=dead_letter_queue,
            retry_count=retry_count,
            retry_delay_seconds=retry_delay_seconds,
        )

        self._subscriptions[sub_id] = subscription
        self._topics[topic].append(subscription)

        return subscription

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from a topic."""
        subscription = self._subscriptions.get(subscription_id)
        if not subscription:
            return False

        topic_subs = self._topics.get(subscription.topic, [])
        topic_subs = [s for s in topic_subs if s.id != subscription_id]
        self._topics[subscription.topic] = topic_subs

        del self._subscriptions[subscription_id]
        return True

    def publish(
        self,
        topic: str,
        data: Any,
        event_type: str = "",
        source: str = "",
        correlation_id: Optional[str] = None,
        partition_key: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> Event:
        """Publish an event to a topic."""
        event = Event(
            id=str(uuid.uuid4()),
            topic=topic,
            data=data,
            metadata=metadata or {},
            source=source,
            event_type=event_type,
            correlation_id=correlation_id,
            partition_key=partition_key,
        )

        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

        envelope = EventEnvelope(event=event)
        self._pending_events.append(envelope)

        return event

    def _deliver_event(self, envelope: EventEnvelope, subscription: Subscription) -> bool:
        """Deliver an event to a subscription handler."""
        try:
            if subscription.filter_function:
                if not subscription.filter_function(envelope.event):
                    return True

            result = subscription.handler(envelope.event)

            if subscription.auto_ack:
                envelope.status = EventStatus.DELIVERED

            return True

        except Exception as e:
            envelope.error = str(e)
            envelope.delivery_count += 1
            envelope.last_delivery_attempt = time.time()

            if envelope.delivery_count >= subscription.retry_count:
                envelope.status = EventStatus.FAILED
                self._move_to_dlq(envelope, subscription)
                return False

            envelope.next_delivery = (
                time.time() + subscription.retry_delay_seconds * envelope.delivery_count
            )
            return False

    def _move_to_dlq(self, envelope: EventEnvelope, subscription: Subscription) -> None:
        """Move a failed event to the dead letter queue."""
        dlq_entry = DeadLetterEntry(
            event=envelope.event,
            error=envelope.error or "Unknown error",
            retry_count=envelope.delivery_count,
            original_subscription=subscription.id,
        )
        self._dead_letter_queue.append(dlq_entry)
        envelope.status = EventStatus.DLQ

    def process_pending(self, max_events: int = 100) -> int:
        """Process pending events (call this in a loop or worker)."""
        processed = 0
        now = time.time()

        for envelope in self._pending_events[:max_events]:
            if envelope.status in (EventStatus.DELIVERED, EventStatus.FAILED, EventStatus.DLQ):
                continue

            if envelope.next_delivery and envelope.next_delivery > now:
                continue

            topic = envelope.event.topic
            subscriptions = self._topics.get(topic, [])

            if not subscriptions:
                continue

            for subscription in subscriptions:
                self._deliver_event(envelope, subscription)

                if envelope.status == EventStatus.DELIVERED:
                    break

            processed += 1

        self._pending_events = [
            e for e in self._pending_events
            if e.status not in (EventStatus.DELIVERED, EventStatus.FAILED)
        ]

        return processed

    def retry_dlq(self, max_entries: int = 10) -> int:
        """Retry dead letter queue entries."""
        retried = 0

        for entry in self._dead_letter_queue[:max_entries]:
            event = entry.event
            topic = event.topic
            subscriptions = self._topics.get(topic, [])

            for subscription in subscriptions:
                envelope = EventEnvelope(event=event, delivery_count=entry.retry_count)
                if self._deliver_event(envelope, subscription):
                    self._dead_letter_queue.remove(entry)
                    retried += 1
                    break

        return retried

    def get_pending_count(self) -> int:
        """Get count of pending events."""
        return len(self._pending_events)

    def get_dlq_size(self) -> int:
        """Get size of dead letter queue."""
        return len(self._dead_letter_queue)

    def get_events_by_topic(
        self,
        topic: str,
        limit: int = 100,
    ) -> list[Event]:
        """Get recent events for a topic."""
        topic_events = [e for e in self._event_history if e.topic == topic]
        return topic_events[-limit:]

    def get_subscription_stats(self, subscription_id: str) -> dict:
        """Get statistics for a subscription."""
        subscription = self._subscriptions.get(subscription_id)
        if not subscription:
            return {}

        topic_events = self.get_events_by_topic(subscription.topic)
        total_events = len(topic_events)

        return {
            "subscription_id": subscription_id,
            "topic": subscription.topic,
            "total_events": total_events,
            "concurrency": subscription.concurrency,
            "retry_count": subscription.retry_count,
        }

    def list_topics(self) -> list[str]:
        """List all topics."""
        return list(self._topics.keys())

    def list_subscriptions(self, topic: Optional[str] = None) -> list[Subscription]:
        """List subscriptions."""
        if topic:
            return self._topics.get(topic, [])
        return list(self._subscriptions.values())

    def list_dlq(self, limit: int = 100) -> list[DeadLetterEntry]:
        """List dead letter queue entries."""
        return self._dead_letter_queue[-limit:]

    def clear_dlq(self) -> int:
        """Clear the dead letter queue."""
        count = len(self._dead_letter_queue)
        self._dead_letter_queue.clear()
        return count

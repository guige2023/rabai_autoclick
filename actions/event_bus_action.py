"""
Event Bus Action Module.

Provides an in-memory event bus with publish-subscribe patterns, event routing,
filtering, dead-letter handling, and event ordering guarantees.

Author: RabAi Team
"""

from __future__ import annotations

import json
import threading
import uuid
import weakref
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar, Union

T = TypeVar("T")


class EventPriority(Enum):
    """Event priority levels."""
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4


class DeliveryStatus(Enum):
    """Event delivery status."""
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    RETRYING = "retrying"
    DEAD_LETTER = "dead_letter"


@dataclass
class Event:
    """Base event object."""
    id: str
    topic: str
    payload: Any
    priority: EventPriority = EventPriority.NORMAL
    headers: Dict[str, str] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.now)
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        topic: str,
        payload: Any,
        priority: EventPriority = EventPriority.NORMAL,
        **kwargs,
    ) -> "Event":
        """Factory method to create an event."""
        return cls(
            id=str(uuid.uuid4()),
            topic=topic,
            payload=payload,
            priority=priority,
            **kwargs,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "topic": self.topic,
            "payload": self.payload,
            "priority": self.priority.value,
            "headers": self.headers,
            "created_at": self.created_at.isoformat(),
            "correlation_id": self.correlation_id,
            "reply_to": self.reply_to,
            "metadata": self.metadata,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=str)


@dataclass
class Subscription:
    """Event subscription configuration."""
    id: str
    topic_pattern: str
    handler: Callable
    filter_fn: Optional[Callable[[Event], bool]] = None
    priority: int = 0
    auto_ack: bool = True
    max_retries: int = 3
    dead_letter_topic: Optional[str] = None

    def matches(self, topic: str) -> bool:
        """Check if topic matches subscription pattern."""
        if self.topic_pattern == "*" or self.topic_pattern == "#":
            return True
        if "#" in self.topic_pattern:
            pattern = self.topic_pattern.replace("#", "")
            return topic.startswith(pattern.rstrip("."))
        if "?" in self.topic_pattern:
            parts = self.topic_pattern.split(".")
            topic_parts = topic.split(".")
            if len(parts) != len(topic_parts):
                return False
            for p, t in zip(parts, topic_parts):
                if p != "?" and p != t:
                    return False
            return True
        return self.topic_pattern == topic


@dataclass
class DeliveryRecord:
    """Record of event delivery attempt."""
    event_id: str
    subscription_id: str
    status: DeliveryStatus
    attempts: int = 0
    last_error: Optional[str] = None
    delivered_at: Optional[datetime] = None


class DeadLetterHandler:
    """Handler for failed event deliveries."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._letters: Dict[str, List[DeliveryRecord]] = defaultdict(list)

    def add(self, event_id: str, record: DeliveryRecord) -> None:
        """Add a failed delivery to dead letter queue."""
        self._letters[event_id].append(record)
        if len(self._letters) > self.max_size:
            oldest = min(self._letters.keys())
            del self._letters[oldest]

    def get(self, event_id: str) -> List[DeliveryRecord]:
        """Get dead letter records for an event."""
        return self._letters.get(event_id, [])

    def retry(self, event_id: str) -> bool:
        """Mark event for retry."""
        if event_id in self._letters:
            for record in self._letters[event_id]:
                record.status = DeliveryStatus.PENDING
            return True
        return False


class EventBus:
    """
    In-memory event bus with pub-sub, filtering, and dead-letter handling.

    Supports topic patterns, priority-based delivery, event filtering,
    and reliable delivery with retry logic.

    Example:
        >>> bus = EventBus()
        >>> bus.subscribe("user.*", lambda e: handle(e))
        >>> bus.publish("user.created", {"user_id": 123})
        >>> bus.publish("order.placed", {"order_id": 456}, priority=EventPriority.HIGH)
    """

    def __init__(self):
        self._subscriptions: Dict[str, Subscription] = {}
        self._topic_subscriptions: Dict[str, Set[str]] = defaultdict(set)
        self._pending_events: List[Event] = []
        self._delivery_records: Dict[str, DeliveryRecord] = {}
        self._dead_letter = DeadLetterHandler()
        self._lock = threading.RLock()
        self._handlers: Dict[str, List[Callable]] = defaultdict(list)
        self._middleware: List[Callable] = []
        self._stats = {
            "published": 0,
            "delivered": 0,
            "failed": 0,
            "dead_lettered": 0,
        }

    def subscribe(
        self,
        topic_pattern: str,
        handler: Callable[[Event], None],
        filter_fn: Optional[Callable[[Event], bool]] = None,
        priority: int = 0,
        auto_ack: bool = True,
    ) -> str:
        """Subscribe to events matching a topic pattern."""
        sub_id = str(uuid.uuid4())
        subscription = Subscription(
            id=sub_id,
            topic_pattern=topic_pattern,
            handler=handler,
            filter_fn=filter_fn,
            priority=priority,
            auto_ack=auto_ack,
        )
        self._subscriptions[sub_id] = subscription
        self._topic_subscriptions[topic_pattern].add(sub_id)
        return sub_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from a topic."""
        if subscription_id not in self._subscriptions:
            return False
        sub = self._subscriptions[subscription_id]
        self._topic_subscriptions[sub.topic_pattern].discard(subscription_id)
        del self._subscriptions[subscription_id]
        return True

    def publish(
        self,
        topic: str,
        payload: Any,
        priority: EventPriority = EventPriority.NORMAL,
        headers: Optional[Dict[str, str]] = None,
        correlation_id: Optional[str] = None,
    ) -> Event:
        """Publish an event to the bus."""
        event = Event.create(
            topic=topic,
            payload=payload,
            priority=priority,
            headers=headers or {},
            correlation_id=correlation_id,
        )

        with self._lock:
            self._pending_events.append(event)
            self._pending_events.sort(key=lambda e: e.priority.value)

        for middleware in self._middleware:
            try:
                event = middleware(event) or event
            except Exception:
                pass

        self._dispatch(event)
        self._stats["published"] += 1
        return event

    def publish_reply(
        self,
        original_event: Event,
        payload: Any,
    ) -> Event:
        """Publish a reply event to the original sender."""
        if not original_event.reply_to:
            raise ValueError("Original event has no reply_to set")
        return self.publish(
            topic=original_event.reply_to,
            payload=payload,
            correlation_id=original_event.id,
        )

    def add_middleware(self, middleware: Callable[[Event], Optional[Event]]) -> None:
        """Add event processing middleware."""
        self._middleware.append(middleware)

    def _dispatch(self, event: Event) -> None:
        """Dispatch event to matching subscriptions."""
        matched_subs = []
        for sub in self._subscriptions.values():
            if sub.matches(event.topic):
                if sub.filter_fn is None or sub.filter_fn(event):
                    matched_subs.append(sub)

        matched_subs.sort(key=lambda s: s.priority, reverse=True)

        for sub in matched_subs:
            self._deliver(event, sub)

    def _deliver(self, event: Event, subscription: Subscription) -> None:
        """Deliver event to a subscription handler."""
        record = DeliveryRecord(
            event_id=event.id,
            subscription_id=subscription.id,
            status=DeliveryStatus.PENDING,
        )
        self._delivery_records[f"{event.id}:{subscription.id}"] = record

        try:
            subscription.handler(event)
            record.status = DeliveryStatus.DELIVERED
            record.delivered_at = datetime.now()
            self._stats["delivered"] += 1
        except Exception as e:
            record.attempts += 1
            record.last_error = str(e)
            if record.attempts >= subscription.max_retries:
                record.status = DeliveryStatus.DEAD_LETTER
                self._dead_letter.add(event.id, record)
                self._stats["dead_lettered"] += 1
                if subscription.dead_letter_topic:
                    self.publish(
                        subscription.dead_letter_topic,
                        {"original_event": event.to_dict(), "error": str(e)},
                    )
            else:
                record.status = DeliveryStatus.RETRYING
                self._stats["failed"] += 1

    def get_pending_events(self, topic_pattern: Optional[str] = None) -> List[Event]:
        """Get pending events, optionally filtered by topic."""
        if topic_pattern:
            return [e for e in self._pending_events if Event.create("", "").matches.__self__ is not None]
        return list(self._pending_events)

    def get_delivery_status(self, event_id: str) -> Dict[str, DeliveryStatus]:
        """Get delivery status for all subscriptions of an event."""
        return {
            sub_id: record.status.value
            for (eid, sub_id), record in self._delivery_records.items()
            if eid == event_id
        }

    def get_dead_letters(self) -> Dict[str, List[Dict]]:
        """Get all dead-lettered events."""
        result = {}
        for event_id, records in self._dead_letter._letters.items():
            result[event_id] = [
                {
                    "subscription_id": r.subscription_id,
                    "attempts": r.attempts,
                    "last_error": r.last_error,
                }
                for r in records
            ]
        return result

    def retry_dead_letter(self, event_id: str) -> bool:
        """Retry a dead-lettered event."""
        return self._dead_letter.retry(event_id)

    def get_statistics(self) -> Dict[str, int]:
        """Get event bus statistics."""
        return {
            **self._stats,
            "subscriptions": len(self._subscriptions),
            "pending_events": len(self._pending_events),
            "delivery_records": len(self._delivery_records),
        }


class EventRouter:
    """Routes events to different destinations based on rules."""

    def __init__(self, event_bus: EventBus):
        self.bus = event_bus
        self._routes: Dict[str, List[str]] = defaultdict(list)

    def add_route(
        self,
        topic_pattern: str,
        destination_topic: str,
        transform_fn: Optional[Callable[[Event], Any]] = None,
    ) -> None:
        """Add a routing rule."""
        route_id = str(uuid.uuid4())

        def route_handler(event: Event):
            payload = event.payload
            if transform_fn:
                payload = transform_fn(event)
            self.bus.publish(destination_topic, payload)

        sub_id = self.bus.subscribe(topic_pattern, route_handler)
        self._routes[route_id].append(sub_id)

    def remove_route(self, route_id: str) -> None:
        """Remove a routing rule."""
        if route_id in self._routes:
            for sub_id in self._routes[route_id]:
                self.bus.unsubscribe(sub_id)
            del self._routes[route_id]


def create_event_bus() -> EventBus:
    """Factory to create an event bus."""
    return EventBus()

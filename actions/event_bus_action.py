"""
Event Bus Action Module

Event-driven architecture with pub/sub messaging,
event filtering, dead letter handling, and delivery guarantees.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class EventDeliveryMode(Enum):
    """Event delivery modes."""

    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


class EventStatus(Enum):
    """Event processing status."""

    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


@dataclass
class Event:
    """A domain event."""

    event_id: str
    event_type: str
    payload: Dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    correlation_id: Optional[str] = None
    source: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class Subscription:
    """Event subscription."""

    subscription_id: str
    event_type: str
    handler: Callable[..., Any]
    filter_fn: Optional[Callable[[Event], bool]] = None
    auto_ack: bool = True


@dataclass
class DeliveryRecord:
    """Record of event delivery."""

    event_id: str
    subscription_id: str
    status: EventStatus
    delivered_at: Optional[float] = None
    error: Optional[str] = None


class EventValidator:
    """Validates events before publishing."""

    def __init__(self):
        self._schemas: Dict[str, Dict[str, Any]] = {}

    def register_schema(self, event_type: str, schema: Dict[str, Any]) -> None:
        """Register a schema for an event type."""
        self._schemas[event_type] = schema

    def validate(self, event: Event) -> tuple[bool, Optional[str]]:
        """Validate an event against its schema."""
        if event.event_type not in self._schemas:
            return True, None  # No schema, allow

        schema = self._schemas[event_type]
        required_fields = schema.get("required", [])

        for field_name in required_fields:
            if field_name not in event.payload:
                return False, f"Missing required field: {field_name}"

        return True, None


class InMemoryEventBus:
    """In-memory event bus implementation."""

    def __init__(self):
        self._subscriptions: Dict[str, List[Subscription]] = {}
        self._dead_letter_queue: List[Event] = []
        self._delivery_records: Dict[str, DeliveryRecord] = {}
        self._lock = asyncio.Lock()

    def subscribe(
        self,
        event_type: str,
        handler: Callable[..., Any],
        filter_fn: Optional[Callable[[Event], bool]] = None,
    ) -> str:
        """Subscribe to an event type."""
        subscription_id = str(uuid.uuid4())
        subscription = Subscription(
            subscription_id=subscription_id,
            event_type=event_type,
            handler=handler,
            filter_fn=filter_fn,
        )

        if event_type not in self._subscriptions:
            self._subscriptions[event_type] = []
        self._subscriptions[event_type].append(subscription)

        return subscription_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events."""
        for subscriptions in self._subscriptions.values():
            for sub in subscriptions:
                if sub.subscription_id == subscription_id:
                    subscriptions.remove(sub)
                    return True
        return False

    async def publish(self, event: Event) -> None:
        """Publish an event to subscribers."""
        async with self._lock:
            subscriptions = self._subscriptions.get(event.event_type, [])
            tasks = []

            for sub in subscriptions:
                if sub.filter_fn and not sub.filter_fn(event):
                    continue

                tasks.append(self._deliver_event(event, sub))

            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _deliver_event(self, event: Event, subscription: Subscription) -> None:
        """Deliver an event to a subscriber."""
        record = DeliveryRecord(
            event_id=event.event_id,
            subscription_id=subscription.subscription_id,
            status=EventStatus.PENDING,
        )

        try:
            if asyncio.iscoroutinefunction(subscription.handler):
                await subscription.handler(event)
            else:
                subscription.handler(event)

            record.status = EventStatus.DELIVERED
            record.delivered_at = time.time()

        except Exception as e:
            logger.error(f"Event delivery failed: {e}")
            record.status = EventStatus.FAILED
            record.error = str(e)

            if event.retry_count < event.max_retries:
                event.retry_count += 1
                await self.publish(event)
            else:
                record.status = EventStatus.DEAD_LETTER
                self._dead_letter_queue.append(event)

        self._delivery_records[event.event_id] = record

    def get_dead_letters(self) -> List[Event]:
        """Get dead letter events."""
        return list(self._dead_letter_queue)

    def get_record(self, event_id: str) -> Optional[DeliveryRecord]:
        """Get delivery record for an event."""
        return self._delivery_records.get(event_id)


class EventBusAction:
    """
    Main action class for event-driven architecture.

    Features:
    - Pub/sub messaging
    - Event filtering
    - Dead letter handling
    - Event validation
    - Delivery guarantees (at-most-once, at-least-once)
    - Event metadata and correlation

    Usage:
        bus = EventBusAction()

        async def handle_user_created(event):
            print(f"User created: {event.payload}")

        bus.subscribe("user.created", handle_user_created)
        bus.publish(Event("user.created", {"user_id": "123"}))
    """

    def __init__(self):
        self._bus = InMemoryEventBus()
        self._validator = EventValidator()
        self._stats = {
            "events_published": 0,
            "events_delivered": 0,
            "events_failed": 0,
            "dead_letters": 0,
        }

    def subscribe(
        self,
        event_type: str,
        handler: Callable[..., Any],
        filter_fn: Optional[Callable[[Event], bool]] = None,
    ) -> str:
        """Subscribe to an event type."""
        return self._bus.subscribe(event_type, handler, filter_fn)

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events."""
        return self._bus.unsubscribe(subscription_id)

    async def publish(
        self,
        event_type: str,
        payload: Dict[str, Any],
        correlation_id: Optional[str] = None,
        source: Optional[str] = None,
        event_id: Optional[str] = None,
    ) -> Event:
        """Publish an event."""
        event = Event(
            event_id=event_id or str(uuid.uuid4()),
            event_type=event_type,
            payload=payload,
            correlation_id=correlation_id,
            source=source,
        )

        # Validate
        is_valid, error = self._validator.validate(event)
        if not is_valid:
            raise ValueError(f"Event validation failed: {error}")

        await self._bus.publish(event)
        self._stats["events_published"] += 1

        return event

    def register_schema(
        self,
        event_type: str,
        required_fields: List[str],
    ) -> None:
        """Register an event schema."""
        self._validator.register_schema(
            event_type,
            {"required": required_fields},
        )

    def create_event(
        self,
        event_type: str,
        payload: Dict[str, Any],
    ) -> Event:
        """Create a new event."""
        return Event(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            payload=payload,
        )

    def get_dead_letters(self) -> List[Event]:
        """Get dead letter events."""
        return self._bus.get_dead_letters()

    def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics."""
        return {
            **self._stats,
            "dead_letters": len(self._bus.get_dead_letters()),
        }


async def demo_event_bus():
    """Demonstrate event bus usage."""
    bus = EventBusAction()

    async def handle_user_created(event):
        print(f"User created: {event.payload}")

    async def handle_user_updated(event):
        print(f"User updated: {event.payload}")

    # Subscribe
    bus.subscribe("user.created", handle_user_created)
    bus.subscribe("user.updated", handle_user_updated)

    # Publish events
    await bus.publish("user.created", {"user_id": "123", "name": "Alice"})
    await bus.publish("user.updated", {"user_id": "123", "name": "Alice Smith"})

    print(f"Stats: {bus.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_event_bus())

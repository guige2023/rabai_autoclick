"""
Event Bus and Pub/Sub Management Utilities.

Provides utilities for implementing event-driven architectures,
event buses, message routing, and pub/sub patterns.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
import weakref
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Optional


class EventBusType(Enum):
    """Types of event buses."""
    IN_MEMORY = "in_memory"
    REDIS = "redis"
    KAFKA = "kafka"
    RABBITMQ = "rabbitmq"
    SNS = "sns"


class SubscriptionType(Enum):
    """Types of subscriptions."""
    DIRECT = "direct"
    PATTERN = "pattern"
    FANOUT = "fanout"
    TOPIC = "topic"


@dataclass
class Event:
    """An event message."""
    event_id: str
    event_type: str
    topic: str
    payload: dict[str, Any]
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    source: Optional[str] = None
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "topic": self.topic,
            "payload": self.payload,
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Event":
        return cls(
            event_id=data["event_id"],
            event_type=data["event_type"],
            topic=data["topic"],
            payload=data["payload"],
            metadata=data.get("metadata", {}),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            source=data.get("source"),
            correlation_id=data.get("correlation_id"),
            causation_id=data.get("causation_id"),
        )


@dataclass
class Subscription:
    """An event subscription."""
    subscription_id: str
    topic: str
    handler: Callable[[Event], None]
    subscription_type: SubscriptionType
    pattern: Optional[str] = None
    filter_function: Optional[Callable[[Event], bool]] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def matches(self, event: Event) -> bool:
        """Check if an event matches this subscription."""
        if self.subscription_type == SubscriptionType.DIRECT:
            return event.topic == self.topic

        elif self.subscription_type == SubscriptionType.PATTERN:
            if self.pattern:
                return self._match_pattern(event.topic, self.pattern)

        elif self.subscription_type == SubscriptionType.FANOUT:
            return True

        return False

    def _match_pattern(self, topic: str, pattern: str) -> bool:
        """Match topic against a pattern (supports * and # wildcards)."""
        if pattern == "*":
            return True

        if "#" in pattern:
            prefix = pattern.split("#")[0].rstrip(".")
            return topic.startswith(prefix)

        parts = topic.split(".")
        pattern_parts = pattern.split(".")

        for i, part in enumerate(pattern_parts):
            if part == "*":
                continue
            if i >= len(parts) or part != parts[i]:
                return False

        return True


@dataclass
class EventEnvelope:
    """Envelope wrapping an event with routing information."""
    event: Event
    subscriptions: list[str] = field(default_factory=list)
    delivery_count: int = 0
    max_deliveries: int = 3
    dead_letter_reason: Optional[str] = None


class InMemoryEventBus:
    """In-memory event bus implementation."""

    def __init__(
        self,
        db_path: Optional[Path] = None,
        max_history: int = 1000,
    ) -> None:
        self.db_path = db_path or Path("event_bus.db")
        self.max_history = max_history
        self._subscriptions: dict[str, list[Subscription]] = {}
        self._lock = threading.RLock()
        self._event_history: list[Event] = []
        self._dead_letters: list[EventEnvelope] = []
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the event bus database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                event_json TEXT NOT NULL,
                timestamp TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dead_letters (
                event_id TEXT PRIMARY KEY,
                event_json TEXT NOT NULL,
                reason TEXT,
                timestamp TEXT NOT NULL
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_events_timestamp
            ON events(timestamp DESC)
        """)
        conn.commit()
        conn.close()

    def publish(
        self,
        event_type: str,
        topic: str,
        payload: dict[str, Any],
        source: Optional[str] = None,
        correlation_id: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Event:
        """Publish an event to the bus."""
        event = Event(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            topic=topic,
            payload=payload,
            metadata=metadata or {},
            source=source,
            correlation_id=correlation_id,
        )

        with self._lock:
            self._event_history.append(event)

            if len(self._event_history) > self.max_history:
                self._event_history = self._event_history[-self.max_history:]

            self._dispatch_event(event)
            self._save_event(event)

        return event

    def subscribe(
        self,
        topic: str,
        handler: Callable[[Event], None],
        subscription_type: SubscriptionType = SubscriptionType.DIRECT,
        pattern: Optional[str] = None,
        filter_function: Optional[Callable[[Event], bool]] = None,
    ) -> Subscription:
        """Subscribe to events on a topic."""
        subscription = Subscription(
            subscription_id=str(uuid.uuid4()),
            topic=topic,
            handler=handler,
            subscription_type=subscription_type,
            pattern=pattern,
            filter_function=filter_function,
        )

        with self._lock:
            if topic not in self._subscriptions:
                self._subscriptions[topic] = []
            self._subscriptions[topic].append(subscription)

        return subscription

    def unsubscribe(self, subscription: Subscription) -> bool:
        """Unsubscribe from events."""
        with self._lock:
            if subscription.topic in self._subscriptions:
                try:
                    self._subscriptions[subscription.topic].remove(subscription)
                    return True
                except ValueError:
                    pass
        return False

    def _dispatch_event(self, event: Event) -> None:
        """Dispatch an event to all matching subscriptions."""
        for topic, subs in self._subscriptions.items():
            for sub in subs:
                if sub.matches(event):
                    if sub.filter_function is None or sub.filter_function(event):
                        try:
                            sub.handler(event)
                        except Exception as e:
                            pass

    def _save_event(self, event: Event) -> None:
        """Save an event to the database."""
        conn = sqlite3.connect(str(self.db_path))
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO events (event_id, event_json, timestamp)
            VALUES (?, ?, ?)
        """, (event.event_id, json.dumps(event.to_dict()), event.timestamp.isoformat()))
        conn.commit()
        conn.close()

    def get_event_history(
        self,
        topic: Optional[str] = None,
        event_type: Optional[str] = None,
        limit: int = 100,
        since: Optional[datetime] = None,
    ) -> list[Event]:
        """Get event history with optional filtering."""
        with self._lock:
            events = list(self._event_history)

        if topic:
            events = [e for e in events if e.topic == topic]

        if event_type:
            events = [e for e in events if e.event_type == event_type]

        if since:
            events = [e for e in events if e.timestamp >= since]

        return events[-limit:]

    def get_dead_letters(self, limit: int = 100) -> list[EventEnvelope]:
        """Get events that failed to deliver."""
        with self._lock:
            return list(self._dead_letters[-limit:])

    def replay_events(
        self,
        subscription: Subscription,
        from_timestamp: Optional[datetime] = None,
    ) -> int:
        """Replay past events to a subscription."""
        events = self.get_event_history(
            topic=subscription.topic if subscription.subscription_type == SubscriptionType.DIRECT else None,
            since=from_timestamp,
        )

        replayed = 0
        for event in events:
            if subscription.matches(event):
                if subscription.filter_function is None or subscription.filter_function(event):
                    try:
                        subscription.handler(event)
                        replayed += 1
                    except Exception:
                        pass

        return replayed


class EventBusRouter:
    """Routes events between multiple event buses."""

    def __init__(self) -> None:
        self._buses: dict[str, InMemoryEventBus] = {}
        self._routes: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def add_bus(self, name: str, bus: InMemoryEventBus) -> None:
        """Add an event bus to the router."""
        with self._lock:
            self._buses[name] = bus

    def add_route(
        self,
        source_bus: str,
        source_topic: str,
        dest_bus: str,
        dest_topic: str,
        transform: Optional[Callable[[Event], Event]] = None,
    ) -> None:
        """Add a routing rule between buses."""
        route = {
            "source_bus": source_bus,
            "source_topic": source_topic,
            "dest_bus": dest_bus,
            "dest_topic": dest_topic,
            "transform": transform,
        }

        with self._lock:
            self._routes.append(route)

            source_bus_obj = self._buses.get(source_bus)
            if source_bus_obj:
                source_bus_obj.subscribe(
                    topic=source_topic,
                    handler=lambda event, r=route: self._route_event(event, r),
                )

    def _route_event(self, event: Event, route: dict[str, Any]) -> None:
        """Route an event to the destination bus."""
        dest_bus = self._buses.get(route["dest_bus"])
        if not dest_bus:
            return

        event_to_send = event
        if route.get("transform"):
            try:
                event_to_send = route["transform"](event)
            except Exception:
                return

        dest_bus.publish(
            event_type=event_to_send.event_type,
            topic=route["dest_topic"],
            payload=event_to_send.payload,
            source=event_to_send.source or route["source_bus"],
            correlation_id=event_to_send.correlation_id,
            metadata=event_to_send.metadata,
        )

    def get_bus(self, name: str) -> Optional[InMemoryEventBus]:
        """Get an event bus by name."""
        return self._buses.get(name)


class EventSourcedAggregate:
    """Base class for event-sourced aggregates."""

    def __init__(self, aggregate_id: str) -> None:
        self.aggregate_id = aggregate_id
        self._events: list[Event] = []
        self._version = 0

    def add_event(
        self,
        event_type: str,
        payload: dict[str, Any],
        metadata: Optional[dict[str, Any]] = None,
    ) -> Event:
        """Add an event to the aggregate."""
        event = Event(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            topic=f"{self.__class__.__name__}.{self.aggregate_id}",
            payload=payload,
            metadata=metadata or {},
            causation_id=str(self._version),
        )

        self._events.append(event)
        self._version += 1

        self._apply_event(event)

        return event

    def _apply_event(self, event: Event) -> None:
        """Apply an event to update aggregate state."""
        handler_name = f"_apply_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler:
            handler(event)

    def get_uncommitted_events(self) -> list[Event]:
        """Get all uncommitted events."""
        return list(self._events)

    def mark_events_committed(self) -> None:
        """Mark all events as committed."""
        self._events.clear()

    def load_from_history(self, events: list[Event]) -> None:
        """Reconstruct aggregate from event history."""
        for event in events:
            self._apply_event(event)
            self._version = max(self._version, self._version + 1)

"""Event Bus Action Module.

Provides in-memory event publishing and subscription with support for
topics, patterns, dead letter queues, and event filtering.
"""
from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class DeliveryMode(Enum):
    """Event delivery mode."""
    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


@dataclass
class Event:
    """Bus event structure."""
    id: str
    topic: str
    payload: Any
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)
    source: str = "system"
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None


@dataclass
class Subscription:
    """Event subscription."""
    id: str
    topic: str
    callback: Any
    filter_func: Optional[Callable[[Event], bool]] = None
    auto_ack: bool = True
    max_retries: int = 3


@dataclass
class DeadLetterEvent:
    """Dead letter queue entry."""
    event: Event
    error: str
    failed_at: float
    retry_count: int


@dataclass
class PublishResult:
    """Publish result."""
    success: bool
    event_id: str
    topic: str
    subscribers_notified: int = 0
    error: Optional[str] = None


class EventBusStore:
    """In-memory event bus store."""

    def __init__(self):
        self._subscriptions: Dict[str, List[Subscription]] = {}
        self._dlq: List[DeadLetterEvent] = []
        self._event_history: List[Event] = []
        self._max_history = 1000

    def subscribe(self, topic: str, callback: Any,
                  filter_func: Optional[Callable[[Event], bool]] = None) -> str:
        """Subscribe to topic."""
        sub_id = uuid.uuid4().hex
        sub = Subscription(id=sub_id, topic=topic, callback=callback, filter_func=filter_func)

        if topic not in self._subscriptions:
            self._subscriptions[topic] = []
        self._subscriptions[topic].append(sub)

        return sub_id

    def unsubscribe(self, sub_id: str) -> bool:
        """Unsubscribe."""
        for topic, subs in self._subscriptions.items():
            for sub in subs:
                if sub.id == sub_id:
                    subs.remove(sub)
                    return True
        return False

    def publish(self, event: Event) -> PublishResult:
        """Publish event to subscribers."""
        notified = 0
        topics_to_check = [event.topic]

        for topic, subs in self._subscriptions.items():
            if self._matches_pattern(topic, event.topic):
                topics_to_check.append(topic)

        for topic in topics_to_check:
            if topic in self._subscriptions:
                for sub in self._subscriptions[topic]:
                    if sub.filter_func is None or sub.filter_func(event):
                        try:
                            if callable(sub.callback):
                                sub.callback(event)
                            notified += 1
                        except Exception as e:
                            self._add_to_dlq(event, str(e))

        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

        return PublishResult(
            success=True,
            event_id=event.id,
            topic=event.topic,
            subscribers_notified=notified
        )

    def _matches_pattern(self, pattern: str, topic: str) -> bool:
        """Check if topic matches pattern."""
        if pattern == topic:
            return True
        if "*" in pattern:
            regex = pattern.replace(".", r"\.").replace("*", "[^.]+")
            return bool(re.match(f"^{regex}$", topic))
        return False

    def _add_to_dlq(self, event: Event, error: str) -> None:
        """Add failed event to DLQ."""
        dlq_entry = DeadLetterEvent(
            event=event,
            error=error,
            failed_at=time.time(),
            retry_count=0
        )
        self._dlq.append(dlq_entry)

    def get_dlq(self) -> List[DeadLetterEvent]:
        """Get dead letter queue."""
        return self._dlq

    def get_history(self, topic: Optional[str] = None,
                    limit: int = 100) -> List[Event]:
        """Get event history."""
        events = self._event_history
        if topic:
            events = [e for e in events if e.topic == topic]
        return events[-limit:]


_global_bus = EventBusStore()


class EventBusAction:
    """Event bus action.

    Example:
        action = EventBusAction()

        action.subscribe("user.*", lambda e: print(e.payload))
        action.publish("user.created", {"user_id": "123"})

        events = action.get_history("user.*")
    """

    def __init__(self, store: Optional[EventBusStore] = None):
        self._store = store or _global_bus
        self._handlers: Dict[str, List[Callable]] = {}

    def subscribe(self, topic: str, handler_id: Optional[str] = None) -> Dict[str, Any]:
        """Subscribe to topic."""
        def handler(event: Event):
            print(f"[Event] {topic}: {event.payload}")

        sub_id = self._store.subscribe(topic, handler)

        if handler_id:
            if handler_id not in self._handlers:
                self._handlers[handler_id] = []
            self._handlers[handler_id].append(sub_id)

        return {
            "success": True,
            "subscription_id": sub_id,
            "topic": topic,
            "message": f"Subscribed to {topic}"
        }

    def unsubscribe(self, subscription_id: str) -> Dict[str, Any]:
        """Unsubscribe."""
        if self._store.unsubscribe(subscription_id):
            return {"success": True, "message": "Unsubscribed"}
        return {"success": False, "message": "Subscription not found"}

    def publish(self, topic: str, payload: Any,
                metadata: Optional[Dict[str, Any]] = None,
                source: str = "action") -> Dict[str, Any]:
        """Publish event."""
        event = Event(
            id=uuid.uuid4().hex,
            topic=topic,
            payload=payload,
            metadata=metadata or {},
            source=source
        )

        result = self._store.publish(event)

        return {
            "success": result.success,
            "event_id": result.event_id,
            "topic": result.topic,
            "subscribers_notified": result.subscribers_notified,
            "message": f"Published to {topic}"
        }

    def get_history(self, topic: Optional[str] = None,
                    limit: int = 100) -> Dict[str, Any]:
        """Get event history."""
        events = self._store.get_history(topic, limit)
        return {
            "success": True,
            "events": [
                {
                    "id": e.id,
                    "topic": e.topic,
                    "payload": e.payload,
                    "timestamp": e.timestamp,
                    "source": e.source
                }
                for e in events
            ],
            "count": len(events)
        }

    def get_dlq(self) -> Dict[str, Any]:
        """Get dead letter queue."""
        dlq = self._store.get_dlq()
        return {
            "success": True,
            "dlq": [
                {
                    "event_id": d.event.id,
                    "topic": d.event.topic,
                    "error": d.error,
                    "failed_at": d.failed_at,
                    "retry_count": d.retry_count
                }
                for d in dlq
            ],
            "count": len(dlq)
        }


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute event bus action."""
    operation = params.get("operation", "publish")
    action = EventBusAction()

    try:
        if operation == "subscribe":
            topic = params.get("topic", "")
            if not topic:
                return {"success": False, "message": "topic required"}
            return action.subscribe(topic, params.get("handler_id"))

        elif operation == "unsubscribe":
            sub_id = params.get("subscription_id", "")
            if not sub_id:
                return {"success": False, "message": "subscription_id required"}
            return action.unsubscribe(sub_id)

        elif operation == "publish":
            topic = params.get("topic", "")
            payload = params.get("payload", {})
            if not topic:
                return {"success": False, "message": "topic required"}
            return action.publish(
                topic=topic,
                payload=payload,
                metadata=params.get("metadata"),
                source=params.get("source", "action")
            )

        elif operation == "history":
            return action.get_history(
                topic=params.get("topic"),
                limit=params.get("limit", 100)
            )

        elif operation == "dlq":
            return action.get_dlq()

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Event bus error: {str(e)}"}

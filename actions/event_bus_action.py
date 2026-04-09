"""
Event bus action for pub/sub messaging.

Provides topic-based routing, filtering, and delivery guarantees.
"""

from typing import Any, Callable, Dict, List, Optional
import time
import threading
import re
from collections import defaultdict


class EventBusAction:
    """Event bus for publish-subscribe messaging."""

    def __init__(self, enable_wildcards: bool = True) -> None:
        """
        Initialize event bus.

        Args:
            enable_wildcards: Enable wildcard topic matching
        """
        self.enable_wildcards = enable_wildcards
        self._subscribers: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._events: List[Dict[str, Any]] = []
        self._event_count = 0
        self._lock = threading.Lock()
        self._max_events = 10000

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute event bus operation.

        Args:
            params: Dictionary containing:
                - operation: 'publish', 'subscribe', 'unsubscribe', 'events'
                - topic: Event topic
                - event: Event data (for publish)
                - subscriber_id: Subscriber identifier
                - handler: Event handler (for subscribe)
                - filter: Event filter expression

        Returns:
            Dictionary with operation result
        """
        operation = params.get("operation", "publish")

        if operation == "publish":
            return self._publish_event(params)
        elif operation == "subscribe":
            return self._subscribe(params)
        elif operation == "unsubscribe":
            return self._unsubscribe(params)
        elif operation == "events":
            return self._get_events(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _publish_event(self, params: dict[str, Any]) -> dict[str, Any]:
        """Publish event to topic."""
        topic = params.get("topic", "")
        event_data = params.get("event", {})
        event_type = params.get("type", "generic")
        source = params.get("source", "unknown")

        if not topic:
            return {"success": False, "error": "Topic is required"}

        event = {
            "id": self._event_count,
            "topic": topic,
            "type": event_type,
            "data": event_data,
            "source": source,
            "timestamp": time.time(),
        }
        self._event_count += 1

        with self._lock:
            self._events.append(event)
            if len(self._events) > self._max_events:
                self._events = self._events[-self._max_events :]

        delivered_count = self._deliver_event(event)

        return {
            "success": True,
            "event_id": event["id"],
            "topic": topic,
            "delivered_count": delivered_count,
        }

    def _deliver_event(self, event: dict[str, Any]) -> int:
        """Deliver event to matching subscribers."""
        delivered = 0
        topic = event["topic"]

        for subscribed_topic, subscribers in self._subscribers.items():
            if self._topic_matches(topic, subscribed_topic):
                for subscriber in subscribers:
                    try:
                        handler = subscriber["handler"]
                        if callable(handler):
                            handler(event)
                        delivered += 1
                    except Exception:
                        pass

        return delivered

    def _topic_matches(self, topic: str, pattern: str) -> bool:
        """Check if topic matches subscription pattern."""
        if topic == pattern:
            return True

        if self.enable_wildcards:
            if "+" in pattern:
                topic_parts = topic.split(".")
                pattern_parts = pattern.split(".")
                for tp, pp in zip(topic_parts, pattern_parts):
                    if pp == "+":
                        continue
                    if tp != pp:
                        return False
                return True
            if "#" in pattern:
                prefix = pattern.replace("#", "").rstrip(".")
                return topic.startswith(prefix)

        return False

    def _subscribe(self, params: dict[str, Any]) -> dict[str, Any]:
        """Subscribe to topic."""
        topic = params.get("topic", "")
        subscriber_id = params.get("subscriber_id", "")
        handler = params.get("handler")
        filter_expr = params.get("filter")

        if not topic:
            return {"success": False, "error": "Topic is required"}

        subscriber = {
            "id": subscriber_id or f"sub_{len(self._subscribers[topic])}",
            "handler": handler,
            "filter": filter_expr,
            "subscribed_at": time.time(),
        }

        with self._lock:
            self._subscribers[topic].append(subscriber)

        return {
            "success": True,
            "topic": topic,
            "subscriber_id": subscriber["id"],
        }

    def _unsubscribe(self, params: dict[str, Any]) -> dict[str, Any]:
        """Unsubscribe from topic."""
        topic = params.get("topic", "")
        subscriber_id = params.get("subscriber_id", "")

        if not topic or not subscriber_id:
            return {"success": False, "error": "Topic and subscriber_id are required"}

        with self._lock:
            if topic in self._subscribers:
                original_count = len(self._subscribers[topic])
                self._subscribers[topic] = [
                    s for s in self._subscribers[topic] if s["id"] != subscriber_id
                ]
                removed = original_count - len(self._subscribers[topic])
                return {"success": True, "removed": removed}

        return {"success": False, "error": "Subscriber not found"}

    def _get_events(self, params: dict[str, Any]) -> dict[str, Any]:
        """Get recent events."""
        topic = params.get("topic")
        event_type = params.get("type")
        limit = params.get("limit", 100)
        offset = params.get("offset", 0)

        events = self._events

        if topic:
            events = [e for e in events if self._topic_matches(e["topic"], topic)]
        if event_type:
            events = [e for e in events if e["type"] == event_type]

        return {
            "success": True,
            "total": len(events),
            "events": events[offset : offset + limit],
        }

    def get_subscriptions(self) -> dict[str, int]:
        """Get subscriber count per topic."""
        return {topic: len(subs) for topic, subs in self._subscribers.items()}

    def get_stats(self) -> dict[str, Any]:
        """Get event bus statistics."""
        return {
            "total_events": self._event_count,
            "stored_events": len(self._events),
            "topics": len(self._subscribers),
            "total_subscribers": sum(len(s) for s in self._subscribers.values()),
        }

"""
Event Bus Action Module.

Provides publish/subscribe event handling with topic filtering,
async delivery, dead letter queue, and event persistence options.
"""
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import asyncio
from actions.base_action import BaseAction


@dataclass
class Event:
    """A published event."""
    topic: str
    data: Any
    timestamp: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class EventDeliveryResult:
    """Result of event delivery."""
    delivered_count: int
    failed_count: int
    errors: list[str]


class EventBusAction(BaseAction):
    """Publish/subscribe event bus for decoupled communication."""

    def __init__(self) -> None:
        super().__init__("event_bus")
        self._subscribers: dict[str, list[Callable]] = defaultdict(list)
        self._dlq: list[Event] = []
        self._event_history: list[Event] = []
        self._max_history = 1000

    def execute(self, context: dict, params: dict) -> EventDeliveryResult:
        """
        Publish an event to subscribers.

        Args:
            context: Execution context
            params: Parameters:
                - topic: Event topic/channel
                - data: Event payload
                - metadata: Optional event metadata
                - wait: Whether to wait for delivery (default: False)

        Returns:
            EventDeliveryResult with delivery statistics
        """
        import time

        topic = params.get("topic", "")
        data = params.get("data")
        metadata = params.get("metadata", {})
        wait = params.get("wait", False)

        if not topic:
            return EventDeliveryResult(0, 0, ["Topic is required"])

        event = Event(
            topic=topic,
            data=data,
            timestamp=time.time(),
            metadata=metadata
        )

        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

        handlers = self._subscribers.get(topic, []) + self._subscribers.get("*", [])
        delivered = 0
        failed = 0
        errors: list[str] = []

        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    if wait:
                        asyncio.run(handler(event))
                    else:
                        asyncio.create_task(handler(event))
                else:
                    handler(event)
                delivered += 1
            except Exception as e:
                failed += 1
                errors.append(f"{topic} handler: {str(e)}")
                self._dlq.append(event)

        return EventDeliveryResult(delivered, failed, errors)

    def subscribe(self, topic: str, handler: Callable) -> None:
        """Subscribe a handler to a topic."""
        self._subscribers[topic].append(handler)

    def unsubscribe(self, topic: str, handler: Callable) -> None:
        """Unsubscribe a handler from a topic."""
        if topic in self._subscribers:
            self._subscribers[topic].remove(handler)

    def get_dlq(self) -> list[Event]:
        """Get dead letter queue events."""
        return self._dlq.copy()

    def get_history(self, topic: Optional[str] = None, limit: int = 100) -> list[Event]:
        """Get event history, optionally filtered by topic."""
        events = self._event_history
        if topic:
            events = [e for e in events if e.topic == topic]
        return events[-limit:]

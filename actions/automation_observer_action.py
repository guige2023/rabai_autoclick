"""
Automation Observer Action Module.

Provides observer pattern implementation for automation workflows
with support for event filtering, priority subscription,
and async notification delivery.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Optional


class EventFilter:
    """Event filter for subscription."""

    def __init__(
        self,
        event_types: Optional[list[str]] = None,
        source: Optional[str] = None,
        predicate: Optional[Callable[[dict], bool]] = None,
    ):
        self.event_types = set(event_types) if event_types else set()
        self.source = source
        self.predicate = predicate

    def matches(self, event: dict[str, Any]) -> bool:
        """Check if event matches filter."""
        if self.event_types and event.get("type") not in self.event_types:
            return False
        if self.source and event.get("source") != self.source:
            return False
        if self.predicate and not self.predicate(event):
            return False
        return True


@dataclass
class ObserverSubscription:
    """Observer subscription details."""
    subscription_id: str
    observer_id: str
    filter: EventFilter
    callback: Callable
    priority: int = 0
    async_handler: bool = True
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class AutomationEvent:
    """Automation event structure."""
    event_id: str
    type: str
    source: str
    payload: dict[str, Any]
    timestamp: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)


class ObserverSubject:
    """Subject that emits events to observers."""

    def __init__(self, subject_id: str = ""):
        self.subject_id = subject_id
        self._subscriptions: dict[str, ObserverSubscription] = {}
        self._history: list[AutomationEvent] = []
        self._max_history: int = 1000

    def subscribe(
        self,
        observer_id: str,
        callback: Callable,
        event_types: Optional[list[str]] = None,
        source: Optional[str] = None,
        predicate: Optional[Callable[[dict], bool]] = None,
        priority: int = 0,
    ) -> str:
        """Subscribe to events."""
        subscription_id = str(uuid.uuid4())
        filter_obj = EventFilter(event_types, source, predicate)
        subscription = ObserverSubscription(
            subscription_id=subscription_id,
            observer_id=observer_id,
            filter=filter_obj,
            callback=callback,
            priority=priority,
        )
        self._subscriptions[subscription_id] = subscription
        return subscription_id

    def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from events."""
        if subscription_id in self._subscriptions:
            del self._subscriptions[subscription_id]
            return True
        return False

    def unsubscribe_all(self, observer_id: str) -> int:
        """Unsubscribe all for an observer."""
        to_remove = [
            sid for sid, sub in self._subscriptions.items()
            if sub.observer_id == observer_id
        ]
        for sid in to_remove:
            del self._subscriptions[sid]
        return len(to_remove)

    async def emit(
        self,
        event_type: str,
        payload: dict[str, Any],
        source: str = "",
        metadata: Optional[dict[str, Any]] = None,
    ) -> list[Any]:
        """Emit event to all matching subscribers."""
        event = AutomationEvent(
            event_id=str(uuid.uuid4()),
            type=event_type,
            source=source,
            payload=payload,
            metadata=metadata or {},
        )
        self._history.append(event)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]

        results = []
        matching = [
            sub for sub in self._subscriptions.values()
            if sub.filter.matches({
                "type": event_type,
                "source": source,
                **payload,
            })
        ]
        matching.sort(key=lambda s: s.priority, reverse=True)

        for sub in matching:
            result = sub.callback(event)
            if sub.async_handler and asyncio.iscoroutine(result):
                result = await result
            results.append(result)
        return results

    def get_history(
        self,
        event_type: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> list[AutomationEvent]:
        """Get event history."""
        events = self._history
        if event_type:
            events = [e for e in events if e.type == event_type]
        if since:
            events = [e for e in events if e.timestamp >= since]
        return events[-limit:]


class EventBus(ObserverSubject):
    """Global event bus for application-wide events."""

    _instance: Optional["EventBus"] = None

    @classmethod
    def get_instance(cls) -> "EventBus":
        """Get singleton instance."""
        if cls._instance is None:
            cls._instance = EventBus()
        return cls._instance

    def __init__(self):
        super().__init__("global")
        self._channels: dict[str, ObserverSubject] = {}

    def get_channel(self, channel: str) -> ObserverSubject:
        """Get or create a channel."""
        if channel not in self._channels:
            self._channels[channel] = ObserverSubject(subject_id=channel)
        return self._channels[channel]


class ObserverRegistry:
    """Registry for managing observer lifecycle."""

    def __init__(self):
        self._observers: dict[str, dict[str, Any]] = {}

    def register(
        self,
        observer_id: str,
        name: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Register an observer."""
        self._observers[observer_id] = {
            "name": name,
            "metadata": metadata or {},
            "registered_at": datetime.now(timezone.utc),
            "subscriptions": [],
        }

    def unregister(self, observer_id: str) -> bool:
        """Unregister an observer."""
        if observer_id in self._observers:
            del self._observers[observer_id]
            return True
        return False

    def track_subscription(
        self,
        observer_id: str,
        subscription_id: str,
    ) -> None:
        """Track subscription for an observer."""
        if observer_id in self._observers:
            self._observers[observer_id]["subscriptions"].append(subscription_id)

    def get_observers(self) -> list[dict[str, Any]]:
        """Get all registered observers."""
        return list(self._observers.values())


class AsyncEventProcessor:
    """Async event processor with batching and backpressure."""

    def __init__(
        self,
        batch_size: int = 10,
        flush_interval: float = 1.0,
        max_queue_size: int = 1000,
    ):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_queue_size = max_queue_size
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_queue_size)
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def enqueue(self, event: AutomationEvent) -> bool:
        """Add event to processing queue."""
        try:
            self._queue.put_nowait(event)
            return True
        except asyncio.QueueFull:
            return False

    async def start(self, handler: Callable[[list[AutomationEvent]], Any]) -> None:
        """Start processing events."""
        self._running = True
        self._task = asyncio.create_task(self._process_loop(handler))

    async def stop(self) -> None:
        """Stop processing."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _process_loop(self, handler: Callable) -> None:
        """Main processing loop."""
        while self._running:
            batch = []
            try:
                while len(batch) < self.batch_size:
                    try:
                        event = await asyncio.wait_for(
                            self._queue.get(),
                            timeout=self.flush_interval,
                        )
                        batch.append(event)
                    except asyncio.TimeoutError:
                        break
            except asyncio.CancelledError:
                break

            if batch:
                result = handler(batch)
                if asyncio.iscoroutine(result):
                    await result


def create_observer_subject(subject_id: str = "") -> ObserverSubject:
    """Create a new observer subject."""
    return ObserverSubject(subject_id)


async def demo():
    """Demo observer pattern."""
    bus = EventBus.get_instance()

    results = []

    def handler1(event: AutomationEvent):
        results.append(f"handler1: {event.type}")
        return f"handled: {event.type}"

    def handler2(event: AutomationEvent):
        results.append(f"handler2: {event.type}")

    bus.subscribe("obs1", handler1, event_types=["click", "submit"])
    bus.subscribe("obs2", handler2, event_types=["click"])

    await bus.emit("click", {"x": 100, "y": 200})
    await bus.emit("submit", {"form": "data"})
    await bus.emit("hover", {"x": 50})  # Should be ignored

    print(f"Results: {results}")


if __name__ == "__main__":
    asyncio.run(demo())

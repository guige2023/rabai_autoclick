"""event_queue action module for rabai_autoclick.

Provides async event queue processing with pub/sub patterns,
event routing, dead-letter queues, and reliable delivery guarantees.
"""

from __future__ import annotations

import json
import threading
import time
import uuid
from collections import deque, defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Generic, Optional, TypeVar, Union
from concurrent.futures import Future

__all__ = [
    "Event",
    "EventQueue",
    "PubSub",
    "Subscriber",
    "Topic",
    "Router",
    "DeadLetterQueue",
    "EventBus",
    "create_event",
    "subscribe",
    "publish",
    "EventPriority",
    "DeliveryMode",
]


T = TypeVar("T")


class EventPriority(Enum):
    """Event priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class DeliveryMode(Enum):
    """Event delivery guarantees."""
    AT_MOST_ONCE = auto()
    AT_LEAST_ONCE = auto()
    EXACTLY_ONCE = auto()


@dataclass
class Event(Generic[T]):
    """Base event class with metadata."""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    type: str = ""
    topic: str = ""
    payload: Optional[T] = None
    timestamp: float = field(default_factory=time.time)
    priority: EventPriority = EventPriority.NORMAL
    headers: dict[str, str] = field(default_factory=dict)
    retry_count: int = 0
    max_retries: int = 3
    correlation_id: Optional[str] = None
    reply_to: Optional[str] = None

    def ack(self) -> bool:
        """Acknowledge successful processing."""
        return True

    def nack(self, reason: str = "") -> None:
        """Negative acknowledge - mark for retry or dead letter."""
        self.retry_count += 1
        if self.retry_count >= self.max_retries:
            raise DeadLetterError(f"Event {self.id} exceeded max retries: {reason}")

    def is_expired(self, ttl_seconds: float) -> bool:
        """Check if event has exceeded time-to-live."""
        return (time.time() - self.timestamp) > ttl_seconds


class DeadLetterError(Exception):
    """Raised when event is sent to dead letter queue."""
    pass


class Subscriber(Generic[T]):
    """Event subscriber with callback and filters."""

    def __init__(
        self,
        callback: Callable[[Event[T]], None],
        topic: str = "*",
        filter_fn: Optional[Callable[[Event[T]], bool]] = None,
        priority: EventPriority = EventPriority.NORMAL,
    ) -> None:
        self.callback = callback
        self.topic = topic
        self.filter_fn = filter_fn
        self.priority = priority
        self.active = True
        self._stats = {"received": 0, "processed": 0, "errors": 0}

    def __call__(self, event: Event[T]) -> None:
        """Handle an event if it passes filters."""
        if not self.active:
            return
        if self.topic != "*" and self.topic != event.topic:
            return
        if self.filter_fn is not None and not self.filter_fn(event):
            return
        self._stats["received"] += 1
        try:
            self.callback(event)
            self._stats["processed"] += 1
        except Exception as e:
            self._stats["errors"] += 1
            raise

    def stats(self) -> dict[str, int]:
        return dict(self._stats)


class Topic:
    """Named topic for pub/sub routing."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._subscribers: list[Subscriber] = []
        self._lock = threading.RLock()

    def subscribe(self, subscriber: Subscriber) -> None:
        """Add a subscriber to this topic."""
        with self._lock:
            self._subscribers.append(subscriber)
            self._subscribers.sort(key=lambda s: s.priority.value, reverse=True)

    def unsubscribe(self, subscriber: Subscriber) -> bool:
        """Remove a subscriber from this topic."""
        with self._lock:
            try:
                self._subscribers.remove(subscriber)
                return True
            except ValueError:
                return False

    def publish(self, event: Event) -> int:
        """Deliver event to all matching subscribers.

        Returns:
            Number of subscribers that received the event.
        """
        delivered = 0
        with self._lock:
            for sub in self._subscribers:
                if sub.active and (sub.topic == "*" or sub.topic == event.topic):
                    try:
                        sub(event)
                        delivered += 1
                    except Exception:
                        pass
        return delivered


class PubSub:
    """Publisher-subscriber event system."""

    def __init__(self, default_topic: str = "default") -> None:
        self._topics: dict[str, Topic] = {}
        self._default_topic = default_topic
        self._get_or_create_topic(default_topic)
        self._lock = threading.RLock()
        self._stats = {"published": 0, "delivered": 0}

    def _get_or_create_topic(self, name: str) -> Topic:
        if name not in self._topics:
            self._topics[name] = Topic(name)
        return self._topics[name]

    def subscribe(
        self,
        callback: Callable[[Event], None],
        topic: str = "*",
        filter_fn: Optional[Callable[[Event], bool]] = None,
        priority: EventPriority = EventPriority.NORMAL,
    ) -> Subscriber:
        """Subscribe to events on a topic.

        Args:
            callback: Function to call when event is received.
            topic: Topic name (use * for all topics).
            filter_fn: Optional filter to apply before callback.
            priority: Higher priority subscribers receive events first.

        Returns:
            Subscriber instance that can be used to unsubscribe.
        """
        sub = Subscriber(callback=callback, topic=topic, filter_fn=filter_fn, priority=priority)
        topic_obj = self._get_or_create_topic(topic)
        topic_obj.subscribe(sub)
        return sub

    def unsubscribe(self, subscriber: Subscriber) -> bool:
        """Unsubscribe a previously registered subscriber."""
        with self._lock:
            for t in self._topics.values():
                if t.unsubscribe(subscriber):
                    return True
        return False

    def publish(self, event: Event) -> int:
        """Publish an event to its topic.

        Returns:
            Number of subscribers that received the event.
        """
        with self._lock:
            self._stats["published"] += 1
            topic = self._get_or_create_topic(event.topic)
            delivered = topic.publish(event)
            self._stats["delivered"] += delivered
        return delivered

    def create_topic(self, name: str) -> Topic:
        """Create a new topic."""
        with self._lock:
            return self._get_or_create_topic(name)

    def stats(self) -> dict[str, Any]:
        """Return pub/sub statistics."""
        with self._lock:
            return {
                "topics": len(self._topics),
                "published": self._stats["published"],
                "delivered": self._stats["delivered"],
                "topic_stats": {
                    name: len(t._subscribers)
                    for name, t in self._topics.items()
                },
            }


class DeadLetterQueue:
    """Queue for events that fail processing."""

    def __init__(self, max_size: int = 1000) -> None:
        self.max_size = max_size
        self._queue: deque[Event] = deque(maxlen=max_size)
        self._lock = threading.Lock()
        self._stats = {"received": 0, "replayed": 0}

    def put(self, event: Event, reason: str = "") -> None:
        """Add failed event to dead letter queue."""
        with self._lock:
            event.headers["dlq_reason"] = reason
            self._queue.append(event)
            self._stats["received"] += 1

    def get(self, timeout: Optional[float] = None) -> Optional[Event]:
        """Retrieve a failed event for reprocessing."""
        with self._lock:
            if self._queue:
                return self._queue.popleft()
        return None

    def peek(self) -> Optional[Event]:
        """Peek at next dead letter without removing."""
        with self._lock:
            if self._queue:
                return self._queue[0]
        return None

    def replay(self, count: int = 1) -> list[Event]:
        """Replay specified number of dead letter events.

        Returns:
            List of events removed for replay.
        """
        events = []
        with self._lock:
            for _ in range(min(count, len(self._queue))):
                events.append(self._queue.popleft())
                self._stats["replayed"] += 1
        return events

    def size(self) -> int:
        """Current number of events in DLQ."""
        with self._lock:
            return len(self._queue)

    def clear(self) -> int:
        """Clear all events from DLQ.

        Returns:
            Number of events removed.
        """
        with self._lock:
            count = len(self._queue)
            self._queue.clear()
            return count


class Router:
    """Event router with pattern-based routing rules."""

    def __init__(self, pubsub: PubSub) -> None:
        self._pubsub = pubsub
        self._rules: dict[str, list[str]] = defaultdict(list)
        self._lock = threading.RLock()

    def add_rule(self, source_topic: str, dest_topic: str) -> None:
        """Add routing rule from source to destination topic."""
        with self._lock:
            self._rules[source_topic].append(dest_topic)

    def remove_rule(self, source_topic: str, dest_topic: str) -> bool:
        """Remove a routing rule."""
        with self._lock:
            try:
                self._rules[source_topic].remove(dest_topic)
                return True
            except ValueError:
                return False

    def route(self, event: Event) -> int:
        """Route an event to all matching destinations.

        Returns:
            Total number of deliveries across all destinations.
        """
        with self._lock:
            destinations = self._rules.get(event.topic, [])[:]
        total = 0
        for dest in destinations:
            routed_event = Event(
                type=event.type,
                topic=dest,
                payload=event.payload,
                headers=dict(event.headers),
                correlation_id=event.id,
            )
            total += self._pubsub.publish(routed_event)
        return total


class EventQueue:
    """Async event processing queue with consumer groups."""

    def __init__(
        self,
        name: str = "event_queue",
        max_size: int = 1000,
        consumer_count: int = 1,
    ) -> None:
        self.name = name
        self.max_size = max_size
        self._queue: deque[Event] = deque(maxlen=max_size)
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)
        self._closed = False
        self._consumer_threads: list[threading.Thread] = []
        self._consumer_count = consumer_count
        self._running = False
        self._stats = {"enqueued": 0, "dequeued": 0, "processed": 0}

    def enqueue(self, event: Event, timeout: Optional[float] = None) -> bool:
        """Add event to queue.

        Returns:
            True if enqueued, False if timeout/full.
        """
        with self._not_full:
            while len(self._queue) >= self.max_size and not self._closed:
                if not self._not_full.wait(timeout=timeout):
                    return False
            if self._closed:
                return False
            self._queue.append(event)
            self._stats["enqueued"] += 1
            self._not_empty.notify()
            return True

    def dequeue(self, timeout: Optional[float] = None) -> Optional[Event]:
        """Remove and return next event from queue."""
        with self._not_empty:
            while len(self._queue) == 0 and not self._closed:
                if not self._not_empty.wait(timeout=timeout):
                    return None
            if self._closed and len(self._queue) == 0:
                return None
            event = self._queue.popleft()
            self._stats["dequeued"] += 1
            self._not_full.notify()
            return event

    def start_consumers(
        self,
        handler: Callable[[Event], None],
    ) -> None:
        """Start background consumer threads."""
        self._running = True
        for i in range(self._consumer_count):
            t = threading.Thread(target=self._consume_loop, args=(handler, i), daemon=True)
            t.start()
            self._consumer_threads.append(t)

    def _consume_loop(self, handler: Callable[[Event], None], consumer_id: int) -> None:
        """Consumer loop running in background thread."""
        while self._running:
            event = self.dequeue(timeout=1.0)
            if event is None:
                continue
            try:
                handler(event)
                self._stats["processed"] += 1
            except Exception:
                pass

    def stop_consumers(self) -> None:
        """Signal consumers to stop."""
        self._running = False
        for t in self._consumer_threads:
            t.join(timeout=2.0)
        self._consumer_threads.clear()

    def close(self) -> None:
        """Close queue and wake all waiters."""
        self._closed = True
        self._not_empty.notify_all()
        self._not_full.notify_all()
        self.stop_consumers()

    def stats(self) -> dict[str, Any]:
        """Return queue statistics."""
        with self._lock:
            return {
                "name": self.name,
                "size": len(self._queue),
                "max_size": self.max_size,
                "enqueued": self._stats["enqueued"],
                "dequeued": self._stats["dequeued"],
                "processed": self._stats["processed"],
                "closed": self._closed,
            }


class EventBus:
    """Central event bus combining pub/sub and queueing."""

    def __init__(self) -> None:
        self._pubsub = PubSub()
        self._dlq = DeadLetterQueue()
        self._queues: dict[str, EventQueue] = {}
        self._lock = threading.RLock()

    def subscribe(
        self,
        callback: Callable[[Event], None],
        topic: str = "*",
    ) -> Subscriber:
        """Subscribe to events on a topic."""
        return self._pubsub.subscribe(callback, topic)

    def publish(self, event: Event) -> int:
        """Publish event to its topic."""
        try:
            return self._pubsub.publish(event)
        except Exception as e:
            self._dlq.put(event, reason=str(e))
            return 0

    def create_queue(self, name: str, max_size: int = 1000) -> EventQueue:
        """Create a named event queue."""
        with self._lock:
            q = EventQueue(name=name, max_size=max_size)
            self._queues[name] = q
            return q

    def get_queue(self, name: str) -> Optional[EventQueue]:
        """Get a queue by name."""
        with self._lock:
            return self._queues.get(name)

    def get_dlq(self) -> DeadLetterQueue:
        """Get the dead letter queue."""
        return self._dlq

    def stats(self) -> dict[str, Any]:
        """Return comprehensive event bus stats."""
        return {
            "pubsub": self._pubsub.stats(),
            "dlq": self._dlq.size(),
            "queues": {name: q.stats() for name, q in self._queues.items()},
        }


def create_event(
    topic: str,
    payload: Any = None,
    priority: EventPriority = EventPriority.NORMAL,
    **kwargs: Any,
) -> Event:
    """Factory to create a new event."""
    return Event(topic=topic, payload=payload, priority=priority, **kwargs)


def subscribe(
    bus: EventBus,
    topic: str,
    callback: Callable[[Event], None],
) -> Subscriber:
    """Subscribe callback to topic on event bus."""
    return bus.subscribe(callback, topic)


def publish(bus: EventBus, event: Event) -> int:
    """Publish event on event bus."""
    return bus.publish(event)

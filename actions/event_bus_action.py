"""
Event Bus Action Module.

Provides pub/sub messaging with topic filtering, dead letter handling,
and async event processing.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional
import uuid
import time


class EventPriority(Enum):
    """Event priority levels."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class Event:
    """Event message."""
    id: str
    topic: str
    data: Any
    priority: EventPriority = EventPriority.NORMAL
    timestamp: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class Subscription:
    """Event subscription."""
    id: str
    topic: str
    handler: Callable[[Event], Any]
    filter_func: Optional[Callable[[Event], bool]] = None
    auto_ack: bool = True


@dataclass
class DeadLetterEvent:
    """Event that failed processing."""
    event: Event
    error: Exception
    failed_at: float = field(default_factory=time.time)
    handler_id: Optional[str] = None


class TopicMatcher:
    """Matches event topics with wildcard support."""

    def __init__(self, pattern: str):
        self.pattern = pattern
        self._parts = pattern.split(".")

    def matches(self, topic: str) -> bool:
        """Check if topic matches pattern."""
        topic_parts = topic.split(".")
        return self._match_parts(self._parts, topic_parts)

    def _match_parts(self, pattern_parts: list[str], topic_parts: list[str]) -> bool:
        """Recursively match parts."""
        if not pattern_parts and not topic_parts:
            return True
        if not pattern_parts:
            return True
        if not topic_parts:
            return False

        pattern = pattern_parts[0]
        if pattern == "#":
            return True
        if pattern == "*":
            return self._match_parts(pattern_parts[1:], topic_parts[1:])

        if pattern != topic_parts[0]:
            return False

        return self._match_parts(pattern_parts[1:], topic_parts[1:])


class EventBus:
    """Pub/sub event bus."""

    def __init__(self):
        self._subscriptions: dict[str, list[Subscription]] = {}
        self._lock = asyncio.Lock()
        self._dead_letters: list[DeadLetterEvent] = []
        self._event_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._processing = False
        self._handlers: dict[str, asyncio.Task] = {}

    async def subscribe(
        self,
        topic: str,
        handler: Callable[[Event], Any],
        filter_func: Optional[Callable[[Event], bool]] = None,
        auto_ack: bool = True
    ) -> str:
        """Subscribe to a topic."""
        sub_id = str(uuid.uuid4())
        subscription = Subscription(
            id=sub_id,
            topic=topic,
            handler=handler,
            filter_func=filter_func,
            auto_ack=auto_ack
        )

        async with self._lock:
            if topic not in self._subscriptions:
                self._subscriptions[topic] = []
            self._subscriptions[topic].append(subscription)

        return sub_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe from topic."""
        async with self._lock:
            for topic, subs in self._subscriptions.items():
                for sub in subs:
                    if sub.id == subscription_id:
                        subs.remove(sub)
                        return True
        return False

    async def publish(
        self,
        topic: str,
        data: Any,
        priority: EventPriority = EventPriority.NORMAL,
        metadata: Optional[dict] = None
    ) -> str:
        """Publish event to topic."""
        event = Event(
            id=str(uuid.uuid4()),
            topic=topic,
            data=data,
            priority=priority,
            metadata=metadata or {}
        )

        await self._event_queue.put((
            -priority.value,
            event.timestamp,
            event
        ))

        return event.id

    async def _process_event(self, event: Event) -> None:
        """Process single event."""
        async with self._lock:
            matching_topics = [
                t for t in self._subscriptions.keys()
                if TopicMatcher(t).matches(event.topic)
            ]

        for topic in matching_topics:
            subs = self._subscriptions.get(topic, [])
            for sub in subs:
                if sub.filter_func and not sub.filter_func(event):
                    continue

                try:
                    if asyncio.iscoroutinefunction(sub.handler):
                        await sub.handler(event)
                    else:
                        await asyncio.to_thread(sub.handler, event)
                except Exception as e:
                    if event.retry_count < event.max_retries:
                        event.retry_count += 1
                        await self._event_queue.put((
                            -event.priority.value,
                            event.timestamp,
                            event
                        ))
                    else:
                        self._dead_letters.append(DeadLetterEvent(
                            event=event,
                            error=e,
                            handler_id=sub.id
                        ))

    async def _process_queue(self) -> None:
        """Process event queue."""
        while self._processing:
            try:
                _, _, event = await asyncio.wait_for(
                    self._event_queue.get(),
                    timeout=1.0
                )
                await self._process_event(event)
            except asyncio.TimeoutError:
                continue
            except Exception:
                pass

    async def start(self) -> None:
        """Start event bus processing."""
        self._processing = True
        for _ in range(5):
            task = asyncio.create_task(self._process_queue())
            self._handlers[task.get_name()] = task

    async def stop(self) -> None:
        """Stop event bus processing."""
        self._processing = False
        for task in self._handlers.values():
            task.cancel()
        self._handlers.clear()

    def get_dead_letters(self) -> list[DeadLetterEvent]:
        """Get dead letter events."""
        return self._dead_letters.copy()

    def clear_dead_letters(self) -> None:
        """Clear dead letter events."""
        self._dead_letters.clear()


class EventBusAction:
    """
    Event bus for pub/sub messaging.

    Example:
        bus = EventBusAction()

        await bus.subscribe("user.*", handle_user_event)
        await bus.publish("user.created", {"user_id": 123})

        await bus.start()
    """

    def __init__(self):
        self._bus = EventBus()

    async def subscribe(
        self,
        topic: str,
        handler: Callable[[Event], Any],
        filter_func: Optional[Callable[[Event], bool]] = None
    ) -> str:
        """Subscribe to topic."""
        return await self._bus.subscribe(topic, handler, filter_func)

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe."""
        return await self._bus.unsubscribe(subscription_id)

    async def publish(
        self,
        topic: str,
        data: Any,
        priority: EventPriority = EventPriority.NORMAL
    ) -> str:
        """Publish event."""
        return await self._bus.publish(topic, data, priority)

    async def start(self) -> None:
        """Start processing."""
        await self._bus.start()

    async def stop(self) -> None:
        """Stop processing."""
        await self._bus.stop()

    def get_dead_letters(self) -> list[DeadLetterEvent]:
        """Get failed events."""
        return self._bus.get_dead_letters()

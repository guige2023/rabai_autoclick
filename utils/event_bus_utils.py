"""
Event bus for pub/sub messaging between components.

Supports synchronous and asynchronous event handling, filtering, and routing.
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Awaitable, Callable


class EventPriority(Enum):
    """Priority levels for event handlers."""

    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class Event:
    """Base event object."""

    type: str
    data: Any = None
    timestamp: float = field(default_factory=time.time)
    source: str = "unknown"
    priority: EventPriority = EventPriority.NORMAL


@dataclass
class Subscription:
    """Represents an event subscription."""

    event_type: str
    handler: Callable[..., Any]
    priority: EventPriority
    pattern: re.Pattern | None = None
    async_handler: bool = False


class EventBus:
    """
    Central event bus for publish/subscribe messaging.

    Example:
        bus = EventBus()
        bus.subscribe("click", on_click, priority=EventPriority.HIGH)
        bus.publish(Event("click", {"x": 100, "y": 200}))
    """

    def __init__(self, max_queue_size: int = 1000) -> None:
        self._subscriptions: dict[str, list[Subscription]] = {}
        self._pattern_subscriptions: list[Subscription] = []
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=max_queue_size)
        self._running = False
        self._handlers: list[asyncio.Task[None]] = []

    def subscribe(
        self,
        event_type: str,
        handler: Callable[..., Any],
        priority: EventPriority = EventPriority.NORMAL,
        pattern: str | None = None,
    ) -> Subscription:
        """Subscribe to events of a specific type or pattern."""
        compiled_pattern = re.compile(pattern) if pattern else None
        async_handler = asyncio.iscoroutinefunction(handler)

        sub = Subscription(
            event_type=event_type,
            handler=handler,
            priority=priority,
            pattern=compiled_pattern,
            async_handler=async_handler,
        )

        if pattern:
            self._pattern_subscriptions.append(sub)
            self._pattern_subscriptions.sort(key=lambda s: s.priority.value, reverse=True)
        else:
            if event_type not in self._subscriptions:
                self._subscriptions[event_type] = []
            self._subscriptions[event_type].append(sub)
            self._subscriptions[event_type].sort(
                key=lambda s: s.priority.value, reverse=True
            )

        return sub

    def unsubscribe(self, subscription: Subscription) -> bool:
        """Remove a subscription."""
        if subscription.pattern:
            try:
                self._pattern_subscriptions.remove(subscription)
                return True
            except ValueError:
                return False
        else:
            subs = self._subscriptions.get(subscription.event_type, [])
            try:
                subs.remove(subscription)
                return True
            except ValueError:
                return False

    def publish(self, event: Event) -> None:
        """Publish an event synchronously to all matching subscribers."""
        direct_subs = self._subscriptions.get(event.type, [])

        for sub in direct_subs:
            self._dispatch(sub, event)

        for sub in self._pattern_subscriptions:
            if sub.pattern and sub.pattern.match(event.type):
                self._dispatch(sub, event)

    async def publish_async(self, event: Event) -> None:
        """Publish an event asynchronously via queue."""
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            raise RuntimeError(f"Event queue full, cannot publish {event.type}")

    async def start(self) -> None:
        """Start the async event processor."""
        self._running = True
        self._handlers = [asyncio.create_task(self._process_events())]

    async def stop(self) -> None:
        """Stop the async event processor."""
        self._running = False
        for handler in self._handlers:
            handler.cancel()
        await asyncio.gather(*self._handlers, return_exceptions=True)
        self._handlers.clear()

    async def _process_events(self) -> None:
        """Process events from the queue."""
        while self._running:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                self.publish(event)
            except asyncio.TimeoutError:
                continue
            except Exception:  # noqa: BLE001
                pass

    def _dispatch(self, sub: Subscription, event: Event) -> None:
        """Dispatch an event to a subscription handler."""
        try:
            if sub.async_handler:
                asyncio.create_task(self._safe_dispatch_async(sub, event))
            else:
                sub.handler(event)
        except Exception:  # noqa: BLE001
            pass

    async def _safe_dispatch_async(self, sub: Subscription, event: Event) -> None:
        """Safely dispatch to an async handler."""
        try:
            result = sub.handler(event)
            if asyncio.iscoroutine(result):
                await result
        except Exception:  # noqa: BLE001
            pass


class EventBusBuilder:
    """Fluent builder for EventBus configuration."""

    def __init__(self) -> None:
        self._max_queue_size = 1000
        self._middleware: list[Callable[[Event], Event | None]] = []

    def max_queue_size(self, size: int) -> EventBusBuilder:
        """Set maximum async queue size."""
        self._max_queue_size = size
        return self

    def add_middleware(
        self, middleware: Callable[[Event], Event | None]
    ) -> EventBusBuilder:
        """Add middleware that can filter or transform events."""
        self._middleware.append(middleware)
        return self

    def build(self) -> EventBus:
        """Build the configured EventBus."""
        bus = EventBus(max_queue_size=self._max_queue_size)
        return bus

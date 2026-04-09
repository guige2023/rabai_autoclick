"""Event Bus Action Module.

Publish-subscribe event bus for decoupling components.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Generic, TypeVar
import uuid

T = TypeVar("T")


class EventPriority(Enum):
    """Event handler priority."""
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class Event:
    """Base event class."""
    event_id: str
    event_type: str
    timestamp: datetime
    payload: dict[str, Any] = field(default_factory=dict)
    source: str | None = None


@dataclass
class EventSubscription:
    """Event subscription details."""
    subscription_id: str
    event_type: str
    handler: Callable[[Event], Any]
    priority: EventPriority
    filter_func: Callable[[Event], bool] | None = None
    async_handler: bool = False


class EventHandler(ABC):
    """Abstract event handler."""

    @abstractmethod
    async def handle(self, event: Event) -> None:
        """Handle an event."""
        pass


class EventBus:
    """Central event bus for publish-subscribe."""

    def __init__(self) -> None:
        self._subscriptions: dict[str, list[EventSubscription]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self._event_history: list[Event] = []
        self._max_history: int = 1000

    async def subscribe(
        self,
        event_type: str,
        handler: Callable[[Event], Any],
        priority: EventPriority = EventPriority.NORMAL,
        filter_func: Callable[[Event], bool] | None = None,
    ) -> str:
        """Subscribe to an event type. Returns subscription ID."""
        sub_id = str(uuid.uuid4())
        is_async = asyncio.iscoroutinefunction(handler)
        sub = EventSubscription(
            subscription_id=sub_id,
            event_type=event_type,
            handler=handler,
            priority=priority,
            filter_func=filter_func,
            async_handler=is_async
        )
        async with self._lock:
            self._subscriptions[event_type].append(sub)
            self._subscriptions[event_type].sort(key=lambda s: s.priority.value)
        return sub_id

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unsubscribe by subscription ID."""
        async with self._lock:
            for event_type, subs in self._subscriptions.items():
                self._subscriptions[event_type] = [
                    s for s in subs if s.subscription_id != subscription_id
                ]
                if not self._subscriptions[event_type]:
                    del self._subscriptions[event_type]
            return True

    async def publish(self, event_type: str, payload: dict[str, Any] | None = None, source: str | None = None) -> Event:
        """Publish an event to all subscribers."""
        event = Event(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            timestamp=datetime.now(timezone.utc),
            payload=payload or {},
            source=source
        )
        async with self._lock:
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history = self._event_history[-self._max_history:]
            subs = list(self._subscriptions.get(event_type, []))
        for sub in subs:
            if sub.filter_func and not sub.filter_func(event):
                continue
            if sub.async_handler:
                asyncio.create_task(sub.handler(event))
            else:
                try:
                    result = sub.handler(event)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    pass
        return event

    async def publish_async(self, event_type: str, payload: dict[str, Any] | None = None, source: str | None = None) -> Event:
        """Publish and wait for all handlers."""
        event = await self.publish(event_type, payload, source)
        await asyncio.sleep(0)
        return event

    def get_history(self, event_type: str | None = None, limit: int = 100) -> list[Event]:
        """Get event history."""
        if event_type:
            return [e for e in self._event_history if e.event_type == event_type][-limit:]
        return self._event_history[-limit:]


_global_event_bus: EventBus | None = None


def get_event_bus() -> EventBus:
    """Get the global event bus instance."""
    global _global_event_bus
    if _global_event_bus is None:
        _global_event_bus = EventBus()
    return _global_event_bus

"""Event bus utilities for pub/sub messaging and event-driven architecture."""

from __future__ import annotations

import json
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable
from uuid import uuid4

__all__ = ["Event", "EventBus", "EventHandler", "event_bus"]


class EventPriority(Enum):
    LOW = 0
    NORMAL = 1
    HIGH = 2


@dataclass
class Event:
    """An event object."""
    id: str = field(default_factory=lambda: str(uuid4()))
    type: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    source: str = ""
    timestamp: float = field(default_factory=time.time)
    correlation_id: str | None = None
    priority: EventPriority = EventPriority.NORMAL

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "data": self.data,
            "source": self.source,
            "timestamp": self.timestamp,
            "correlation_id": self.correlation_id,
            "priority": self.priority.name,
        }

    def __repr__(self) -> str:
        return f"Event(type={self.type!r}, id={self.id[:8]})"


@dataclass
class EventHandler:
    """Registered event handler."""
    callback: Callable[[Event], Any]
    event_type: str
    priority: EventPriority = EventPriority.NORMAL
    async_mode: bool = False
    filter_fn: Callable[[Event], bool] | None = None


class EventBus:
    """Thread-safe in-memory event bus with filtering and async support."""

    def __init__(self) -> None:
        self._handlers: dict[str, list[EventHandler]] = defaultdict(list)
        self._lock = threading.RLock()
        self._event_history: list[Event] = []
        self._max_history = 1000
        self._middleware: list[Callable[[Event], Event | None]] = []
        self._global_handlers: list[EventHandler] = []
        self._stats = {"published": 0, "delivered": 0, "filtered": 0}

    def subscribe(
        self,
        event_type: str,
        handler: Callable[[Event], Any],
        priority: EventPriority = EventPriority.NORMAL,
        filter_fn: Callable[[Event], bool] | None = None,
    ) -> EventHandler:
        """Subscribe a handler to an event type."""
        eh = EventHandler(
            callback=handler,
            event_type=event_type,
            priority=priority,
            filter_fn=filter_fn,
        )
        with self._lock:
            self._handlers[event_type].append(eh)
            self._handlers[event_type].sort(key=lambda h: h.priority.value, reverse=True)
        return eh

    def subscribe_any(
        self,
        handler: Callable[[Event], Any],
        priority: EventPriority = EventPriority.NORMAL,
    ) -> EventHandler:
        """Subscribe to all events regardless of type."""
        eh = EventHandler(callback=handler, event_type="*", priority=priority)
        with self._lock:
            self._global_handlers.append(eh)
            self._global_handlers.sort(key=lambda h: h.priority.value, reverse=True)
        return eh

    def unsubscribe(self, handler: EventHandler) -> bool:
        """Unsubscribe a handler."""
        with self._lock:
            if handler.event_type == "*":
                if handler in self._global_handlers:
                    self._global_handlers.remove(handler)
                    return True
                return False
            if handler.event_type in self._handlers:
                if handler in self._handlers[handler.event_type]:
                    self._handlers[handler.event_type].remove(handler)
                    return True
        return False

    def publish(self, event: Event) -> int:
        """Publish an event to all subscribers. Returns delivery count."""
        self._stats["published"] += 1

        with self._lock:
            self._event_history.append(event)
            if len(self._event_history) > self._max_history:
                self._event_history.pop(0)

        for mw in self._middleware:
            result = mw(event)
            if result is None:
                self._stats["filtered"] += 1
                return 0
            if result != event:
                event = result

        delivered = 0
        handlers: list[EventHandler] = []
        with self._lock:
            handlers.extend(self._handlers.get(event.type, []))
            handlers.extend(self._global_handlers)

        for handler in handlers:
            if handler.filter_fn and not handler.filter_fn(event):
                continue
            try:
                if handler.async_mode:
                    thread = threading.Thread(target=handler.callback, args=(event,))
                    thread.start()
                else:
                    handler.callback(event)
                delivered += 1
                self._stats["delivered"] += 1
            except Exception:
                pass

        return delivered

    def emit(self, event_type: str, data: dict[str, Any] | None = None, **kwargs: Any) -> int:
        """Convenience method to emit an event."""
        event = Event(type=event_type, data=data or {}, **kwargs)
        return self.publish(event)

    def add_middleware(self, middleware: Callable[[Event], Event | None]) -> None:
        self._middleware.append(middleware)

    def history(
        self,
        event_type: str | None = None,
        limit: int = 100,
    ) -> list[Event]:
        with self._lock:
            history = self._event_history
            if event_type:
                history = [e for e in history if e.type == event_type]
            return list(reversed(history[-limit:]))

    def stats(self) -> dict[str, int]:
        return dict(self._stats)

    def clear_history(self) -> None:
        with self._lock:
            self._event_history.clear()


event_bus = EventBus()

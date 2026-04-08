"""
Observer pattern implementation for event handling.

Provides a flexible event system with support for
synchronous and asynchronous event handling, filtering, and priority.

Example:
    >>> from utils.observer_utils_v2 import EventEmitter, on
    >>> emitter = EventEmitter()
    >>> emitter.on("click", lambda data: print(data))
    >>> emitter.emit("click", {"x": 100})
"""

from __future__ import annotations

import asyncio
import functools
import threading
import uuid
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Union


@dataclass
class Event:
    """Event object passed to handlers."""
    name: str
    data: Any
    timestamp: float
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))


class EventHandler:
    """Base class for event handlers."""

    def __init__(
        self,
        callback: Callable,
        once: bool = False,
        priority: int = 0,
        filter_fn: Optional[Callable[[Event], bool]] = None,
    ) -> None:
        """
        Initialize the event handler.

        Args:
            callback: Function to call when event is emitted.
            once: If True, handler is called only once.
            priority: Handler priority (higher = called first).
            filter_fn: Optional function to filter events.
        """
        self.callback = callback
        self.once = once
        self.priority = priority
        self.filter_fn = filter_fn
        self.id = str(uuid.uuid4())
        self._called = False

    def __call__(self, event: Event) -> Any:
        """Execute the handler."""
        if self._called and self.once:
            return None

        if self.filter_fn and not self.filter_fn(event):
            return None

        self._called = True
        return self.callback(event)


class EventEmitter:
    """
    Event emitter for publish-subscribe messaging.

    Supports multiple event types, wildcard listeners,
    and async event handling.

    Attributes:
        async_mode: If True, emit events asynchronously.
    """

    def __init__(self, async_mode: bool = False) -> None:
        """
        Initialize the event emitter.

        Args:
            async_mode: If True, emit events asynchronously.
        """
        self.async_mode = async_mode
        self._handlers: Dict[str, List[EventHandler]] = defaultdict(list)
        self._wildcard_handlers: List[EventHandler] = []
        self._lock = threading.RLock()

    def on(
        self,
        event_name: str,
        callback: Callable[[Event], Any],
        once: bool = False,
        priority: int = 0,
        filter_fn: Optional[Callable[[Event], bool]] = None,
    ) -> str:
        """
        Register an event handler.

        Args:
            event_name: Name of the event to listen for.
            callback: Function to call when event is emitted.
            once: If True, handler is called only once.
            priority: Handler priority.
            filter_fn: Optional event filter function.

        Returns:
            Handler ID for later removal.
        """
        handler = EventHandler(
            callback=callback,
            once=once,
            priority=priority,
            filter_fn=filter_fn,
        )

        with self._lock:
            self._handlers[event_name].append(handler)
            self._handlers[event_name].sort(key=lambda h: h.priority, reverse=True)

        return handler.id

    def once(
        self,
        event_name: str,
        callback: Callable[[Event], Any],
        priority: int = 0,
    ) -> str:
        """
        Register a handler that is called only once.

        Args:
            event_name: Event name.
            callback: Function to call.
            priority: Handler priority.

        Returns:
            Handler ID.
        """
        return self.on(event_name, callback, once=True, priority=priority)

    def on_wildcard(
        self,
        callback: Callable[[Event], Any],
        once: bool = False,
        priority: int = 0,
    ) -> str:
        """
        Register a handler for all events.

        Args:
            callback: Function to call.
            once: If True, called only once.
            priority: Handler priority.

        Returns:
            Handler ID.
        """
        handler = EventHandler(
            callback=callback,
            once=once,
            priority=priority,
        )

        with self._lock:
            self._wildcard_handlers.append(handler)
            self._wildcard_handlers.sort(key=lambda h: h.priority, reverse=True)

        return handler.id

    def off(self, handler_id: str) -> bool:
        """
        Remove an event handler by ID.

        Args:
            handler_id: ID returned by on().

        Returns:
            True if handler was found and removed.
        """
        with self._lock:
            for handlers in self._handlers.values():
                for i, handler in enumerate(handlers):
                    if handler.id == handler_id:
                        handlers.pop(i)
                        return True

            for i, handler in enumerate(self._wildcard_handlers):
                if handler.id == handler_id:
                    self._wildcard_handlers.pop(i)
                    return True

        return False

    def off_all(self, event_name: Optional[str] = None) -> None:
        """
        Remove all handlers, or all handlers for a specific event.

        Args:
            event_name: Optional event name to clear.
        """
        with self._lock:
            if event_name:
                self._handlers.pop(event_name, None)
            else:
                self._handlers.clear()
                self._wildcard_handlers.clear()

    def emit(
        self,
        event_name: str,
        data: Any = None,
    ) -> List[Any]:
        """
        Emit an event to all registered handlers.

        Args:
            event_name: Name of the event.
            data: Event data.

        Returns:
            List of handler return values.
        """
        import time

        event = Event(
            name=event_name,
            data=data,
            timestamp=time.time(),
        )

        results: List[Any] = []

        with self._lock:
            handlers = list(self._handlers.get(event_name, []))
            wildcards = list(self._wildcard_handlers)

        for handler in handlers:
            try:
                result = handler(event)
                if result is not None:
                    results.append(result)
            except Exception:
                pass

        for handler in wildcards:
            try:
                result = handler(event)
                if result is not None:
                    results.append(result)
            except Exception:
                pass

        return results

    async def emit_async(
        self,
        event_name: str,
        data: Any = None,
    ) -> List[Any]:
        """
        Emit an event asynchronously.

        Args:
            event_name: Name of the event.
            data: Event data.

        Returns:
            List of handler return values.
        """
        import time

        event = Event(
            name=event_name,
            data=data,
            timestamp=time.time(),
        )

        results: List[Any] = []

        with self._lock:
            handlers = list(self._handlers.get(event_name, []))
            wildcards = list(self._wildcard_handlers)

        async def call_handler(handler: EventHandler) -> Any:
            if asyncio.iscoroutinefunction(handler.callback):
                return await handler.callback(event)
            return handler.callback(event)

        tasks = [call_handler(h) for h in handlers + wildcards]

        for coro in asyncio.as_completed(tasks):
            try:
                result = await coro
                if result is not None:
                    results.append(result)
            except Exception:
                pass

        return results

    def listeners(self, event_name: str) -> int:
        """
        Get the number of handlers for an event.

        Args:
            event_name: Event name.

        Returns:
            Number of registered handlers.
        """
        with self._lock:
            return len(self._handlers.get(event_name, []))

    def event_names(self) -> List[str]:
        """
        Get all event names with registered handlers.

        Returns:
            List of event names.
        """
        with self._lock:
            return list(self._handlers.keys())


class Observable(ABC):
    """
    Abstract base class for observable objects.

    Subclass this to make an object observable.
    """

    def __init__(self) -> None:
        """Initialize the observable."""
        self._emitter = EventEmitter()

    @property
    def emitter(self) -> EventEmitter:
        """Get the event emitter."""
        return self._emitter

    def notify(self, event_name: str, data: Any = None) -> None:
        """
        Notify observers of an event.

        Args:
            event_name: Event name.
            data: Event data.
        """
        self._emitter.emit(event_name, data)


def on(
    event_name: str,
    once: bool = False,
    priority: int = 0,
) -> Callable:
    """
    Decorator to register an event handler.

    Args:
        event_name: Event name.
        once: If True, called only once.
        priority: Handler priority.

    Returns:
        Decorator function.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            if hasattr(self, "_emitter"):
                self._emitter.on(event_name, func, once=once, priority=priority)
            return func(self, *args, **kwargs)
        return wrapper
    return decorator

"""Observer pattern utilities for RabAI AutoClick.

Provides:
- Observable/Observer base classes
- Event emitter
- Weak reference observers
- Async observer support
"""

from __future__ import annotations

import asyncio
import weakref
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Set,
)


EventHandler = Callable[..., Any]


@dataclass
class Event:
    """An event with type and data."""

    type: str
    data: Any = None
    source: Optional[Any] = None


class Observer:
    """Base observer class."""

    def on_event(self, event: Event) -> None:
        """Handle an event."""
        pass


class Observable:
    """Observable subject that notifies observers.

    Example:
        class ClickSubject(Observable):
            pass

        subject = ClickSubject()

        def on_click(event):
            print(f"Clicked: {event.data}")

        subject.subscribe("click", on_click)
        subject.emit(Event("click", data="button"))
    """

    def __init__(self) -> None:
        self._handlers: Dict[str, Set[EventHandler]] = {}
        self._observers: Dict[str, List[Observer]] = {}
        self._lock = asyncio.Lock()

    def subscribe(
        self,
        event_type: str,
        handler: EventHandler,
    ) -> None:
        """Subscribe to an event type.

        Args:
            event_type: Type of event to subscribe to.
            handler: Callback function (event) -> None.
        """
        if event_type not in self._handlers:
            self._handlers[event_type] = set()
        self._handlers[event_type].add(handler)

    def unsubscribe(
        self,
        event_type: str,
        handler: EventHandler,
    ) -> bool:
        """Unsubscribe from an event type.

        Args:
            event_type: Event type to unsubscribe from.
            handler: Handler to remove.

        Returns:
            True if handler was found and removed.
        """
        if event_type not in self._handlers:
            return False
        try:
            self._handlers[event_type].remove(handler)
            return True
        except KeyError:
            return False

    def emit(self, event: Event) -> None:
        """Emit an event to all subscribers.

        Args:
            event: Event to emit.
        """
        if event.type in self._handlers:
            for handler in self._handlers[event.type]:
                try:
                    handler(event)
                except Exception:
                    pass

    def on(
        self,
        event_type: str,
    ) -> Callable[[EventHandler], EventHandler]:
        """Decorator to subscribe to an event type.

        Args:
            event_type: Event type to subscribe to.

        Returns:
            Decorator function.
        """
        def decorator(handler: EventHandler) -> EventHandler:
            self.subscribe(event_type, handler)
            return handler
        return decorator

    def once(
        self,
        event_type: str,
        handler: EventHandler,
    ) -> None:
        """Subscribe to an event type for one emission only.

        Args:
            event_type: Event type.
            handler: Handler function.
        """
        def wrapper(event: Event) -> None:
            handler(event)
            self.unsubscribe(event_type, wrapper)

        self.subscribe(event_type, wrapper)

    def clear(self, event_type: Optional[str] = None) -> None:
        """Clear handlers.

        Args:
            event_type: If provided, clear only this type. Otherwise clear all.
        """
        if event_type:
            self._handlers.pop(event_type, None)
        else:
            self._handlers.clear()

    def handler_count(self, event_type: str) -> int:
        """Get number of handlers for event type."""
        return len(self._handlers.get(event_type, set()))


class AsyncObservable(Observable):
    """Async version of Observable."""

    async def emit(self, event: Event) -> None:
        if event.type in self._handlers:
            for handler in self._handlers[event.type]:
                try:
                    result = handler(event)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception:
                    pass

    async def emit_batch(self, events: List[Event]) -> None:
        """Emit multiple events concurrently."""
        await asyncio.gather(*(self.emit(e) for e in events))


class WeakObservable:
    """Observable that holds weak references to handlers.

    Handlers are automatically removed when they are garbage collected.
    """

    def __init__(self) -> None:
        self._handlers: Dict[str, List[weakref.ref]] = {}

    def subscribe(
        self,
        event_type: str,
        handler: EventHandler,
    ) -> None:
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(weakref.ref(handler))

    def emit(self, event: Event) -> None:
        if event.type in self._handlers:
            dead_refs: List[weakref.ref] = []
            for ref in self._handlers[event.type]:
                handler = ref()
                if handler is None:
                    dead_refs.append(ref)
                    continue
                try:
                    handler(event)
                except Exception:
                    pass
            for ref in dead_refs:
                self._handlers[event.type].remove(ref)


class EventBus:
    """Centralized event bus for application-wide events."""

    _instance: Optional[EventBus] = None

    def __new__(cls) -> EventBus:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._channels: Dict[str, Observable] = {}
        return cls._instance

    def channel(self, name: str) -> Observable:
        """Get or create a named channel.

        Args:
            name: Channel name.

        Returns:
            Observable for that channel.
        """
        if name not in self._channels:
            self._channels[name] = Observable()
        return self._channels[name]

    def emit(self, channel: str, event: Event) -> None:
        """Emit event on a channel."""
        if channel in self._channels:
            self._channels[channel].emit(event)

    def clear(self, channel: Optional[str] = None) -> None:
        """Clear channels."""
        if channel:
            self._channels.pop(channel, None)
        else:
            self._channels.clear()

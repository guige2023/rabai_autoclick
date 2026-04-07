"""observer_action module for rabai_autoclick.

Provides observer pattern implementation: subject/observer,
event notification, and listener management.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Set

__all__ = [
    "Observer",
    "Subject",
    "Event",
    "EventBus",
    "Observable",
    "WeakObserver",
    "ObserverRegistry",
    "notify_observers",
]


T = type("T", (), {})


class Observer:
    """Base observer interface."""

    def update(self, event: "Event") -> None:
        """Called when observed subject changes."""
        raise NotImplementedError


@dataclass
class Event:
    """Event object passed to observers."""
    type: str
    data: Any = None
    source: Any = None
    timestamp: float = 0.0

    def __post_init__(self) -> None:
        if self.timestamp == 0.0:
            import time
            self.timestamp = time.time()


class Subject:
    """Subject that notifies observers of changes."""

    def __init__(self) -> None:
        self._observers: Dict[str, List[Callable]] = defaultdict(list)
        self._lock = threading.RLock()
        self._global_observers: List[Callable] = []

    def attach(
        self,
        event_type: str,
        handler: Callable[[Event], None],
    ) -> None:
        """Attach observer handler for event type.

        Args:
            event_type: Type of events to listen for.
            handler: Callback function.
        """
        with self._lock:
            self._observers[event_type].append(handler)

    def detach(
        self,
        event_type: str,
        handler: Callable[[Event], None],
    ) -> bool:
        """Detach observer handler.

        Returns:
            True if handler was found and removed.
        """
        with self._lock:
            try:
                self._observers[event_type].remove(handler)
                return True
            except ValueError:
                return False

    def attach_global(self, handler: Callable[[Event], None]) -> None:
        """Attach handler for all events."""
        with self._lock:
            self._global_observers.append(handler)

    def detach_global(self, handler: Callable[[Event], None]) -> bool:
        """Detach global handler."""
        with self._lock:
            try:
                self._global_observers.remove(handler)
                return True
            except ValueError:
                return False

    def notify(
        self,
        event_type: str,
        data: Any = None,
        source: Any = None,
    ) -> None:
        """Notify all observers of an event.

        Args:
            event_type: Type of event.
            data: Event data.
            source: Source of event.
        """
        event = Event(type=event_type, data=data, source=source)

        with self._lock:
            handlers = list(self._observers.get(event_type, []))
            global_handlers = list(self._global_observers)

        for handler in handlers:
            try:
                handler(event)
            except Exception:
                pass

        for handler in global_handlers:
            try:
                handler(event)
            except Exception:
                pass

    def clear(self) -> None:
        """Remove all observers."""
        with self._lock:
            self._observers.clear()
            self._global_observers.clear()


class EventBus:
    """Central event bus for pub/sub."""

    def __init__(self) -> None:
        self._handlers: Dict[str, List[Callable]] = defaultdict(list)
        self._global_handlers: List[Callable] = []
        self._lock = threading.RLock()

    def subscribe(
        self,
        event_type: str,
        handler: Callable[[Event], None],
    ) -> None:
        """Subscribe to an event type."""
        with self._lock:
            self._handlers[event_type].append(handler)

    def unsubscribe(
        self,
        event_type: str,
        handler: Callable[[Event], None],
    ) -> bool:
        """Unsubscribe from an event type."""
        with self._lock:
            try:
                self._handlers[event_type].remove(handler)
                return True
            except ValueError:
                return False

    def subscribe_all(self, handler: Callable[[Event], None]) -> None:
        """Subscribe to all events."""
        with self._lock:
            self._global_handlers.append(handler)

    def unsubscribe_all(self, handler: Callable[[Event], None]) -> bool:
        """Unsubscribe from all events."""
        with self._lock:
            try:
                self._global_handlers.remove(handler)
                return True
            except ValueError:
                return False

    def publish(self, event: Event) -> None:
        """Publish an event to all subscribers."""
        with self._lock:
            handlers = list(self._handlers.get(event.type, []))
            global_handlers = list(self._global_handlers)

        for handler in handlers:
            try:
                handler(event)
            except Exception:
                pass

        for handler in global_handlers:
            try:
                handler(event)
            except Exception:
                pass


class Observable(Generic[T]):
    """Observable value that notifies on change."""

    def __init__(self, initial_value: Optional[T] = None) -> None:
        self._value = initial_value
        self._observers: List[Callable[[T, T], None]] = []
        self._lock = threading.Lock()

    @property
    def value(self) -> T:
        """Get current value."""
        with self._lock:
            return self._value

    @value.setter
    def value(self, new_value: T) -> None:
        """Set value and notify observers."""
        with self._lock:
            old_value = self._value
            if old_value != new_value:
                self._value = new_value
                observers = list(self._observers)
        for observer in observers:
            try:
                observer(old_value, new_value)
            except Exception:
                pass

    def observe(self, observer: Callable[[T, T], None]) -> None:
        """Add observer called on value change."""
        with self._lock:
            self._observers.append(observer)

    def unobserve(self, observer: Callable[[T, T], None]) -> bool:
        """Remove observer."""
        with self._lock:
            try:
                self._observers.remove(observer)
                return True
            except ValueError:
                return False


class WeakObserver:
    """Observer that holds weak reference to handler."""

    def __init__(self, handler: Callable[[Event], None]) -> None:
        import weakref
        self._ref = weakref.ref(handler)
        self._handler = handler

    def __call__(self, event: Event) -> None:
        handler = self._ref()
        if handler is not None:
            handler(event)


class ObserverRegistry:
    """Registry for managing observer lifecycle."""

    def __init__(self) -> None:
        self._observers: Dict[str, List[Any]] = defaultdict(list)
        self._lock = threading.Lock()

    def register(
        self,
        owner: Any,
        event_type: str,
        handler: Callable[[Event], None],
        weak: bool = False,
    ) -> None:
        """Register an observer.

        Args:
            owner: Owner of this registration.
            event_type: Event type to listen for.
            handler: Handler function.
            weak: Use weak reference.
        """
        with self._lock:
            if weak:
                observer = WeakObserver(handler)
            else:
                observer = handler
            self._observers[owner].append((event_type, observer))

    def unregister_owner(self, owner: Any) -> int:
        """Unregister all observers for an owner.

        Returns:
            Number of observers removed.
        """
        with self._lock:
            if owner in self._observers:
                count = len(self._observers[owner])
                del self._observers[owner]
                return count
        return 0

    def get_observers(self, owner: Any) -> List[tuple]:
        """Get all observers for an owner."""
        with self._lock:
            return list(self._observers.get(owner, []))


def notify_observers(
    observers: List[Callable],
    event: Event,
) -> None:
    """Notify all observers of an event.

    Args:
        observers: List of observer callables.
        event: Event to notify.
    """
    for observer in observers:
        try:
            observer(event)
        except Exception:
            pass

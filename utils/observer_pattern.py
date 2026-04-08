"""Observer pattern utilities for event-driven automation.

Provides a type-safe observer/subject implementation
for decoupled event handling in automation workflows.

Example:
    >>> from utils.observer_pattern import Subject, Observer, event_handler
    >>> class ClickSubject(Subject):
    ...     def notify_click(self, x, y):
    ...         self.emit('click', x=x, y=y)
    >>> @event_handler('click')
    ... def on_click(x, y):
    ...     print(f"Clicked at {x}, {y}")
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional

__all__ = [
    "Observer",
    "Subject",
    "EventEmitter",
    "event_handler",
    "observable",
]


class Observer(ABC):
    """Abstract base class for observers."""

    @abstractmethod
    def on_notify(self, event: str, data: dict) -> None:
        """Called when the subject emits an event."""
        pass


class Subject:
    """Subject that maintains a list of observers and notifies them.

    Example:
        >>> subject = Subject()
        >>> subject.attach(observer1)
        >>> subject.attach(observer2)
        >>> subject.emit('update', value=42)
    """

    def __init__(self):
        self._observers: List[Observer] = []
        self._lock = __import__("threading").Lock()

    def attach(self, observer: Observer) -> None:
        """Register an observer."""
        with self._lock:
            if observer not in self._observers:
                self._observers.append(observer)

    def detach(self, observer: Observer) -> None:
        """Unregister an observer."""
        with self._lock:
            if observer in self._observers:
                self._observers.remove(observer)

    def detach_all(self) -> None:
        """Unregister all observers."""
        with self._lock:
            self._observers.clear()

    def emit(self, event: str, **data) -> None:
        """Notify all observers of an event."""
        with self._lock:
            observers = list(self._observers)

        for observer in observers:
            try:
                observer.on_notify(event, data)
            except Exception:
                pass

    @property
    def observer_count(self) -> int:
        return len(self._observers)


class EventEmitter:
    """A more flexible event emitter without ABC.

    Example:
        >>> emitter = EventEmitter()
        >>> emitter.on('click', lambda x, y: print(f"Clicked {x},{y}"))
        >>> emitter.emit('click', x=100, y=200)
    """

    def __init__(self):
        self._handlers: Dict[str, List[Callable]] = {}

    def on(self, event: str, handler: Callable) -> "EventEmitter":
        """Register a handler for an event.

        Returns self for chaining.
        """
        self._handlers.setdefault(event, []).append(handler)
        return self

    def off(self, event: str, handler: Optional[Callable] = None) -> None:
        """Unregister a handler (or all handlers for an event)."""
        if handler is None:
            self._handlers.pop(event, None)
        else:
            if event in self._handlers:
                self._handlers[event] = [h for h in self._handlers[event] if h != handler]

    def once(self, event: str, handler: Callable) -> "EventEmitter":
        """Register a handler that fires only once."""
        def wrapper(**data):
            handler(**data)
            self.off(event, wrapper)
        return self.on(event, wrapper)

    def emit(self, event: str, **data) -> None:
        """Emit an event to all registered handlers."""
        for handler in self._handlers.get(event, []):
            try:
                handler(**data)
            except Exception:
                pass

    def handlers(self, event: str) -> List[Callable]:
        """Get all handlers for an event."""
        return list(self._handlers.get(event, []))


def event_handler(event: str) -> Callable:
    """Decorator to register an event handler.

    Example:
        >>> @event_handler('click')
        ... def on_click(x, y):
        ...     print(f"Clicked at {x}, {y}")
    """
    def decorator(fn: Callable) -> Callable:
        if not hasattr(fn, "_event_handlers"):
            fn._event_handlers = []
        fn._event_handlers.append(event)
        return fn
    return decorator


def observable(cls):
    """Class decorator that adds observer capabilities.

    Example:
        >>> @observable
        ... class MyEmitter:
        ...     def do_action(self):
        ...         self.emit('action', value=123)
    """
    original_init = cls.__init__

    def new_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        Subject.__init__(self)

    cls.__init__ = new_init

    # Add emit method if not already present
    if "emit" not in cls.__dict__:
        def emit(self, event: str, **data) -> None:
            Subject.emit(self, event, **data)
        cls.emit = emit

    return cls

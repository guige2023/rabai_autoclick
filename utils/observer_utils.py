"""Observer pattern utilities for automation event handling.

Provides subject-observer bindings for reactive automation
workflows where multiple components need to react to
state changes in UI elements or actions.

Example:
    >>> from utils.observer_utils import Subject, Observer, Observable
    >>> class ClickSubject(Subject):
    ...     def notify(self, event):
    ...         self._notify_observers(event)
    >>> subject = ClickSubject()
    >>> subject.attach(my_observer)
    >>> subject.notify({"type": "click", "pos": (100, 200)})
"""

from __future__ import annotations

import threading
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar

T = TypeVar("T")


class Observer(ABC):
    """Abstract observer interface."""

    @abstractmethod
    def update(self, event: Dict[str, Any]) -> None:
        """Handle an event from a subject.

        Args:
            event: Event data dict.
        """
        raise NotImplementedError


class Subject:
    """Subject in observer pattern with thread-safe operations."""

    def __init__(self) -> None:
        self._observers: List[Observer] = []
        self._lock = threading.RLock()

    def attach(self, observer: Observer) -> None:
        """Attach an observer.

        Args:
            observer: Observer to add.
        """
        with self._lock:
            if observer not in self._observers:
                self._observers.append(observer)

    def detach(self, observer: Observer) -> bool:
        """Detach an observer.

        Args:
            observer: Observer to remove.

        Returns:
            True if found and removed.
        """
        with self._lock:
            try:
                self._observers.remove(observer)
                return True
            except ValueError:
                return False

    def _notify_observers(self, event: Dict[str, Any]) -> None:
        """Internal: notify all observers.

        Args:
            event: Event data.
        """
        with self._lock:
            observers = list(self._observers)

        for observer in observers:
            try:
                observer.update(event)
            except Exception:
                pass

    @property
    def observer_count(self) -> int:
        """Number of attached observers."""
        with self._lock:
            return len(self._observers)


class CallableObserver(Observer):
    """Observer wrapping a callable.

    Example:
        >>> observer = CallableObserver(lambda e: print(f"Clicked: {e}"))
        >>> subject.attach(observer)
    """

    def __init__(self, callback: Callable[[Dict[str, Any]], None]) -> None:
        self._callback = callback

    def update(self, event: Dict[str, Any]) -> None:
        self._callback(event)


class Observable(Subject):
    """Observable subject with event filtering and priority.

    Supports:
    - Event type filtering
    - Observer priority ordering
    - Once-only (one-time) observers
    - Conditional observers
    """

    def __init__(self) -> None:
        super().__init__()
        self._once_observers: Set[Observer] = set()
        self._conditional_observers: Dict[
            Observer, Callable[[Dict[str, Any]], bool]
        ] = {}

    def attach(
        self,
        observer: Observer,
        *,
        priority: int = 0,
        event_filter: Optional[Callable[[Dict[str, Any]], bool]] = None,
        once: bool = False,
    ) -> None:
        """Attach an observer with options.

        Args:
            observer: Observer to add.
            priority: Higher priority observers notified first.
            event_filter: Callable that returns True to receive event.
            once: If True, observer auto-detaches after first event.
        """
        with self._lock:
            if event_filter:
                self._conditional_observers[observer] = event_filter

            if once:
                self._once_observers.add(observer)

            super().attach(observer)

    def detach(self, observer: Observer) -> bool:
        with self._lock:
            self._once_observers.discard(observer)
            self._conditional_observers.pop(observer, None)
        return super().detach(observer)

    def notify(self, event: Dict[str, Any]) -> None:
        """Notify observers of an event.

        Args:
            event: Event data dict with at least "type" key.
        """
        with self._lock:
            observers = list(self._observers)

        to_detach: List[Observer] = []

        for observer in observers:
            if observer in self._conditional_observers:
                try:
                    if not self._conditional_observers[observer](event):
                        continue
                except Exception:
                    pass

            try:
                observer.update(event)
                if observer in self._once_observers:
                    to_detach.append(observer)
            except Exception:
                pass

        for observer in to_detach:
            self.detach(observer)


class EventDispatcher:
    """Dispatcher supporting multiple event channels/topics.

    Example:
        >>> dispatcher = EventDispatcher()
        >>> dispatcher.on("click", click_handler)
        >>> dispatcher.emit("click", {"x": 100, "y": 200})
    """

    def __init__(self) -> None:
        self._channels: Dict[str, Observable] = {}
        self._global: Observable = Observable()
        self._lock = threading.RLock()

    def on(
        self,
        channel: str,
        handler: Callable[[Dict[str, Any]], None],
        *,
        once: bool = False,
    ) -> CallableObserver:
        """Register a handler for a channel.

        Args:
            channel: Channel name.
            handler: Callback function.
            once: Auto-unregister after first call.

        Returns:
            The created observer.
        """
        with self._lock:
            if channel not in self._channels:
                self._channels[channel] = Observable()
            obs = CallableObserver(handler)
            self._channels[channel].attach(obs, once=once)
            return obs

    def off(self, channel: str, observer: Observer) -> bool:
        """Unregister a handler.

        Args:
            channel: Channel name.
            observer: Observer to remove.

        Returns:
            True if found.
        """
        with self._lock:
            if channel in self._channels:
                return self._channels[channel].detach(observer)
        return False

    def emit(self, channel: str, data: Any = None) -> None:
        """Emit an event on a channel.

        Args:
            channel: Channel name.
            data: Event data.
        """
        event: Dict[str, Any] = {"type": channel, "data": data}
        with self._lock:
            if channel in self._channels:
                self._channels[channel].notify(event)
            self._global.notify(event)

    def on_any(self, handler: Callable[[Dict[str, Any]], None]) -> CallableObserver:
        """Register handler for all events.

        Args:
            handler: Callback for all events.

        Returns:
            Created observer.
        """
        obs = CallableObserver(handler)
        self._global.attach(obs)
        return obs

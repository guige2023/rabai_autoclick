"""
Observer pattern implementation.

Provides subject/observer event system
with filtering and async support.
"""

from __future__ import annotations

import threading
import asyncio
from typing import Any, Callable, Generic, TypeVar
from dataclasses import dataclass


T = TypeVar("T")


@dataclass
class Event:
    """Base event object."""
    type: str
    data: Any = None


class Observer(Generic[T]):
    """Base observer interface."""

    def on_next(self, event: Event) -> None:
        """Handle next event."""
        pass

    def on_error(self, error: Exception) -> None:
        """Handle error."""
        pass

    def on_complete(self) -> None:
        """Handle completion."""
        pass


class Subject(Generic[T]):
    """
    Subject in observer pattern.

    Notifies all registered observers of events.
    """

    def __init__(self):
        self._observers: list[Observer[T]] = []
        self._lock = threading.Lock()
        self._completed = False

    def subscribe(self, observer: Observer[T]) -> None:
        """Subscribe an observer."""
        with self._lock:
            if observer not in self._observers:
                self._observers.append(observer)

    def unsubscribe(self, observer: Observer[T]) -> None:
        """Unsubscribe an observer."""
        with self._lock:
            if observer in self._observers:
                self._observers.remove(observer)

    def unsubscribe_all(self) -> None:
        """Remove all observers."""
        with self._lock:
            self._observers.clear()

    def emit(self, event: Event) -> None:
        """Emit event to all observers."""
        with self._lock:
            if self._completed:
                return
            observers = list(self._observers)

        for observer in observers:
            try:
                observer.on_next(event)
            except Exception as e:
                try:
                    observer.on_error(e)
                except Exception:
                    pass

    def error(self, error: Exception) -> None:
        """Emit error to all observers."""
        with self._lock:
            if self._completed:
                return
            self._completed = True
            observers = list(self._observers)
        for observer in observers:
            try:
                observer.on_error(error)
            except Exception:
                pass

    def complete(self) -> None:
        """Signal completion to all observers."""
        with self._lock:
            if self._completed:
                return
            self._completed = True
            observers = list(self._observers)
        for observer in observers:
            try:
                observer.on_complete()
            except Exception:
                pass

    @property
    def observer_count(self) -> int:
        return len(self._observers)


class FilteredSubject(Subject[T]):
    """Subject with event type filtering."""

    def __init__(self):
        super().__init__()
        self._type_filters: dict[str, set[Observer[T]]] = {}

    def subscribe(
        self,
        observer: Observer[T],
        event_type: str | None = None,
    ) -> None:
        super().subscribe(observer)
        if event_type:
            if event_type not in self._type_filters:
                self._type_filters[event_type] = set()
            self._type_filters[event_type].add(observer)

    def emit(self, event: Event) -> None:
        if event.type in self._type_filters:
            observers = list(self._type_filters[event.type])
            for observer in observers:
                try:
                    observer.on_next(event)
                except Exception as e:
                    try:
                        observer.on_error(e)
                    except Exception:
                        pass
        else:
            super().emit(event)


class CallableObserver(Observer[T]):
    """Observer wrapping a callable."""

    def __init__(
        self,
        on_next: Callable[[Event], None] | None = None,
        on_error: Callable[[Exception], None] | None = None,
        on_complete: Callable[[], None] | None = None,
    ):
        self._on_next = on_next
        self._on_error = on_error
        self._on_complete = on_complete

    def on_next(self, event: Event) -> None:
        if self._on_next:
            self._on_next(event)

    def on_error(self, error: Exception) -> None:
        if self._on_error:
            self._on_error(error)

    def on_complete(self) -> None:
        if self._on_complete:
            self._on_complete()


def create_observer(
    on_next: Callable[[Event], None] | None = None,
) -> CallableObserver:
    """Factory to create callable observer."""
    return CallableObserver(on_next=on_next)

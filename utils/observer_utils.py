"""Observer pattern utilities: subject-observer, property change listeners, and signals."""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

__all__ = [
    "Observer",
    "Subject",
    "Signal",
    "PropertyObserver",
    "observable_property",
]


T = TypeVar("T")


@dataclass
class Observer(Generic[T]):
    """Registered observer with callback."""
    id: str
    callback: Callable[[T], None]
    filter: Callable[[T], bool] | None = None
    once: bool = False
    priority: int = 0


class Subject(Generic[T]):
    """Subject that notifies observers of events."""

    def __init__(self) -> None:
        self._observers: dict[str, list[Observer[T]]] = defaultdict(list)
        self._lock = threading.RLock()

    def observe(
        self,
        observer_id: str,
        callback: Callable[[T], None],
        filter_fn: Callable[[T], bool] | None = None,
        once: bool = False,
        priority: int = 0,
    ) -> Observer[T]:
        obs = Observer(
            id=observer_id,
            callback=callback,
            filter=filter_fn,
            once=once,
            priority=priority,
        )
        with self._lock:
            self._observers[observer_id].append(obs)
            self._observers[observer_id].sort(key=lambda o: o.priority, reverse=True)
        return obs

    def unobserve(self, observer_id: str) -> None:
        with self._lock:
            self._observers.pop(observer_id, None)

    def notify(self, event: T) -> int:
        delivered = 0
        with self._lock:
            for obs_id, observers in list(self._observers.items()):
                to_remove: list[Observer[T]] = []
                for obs in observers:
                    if obs.filter and not obs.filter(event):
                        continue
                    try:
                        obs.callback(event)
                        delivered += 1
                        if obs.once:
                            to_remove.append(obs)
                    except Exception:
                        pass
                for obs in to_remove:
                    observers.remove(obs)
                if not observers:
                    del self._observers[obs_id]
        return delivered


class Signal:
    """Signal/slot pattern for event emission."""

    def __init__(self) -> None:
        self._slots: list[Callable[..., None]] = []
        self._lock = threading.Lock()

    def connect(self, slot: Callable[..., None]) -> None:
        with self._lock:
            self._slots.append(slot)

    def disconnect(self, slot: Callable[..., None]) -> None:
        with self._lock:
            if slot in self._slots:
                self._slots.remove(slot)

    def emit(self, *args: Any, **kwargs: Any) -> None:
        with self._lock:
            slots = list(self._slots)
        for slot in slots:
            try:
                slot(*args, **kwargs)
            except Exception:
                pass

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        self.emit(*args, **kwargs)


class PropertyObserver:
    """Observer for property changes on an object."""

    def __init__(self) -> None:
        self._callbacks: dict[str, list[Callable[[Any, Any], None]]] = defaultdict(list)
        self._values: dict[str, Any] = {}
        self._lock = threading.Lock()

    def watch(
        self,
        property_name: str,
        callback: Callable[[Any, Any], None],
    ) -> None:
        self._callbacks[property_name].append(callback)

    def set(self, property_name: str, new_value: Any) -> None:
        old_value = self._values.get(property_name)
        if old_value != new_value:
            with self._lock:
                self._values[property_name] = new_value
            for cb in self._callbacks.get(property_name, []):
                try:
                    cb(old_value, new_value)
                except Exception:
                    pass

    def get(self, property_name: str, default: Any = None) -> Any:
        return self._values.get(property_name, default)


def observable_property(name: str | None = None):
    """Decorator to make a property observable."""
    def decorator(fn: Callable[[Any], T]) -> property:
        prop_name = name or fn.__name__

        class ObservableDescriptor:
            def __get__(self, obj: Any, objtype: Any | None = None) -> T:
                return fn(obj)

            def __set__(self, obj: Any, value: T) -> None:
                if hasattr(obj, "_property_observer"):
                    obj._property_observer.set(prop_name, value)
                fn(obj)  # type: ignore
                object.__setattr__(obj, f"_{prop_name}", value)

        return property(ObservableDescriptor())
    return decorator

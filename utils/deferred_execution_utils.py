"""Deferred execution utilities: lazy evaluation, memoization, and delayed computation."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "Lazy",
    "Deferred",
    "memoize",
    "defer",
]


@dataclass
class Lazy:
    """Lazy-evaluated value that computes on first access."""

    _value: Any = field(default=None, repr=False)
    _computed: bool = field(default=False, repr=False)
    _func: Callable[[], Any] | None = field(default=None, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    @classmethod
    def create(cls, func: Callable[[], Any]) -> "Lazy":
        return cls(_func=func)

    def get(self) -> Any:
        with self._lock:
            if not self._computed:
                self._value = self._func() if self._func else None
                self._computed = True
            return self._value

    def __repr__(self) -> str:
        return f"Lazy({self.get()!r})"

    def __str__(self) -> str:
        return str(self.get())

    def __int__(self) -> int:
        return int(self.get())

    def __float__(self) -> float:
        return float(self.get())


@dataclass
class Deferred:
    """A deferred computation that can be resolved later."""

    _value: Any = field(default=None, repr=False)
    _error: Exception | None = field(default=None, repr=False)
    _resolved: bool = field(default=False, repr=False)
    _callbacks: list[Callable[[Any], None]] = field(
        default_factory=list, repr=False
    )
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def resolve(self, value: Any) -> None:
        with self._lock:
            self._value = value
            self._resolved = True
            for cb in self._callbacks:
                try:
                    cb(value)
                except Exception:
                    pass

    def reject(self, error: Exception) -> None:
        with self._lock:
            self._error = error
            self._resolved = True

    def then(self, callback: Callable[[Any], Any]) -> "Deferred":
        """Chain a callback to be called when resolved."""
        def wrapper(value: Any) -> Any:
            return callback(value)
        with self._lock:
            if self._resolved:
                if self._error is None:
                    try:
                        result = callback(self._value)
                        new_deferred = Deferred()
                        new_deferred.resolve(result)
                        return new_deferred
                    except Exception as e:
                        new_deferred = Deferred()
                        new_deferred.reject(e)
                        return new_deferred
            else:
                new_deferred = Deferred()
                def on_resolve(v: Any) -> None:
                    try:
                        new_deferred.resolve(callback(v))
                    except Exception as e:
                        new_deferred.reject(e)
                self._callbacks.append(on_resolve)
                return new_deferred

    @property
    def is_resolved(self) -> bool:
        return self._resolved


def memoize(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator that caches function results by arguments."""
    cache: dict[tuple, Any] = {}
    lock = threading.Lock()

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        key = (args, tuple(sorted(kwargs.items())))
        with lock:
            if key not in cache:
                cache[key] = func(*args, **kwargs)
            return cache[key]

    wrapper._cache = cache
    return wrapper


def defer(func: Callable[[], Any]) -> Deferred:
    """Execute a function and return a Deferred that resolves with the result."""
    d = Deferred()

    def run():
        try:
            result = func()
            d.resolve(result)
        except Exception as e:
            d.reject(e)

    threading.Thread(target=run, daemon=True).start()
    return d

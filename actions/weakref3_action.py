"""Weakref utilities v3 - cache and memoization patterns.

Weakref utilities for caching, memoization,
 and resource management.
"""

from __future__ import annotations

import weakref
import gc
from typing import Any, Callable, Generic, TypeVar

__all__ = [
    "WeakMemo",
    "WeakLRU",
    "ResourcePool",
    "CachedProperty",
    "memoize_weak",
    "auto_gc",
]


T = TypeVar("T")


class WeakMemo(Generic[T]):
    """Memoization with weak references."""

    def __init__(self, func: Callable[..., T]) -> None:
        self._func = func
        self._cache: dict[Any, weakref.ref] = {}

    def __call__(self, *args: Any, **kwargs: Any) -> T | None:
        key = (args, tuple(sorted(kwargs.items())))
        if key in self._cache:
            ref = self._cache[key]
            result = ref()
            if result is not None:
                return result
            del self._cache[key]
        result = self._func(*args, **kwargs)
        self._cache[key] = weakref.ref(result)
        return result


class WeakLRU(Generic[T]):
    """LRU cache with weak references."""

    def __init__(self, capacity: int = 128) -> None:
        self._capacity = capacity
        self._cache: dict[Any, T] = {}
        self._order: list[Any] = []

    def get(self, key: Any) -> T | None:
        """Get from cache."""
        if key in self._cache:
            self._order.remove(key)
            self._order.append(key)
            return self._cache[key]
        return None

    def put(self, key: Any, value: T) -> None:
        """Put into cache."""
        if key in self._cache:
            self._order.remove(key)
        elif len(self._cache) >= self._capacity:
            oldest = self._order.pop(0)
            del self._cache[oldest]
        self._cache[key] = value
        self._order.append(key)


class ResourcePool(Generic[T]):
    """Pool for managing weak-referenced resources."""

    def __init__(self, factory: Callable[[], T]) -> None:
        self._factory = factory
        self._available: list[T] = []
        self._in_use: set[int] = set()

    def acquire(self) -> T:
        """Acquire resource from pool."""
        if self._available:
            obj = self._available.pop()
        else:
            obj = self._factory()
        self._in_use.add(id(obj))
        return obj

    def release(self, obj: T) -> None:
        """Release resource back to pool."""
        obj_id = id(obj)
        if obj_id in self._in_use:
            self._in_use.discard(obj_id)
            self._available.append(obj)


class CachedProperty(Generic[T]):
    """Cached property descriptor."""

    def __init__(self, func: Callable[[Any], T]) -> None:
        self._func = func
        self._cache: dict[int, T] = {}

    def __get__(self, obj: Any, objtype: type | None = None) -> T:
        if obj is None:
            return self
        obj_id = id(obj)
        if obj_id not in self._cache:
            self._cache[obj_id] = self._func(obj)
        return self._cache[obj_id]


def memoize_weak(func: Callable[..., T]) -> WeakMemo[T]:
    """Create weak-reference memoized wrapper.

    Args:
        func: Function to memoize.

    Returns:
        WeakMemo wrapper.
    """
    return WeakMemo(func)


def auto_gc(threshold: int = 1000) -> None:
    """Trigger garbage collection when threshold exceeded.

    Args:
        threshold: Object count threshold.
    """
    if len(gc.get_objects()) > threshold:
        gc.collect()

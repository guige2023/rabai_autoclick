"""Weakref utilities v2 - advanced weak reference patterns.

Extended weakref utilities including weak dictionaries,
 caches, and observable references.
"""

from __future__ import annotations

import gc
import weakref
from typing import Any, Callable, Generic, TypeVar, Iterator
from weakref import WeakKeyDictionary, WeakValueDictionary, ref

__all__ = [
    "WeakRefWrapper",
    "WeakCache",
    "WeakLRUCache",
    "WeakIdentityDict",
    "WeakSetEx",
    "ReferenceTracker",
    "finalize",
    "get_ref",
    "make_weak",
    "weak_method",
    "weak_callback",
    "auto_cleanup",
    "ObjectPoolWeakRef",
    "MemoizedProperty",
    "CachedProperty",
    "WeakDefaults",
    "FinalizedObject",
]


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class WeakRefWrapper(Generic[T]):
    """Wrapper for weak reference with fallback."""

    def __init__(self, obj: T, callback: Callable[[weakref], None] | None = None) -> None:
        self._ref = weakref.ref(obj, callback)

    @property
    def object(self) -> T | None:
        """Get object or None if collected."""
        return self._ref()

    def is_alive(self) -> bool:
        """Check if object is still alive."""
        return self._ref() is not None

    def __call__(self) -> T | None:
        return self.object


class WeakCache(Generic[K, V]):
    """Cache that holds weak references to values."""

    def __init__(self, maxsize: int | None = None) -> None:
        self._data: dict[K, weakref.ref] = {}
        self._maxsize = maxsize
        self._access_order: list[K] = []

    def get(self, key: K) -> V | None:
        """Get value from cache.

        Args:
            key: Cache key.

        Returns:
            Value or None.
        """
        if key not in self._data:
            return None
        ref = self._data[key]
        value = ref()
        if value is None:
            del self._data[key]
            if key in self._access_order:
                self._access_order.remove(key)
            return None
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)
        return value

    def put(self, key: K, value: V) -> None:
        """Put value in cache.

        Args:
            key: Cache key.
            value: Value to cache.
        """
        if key in self._data:
            if key in self._access_order:
                self._access_order.remove(key)
        elif self._maxsize and len(self._data) >= self._maxsize:
            self._evict_oldest()
        self._data[key] = weakref.ref(value)
        self._access_order.append(key)

    def _evict_oldest(self) -> None:
        """Evict least recently used entry."""
        if self._access_order:
            oldest = self._access_order.pop(0)
            if oldest in self._data:
                del self._data[oldest]

    def __contains__(self, key: K) -> bool:
        return key in self._data and self._data[key]() is not None

    def __len__(self) -> int:
        gc.collect()
        return sum(1 for ref in self._data.values() if ref() is not None)


class WeakLRUCache(Generic[K, V]):
    """LRU cache with weak references."""

    def __init__(self, capacity: int = 128) -> None:
        self._capacity = capacity
        self._cache: dict[K, V] = {}
        self._order: list[K] = []

    def get(self, key: K) -> V | None:
        """Get value.

        Args:
            key: Cache key.

        Returns:
            Value or None.
        """
        if key not in self._cache:
            return None
        self._order.remove(key)
        self._order.append(key)
        return self._cache[key]

    def put(self, key: K, value: V) -> None:
        """Put value.

        Args:
            key: Cache key.
            value: Value to cache.
        """
        if key in self._cache:
            self._order.remove(key)
        elif len(self._cache) >= self._capacity:
            oldest = self._order.pop(0)
            del self._cache[oldest]
        self._cache[key] = value
        self._order.append(key)

    def __contains__(self, key: K) -> bool:
        return key in self._cache

    def __len__(self) -> int:
        return len(self._cache)


class WeakIdentityDict(Generic[V]):
    """Dictionary using object identity for comparison."""

    def __init__(self) -> None:
        self._data: dict[int, V] = {}
        self._id_to_key: dict[int, int] = {}

    def __setitem__(self, key: object, value: V) -> None:
        """Set item by identity."""
        key_id = id(key)
        self._data[key_id] = value
        self._id_to_key[key_id] = key_id

    def __getitem__(self, key: object) -> V:
        """Get item by identity."""
        return self._data[id(key)]

    def __contains__(self, key: object) -> bool:
        """Check by identity."""
        return id(key) in self._data

    def __delitem__(self, key: object) -> None:
        """Delete by identity."""
        key_id = id(key)
        del self._data[key_id]
        del self._id_to_key[key_id]

    def __len__(self) -> int:
        return len(self._data)


class WeakSetEx(Generic[T]):
    """Weak set with cleanup callbacks."""

    def __init__(self, callback: Callable[[T], None] | None = None) -> None:
        self._refs: list[weakref] = []
        self._callback = callback

    def add(self, obj: T) -> None:
        """Add object to set.

        Args:
            obj: Object to add.
        """
        ref = weakref(obj, self._on_deleted)
        self._refs.append(ref)

    def _on_deleted(self, ref: weakref) -> None:
        """Handle object deletion."""
        if self._callback:
            self._callback(ref())

    def __iter__(self) -> Iterator[T]:
        """Iterate over alive objects."""
        gc.collect()
        return iter(r() for r in self._refs if r() is not None)

    def __contains__(self, obj: T) -> bool:
        """Check if object is in set."""
        for r in self._refs:
            if r() is obj:
                return True
        return False

    def __len__(self) -> int:
        gc.collect()
        return sum(1 for r in self._refs if r() is not None)


class ReferenceTracker:
    """Track weak references and detect leaks."""

    def __init__(self) -> None:
        self._refs: list[weakref] = []
        self._alive_count = 0

    def track(self, obj: object) -> None:
        """Track an object.

        Args:
            obj: Object to track.
        """
        ref = weakref(obj, self._on_deleted)
        self._refs.append(ref)
        self._alive_count += 1

    def _on_deleted(self, ref: weakref) -> None:
        """Handle deletion."""
        self._alive_count -= 1

    def alive_count(self) -> int:
        """Get count of alive objects."""
        gc.collect()
        return sum(1 for r in self._refs if r() is not None)

    def total_tracked(self) -> int:
        """Get total tracked count."""
        return len(self._refs)

    def get_alive(self) -> list:
        """Get list of alive objects."""
        gc.collect()
        return [r() for r in self._refs if r() is not None]


def finalize(obj: T, func: Callable[..., Any], *args: Any, **kwargs: Any) -> weakref.finalize:
    """Register finalizer for object.

    Args:
        obj: Object to finalize.
        func: Function to call.
        *args: Positional args.
        **kwargs: Keyword args.

    Returns:
        Finalize object.
    """
    return weakref.finalize(obj, func, *args, **kwargs)


def get_ref(obj: T) -> weakref.ref[T]:
    """Create weak reference to object.

    Args:
        obj: Object to reference.

    Returns:
        Weak reference.
    """
    return weakref.ref(obj)


def make_weak(obj: T) -> weakref.ref[T]:
    """Create weak reference (alias for get_ref)."""
    return get_ref(obj)


def weak_method(method: Callable[..., Any]) -> Callable[..., Any]:
    """Create weak reference to bound method.

    Args:
        method: Bound method.

    Returns:
        Weak reference wrapper.
    """
    ref = weakref(method.__self__)
    func = method.__func__
    def weak_call(*args, **kwargs):
        obj = ref()
        if obj is None:
            raise ReferenceError("Object no longer exists")
        return func(obj, *args, **kwargs)
    return weak_call


def weak_callback(callback: Callable[[Any], None]) -> Callable[[weakref], None]:
    """Create weak reference callback.

    Args:
        callback: Function to call with dereferenced object.

    Returns:
        Callback for weakref.
    """
    def on_delete(ref: weakref) -> None:
        callback(ref())
    return on_delete


def auto_cleanup(obj: Any) -> weakref.finalize:
    """Register auto-cleanup finalizer.

    Args:
        obj: Object to finalize.

    Returns:
        Finalize object.
    """
    def cleanup():
        if hasattr(obj, "close"):
            obj.close()
        elif hasattr(obj, "cleanup"):
            obj.cleanup()
    return finalize(obj, cleanup)


class ObjectPoolWeakRef(Generic[T]):
    """Object pool using weak references."""

    def __init__(self, factory: Callable[[], T]) -> None:
        self._factory = factory
        self._available: list[T] = []
        self._in_use: set[int] = set()

    def acquire(self) -> T:
        """Acquire object from pool.

        Returns:
            Object instance.
        """
        if self._available:
            obj = self._available.pop()
        else:
            obj = self._factory()
        self._in_use.add(id(obj))
        return obj

    def release(self, obj: T) -> None:
        """Release object back to pool.

        Args:
            obj: Object to release.
        """
        obj_id = id(obj)
        if obj_id in self._in_use:
            self._in_use.discard(obj_id)
            self._available.append(obj)

    def clear(self) -> None:
        """Clear available pool."""
        self._available.clear()


class MemoizedProperty(Generic[T]):
    """Descriptor for memoized property."""

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


class CachedProperty(Generic[T]):
    """Cached property with expiration."""

    def __init__(self, func: Callable[[Any], T], ttl: float = 60.0) -> None:
        self._func = func
        self._ttl = ttl
        self._cache: dict[int, tuple[float, T]] = {}

    def __get__(self, obj: Any, objtype: type | None = None) -> T:
        import time
        if obj is None:
            return self
        obj_id = id(obj)
        now = time.time()
        if obj_id in self._cache:
            timestamp, value = self._cache[obj_id]
            if now - timestamp < self._ttl:
                return value
        value = self._func(obj)
        self._cache[obj_id] = (now, value)
        return value


class WeakDefaults(Generic[K, V]):
    """Dict with weak value defaults."""

    def __init__(self, default_factory: Callable[[], V] | None = None) -> None:
        self._data: dict[K, weakref.ref] = {}
        self._default_factory = default_factory

    def __getitem__(self, key: K) -> V:
        """Get item or default."""
        if key not in self._data:
            if self._default_factory is None:
                raise KeyError(key)
            default = self._default_factory()
            self._data[key] = weakref.ref(default)
            return default
        ref = self._data[key]
        value = ref()
        if value is None:
            del self._data[key]
            raise KeyError(key)
        return value

    def __setitem__(self, key: K, value: V) -> None:
        """Set item."""
        self._data[key] = weakref.ref(value)

    def __contains__(self, key: K) -> bool:
        """Check if key exists and value alive."""
        if key not in self._data:
            return False
        ref = self._data[key]
        return ref() is not None


class FinalizedObject(Generic[T]):
    """Wrapper that auto-finalizes on deletion."""

    def __init__(self, obj: T, finalizer: Callable[[T], None]) -> None:
        self._obj = obj
        self._finalizer = weakref.finalize(obj, finalizer)

    @property
    def object(self) -> T:
        """Get wrapped object."""
        return self._obj

    def alive(self) -> bool:
        """Check if still alive."""
        return self._finalizer.alive

    def __del__(self) -> None:
        """Run finalizer."""
        self._finalizer()

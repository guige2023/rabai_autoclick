"""
LRU Cache utilities with various implementations.

Provides thread-safe LRU cache, TTL cache, and
multi-key cache decorators.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from functools import wraps
from threading import Lock
from typing import Any, Callable, TypeVar


T = TypeVar("T")


def lru_cache(maxsize: int = 128):
    """
    Least Recently Used cache decorator.

    Args:
        maxsize: Maximum number of cached entries

    Example:
        >>> @lru_cache(maxsize=100)
        ... def expensive_func(x):
        ...     return x * 2
    """
    def decorator(func: Callable) -> Callable:
        cache: OrderedDict = OrderedDict()
        hits = misses = 0

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal hits, misses
            key = (args, tuple(sorted(kwargs.items())))
            if key in cache:
                cache.move_to_end(key)
                hits += 1
                return cache[key]
            result = func(*args, **kwargs)
            cache[key] = result
            if len(cache) > maxsize:
                cache.popitem(last=False)
            misses += 1
            return result

        def cache_info():
            return {"hits": hits, "misses": misses, "size": len(cache), "maxsize": maxsize}

        def cache_clear():
            nonlocal hits, misses
            cache.clear()
            hits = misses = 0

        wrapper.cache_info = cache_info
        wrapper.cache_clear = cache_clear
        return wrapper
    return decorator


def ttl_cache(ttl_seconds: float, maxsize: int = 128):
    """
    Cache with time-to-live expiration.

    Args:
        ttl_seconds: How long each entry is valid
        maxsize: Maximum number of cached entries
    """
    def decorator(func: Callable) -> Callable:
        cache: OrderedDict = OrderedDict()
        timestamps: OrderedDict = OrderedDict()
        lock = Lock()
        hits = misses = 0

        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal hits, misses
            key = (args, tuple(sorted(kwargs.items())))
            with lock:
                if key in cache:
                    if time.time() - timestamps[key] < ttl_seconds:
                        cache.move_to_end(key)
                        timestamps[key] = time.time()
                        hits += 1
                        return cache[key]
                    else:
                        del cache[key]
                        del timestamps[key]
                result = func(*args, **kwargs)
                cache[key] = result
                timestamps[key] = time.time()
                if len(cache) > maxsize:
                    oldest = next(iter(timestamps))
                    del cache[oldest]
                    del timestamps[oldest]
                misses += 1
                return result

        def cache_info():
            with lock:
                return {"hits": hits, "misses": misses, "size": len(cache), "maxsize": maxsize, "ttl": ttl_seconds}

        def cache_clear():
            nonlocal hits, misses
            with lock:
                cache.clear()
                timestamps.clear()
                hits = misses = 0

        wrapper.cache_info = cache_info
        wrapper.cache_clear = cache_clear
        return wrapper
    return decorator


class ThreadSafeCache:
    """Thread-safe in-memory cache with get/put/delete operations."""

    def __init__(self, maxsize: int = 1000) -> None:
        self._cache: dict = {}
        self._maxsize = maxsize
        self._lock = Lock()

    def get(self, key: str, default: T = None) -> T:
        """Get value from cache."""
        with self._lock:
            return self._cache.get(key, default)

    def put(self, key: str, value: Any) -> None:
        """Put value into cache."""
        with self._lock:
            if len(self._cache) >= self._maxsize and key not in self._cache:
                first_key = next(iter(self._cache))
                del self._cache[first_key]
            self._cache[key] = value

    def delete(self, key: str) -> bool:
        """Delete key from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """Get current cache size."""
        with self._lock:
            return len(self._cache)

    def keys(self) -> list:
        """Get all cache keys."""
        with self._lock:
            return list(self._cache.keys())


def memoize_with_expiry(seconds: float):
    """Memoize with absolute time expiry."""
    cache: dict = {}
    expiry: dict = {}

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            now = time.time()
            if key in cache and expiry.get(key, 0) > now:
                return cache[key]
            result = func(*args, **kwargs)
            cache[key] = result
            expiry[key] = now + seconds
            return result
        return wrapper
    return decorator

"""Memoization utilities: decorators for caching function results with various policies."""

from __future__ import annotations

import functools
import threading
import time
import weakref
from collections import OrderedDict
from typing import Any, Callable, Hashable

__all__ = [
    "memoize",
    "lru_cache",
    "ttl_cache",
    "memoize_async",
]


def memoize(maxsize: int = 128):
    """Memoization decorator with optional maxsize (LRU eviction)."""
    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        cache: OrderedDict[Any, Any] = OrderedDict()
        lock = threading.Lock()

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            key = (args, tuple(sorted(kwargs.items())))
            with lock:
                if key in cache:
                    cache.move_to_end(key)
                    return cache[key]
            result = fn(*args, **kwargs)
            with lock:
                if key in cache:
                    cache.move_to_end(key)
                else:
                    cache[key] = result
                    if maxsize and len(cache) > maxsize:
                        cache.popitem(last=False)
            return result

        wrapper.cache_clear = lambda: cache.clear()
        wrapper.cache_info = lambda: {"size": len(cache), "maxsize": maxsize}
        return wrapper
    return decorator


def lru_cache(maxsize: int = 128):
    """LRU cache decorator."""
    return memoize(maxsize=maxsize)


def ttl_cache(ttl_seconds: float = 300.0, maxsize: int = 128):
    """Cache with TTL (time-to-live) expiration."""

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        cache: OrderedDict[Any, tuple[float, Any]] = OrderedDict()
        lock = threading.Lock()

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            key = (args, tuple(sorted(kwargs.items())))
            now = time.time()
            with lock:
                if key in cache:
                    expiry, value = cache[key]
                    if now < expiry:
                        cache.move_to_end(key)
                        return value
                    del cache[key]

            result = fn(*args, **kwargs)
            with lock:
                cache[key] = (now + ttl_seconds, result)
                if maxsize and len(cache) > maxsize:
                    cache.popitem(last=False)
            return result

        def _evict_expired() -> None:
            now = time.time()
            with lock:
                expired = [k for k, (exp, _) in cache.items() if now >= exp]
                for k in expired:
                    del cache[k]

        wrapper.cache_clear = lambda: cache.clear()
        wrapper.cache_evict_expired = _evict_expired
        wrapper.cache_info = lambda: {"size": len(cache), "maxsize": maxsize, "ttl": ttl_seconds}
        return wrapper
    return decorator


def memoize_async(maxsize: int = 128):
    """Async memoization decorator with lock per key."""

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        cache: dict[Any, tuple[float, Any]] = {}
        locks: dict[Any, threading.Lock] = {}
        lock = threading.Lock()

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = (args, tuple(sorted(kwargs.items())))
            async with _async_lock(lock, locks, key):
                now = time.time()
                if key in cache:
                    expiry, value = cache[key]
                    if now < expiry:
                        return value
                    del cache[key]

                result = await fn(*args, **kwargs)
                cache[key] = (now + 300.0, result)
                if maxsize and len(cache) > maxsize:
                    oldest = min(cache, key=lambda k: cache[k][0])
                    del cache[oldest]
                return result

        wrapper.cache_clear = lambda: cache.clear()
        return wrapper
    return decorator


class _async_lock:
    def __init__(self, global_lock: threading.Lock, locks: dict, key: Hashable) -> None:
        self._key = key
        self._locks = locks
        self._global_lock = global_lock

    async def __aenter__(self) -> None:
        with self._global_lock:
            if self._key not in self._locks:
                self._locks[self._key] = threading.Lock()
            lock = self._locks[self._key]
        lock.acquire()

    async def __aexit__(self, *args: Any) -> None:
        with self._global_lock:
            lock = self._locks.get(self._key)
        if lock:
            lock.release()

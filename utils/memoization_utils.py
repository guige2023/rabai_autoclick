"""
Memoization utilities with TTL and LRU support.

Provides function memoization with time-to-live,
thread-safety, and cache statistics.
"""

from __future__ import annotations

import threading
import time
import functools
from typing import Any, Callable, TypeVar


T = TypeVar("T")
R = TypeVar("R")


def memoize(
    ttl: float | None = None,
    max_size: int | None = None,
) -> Callable[[Callable[..., R]], Callable[..., R]]:
    """
    Decorator to memoize function with optional TTL.

    Args:
        ttl: Time-to-live in seconds
        max_size: Maximum cache size (LRU eviction)

    Returns:
        Decorated function with cache attribute
    """
    def decorator(func: Callable[..., R]) -> Callable[..., R]:
        cache: dict[str, tuple[R, float]] = {}
        lock = threading.Lock()
        access_order: list[str] = []

        @functools.wraps(func)
        def wrapper(*args: object, **kwargs: object) -> R:
            key = f"{args}:{sorted(kwargs.items())}"
            now = time.time()

            with lock:
                if key in cache:
                    value, expiry = cache[key]
                    if ttl is None or now < expiry:
                        if max_size:
                            if key in access_order:
                                access_order.remove(key)
                            access_order.append(key)
                        return value
                    del cache[key]
                    if key in access_order:
                        access_order.remove(key)

                result = func(*args, **kwargs)
                cache[key] = (result, now + ttl if ttl else float("inf"))

                if max_size:
                    access_order.append(key)
                    if len(access_order) > max_size:
                        oldest = access_order.pop(0)
                        cache.pop(oldest, None)

                return result

        def cache_clear() -> None:
            with lock:
                cache.clear()
                access_order.clear()

        def cache_info() -> dict:
            with lock:
                return {
                    "size": len(cache),
                    "max_size": max_size,
                    "ttl": ttl,
                }

        wrapper.cache_clear = cache_clear  # type: ignore
        wrapper.cache_info = cache_info  # type: ignore
        return wrapper
    return decorator


class MemoCache(Generic[T, R]):
    """
    Manual memoization cache.

    Use when decorator pattern isn't suitable.
    """

    def __init__(
        self,
        ttl: float | None = None,
        max_size: int | None = None,
    ):
        self.ttl = ttl
        self.max_size = max_size
        self._cache: dict[T, tuple[R, float]] = {}
        self._lock = threading.Lock()
        self._access_order: list[T] = []
        self._hits = 0
        self._misses = 0

    def get(self, key: T) -> R | None:
        """Get cached value."""
        now = time.time()
        with self._lock:
            if key in self._cache:
                value, expiry = self._cache[key]
                if self.ttl is None or now < expiry:
                    self._hits += 1
                    if self.max_size and key in self._access_order:
                        self._access_order.remove(key)
                        self._access_order.append(key)
                    return value
                del self._cache[key]
                if key in self._access_order:
                    self._access_order.remove(key)
        self._misses += 1
        return None

    def set(self, key: T, value: R) -> None:
        """Set cached value."""
        now = time.time()
        with self._lock:
            if key in self._cache:
                if self.max_size and key in self._access_order:
                    self._access_order.remove(key)
            elif self.max_size and len(self._access_order) >= self.max_size:
                oldest = self._access_order.pop(0)
                self._cache.pop(oldest, None)

            self._cache[key] = (value, now + self.ttl if self.ttl else float("inf"))
            self._access_order.append(key)

    def invalidate(self, key: T) -> None:
        """Remove key from cache."""
        with self._lock:
            self._cache.pop(key, None)
            if key in self._access_order:
                self._access_order.remove(key)

    def clear(self) -> None:
        """Clear all cached values."""
        with self._lock:
            self._cache.clear()
            self._access_order.clear()
        self._hits = 0
        self._misses = 0

    def stats(self) -> dict:
        """Get cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0.0
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "ttl": self.ttl,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
            }


def lru_cache(max_size: int = 128) -> Callable[[Callable[..., R]], Callable[..., R]]:
    """LRU cache without TTL."""
    return memoize(max_size=max_size)


def ttl_cache(ttl: float = 60.0) -> Callable[[Callable[..., R]], Callable[..., R]]:
    """TTL cache without size limit."""
    return memoize(ttl=ttl)

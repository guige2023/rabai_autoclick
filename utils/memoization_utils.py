"""Memoization utilities for RabAI AutoClick.

Provides:
- Function memoization decorators
- LRU cache with statistics
- TTL cache
- Key-based cache invalidation
"""

from __future__ import annotations

import functools
import threading
import time
from typing import (
    Any,
    Callable,
    Dict,
    Optional,
    Tuple,
    TypeVar,
)


T = TypeVar("T")
F = TypeVar("F", bound=Callable[..., Any])


class LRUCache:
    """Thread-safe LRU cache with statistics."""

    def __init__(
        self,
        maxsize: int = 128,
    ) -> None:
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        self._maxsize = maxsize
        self._cache: Dict[str, Any] = {}
        self._order: List[str] = []
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str) -> Tuple[bool, Any]:
        """Get a value from cache.

        Args:
            key: Cache key.

        Returns:
            Tuple of (found, value).
        """
        with self._lock:
            if key in self._cache:
                self._hits += 1
                self._order.remove(key)
                self._order.append(key)
                return True, self._cache[key]
            self._misses += 1
            return False, None

    def set(self, key: str, value: Any) -> None:
        """Set a value in cache.

        Args:
            key: Cache key.
            value: Value to cache.
        """
        with self._lock:
            if key in self._cache:
                self._order.remove(key)
            elif len(self._cache) >= self._maxsize:
                oldest = self._order.pop(0)
                del self._cache[oldest]
            self._cache[key] = value
            self._order.append(key)

    def clear(self) -> None:
        """Clear all cached items."""
        with self._lock:
            self._cache.clear()
            self._order.clear()

    @property
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0.0
            return {
                "hits": self._hits,
                "misses": self._misses,
                "size": len(self._cache),
                "maxsize": self._maxsize,
                "hit_rate": round(hit_rate, 4),
            }


def memoize_lru(
    maxsize: int = 128,
) -> Callable[[F], F]:
    """LRU memoization decorator.

    Args:
        maxsize: Maximum cache size.

    Returns:
        Decorated function with LRU cache.
    """
    cache = LRUCache(maxsize=maxsize)

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = str((args, tuple(sorted(kwargs.items()))))
            found, value = cache.get(key)
            if found:
                return value
            result = func(*args, **kwargs)
            cache.set(key, result)
            return result
        wrapper.cache_stats = cache.stats  # type: ignore
        wrapper.clear_cache = cache.clear  # type: ignore
        return wrapper  # type: ignore
    return decorator


def memoize_ttl(
    ttl: float = 60.0,
) -> Callable[[F], F]:
    """Memoization with time-to-live expiration.

    Args:
        ttl: Time-to-live in seconds.

    Returns:
        Decorated function with TTL cache.
    """
    cache: Dict[str, Tuple[Any, float]] = {}
    lock = threading.Lock()

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = str((args, tuple(sorted(kwargs.items()))))
            with lock:
                if key in cache:
                    value, timestamp = cache[key]
                    if time.monotonic() - timestamp < ttl:
                        return value
                result = func(*args, **kwargs)
                cache[key] = (result, time.monotonic())
                return result
        wrapper.clear_cache = lambda: cache.clear()  # type: ignore
        return wrapper  # type: ignore
    return decorator


def memoize_async(func: F) -> F:
    """Memoization for async functions.

    Args:
        func: Async function to memoize.

    Returns:
        Decorated async function.
    """
    cache: Dict[str, Any] = {}
    lock = threading.Lock()

    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        key = str((args, tuple(sorted(kwargs.items()))))
        with lock:
            if key in cache:
                return cache[key]
        result = await func(*args, **kwargs)
        with lock:
            cache[key] = result
        return result

    def clear() -> None:
        cache.clear()

    wrapper.clear_cache = clear  # type: ignore
    return wrapper  # type: ignore


__all__ = [
    "LRUCache",
    "memoize_lru",
    "memoize_ttl",
    "memoize_async",
]


from typing import List  # noqa: E402

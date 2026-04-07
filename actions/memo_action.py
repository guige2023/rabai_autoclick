"""memo_action module for rabai_autoclick.

Provides memoization utilities: function caching with TTL,
lru cache, and keyed memoization for expensive computations.
"""

from __future__ import annotations

import functools
import threading
import time
from collections import OrderedDict
from typing import Any, Callable, Dict, Optional, Tuple, TypeVar

__all__ = [
    "memo",
    "memo_ttl",
    "lru_cache",
    "MemoCache",
    "MemoKey",
    "clear_memo",
    "memoize",
]


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class MemoKey:
    """Wrapper for creating cache keys from arguments."""

    @staticmethod
    def create(*args: Any, **kwargs: Any) -> Tuple:
        """Create hashable cache key from args/kwargs.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            Hashable tuple suitable for caching.
        """
        key_parts = [args]
        if kwargs:
            key_parts.append(tuple(sorted(kwargs.items())))
        return tuple(key_parts)


class MemoCache(Generic[K, V]):
    """Generic memoization cache with TTL support."""

    def __init__(
        self,
        ttl_seconds: Optional[float] = None,
        max_size: Optional[int] = None,
    ) -> None:
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self._cache: OrderedDict[K, Tuple[float, V]] = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: K) -> Optional[V]:
        """Get cached value if exists and not expired."""
        with self._lock:
            if key not in self._cache:
                return None
            timestamp, value = self._cache[key]
            if self.ttl_seconds and (time.time() - timestamp) > self.ttl_seconds:
                del self._cache[key]
                return None
            self._cache.move_to_end(key)
            return value

    def set(self, key: K, value: V) -> None:
        """Set cached value with current timestamp."""
        with self._lock:
            if self.max_size and len(self._cache) >= self.max_size:
                self._cache.popitem(last=False)
            self._cache[key] = (time.time(), value)

    def delete(self, key: K) -> bool:
        """Delete cached value."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> None:
        """Clear all cached values."""
        with self._lock:
            self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)

    def cleanup_expired(self) -> int:
        """Remove expired entries.

        Returns:
            Number of entries removed.
        """
        if not self.ttl_seconds:
            return 0
        removed = 0
        now = time.time()
        with self._lock:
            expired = [
                k for k, (ts, _) in self._cache.items()
                if (now - ts) > self.ttl_seconds
            ]
            for k in expired:
                del self._cache[k]
                removed += 1
        return removed


def memo(func: Optional[Callable] = None, max_size: Optional[int] = None) -> Callable:
    """Memoization decorator with LRU eviction.

    Args:
        func: Function to memoize.
        max_size: Maximum cache size.

    Returns:
        Decorated function with caching.
    """
    def decorator(f: Callable) -> Callable:
        cache: OrderedDict = OrderedDict()

        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = MemoKey.create(*args, **kwargs)
            if key in cache:
                return cache[key]
            result = f(*args, **kwargs)
            cache[key] = result
            if max_size and len(cache) > max_size:
                cache.popitem(last=False)
            return result

        wrapper.cache = cache
        wrapper.cache_clear = lambda: cache.clear()
        return wrapper

    if func is None:
        return decorator
    return decorator(func)


def memo_ttl(
    ttl_seconds: float,
    max_size: Optional[int] = None,
) -> Callable:
    """Memoization decorator with TTL expiration.

    Args:
        ttl_seconds: Time-to-live for cache entries.
        max_size: Maximum cache size.

    Returns:
        Decorator function.
    """
    def decorator(func: Callable) -> Callable:
        memo_cache = MemoCache(max_size=max_size, ttl_seconds=ttl_seconds)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            key = MemoKey.create(*args, **kwargs)
            result = memo_cache.get(key)
            if result is not None:
                return result
            result = func(*args, **kwargs)
            memo_cache.set(key, result)
            return result

        wrapper._memo_cache = memo_cache
        wrapper.cache_clear = lambda: memo_cache.clear()
        wrapper.cache_cleanup = lambda: memo_cache.cleanup_expired()
        return wrapper
    return decorator


def lru_cache(max_size: int = 128) -> Callable:
    """LRU cache decorator.

    Args:
        max_size: Maximum number of cached entries.

    Returns:
        Decorator function.
    """
    return memo(max_size=max_size)


def memoize(func: Optional[Callable] = None, **kwargs: Any) -> Callable:
    """Generic memoize decorator.

    Args:
        func: Function to memoize.
        **kwargs: Options (ttl, max_size, etc.).

    Returns:
        Decorated function.
    """
    if "ttl" in kwargs:
        return memo_ttl(ttl_seconds=kwargs["ttl"], max_size=kwargs.get("max_size"))(func)
    return memo(max_size=kwargs.get("max_size"))(func)


def clear_memo(func: Callable) -> None:
    """Clear memoization cache for function.

    Args:
        func: Function with cache to clear.
    """
    if hasattr(func, "cache_clear"):
        func.cache_clear()

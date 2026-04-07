"""Cache utilities for RabAI AutoClick.

Provides:
- LRUCache: Least Recently Used cache
- TTLCache: Time-To-Live cache
- Cache decorators
"""

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Generic, Optional, TypeVar


T = TypeVar("T")


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0

    @property
    def total_requests(self) -> int:
        """Total number of cache requests."""
        return self.hits + self.misses

    @property
    def hit_rate(self) -> float:
        """Cache hit rate (0.0 to 1.0)."""
        if self.total_requests == 0:
            return 0.0
        return self.hits / self.total_requests


class LRUCache(Generic[T]):
    """Thread-safe LRU (Least Recently Used) cache.

    Usage:
        cache = LRUCache(max_size=100)
        cache.set("key", "value")
        value = cache.get("key")
    """

    def __init__(self, max_size: int = 128) -> None:
        """Initialize LRU cache.

        Args:
            max_size: Maximum number of items.
        """
        self._max_size = max_size
        self._cache: OrderedDict = OrderedDict()
        self._lock = threading.RLock()
        self._stats = CacheStats()

    def get(self, key: Any, default: Optional[T] = None) -> Optional[T]:
        """Get value from cache.

        Args:
            key: Cache key.
            default: Default value if key not found.

        Returns:
            Cached value or default.
        """
        with self._lock:
            if key in self._cache:
                self._stats.hits += 1
                self._cache.move_to_end(key)
                return self._cache[key]
            self._stats.misses += 1
            return default

    def set(self, key: Any, value: T) -> None:
        """Set value in cache.

        Args:
            key: Cache key.
            value: Value to cache.
        """
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            else:
                if len(self._cache) >= self._max_size:
                    self._cache.popitem(last=False)
                    self._stats.evictions += 1
            self._cache[key] = value

    def __contains__(self, key: Any) -> bool:
        """Check if key is in cache."""
        with self._lock:
            return key in self._cache

    def __len__(self) -> int:
        """Get cache size."""
        with self._lock:
            return len(self._cache)

    def clear(self) -> None:
        """Clear all cached items."""
        with self._lock:
            self._cache.clear()

    @property
    def stats(self) -> CacheStats:
        """Get cache statistics."""
        with self._lock:
            return CacheStats(
                hits=self._stats.hits,
                misses=self._stats.misses,
                evictions=self._stats.evictions,
            )

    def reset_stats(self) -> None:
        """Reset cache statistics."""
        with self._lock:
            self._stats = CacheStats()


class TTLCache(Generic[T]):
    """Thread-safe cache with time-to-live expiration.

    Usage:
        cache = TTLCache(ttl=60, max_size=100)
        cache.set("key", "value")
        value = cache.get("key")  # Returns None after 60 seconds
    """

    def __init__(self, ttl: float = 300, max_size: int = 128) -> None:
        """Initialize TTL cache.

        Args:
            ttl: Time-to-live in seconds.
            max_size: Maximum number of items.
        """
        self._ttl = ttl
        self._max_size = max_size
        self._cache: OrderedDict = OrderedDict()
        self._lock = threading.RLock()
        self._stats = CacheStats()

    def get(self, key: Any, default: Optional[T] = None) -> Optional[T]:
        """Get value from cache if not expired.

        Args:
            key: Cache key.
            default: Default value if key not found or expired.

        Returns:
            Cached value or default.
        """
        with self._lock:
            if key not in self._cache:
                self._stats.misses += 1
                return default

            value, expiry = self._cache[key]
            if time.time() > expiry:
                del self._cache[key]
                self._stats.misses += 1
                return default

            self._stats.hits += 1
            self._cache.move_to_end(key)
            return value

    def set(self, key: Any, value: T, ttl: Optional[float] = None) -> None:
        """Set value in cache with expiration.

        Args:
            key: Cache key.
            value: Value to cache.
            ttl: Optional custom TTL for this item.
        """
        with self._lock:
            expiry = time.time() + (ttl if ttl is not None else self._ttl)

            if key in self._cache:
                self._cache.move_to_end(key)
            else:
                if len(self._cache) >= self._max_size:
                    self._cache.popitem(last=False)
                    self._stats.evictions += 1

            self._cache[key] = (value, expiry)

    def __contains__(self, key: Any) -> bool:
        """Check if key is in cache and not expired."""
        with self._lock:
            if key not in self._cache:
                return False
            _, expiry = self._cache[key]
            if time.time() > expiry:
                del self._cache[key]
                return False
            return True

    def __len__(self) -> int:
        """Get cache size."""
        with self._lock:
            return len(self._cache)

    def clear(self) -> None:
        """Clear all cached items."""
        with self._lock:
            self._cache.clear()

    def cleanup_expired(self) -> int:
        """Remove all expired entries.

        Returns:
            Number of entries removed.
        """
        with self._lock:
            now = time.time()
            expired = [
                key for key, (_, expiry) in self._cache.items()
                if now > expiry
            ]
            for key in expired:
                del self._cache[key]
            return len(expired)

    @property
    def stats(self) -> CacheStats:
        """Get cache statistics."""
        with self._lock:
            return CacheStats(
                hits=self._stats.hits,
                misses=self._stats.misses,
                evictions=self._stats.evictions,
            )


def cached(
    max_size: int = 128,
    ttl: Optional[float] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to cache function results.

    Args:
        max_size: Maximum cache size.
        ttl: Time-to-live in seconds (None = no expiration).

    Returns:
        Decorated function with caching.
    """
    if ttl is not None:
        cache = TTLCache(ttl=ttl, max_size=max_size)
    else:
        cache = LRUCache(max_size=max_size)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            key = (args, tuple(sorted(kwargs.items())))
            result = cache.get(key)
            if result is None:
                result = func(*args, **kwargs)
                cache.set(key, result)
            return result

        wrapper.cache = cache
        wrapper.cache_clear = cache.clear
        return wrapper

    return decorator


class Cache:
    """Generic cache interface.

    Provides a unified interface for different cache implementations.
    """

    def __init__(self, cache_type: str = "lru", **kwargs: Any) -> None:
        """Initialize cache.

        Args:
            cache_type: Type of cache ("lru" or "ttl").
            **kwargs: Arguments for cache constructor.
        """
        if cache_type == "lru":
            self._cache = LRUCache(**kwargs)
        elif cache_type == "ttl":
            self._cache = TTLCache(**kwargs)
        else:
            raise ValueError(f"Unknown cache type: {cache_type}")

    def get(self, key: Any, default: Any = None) -> Any:
        """Get value from cache."""
        return self._cache.get(key, default)

    def set(self, key: Any, value: Any) -> None:
        """Set value in cache."""
        self._cache.set(key, value)

    def __contains__(self, key: Any) -> bool:
        """Check if key is in cache."""
        return key in self._cache

    def __len__(self) -> int:
        """Get cache size."""
        return len(self._cache)

    def clear(self) -> None:
        """Clear cache."""
        self._cache.clear()

    @property
    def stats(self) -> CacheStats:
        """Get cache statistics."""
        return self._cache.stats
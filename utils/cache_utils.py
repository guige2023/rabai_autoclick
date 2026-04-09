"""Cache implementation utilities.

Provides thread-safe caching with TTL, LRU, and
cache statistics for automation workflows.
"""

import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, Hashable, Optional, TypeVar, Union


T = TypeVar("T")
K = TypeVar("K", bound=Hashable)
V = TypeVar("V")


@dataclass
class CacheEntry(Generic[T]):
    """Cache entry with metadata."""
    value: T
    created_at: float = field(default_factory=time.time)
    accessed_at: float = field(default_factory=time.time)
    access_count: int = 0

    def is_expired(self, ttl: float) -> bool:
        """Check if entry has expired."""
        if ttl <= 0:
            return False
        return (time.time() - self.created_at) > ttl

    def touch(self) -> None:
        """Update access time and count."""
        self.accessed_at = time.time()
        self.access_count += 1


class CacheStats:
    """Cache performance statistics.

    Example:
        stats = CacheStats()
        cache = TimedCache(ttl=60, stats=stats)
        cache.get("key")
        print(stats.hit_rate)
    """

    def __init__(self) -> None:
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._expirations = 0
        self._lock = threading.Lock()

    @property
    def hits(self) -> int:
        """Cache hits."""
        return self._hits

    @property
    def misses(self) -> int:
        """Cache misses."""
        return self._misses

    @property
    def hit_rate(self) -> float:
        """Hit rate as percentage."""
        total = self._hits + self._misses
        if total == 0:
            return 0.0
        return (self._hits / total) * 100

    @property
    def evictions(self) -> int:
        """Manual evictions."""
        return self._evictions

    @property
    def expirations(self) -> int:
        """Auto expirations."""
        return self._expirations

    def record_hit(self) -> None:
        """Record a cache hit."""
        with self._lock:
            self._hits += 1

    def record_miss(self) -> None:
        """Record a cache miss."""
        with self._lock:
            self._misses += 1

    def record_eviction(self) -> None:
        """Record an eviction."""
        with self._lock:
            self._evictions += 1

    def record_expiration(self) -> None:
        """Record an expiration."""
        with self._lock:
            self._expirations += 1

    def reset(self) -> None:
        """Reset all statistics."""
        with self._lock:
            self._hits = 0
            self._misses = 0
            self._evictions = 0
            self._expirations = 0

    def to_dict(self) -> dict:
        """Export statistics as dict."""
        return {
            "hits": self.hits,
            "misses": self.misses,
            "hit_rate": self.hit_rate,
            "evictions": self.evictions,
            "expirations": self.expirations,
        }


class TimedCache(Generic[K, V]):
    """Time-based cache with TTL expiration.

    Example:
        cache = TimedCache(ttl=300)  # 5 minute TTL
        cache.set("api_data", {"key": "value"})
        data = cache.get("api_data")
    """

    def __init__(
        self,
        ttl: float = 300.0,
        max_size: int = 0,
        stats: Optional[CacheStats] = None,
    ) -> None:
        self.ttl = ttl
        self.max_size = max_size
        self.stats = stats or CacheStats()
        self._cache: dict[K, CacheEntry] = {}
        self._lock = threading.RLock()

    def get(self, key: K) -> Optional[V]:
        """Get value from cache.

        Args:
            key: Cache key.

        Returns:
            Cached value or None if not found/expired.
        """
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self.stats.record_miss()
                return None

            if entry.is_expired(self.ttl):
                del self._cache[key]
                self.stats.record_expiration()
                self.stats.record_miss()
                return None

            entry.touch()
            self.stats.record_hit()
            return entry.value

    def set(self, key: K, value: V) -> None:
        """Set value in cache.

        Args:
            key: Cache key.
            value: Value to cache.
        """
        with self._lock:
            if self.max_size > 0 and len(self._cache) >= self.max_size:
                self._evict_oldest()

            self._cache[key] = CacheEntry(value=value)

    def delete(self, key: K) -> bool:
        """Delete key from cache.

        Args:
            key: Cache key.

        Returns:
            True if key existed.
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self.stats.record_eviction()
                return True
            return False

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()

    def _evict_oldest(self) -> None:
        """Evict oldest entry."""
        if not self._cache:
            return

        oldest_key = min(
            self._cache.keys(),
            key=lambda k: self._cache[k].accessed_at
        )
        del self._cache[oldest_key]
        self.stats.record_eviction()

    def cleanup_expired(self) -> int:
        """Remove all expired entries.

        Returns:
            Number of entries removed.
        """
        with self._lock:
            expired_keys = [
                k for k, v in self._cache.items()
                if v.is_expired(self.ttl)
            ]
            for key in expired_keys:
                del self._cache[key]
                self.stats.record_expiration()
            return len(expired_keys)

    def size(self) -> int:
        """Current cache size."""
        return len(self._cache)

    def __contains__(self, key: K) -> bool:
        """Check if key exists and is valid."""
        return self.get(key) is not None

    def __len__(self) -> int:
        return self.size()


class LRUCache(Generic[K, V]):
    """Least Recently Used cache with max size.

    Example:
        cache = LRUCache(max_size=100)
        cache.set("data", computation())
    """

    def __init__(
        self,
        max_size: int = 128,
        stats: Optional[CacheStats] = None,
    ) -> None:
        self.max_size = max_size
        self.stats = stats or CacheStats()
        self._cache: OrderedDict[K, V] = OrderedDict()
        self._lock = threading.RLock()

    def get(self, key: K) -> Optional[V]:
        """Get value, updating recency."""
        with self._lock:
            if key not in self._cache:
                self.stats.record_miss()
                return None

            self._cache.move_to_end(key)
            self.stats.record_hit()
            return self._cache[key]

    def set(self, key: K, value: V) -> None:
        """Set value, evicting LRU if full."""
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
            else:
                if len(self._cache) >= self.max_size:
                    self._cache.popitem(last=False)
                    self.stats.record_eviction()

            self._cache[key] = value

    def delete(self, key: K) -> bool:
        """Delete key from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False

    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """Current cache size."""
        return len(self._cache)


def cached(
    ttl: float = 300.0,
    max_size: int = 128,
) -> Callable:
    """Decorator to cache function results.

    Example:
        @cached(ttl=60)
        def expensive_computation(x, y):
            return x ** y
    """
    cache = TimedCache(ttl=ttl, max_size=max_size)

    def decorator(func: Callable[..., V]) -> Callable[..., V]:
        def wrapper(*args: Any, **kwargs: Any) -> V:
            key = (args, tuple(sorted(kwargs.items())))
            result = cache.get(key)  # type: ignore
            if result is not None:
                return result

            result = func(*args, **kwargs)
            cache.set(key, result)  # type: ignore
            return result

        wrapper.cache = cache
        return wrapper

    return decorator


from typing import TypeVar, Generic, Callable, Any

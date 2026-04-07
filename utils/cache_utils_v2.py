"""Cache utilities v2 for RabAI AutoClick.

Provides:
- TTL cache
- LRU cache implementation
- Cache decorators
- Multi-level cache
"""

import threading
import time
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Optional,
    Tuple,
    TypeVar,
)
import hashlib

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class TTLCache(Generic[K, V]):
    """Thread-safe cache with time-to-live expiration."""

    def __init__(
        self,
        ttl: float = 300.0,
        maxsize: int = 0,
    ) -> None:
        """Initialize TTL cache.

        Args:
            ttl: Time-to-live in seconds.
            maxsize: Max items (0 for unlimited).
        """
        self._ttl = ttl
        self._maxsize = maxsize
        self._cache: Dict[K, Tuple[V, float]] = {}
        self._lock = threading.RLock()

    def get(self, key: K) -> Optional[V]:
        """Get value from cache.

        Args:
            key: Cache key.

        Returns:
            Cached value or None if not found/expired.
        """
        with self._lock:
            if key not in self._cache:
                return None
            value, expires_at = self._cache[key]
            if time.time() > expires_at:
                del self._cache[key]
                return None
            return value

    def set(self, key: K, value: V) -> None:
        """Set value in cache.

        Args:
            key: Cache key.
            value: Value to cache.
        """
        with self._lock:
            if self._maxsize > 0 and len(self._cache) >= self._maxsize:
                self._evict_oldest()

            expires_at = time.time() + self._ttl
            self._cache[key] = (value, expires_at)

    def _evict_oldest(self) -> None:
        """Evict oldest entry."""
        if not self._cache:
            return
        oldest_key = min(self._cache, key=lambda k: self._cache[k][1])
        del self._cache[oldest_key]

    def delete(self, key: K) -> bool:
        """Delete a key from cache.

        Args:
            key: Cache key.

        Returns:
            True if key was deleted.
        """
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

    def __contains__(self, key: K) -> bool:
        """Check if key is in cache and not expired."""
        return self.get(key) is not None

    def __len__(self) -> int:
        return self.size()


class LRUCache(Generic[K, V]):
    """Least Recently Used cache."""

    def __init__(self, maxsize: int = 128) -> None:
        """Initialize LRU cache.

        Args:
            maxsize: Maximum number of items.
        """
        self._maxsize = maxsize
        self._cache: Dict[K, V] = {}
        self._order: list[K] = []
        self._lock = threading.Lock()

    def get(self, key: K) -> Optional[V]:
        """Get value and mark as recently used."""
        with self._lock:
            if key not in self._cache:
                return None
            self._order.remove(key)
            self._order.append(key)
            return self._cache[key]

    def set(self, key: K, value: V) -> None:
        """Set value and mark as recently used."""
        with self._lock:
            if key in self._cache:
                self._order.remove(key)
            elif len(self._cache) >= self._maxsize:
                oldest = self._order.pop(0)
                del self._cache[oldest]

            self._cache[key] = value
            self._order.append(key)

    def delete(self, key: K) -> bool:
        """Delete a key from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self._order.remove(key)
                return True
        return False

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._order.clear()

    def size(self) -> int:
        """Get current cache size."""
        with self._lock:
            return len(self._cache)

    def __contains__(self, key: K) -> bool:
        return key in self._cache

    def __len__(self) -> int:
        return self.size()


class LRUCacheWithTTL(Generic[K, V]):
    """LRU cache with TTL support."""

    def __init__(
        self,
        maxsize: int = 128,
        ttl: float = 300.0,
    ) -> None:
        self._maxsize = maxsize
        self._ttl = ttl
        self._cache: Dict[K, Tuple[V, float]] = {}
        self._order: list[K] = []
        self._lock = threading.Lock()

    def get(self, key: K) -> Optional[V]:
        """Get value if not expired."""
        with self._lock:
            if key not in self._cache:
                return None
            value, expires_at = self._cache[key]
            if time.time() > expires_at:
                del self._cache[key]
                self._order.remove(key)
                return None
            self._order.remove(key)
            self._order.append(key)
            return value

    def set(self, key: K, value: V) -> None:
        """Set value with TTL."""
        with self._lock:
            if key in self._cache:
                self._order.remove(key)
            elif len(self._cache) >= self._maxsize:
                oldest = self._order.pop(0)
                del self._cache[oldest]

            expires_at = time.time() + self._ttl
            self._cache[key] = (value, expires_at)
            self._order.append(key)

    def delete(self, key: K) -> bool:
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self._order.remove(key)
                return True
        return False

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._order.clear()

    def size(self) -> int:
        with self._lock:
            return len(self._cache)


def cached(
    ttl: float = 300.0,
    maxsize: int = 128,
) -> Callable:
    """Decorator for caching function results.

    Args:
        ttl: Cache TTL in seconds.
        maxsize: Max cache size.

    Returns:
        Decorated function.
    """
    cache = TTLCache[Any, Any](ttl=ttl, maxsize=maxsize)

    def decorator(func: Callable[..., V]) -> Callable[..., V]:
        def wrapper(*args: Any, **kwargs: Any) -> V:
            key = (args, tuple(sorted(kwargs.items())))
            key_str = str(key)
            key_hash = hashlib.md5(key_str.encode()).digest()

            cached_val = cache.get(key_hash)
            if cached_val is not None:
                return cached_val

            result = func(*args, **kwargs)
            cache.set(key_hash, result)
            return result

        return wrapper
    return decorator


def lru_cached(maxsize: int = 128) -> Callable:
    """Decorator for LRU caching function results.

    Args:
        maxsize: Max cache size.

    Returns:
        Decorated function.
    """
    cache = LRUCache[Any, Any](maxsize=maxsize)

    def decorator(func: Callable[..., V]) -> Callable[..., V]:
        def wrapper(*args: Any, **kwargs: Any) -> V:
            key = (args, tuple(sorted(kwargs.items())))
            key_str = str(key)
            key_hash = hashlib.md5(key_str.encode()).digest()

            cached_val = cache.get(key_hash)
            if cached_val is not None:
                return cached_val

            result = func(*args, **kwargs)
            cache.set(key_hash, result)
            return result

        return wrapper
    return decorator


class MultiLevelCache(Generic[K, V]):
    """Multi-level cache (L1+L2)."""

    def __init__(
        self,
        l1_size: int = 64,
        l2_size: int = 1024,
        ttl: float = 300.0,
    ) -> None:
        """Initialize multi-level cache.

        Args:
            l1_size: L1 (LRU) cache size.
            l2_size: L2 (TTL) cache size.
            ttl: TTL for L2.
        """
        self._l1 = LRUCache[K, V](maxsize=l1_size)
        self._l2 = TTLCache[K, V](ttl=ttl, maxsize=l2_size)

    def get(self, key: K) -> Optional[V]:
        """Get from cache, checking L1 then L2."""
        val = self._l1.get(key)
        if val is not None:
            return val
        val = self._l2.get(key)
        if val is not None:
            self._l1.set(key, val)
        return val

    def set(self, key: K, value: V) -> None:
        """Set in both levels."""
        self._l1.set(key, value)
        self._l2.set(key, value)

    def delete(self, key: K) -> bool:
        """Delete from both levels."""
        d1 = self._l1.delete(key)
        d2 = self._l2.delete(key)
        return d1 or d2

    def clear(self) -> None:
        """Clear all levels."""
        self._l1.clear()
        self._l2.clear()

    def size(self) -> Tuple[int, int]:
        """Get sizes of L1 and L2."""
        return self._l1.size(), self._l2.size()


class CacheStats:
    """Cache statistics tracker."""

    def __init__(self) -> None:
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._lock = threading.Lock()

    def record_hit(self) -> None:
        with self._lock:
            self._hits += 1

    def record_miss(self) -> None:
        with self._lock:
            self._misses += 1

    def record_eviction(self) -> None:
        with self._lock:
            self._evictions += 1

    def get_stats(self) -> dict:
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0.0
            return {
                "hits": self._hits,
                "misses": self._misses,
                "evictions": self._evictions,
                "hit_rate": hit_rate,
                "total_requests": total,
            }

    def reset(self) -> None:
        with self._lock:
            self._hits = 0
            self._misses = 0
            self._evictions = 0

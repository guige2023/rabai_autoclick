"""LRU cache and memoization utilities.

Provides thread-safe LRU cache implementation,
function memoization, and cache eviction policies.
"""

from __future__ import annotations

from typing import (
    TypeVar, Generic, Callable, Optional, Any, Dict, Tuple,
    Iterator, List, Mapping, Hashable, Optional
)
from dataclasses import dataclass
from collections import OrderedDict
import threading
import time
import hashlib
import pickle


T = TypeVar('T')
K = TypeVar('K')
V = TypeVar('V')


@dataclass
class CacheEntry(Generic[V]):
    """Cache entry with value and metadata."""
    value: V
    created_at: float
    accessed_at: float
    access_count: int = 0
    size_bytes: int = 0


class LRUCache(Generic[K, V]):
    """Thread-safe Least Recently Used cache.

    Example:
        cache = LRUCache[str, int](max_size=100, ttl_seconds=3600)
        cache.set("key1", 42)
        print(cache.get("key1"))  # 42
        print(cache.get("key2"))  # None
    """

    def __init__(
        self,
        max_size: int = 128,
        ttl_seconds: Optional[float] = None,
        on_evict: Optional[Callable[[K, V], None]] = None,
    ) -> None:
        """
        Args:
            max_size: Maximum number of entries.
            ttl_seconds: Time-to-live for entries (None = no expiry).
            on_evict: Callback when entry is evicted.
        """
        if max_size < 1:
            raise ValueError("max_size must be at least 1")
        self._max_size = max_size
        self._ttl = ttl_seconds
        self._on_evict = on_evict
        self._cache: OrderedDict[K, CacheEntry[V]] = OrderedDict()
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        """Get value from cache."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._misses += 1
                return default
            if self._is_expired(entry):
                self._evict(key)
                self._misses += 1
                return default
            entry.accessed_at = time.time()
            entry.access_count += 1
            self._cache.move_to_end(key)
            self._hits += 1
            return entry.value

    def set(self, key: K, value: V, size_bytes: int = 0) -> None:
        """Set value in cache."""
        with self._lock:
            if key in self._cache:
                old_entry = self._cache[key]
                if self._on_evict:
                    self._on_evict(key, old_entry.value)
            now = time.time()
            self._cache[key] = CacheEntry(
                value=value,
                created_at=now,
                accessed_at=now,
                access_count=0,
                size_bytes=size_bytes,
            )
            self._cache.move_to_end(key)
            self._evict_if_needed()

    def _is_expired(self, entry: CacheEntry[V]) -> bool:
        """Check if entry has expired."""
        if self._ttl is None:
            return False
        return (time.time() - entry.created_at) > self._ttl

    def _evict_if_needed(self) -> None:
        """Evict oldest entries if over capacity."""
        while len(self._cache) > self._max_size:
            oldest_key, oldest_entry = self._cache.popitem(last=False)
            if self._on_evict:
                self._on_evict(oldest_key, oldest_entry.value)

    def _evict(self, key: K) -> None:
        """Evict specific key."""
        if key in self._cache:
            entry = self._cache.pop(key)
            if self._on_evict:
                self._on_evict(key, entry.value)

    def delete(self, key: K) -> bool:
        """Delete key from cache. Returns True if key existed."""
        with self._lock:
            if key in self._cache:
                self._evict(key)
                return True
            return False

    def clear(self) -> None:
        """Clear all entries from cache."""
        with self._lock:
            if self._on_evict:
                for key, entry in self._cache.items():
                    self._on_evict(key, entry.value)
            self._cache.clear()

    def peek(self, key: K) -> Optional[V]:
        """Get value without updating access time."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            if self._is_expired(entry):
                return None
            return entry.value

    def contains(self, key: K) -> bool:
        """Check if key exists and is not expired."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return False
            return not self._is_expired(entry)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total = self._hits + self._misses
            hit_rate = self._hits / total if total > 0 else 0.0
            return {
                "size": len(self._cache),
                "max_size": self._max_size,
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": hit_rate,
                "ttl": self._ttl,
            }

    def keys(self) -> List[K]:
        """Get all keys in cache."""
        with self._lock:
            return list(self._cache.keys())

    def values(self) -> List[V]:
        """Get all values in cache."""
        with self._lock:
            return [e.value for e in self._cache.values()]

    def items(self) -> List[Tuple[K, V]]:
        """Get all key-value pairs."""
        with self._lock:
            return [(k, e.value) for k, e in self._cache.items()]

    def __len__(self) -> int:
        return len(self._cache)

    def __contains__(self, key: K) -> bool:
        return self.contains(key)


class TTLCache(Generic[K, V]):
    """Simple time-to-live cache with automatic expiration.

    Example:
        cache = TTLCache[str, int](ttl_seconds=60)
        cache.set("key", 42)
        print(cache.get("key"))  # 42
        time.sleep(61)
        print(cache.get("key"))  # None
    """

    def __init__(self, ttl_seconds: float = 60.0) -> None:
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self._ttl = ttl_seconds
        self._cache: Dict[K, Tuple[V, float]] = {}
        self._lock = threading.RLock()

    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        """Get value if exists and not expired."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return default
            value, timestamp = entry
            if time.time() - timestamp > self._ttl:
                del self._cache[key]
                return default
            return value

    def set(self, key: K, value: V) -> None:
        """Set value with current timestamp."""
        with self._lock:
            self._cache[key] = (value, time.time())

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

    def cleanup(self) -> int:
        """Remove all expired entries. Returns count removed."""
        with self._lock:
            now = time.time()
            expired = [
                k for k, (_, ts) in self._cache.items()
                if now - ts > self._ttl
            ]
            for k in expired:
                del self._cache[k]
            return len(expired)

    def __len__(self) -> int:
        return len(self._cache)


def memoize(
    func: Optional[Callable[..., V]] = None,
    max_size: int = 128,
    ttl_seconds: Optional[float] = None,
) -> Callable[..., V]:
    """Decorator for memoizing function results with LRU cache.

    Example:
        @memoize
        def expensive_computation(x, y):
            return x + y

        @memoize(max_size=256)
        def fib(n):
            return fib(n-1) + fib(n-2) if n > 1 else n
    """
    def decorator(f: Callable[..., V]) -> Callable[..., V]:
        cache = LRUCache(max_size=max_size, ttl_seconds=ttl_seconds)

        def make_key(*args: Any, **kwargs: Any) -> Hashable:
            try:
                key = (pickle.dumps(args), pickle.dumps(kwargs))
                return hashlib.md5(key).hexdigest()
            except Exception:
                return (str(args), str(sorted(kwargs.items())))

        def wrapper(*args: Any, **kwargs: Any) -> V:
            key = make_key(*args, **kwargs)
            result = cache.get(key)
            if result is None:
                result = f(*args, **kwargs)
                cache.set(key, result)
            return result

        wrapper.cache = cache  # type: ignore
        wrapper.cache_clear = cache.clear  # type: ignore
        wrapper.cache_info = cache.get_stats  # type: ignore
        return wrapper

    if func is None:
        return decorator
    return decorator(func)


class CacheKey(Generic[K]):
    """Customizable cache key generation.

    Example:
        key_gen = CacheKey()
        key = key_gen.combine("user", user_id, "profile")
        key2 = key_gen.combine("user", user_id, "settings")
    """

    @staticmethod
    def combine(*parts: K) -> str:
        """Combine parts into cache key string."""
        return ":".join(str(p) for p in parts)

    @staticmethod
    def prefix(prefix: str, *parts: K) -> str:
        """Create key with prefix."""
        return f"{prefix}:{CacheKey.combine(*parts)}"


@dataclass
class CacheStats:
    """Cache statistics snapshot."""
    hits: int
    misses: int
    size: int
    max_size: int
    hit_rate: float
    timestamp: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hits": self.hits,
            "misses": self.misses,
            "size": self.size,
            "max_size": self.max_size,
            "hit_rate": self.hit_rate,
            "timestamp": self.timestamp,
        }


def get_cache_stats(cache: LRUCache[K, V]) -> CacheStats:
    """Get statistics snapshot from cache."""
    stats = cache.get_stats()
    return CacheStats(
        hits=stats["hits"],
        misses=stats["misses"],
        size=stats["size"],
        max_size=stats["max_size"],
        hit_rate=stats["hit_rate"],
        timestamp=time.time(),
    )

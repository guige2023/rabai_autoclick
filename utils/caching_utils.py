"""
Caching and memoization utilities.

Provides LRU cache, TTL cache, cache decorators,
cache eviction policies, and memoization for expensive computations.
"""

from __future__ import annotations

import time
import threading
import hashlib
import pickle
from typing import Any, Callable, Optional


T = Any
CacheKey = tuple


def make_hashable(obj: Any) -> Any:
    """Convert unhashable objects to hashable form."""
    if isinstance(obj, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
    if isinstance(obj, list):
        return tuple(make_hashable(x) for x in obj)
    if isinstance(obj, set):
        return frozenset(make_hashable(x) for x in obj)
    return obj


def key_from_args(args: tuple, kwargs: dict | None = None) -> CacheKey:
    """Create a cache key from function arguments."""
    kws = kwargs or {}
    kws_items = tuple(sorted(kws.items()))
    # Convert unhashable types
    args_h = tuple(make_hashable(a) for a in args)
    return (args_h, kws_items)


class LRUCache:
    """Thread-safe LRU cache."""

    def __init__(self, capacity: int):
        self.capacity = capacity
        self._cache: dict[Any, Any] = {}
        self._access_order: list = []
        self._lock = threading.Lock()

    def get(self, key: Any) -> Optional[Any]:
        """Get item, updating access order."""
        with self._lock:
            if key not in self._cache:
                return None
            self._access_order.remove(key)
            self._access_order.append(key)
            return self._cache[key]

    def put(self, key: Any, value: Any) -> None:
        """Put item, evicting LRU if at capacity."""
        with self._lock:
            if key in self._cache:
                self._access_order.remove(key)
            elif len(self._cache) >= self.capacity:
                lru = self._access_order.pop(0)
                del self._cache[lru]
            self._cache[key] = value
            self._access_order.append(key)

    def delete(self, key: Any) -> None:
        """Delete key from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self._access_order.remove(key)

    def clear(self) -> None:
        """Clear all cached items."""
        with self._lock:
            self._cache.clear()
            self._access_order.clear()

    def size(self) -> int:
        return len(self._cache)

    def __contains__(self, key: Any) -> bool:
        return key in self._cache


class TTLCache:
    """Cache with time-to-live expiration."""

    def __init__(self, ttl: float, maxsize: int = 128):
        self.ttl = ttl
        self.maxsize = maxsize
        self._cache: dict[Any, tuple[Any, float]] = {}
        self._lock = threading.Lock()

    def get(self, key: Any) -> Optional[Any]:
        with self._lock:
            if key not in self._cache:
                return None
            value, expires = self._cache[key]
            if time.time() > expires:
                del self._cache[key]
                return None
            return value

    def put(self, key: Any, value: Any) -> None:
        with self._lock:
            if len(self._cache) >= self.maxsize:
                # Evict oldest
                oldest = min(self._cache.items(), key=lambda x: x[1][1])
                del self._cache[oldest[0]]
            self._cache[key] = (value, time.time() + self.ttl)

    def delete(self, key: Any) -> None:
        with self._lock:
            if key in self._cache:
                del self._cache[key]

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def cleanup(self) -> int:
        """Remove expired entries. Returns count of removed items."""
        with self._lock:
            now = time.time()
            expired = [k for k, (_, exp) in self._cache.items() if now > exp]
            for k in expired:
                del self._cache[k]
            return len(expired)

    def size(self) -> int:
        return len(self._cache)


class LFUCache:
    """Least Frequently Used cache."""

    def __init__(self, capacity: int):
        self.capacity = capacity
        self._cache: dict[Any, Any] = {}
        self._freq: dict[Any, int] = {}
        self._lock = threading.Lock()

    def get(self, key: Any) -> Optional[Any]:
        with self._lock:
            if key not in self._cache:
                return None
            self._freq[key] = self._freq.get(key, 0) + 1
            return self._cache[key]

    def put(self, key: Any, value: Any) -> None:
        with self._lock:
            if key in self._cache:
                self._cache[key] = value
                self._freq[key] = self._freq.get(key, 0) + 1
            else:
                if len(self._cache) >= self.capacity:
                    # Evict least frequently used
                    lfu_key = min(self._freq, key=self._freq.get)
                    del self._cache[lfu_key]
                    del self._freq[lfu_key]
                self._cache[key] = value
                self._freq[key] = 1

    def size(self) -> int:
        return len(self._cache)


def memoize(func: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator: memoize function results with LRU cache.

    Thread-safe.
    """
    cache: LRUCache = LRUCache(capacity=128)

    def wrapper(*args: Any, **kwargs: Any) -> T:
        key = key_from_args(args, kwargs)
        result = cache.get(key)
        if result is not None:
            return result
        result = func(*args, **kwargs)
        cache.put(key, result)
        return result

    wrapper.cache = cache
    wrapper.cache_clear = cache.clear
    return wrapper


def memoize_ttl(ttl: float, maxsize: int = 128) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator factory: memoize with TTL.

    Args:
        ttl: Time to live in seconds
        maxsize: Maximum cache size
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        cache = TTLCache(ttl=ttl, maxsize=maxsize)

        def wrapper(*args: Any, **kwargs: Any) -> T:
            key = key_from_args(args, kwargs)
            result = cache.get(key)
            if result is not None:
                return result
            result = func(*args, **kwargs)
            cache.put(key, result)
            return result

        wrapper.cache = cache
        wrapper.cache_clear = cache.clear
        return wrapper
    return decorator


def memoize_with_hash(
    hash_func: Callable[[Any], str] | None = None,
    capacity: int = 256,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator factory: memoize using custom hash function.

    Args:
        hash_func: Function to compute hash key from arguments
        capacity: Cache capacity
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        cache: LRUCache = LRUCache(capacity=capacity)

        def wrapper(*args: Any, **kwargs: Any) -> T:
            if hash_func:
                key = hash_func(args, kwargs)
            else:
                key = key_from_args(args, kwargs)
            result = cache.get(key)
            if result is not None:
                return result
            result = func(*args, **kwargs)
            cache.put(key, result)
            return result

        wrapper.cache = cache
        wrapper.cache_clear = cache.clear
        return wrapper
    return decorator


class CacheStats:
    """Track cache hit/miss statistics."""

    def __init__(self):
        self.hits = 0
        self.misses = 0
        self._lock = threading.Lock()

    def record_hit(self) -> None:
        with self._lock:
            self.hits += 1

    def record_miss(self) -> None:
        with self._lock:
            self.misses += 1

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses
        if total == 0:
            return 0.0
        return self.hits / total

    def reset(self) -> None:
        with self._lock:
            self.hits = 0
            self.misses = 0


def cached_property(func: Callable[..., T]) -> property:
    """
    Decorator: cache result of instance method as a property.

    Thread-safe.
    """
    attr_name = f"_cached_{func.__name__}"
    sentinel = object()

    def getter(self: Any) -> T:
        if not hasattr(self, attr_name) or getattr(self, attr_name, sentinel) is sentinel:
            value = func(self)
            object.__setattr__(self, attr_name, value)
        return getattr(self, attr_name)

    return property(getter)


class BloomFilter:
    """Probabilistic Bloom filter for set membership."""

    def __init__(self, size: int, num_hashes: int = 7):
        self.size = size
        self.num_hashes = num_hashes
        self._bits = [False] * size

    def add(self, item: str) -> None:
        for i in range(self.num_hashes):
            idx = self._hash(item, i)
            self._bits[idx] = True

    def might_contain(self, item: str) -> bool:
        return all(self._bits[self._hash(item, i)] for i in range(self.num_hashes))

    def _hash(self, item: str, seed: int) -> int:
        h = hashlib.md5(f"{seed}:{item}".encode()).hexdigest()
        return int(h, 16) % self.size


def disk_cache(
    cache_dir: str = "/tmp/disk_cache",
    max_bytes: int = 100 * 1024 * 1024,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator factory: cache function results to disk.

    Args:
        cache_dir: Directory for cache files
        max_bytes: Maximum cache size in bytes
    """
    import os
    import functools
    os.makedirs(cache_dir, exist_ok=True)

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            key_str = str(key_from_args(args, kwargs))
            key_hash = hashlib.md5(key_str.encode()).hexdigest()
            cache_file = os.path.join(cache_dir, f"{func.__name__}_{key_hash}.pkl")
            if os.path.exists(cache_file):
                try:
                    with open(cache_file, "rb") as f:
                        return pickle.load(f)
                except Exception:
                    pass
            result = func(*args, **kwargs)
            try:
                with open(cache_file, "wb") as f:
                    pickle.dump(result, f)
            except Exception:
                pass
            return result
        return wrapper
    return decorator

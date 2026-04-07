"""Cache utilities for RabAI AutoClick.

Provides:
- Caching helpers
- Memoization utilities
- Cache management
"""

import time
from typing import Any, Callable, Dict, Optional, Tuple
from functools import wraps


class CacheEntry:
    """Cache entry with value and expiration."""

    def __init__(self, value: Any, ttl: Optional[float] = None):
        self.value = value
        self.created_at = time.time()
        self.ttl = ttl

    def is_expired(self) -> bool:
        """Check if entry is expired."""
        if self.ttl is None:
            return False
        return time.time() - self.created_at > self.ttl


class LRUCache:
    """Least Recently Used cache."""

    def __init__(self, max_size: int = 100):
        self.max_size = max_size
        self._cache: Dict[str, Any] = {}
        self._access_order: list = []

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        if key not in self._cache:
            return None
        entry = self._cache[key]
        if isinstance(entry, CacheEntry) and entry.is_expired():
            self.delete(key)
            return None
        self._access_order.remove(key)
        self._access_order.append(key)
        value = self._cache[key]
        return value.value if isinstance(value, CacheEntry) else value

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Set value in cache."""
        if key in self._cache:
            self._access_order.remove(key)
        elif len(self._cache) >= self.max_size:
            oldest = self._access_order.pop(0)
            del self._cache[oldest]
        if ttl is not None:
            self._cache[key] = CacheEntry(value, ttl)
        else:
            self._cache[key] = value
        self._access_order.append(key)

    def delete(self, key: str) -> bool:
        """Delete value from cache."""
        if key in self._cache:
            del self._cache[key]
            self._access_order.remove(key)
            return True
        return False

    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()
        self._access_order.clear()

    def size(self) -> int:
        """Get cache size."""
        return len(self._cache)

    def keys(self) -> list:
        """Get all cache keys."""
        return list(self._cache.keys())


class TTLCache:
    """Time-To-Live cache."""

    def __init__(self, default_ttl: float = 60.0):
        self.default_ttl = default_ttl
        self._cache: Dict[str, CacheEntry] = {}

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        if key not in self._cache:
            return None
        entry = self._cache[key]
        if entry.is_expired():
            del self._cache[key]
            return None
        return entry.value

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Set value in cache."""
        if ttl is None:
            ttl = self.default_ttl
        self._cache[key] = CacheEntry(value, ttl)

    def delete(self, key: str) -> bool:
        """Delete value from cache."""
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()

    def cleanup_expired(self) -> int:
        """Remove expired entries."""
        expired_keys = [k for k, v in self._cache.items() if v.is_expired()]
        for key in expired_keys:
            del self._cache[key]
        return len(expired_keys)


class memoize:
    """Memoization decorator with TTL support."""

    def __init__(self, ttl: Optional[float] = None, max_size: int = 128):
        self.ttl = ttl
        self.max_size = max_size
        self._cache: Dict[str, CacheEntry] = {}

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = self._make_key(args, kwargs)
            if key in self._cache:
                entry = self._cache[key]
                if not entry.is_expired():
                    return entry.value
            result = func(*args, **kwargs)
            self._cache[key] = CacheEntry(result, self.ttl)
            if len(self._cache) > self.max_size:
                oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k].created_at)
                del self._cache[oldest_key]
            return result
        return wrapper

    def _make_key(self, args: tuple, kwargs: dict) -> str:
        """Make cache key from args and kwargs."""
        key_parts = [str(arg) for arg in args]
        key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
        return f"{hashlib_md5(','.join(key_parts))}" if key_parts else "empty"

    def clear(self) -> None:
        """Clear memoization cache."""
        self._cache.clear()


def cached(ttl: Optional[float] = None, max_size: int = 128):
    """Decorator to cache function results.

    Args:
        ttl: Time to live in seconds.
        max_size: Maximum cache size.

    Returns:
        Decorated function.
    """
    cache: Dict[str, Any] = {}
    cache_meta: Dict[str, float] = {}

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = str(args) + str(sorted(kwargs.items()))
            current_time = time.time()

            if key in cache:
                if ttl is None or current_time - cache_meta[key] < ttl:
                    return cache[key]
            result = func(*args, **kwargs)
            cache[key] = result
            cache_meta[key] = current_time

            if len(cache) > max_size:
                oldest_key = min(cache_meta.keys(), key=lambda k: cache_meta[k])
                del cache[oldest_key]
                del cache_meta[oldest_key]

            return result
        return wrapper
    return decorator


def hashlib_md5(text: str) -> str:
    """Generate MD5 hash.

    Args:
        text: Text to hash.

    Returns:
        MD5 hash string.
    """
    import hashlib
    return hashlib.md5(text.encode()).hexdigest()


def hashlib_sha256(text: str) -> str:
    """Generate SHA256 hash.

    Args:
        text: Text to hash.

    Returns:
        SHA256 hash string.
    """
    import hashlib
    return hashlib.sha256(text.encode()).hexdigest()


class CacheStats:
    """Cache statistics tracker."""

    def __init__(self):
        self.hits = 0
        self.misses = 0
        self.writes = 0
        self.deletes = 0

    def record_hit(self) -> None:
        """Record cache hit."""
        self.hits += 1

    def record_miss(self) -> None:
        """Record cache miss."""
        self.misses += 1

    def record_write(self) -> None:
        """Record cache write."""
        self.writes += 1

    def record_delete(self) -> None:
        """Record cache delete."""
        self.deletes += 1

    def hit_rate(self) -> float:
        """Calculate hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0

    def reset(self) -> None:
        """Reset statistics."""
        self.hits = 0
        self.misses = 0
        self.writes = 0
        self.deletes = 0


def in_memory_cache(max_size: int = 100, ttl: float = 300):
    """Create an in-memory cache.

    Args:
        max_size: Maximum number of entries.
        ttl: Default time-to-live in seconds.

    Returns:
        Tuple of (get, set, clear) functions.
    """
    cache: Dict[str, Tuple[Any, float]] = {}

    def get(key: str) -> Optional[Any]:
        if key in cache:
            value, expiry = cache[key]
            if expiry > time.time():
                return value
            del cache[key]
        return None

    def set(key: str, value: Any, custom_ttl: float = None) -> None:
        expiry = time.time() + (custom_ttl if custom_ttl is not None else ttl)
        if len(cache) >= max_size:
            oldest_key = min(cache.keys(), key=lambda k: cache[k][1])
            del cache[oldest_key]
        cache[key] = (value, expiry)

    def clear() -> None:
        cache.clear()

    return get, set, clear


def lru_cacheDecorator(max_size: int = 128):
    """LRU cache decorator.

    Args:
        max_size: Maximum cache size.

    Returns:
        Decorated function.
    """
    cache: Dict[str, Any] = {}
    order: list = []

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = str(args) + str(sorted(kwargs.items()))
            if key in cache:
                order.remove(key)
                order.append(key)
                return cache[key]
            result = func(*args, **kwargs)
            if len(cache) >= max_size:
                oldest = order.pop(0)
                del cache[oldest]
            cache[key] = result
            order.append(key)
            return result
        return wrapper
    return decorator


def fifo_cache(max_size: int = 128):
    """FIFO cache decorator.

    Args:
        max_size: Maximum cache size.

    Returns:
        Decorated function.
    """
    cache: Dict[str, Any] = {}
    keys_order: list = []

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            key = str(args) + str(sorted(kwargs.items()))
            if key in cache:
                return cache[key]
            result = func(*args, **kwargs)
            if len(cache) >= max_size:
                oldest = keys_order.pop(0)
                del cache[oldest]
            cache[key] = result
            keys_order.append(key)
            return result
        return wrapper
    return decorator

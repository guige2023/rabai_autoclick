"""Caching utilities for memoization and result caching."""

from typing import Callable, Optional, Any, Dict, Tuple
import time
import threading
import hashlib
import pickle


class CacheEntry:
    """Single cache entry with expiration."""

    def __init__(self, value: Any, ttl: Optional[float] = None):
        """Initialize cache entry.
        
        Args:
            value: Cached value.
            ttl: Time-to-live in seconds, None for no expiration.
        """
        self.value = value
        self.created_at = time.time()
        self.ttl = ttl

    @property
    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if self.ttl is None:
            return False
        return time.time() - self.created_at > self.ttl


class TTLCache:
    """Thread-safe cache with time-to-live expiration."""

    def __init__(self, max_size: int = 128, default_ttl: Optional[float] = None):
        """Initialize TTL cache.
        
        Args:
            max_size: Maximum number of entries.
            default_ttl: Default time-to-live in seconds.
        """
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    def _make_key(self, key: str) -> str:
        """Generate cache key hash."""
        return hashlib.md5(key.encode()).hexdigest()

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache.
        
        Args:
            key: Cache key.
        
        Returns:
            Cached value or None if not found or expired.
        """
        with self._lock:
            entry = self._cache.get(self._make_key(key))
            if entry and not entry.is_expired:
                self._hits += 1
                return entry.value
            self._misses += 1
            return None

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Set value in cache.
        
        Args:
            key: Cache key.
            value: Value to cache.
            ttl: Optional TTL override.
        """
        with self._lock:
            if len(self._cache) >= self.max_size:
                self._evict_oldest()
            cache_key = self._make_key(key)
            actual_ttl = ttl if ttl is not None else self.default_ttl
            self._cache[cache_key] = CacheEntry(value, actual_ttl)

    def _evict_oldest(self) -> None:
        """Evict oldest non-expired entry."""
        if not self._cache:
            return
        oldest_key = min(self._cache, key=lambda k: self._cache[k].created_at)
        del self._cache[oldest_key]

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0

    @property
    def hit_rate(self) -> float:
        """Get cache hit rate."""
        total = self._hits + self._misses
        return self._hits / total if total > 0 else 0.0


def memoize(ttl: Optional[float] = None, max_size: int = 128):
    """Decorator to memoize function results.
    
    Args:
        ttl: Optional time-to-live in seconds.
        max_size: Maximum cache size.
    
    Returns:
        Decorated function with caching.
    """
    cache = TTLCache(max_size=max_size, default_ttl=ttl)

    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            key = f"{func.__name__}:{str(args)}:{str(kwargs)}"
            result = cache.get(key)
            if result is not None:
                return result
            result = func(*args, **kwargs)
            cache.set(key, result)
            return result
        wrapper.cache = cache
        return wrapper
    return decorator

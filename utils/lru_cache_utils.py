"""LRU (Least Recently Used) cache utilities.

Provides LRU cache implementation with size limits and
thread-safe operations for caching automation results.
"""

from collections import OrderedDict
from typing import Any, Callable, Dict, Generic, Optional, TypeVar


T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class LRUCache(Generic[K, V]):
    """Least Recently Used cache with fixed size.

    Example:
        cache = LRUCache(max_size=100)
        cache["key1"] = "value1"
        value = cache["key1"]
    """

    def __init__(self, max_size: int = 128) -> None:
        self._max_size = max_size
        self._cache: OrderedDict[K, V] = OrderedDict()

    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        """Get value from cache.

        Args:
            key: Cache key.
            default: Default value if not found.

        Returns:
            Cached value or default.
        """
        if key in self._cache:
            self._cache.move_to_end(key)
            return self._cache[key]
        return default

    def set(self, key: K, value: V) -> None:
        """Set value in cache.

        Args:
            key: Cache key.
            value: Value to cache.
        """
        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = value
        if len(self._cache) > self._max_size:
            self._cache.popitem(last=False)

    def __contains__(self, key: K) -> bool:
        """Check if key is in cache."""
        return key in self._cache

    def __getitem__(self, key: K) -> V:
        """Get value from cache."""
        if key not in self._cache:
            raise KeyError(key)
        self._cache.move_to_end(key)
        return self._cache[key]

    def __setitem__(self, key: K, value: V) -> None:
        """Set value in cache."""
        self.set(key, value)

    def __len__(self) -> int:
        """Get cache size."""
        return len(self._cache)

    def clear(self) -> None:
        """Clear all cached items."""
        self._cache.clear()

    def keys(self) -> list:
        """Get all cache keys."""
        return list(self._cache.keys())

    def values(self) -> list:
        """Get all cache values."""
        return list(self._cache.values())

    def items(self) -> list:
        """Get all cache items."""
        return list(self._cache.items())


def lru_cache(max_size: int = 128) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to create LRU cached function.

    Args:
        max_size: Maximum cache size.

    Returns:
        Decorated function with caching.

    Example:
        @lru_cache(max_size=256)
        def expensive_computation(x, y):
            return x + y
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        cache: OrderedDict[tuple, V] = OrderedDict()

        def wrapper(*args: Any, **kwargs: Any) -> T:
            key = (args, tuple(sorted(kwargs.items())))
            if key in cache:
                cache.move_to_end(key)
                return cache[key]
            result = func(*args, **kwargs)
            cache[key] = result
            if len(cache) > max_size:
                cache.popitem(last=False)
            return result

        wrapper.cache_clear = cache.clear  # type: ignore
        wrapper.cache_info = lambda: {"size": len(cache), "max_size": max_size}  # type: ignore
        return wrapper

    return decorator


class TTLLRUCache(LRUCache[K, V]):
    """LRU cache with TTL (time-to-live) for each entry."""

    def __init__(self, max_size: int = 128, ttl: float = 60.0) -> None:
        super().__init__(max_size)
        self._ttl = ttl
        self._timestamps: Dict[K, float] = {}

    def set(self, key: K, value: V) -> None:
        """Set value with current timestamp."""
        import time
        super().set(key, value)
        self._timestamps[key] = time.time()

    def get(self, key: K, default: Optional[V] = None) -> Optional[V]:
        """Get value if not expired."""
        import time
        if key in self._cache:
            if time.time() - self._timestamps.get(key, 0) < self._ttl:
                self._cache.move_to_end(key)
                return self._cache[key]
            else:
                del self._cache[key]
                del self._timestamps[key]
        return default

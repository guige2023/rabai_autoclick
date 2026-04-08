"""UI element cache for performance optimization in automation.

Provides caching strategies for frequently accessed UI elements
to reduce expensive lookup operations.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Generic, Optional, TypeVar


class CacheStrategy(Enum):
    """Cache eviction strategies."""
    LRU = auto()      # Least Recently Used
    LFU = auto()      # Least Frequently Used
    FIFO = auto()     # First In, First Out
    TTL = auto()      # Time To Live based
    FIFO_LRU = auto() # Hybrid: FIFO with LRU on collision


@dataclass
class CacheEntry(Generic[Any]):
    """A cached value with metadata."""
    key: str
    value: Any
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0
    hit_count: int = 0

    def access(self) -> Any:
        """Record an access and return value."""
        self.last_accessed = time.time()
        self.access_count += 1
        return self.value

    def age(self) -> float:
        """Return age in seconds since creation."""
        return time.time() - self.created_at

    def idle_time(self) -> float:
        """Return seconds since last access."""
        return time.time() - self.last_accessed


class UIElementCache:
    """Cache for UI element lookups.

    Provides configurable caching with TTL, size limits,
    and multiple eviction strategies.
    """

    def __init__(
        self,
        strategy: CacheStrategy = CacheStrategy.LRU,
        max_size: int = 100,
        default_ttl: float = 60.0,
    ) -> None:
        """Initialize the cache.

        Args:
            strategy: Eviction strategy to use.
            max_size: Maximum number of entries.
            default_ttl: Default time-to-live in seconds.
        """
        self._strategy = strategy
        self._max_size = max_size
        self._default_ttl = default_ttl
        self._entries: dict[str, CacheEntry[Any]] = {}
        self._access_order: list[str] = []
        self._hit_count = 0
        self._miss_count = 0

    def get(self, key: str) -> Optional[Any]:
        """Get a cached value by key.

        Returns None if not found or expired.
        """
        entry = self._entries.get(key)
        if entry is None:
            self._miss_count += 1
            return None

        if entry.age() > self._default_ttl:
            self._evict(key)
            self._miss_count += 1
            return None

        entry.hit_count += 1
        self._update_access_order(key)
        self._hit_count += 1
        return entry.access()

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Set a cache entry.

        Args:
            key: Cache key.
            value: Value to cache.
            ttl: Optional TTL override (uses default if None).
        """
        if len(self._entries) >= self._max_size and key not in self._entries:
            self._evict_one()

        entry = CacheEntry(key=key, value=value)
        self._entries[key] = entry
        self._update_access_order(key)

    def invalidate(self, key: str) -> bool:
        """Remove a specific entry. Returns True if found."""
        if key in self._entries:
            self._evict(key)
            return True
        return False

    def clear(self) -> None:
        """Clear all cache entries."""
        self._entries.clear()
        self._access_order.clear()

    def _evict(self, key: str) -> None:
        """Remove an entry by key."""
        if key in self._entries:
            del self._entries[key]
        if key in self._access_order:
            self._access_order.remove(key)

    def _evict_one(self) -> None:
        """Evict one entry based on strategy."""
        if not self._entries:
            return

        if self._strategy == CacheStrategy.LRU:
            lru_key = min(
                self._entries.keys(),
                key=lambda k: self._entries[k].last_accessed,
            )
            self._evict(lru_key)

        elif self._strategy == CacheStrategy.LFU:
            lfu_key = min(
                self._entries.keys(),
                key=lambda k: self._entries[k].access_count,
            )
            self._evict(lfu_key)

        elif self._strategy == CacheStrategy.FIFO:
            self._evict(self._access_order[0])

        elif self._strategy == CacheStrategy.TTL:
            oldest_key = min(
                self._entries.keys(),
                key=lambda k: self._entries[k].created_at,
            )
            self._evict(oldest_key)

        else:
            if self._access_order:
                self._evict(self._access_order[0])

    def _update_access_order(self, key: str) -> None:
        """Update LRU access order."""
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    @property
    def size(self) -> int:
        """Return current number of entries."""
        return len(self._entries)

    @property
    def hit_rate(self) -> float:
        """Return cache hit rate (0.0-1.0)."""
        total = self._hit_count + self._miss_count
        if total == 0:
            return 0.0
        return self._hit_count / total

    @property
    def stats(self) -> dict[str, Any]:
        """Return cache statistics."""
        return {
            "size": self.size,
            "max_size": self._max_size,
            "hit_count": self._hit_count,
            "miss_count": self._miss_count,
            "hit_rate": self.hit_rate,
            "strategy": self._strategy.name,
        }


T = TypeVar("T")


def cached(
    cache: UIElementCache,
    key_func: Optional[Callable[..., str]] = None,
    ttl: Optional[float] = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to cache function results.

    Args:
        cache: UIElementCache instance to use.
        key_func: Optional function to generate cache key from args.
        ttl: Optional TTL for cached results.
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            if key_func:
                key = key_func(*args, **kwargs)
            else:
                key = f"{func.__name__}:{str(args)}:{str(kwargs)}"

            cached_val = cache.get(key)
            if cached_val is not None:
                return cached_val

            result = func(*args, **kwargs)
            cache.set(key, result, ttl)
            return result
        return wrapper
    return decorator

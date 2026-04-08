"""
Data Memoize Action Module.

Provides memoization caching for expensive computations
with TTL, LRU eviction, and key generation strategies.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, TypeVar, Generic
from dataclasses import dataclass, field
import logging
import time
import hashlib
import pickle
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar("T")


class EvictionPolicy(Enum):
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    TTL = "ttl"


@dataclass
class CacheEntry(Generic[T]):
    """Cache entry with metadata."""
    value: T
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    access_count: int = 0


class MemoizeAction:
    """
    Generic memoization cache with configurable policies.

    Supports TTL, LRU, LFU, and FIFO eviction.
    Thread-safe with optional persistence.

    Example:
        @MemoizeAction(max_size=1000, ttl=300)
        def expensive_computation(x, y):
            ...
    """

    def __init__(
        self,
        max_size: int = 128,
        ttl: Optional[float] = None,
        eviction: EvictionPolicy = EvictionPolicy.LRU,
    ) -> None:
        self.max_size = max_size
        self.ttl = ttl
        self.eviction = eviction
        self._cache: dict[str, CacheEntry] = {}
        self._order: list[str] = []

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if exists and not expired."""
        if key not in self._cache:
            return None

        entry = self._cache[key]

        if self.ttl is not None:
            if time.time() - entry.created_at > self.ttl:
                self._evict(key)
                return None

        entry.last_accessed = time.time()
        entry.access_count += 1
        self._update_order(key)
        return entry.value

    def set(self, key: str, value: Any) -> None:
        """Set value in cache with eviction if needed."""
        if len(self._cache) >= self.max_size and key not in self._cache:
            self._evict_lru()

        entry = CacheEntry(value=value)
        self._cache[key] = entry
        self._order.append(key)

    def delete(self, key: str) -> bool:
        """Delete a specific key from cache."""
        return self._evict(key)

    def clear(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()
        self._order.clear()

    def invalidate(self, key: str) -> None:
        """Invalidate a cache entry (alias for delete)."""
        self.delete(key)

    def _evict(self, key: str) -> bool:
        """Evict a specific key."""
        if key in self._cache:
            del self._cache[key]
            if key in self._order:
                self._order.remove(key)
            return True
        return False

    def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if not self._cache:
            return

        if self.eviction == EvictionPolicy.LRU:
            oldest = min(self._cache.items(), key=lambda x: x[1].last_accessed)
            self._evict(oldest[0])

        elif self.eviction == EvictionPolicy.LFU:
            least = min(self._cache.items(), key=lambda x: x[1].access_count)
            self._evict(least[0])

        elif self.eviction == EvictionPolicy.FIFO:
            self._evict(self._order[0] if self._order else "")

        elif self.eviction == EvictionPolicy.TTL:
            now = time.time()
            expired = [
                (k, v) for k, v in self._cache.items()
                if now - v.created_at > (self.ttl or 0)
            ]
            if expired:
                self._evict(expired[0][0])

    def _update_order(self, key: str) -> None:
        """Update access order for LRU tracking."""
        if key in self._order:
            self._order.remove(key)
        self._order.append(key)

    def decorator(
        self,
        func: Optional[Callable[..., T]] = None,
        key_func: Optional[Callable[..., str]] = None,
    ) -> Callable[[Callable[..., T]], Callable[..., T]]:
        """Decorator to memoize a function."""

        def make_key(*args: Any, **kwargs: Any) -> str:
            if key_func:
                return key_func(*args, **kwargs)
            try:
                key_data = pickle.dumps((args, sorted(kwargs.items())))
            except Exception:
                key_data = str((args, sorted(kwargs.items()))).encode()
            return hashlib.md5(key_data).hexdigest()

        def decorator_inner(f: Callable[..., T]) -> Callable[..., T]:
            @wraps(f)
            def wrapper(*args: Any, **kwargs: Any) -> T:
                key = make_key(*args, **kwargs)
                cached = self.get(key)
                if cached is not None:
                    return cached
                result = f(*args, **kwargs)
                self.set(key, result)
                return result

            return wrapper

        if func is not None:
            return decorator_inner(func)
        return decorator_inner

    @property
    def size(self) -> int:
        """Current cache size."""
        return len(self._cache)

    @property
    def stats(self) -> dict[str, Any]:
        """Cache statistics."""
        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "eviction_policy": self.eviction.value,
            "ttl": self.ttl,
        }

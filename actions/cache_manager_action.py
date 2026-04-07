"""
Cache Manager Action Module.

Provides in-memory and disk-based caching with TTL,
cache invalidation, and memoization decorators.

Example:
    >>> from cache_manager_action import CacheManager
    >>> cache = CacheManager(ttl=300)
    >>> cache.set("key", data)
    >>> cached = cache.get("key")
"""
from __future__ import annotations

import hashlib
import json
import os
import pickle
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional


@dataclass
class CacheEntry:
    """A cached value with metadata."""
    value: Any
    created_at: float
    expires_at: float
    hit_count: int = 0


class CacheManager:
    """In-memory cache with TTL and disk persistence."""

    def __init__(
        self,
        ttl: float = 300,
        max_size: int = 1000,
        persist_path: Optional[str] = None,
    ):
        self._cache: dict[str, CacheEntry] = {}
        self._ttl = ttl
        self._max_size = max_size
        self._persist_path = persist_path
        self._hits = 0
        self._misses = 0

        if persist_path and os.path.exists(persist_path):
            self.load()

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
    ) -> None:
        """Store value in cache."""
        now = time.time()
        expires = now + (ttl or self._ttl)

        if len(self._cache) >= self._max_size and key not in self._cache:
            self._evict_oldest()

        self._cache[key] = CacheEntry(
            value=value,
            created_at=now,
            expires_at=expires,
        )

    def get(self, key: str) -> Optional[Any]:
        """Retrieve value from cache if not expired."""
        entry = self._cache.get(key)
        if entry is None:
            self._misses += 1
            return None

        now = time.time()
        if now > entry.expires_at:
            del self._cache[key]
            self._misses += 1
            return None

        entry.hit_count += 1
        self._hits += 1
        return entry.value

    def delete(self, key: str) -> bool:
        """Delete a cache entry."""
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()
        self._hits = 0
        self._misses = 0

    def has(self, key: str) -> bool:
        """Check if key exists and is not expired."""
        entry = self._cache.get(key)
        if entry is None:
            return False
        if time.time() > entry.expires_at:
            del self._cache[key]
            return False
        return True

    def get_or_set(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: Optional[float] = None,
    ) -> Any:
        """Get from cache or compute and store."""
        value = self.get(key)
        if value is not None:
            return value
        value = factory()
        self.set(key, value, ttl)
        return value

    async def get_or_set_async(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: Optional[float] = None,
    ) -> Any:
        """Async version of get_or_set."""
        value = self.get(key)
        if value is not None:
            return value
        if asyncio.iscoroutinefunction(factory):
            value = await factory()
        else:
            value = factory()
        self.set(key, value, ttl)
        return value

    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching pattern."""
        import re
        compiled = re.compile(pattern)
        to_delete = [k for k in self._cache if compiled.search(k)]
        for k in to_delete:
            del self._cache[k]
        return len(to_delete)

    def _evict_oldest(self) -> None:
        if not self._cache:
            return
        oldest_key = min(self._cache, key=lambda k: self._cache[k].created_at)
        del self._cache[oldest_key]

    def save(self) -> bool:
        """Persist cache to disk."""
        if not self._persist_path:
            return False
        try:
            data = {
                "cache": {k: {"v": v.value, "c": v.created_at, "e": v.expires_at} for k, v in self._cache.items()},
                "hits": self._hits,
                "misses": self._misses,
            }
            with open(self._persist_path, "wb") as f:
                pickle.dump(data, f)
            return True
        except Exception:
            return False

    def load(self) -> bool:
        """Load cache from disk."""
        if not self._persist_path or not os.path.exists(self._persist_path):
            return False
        try:
            with open(self._persist_path, "rb") as f:
                data = pickle.load(f)
            self._cache = {
                k: CacheEntry(
                    value=v["v"],
                    created_at=v["c"],
                    expires_at=v["e"],
                )
                for k, v in data.get("cache", {}).items()
            }
            self._hits = data.get("hits", 0)
            self._misses = data.get("misses", 0)
            return True
        except Exception:
            return False

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
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

    def keys(self) -> list[str]:
        """List all cache keys (excluding expired)."""
        self.cleanup()
        return list(self._cache.keys())

    def cleanup(self) -> int:
        """Remove expired entries."""
        now = time.time()
        expired = [k for k, v in self._cache.items() if now > v.expires_at]
        for k in expired:
            del self._cache[k]
        return len(expired)


def memoize(ttl: float = 300, cache: Optional[CacheManager] = None):
    """
    Decorator to memoize function results.

    Example:
        @memoize(ttl=60)
        def expensive_function(arg1, arg2):
            return compute(arg1, arg2)
    """
    _cache = cache or CacheManager(ttl=ttl)

    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs) -> Any:
            key = _make_key(func.__name__, args, kwargs)
            return _cache.get_or_set(key, lambda: func(*args, **kwargs))

        async def async_wrapper(*args, **kwargs) -> Any:
            key = _make_key(func.__name__, args, kwargs)
            return await _cache.get_or_set_async(key, lambda: func(*args, **kwargs))

        def _make_key(func_name: str, args: tuple, kwargs: dict) -> str:
            key_data = {
                "func": func_name,
                "args": args,
                "kwargs": kwargs,
            }
            key_str = json.dumps(key_data, sort_keys=True, default=str)
            return hashlib.md5(key_str.encode()).hexdigest()

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return wrapper

    return decorator


def _make_key(func_name: str, args: tuple, kwargs: dict) -> str:
    key_data = {"func": func_name, "args": args, "kwargs": kwargs}
    return hashlib.md5(json.dumps(key_data, sort_keys=True, default=str).encode()).hexdigest()


if __name__ == "__main__":
    import asyncio

    cache = CacheManager(ttl=60)
    cache.set("test", {"data": "value"}, ttl=10)
    print(f"Get test: {cache.get('test')}")
    print(f"Has test: {cache.has('test')}")
    print(f"Stats: {cache.get_stats()}")

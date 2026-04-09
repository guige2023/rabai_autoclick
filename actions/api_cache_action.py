"""API response caching with TTL and invalidation.

This module provides caching functionality for API responses with
time-to-live (TTL) support, automatic expiration, and manual invalidation.

Example:
    >>> from actions.api_cache_action import APICache
    >>> cache = APICache(ttl=300)
    >>> result = cache.get_or_fetch(get_user, user_id=123)
"""

from __future__ import annotations

import time
import json
import hashlib
import threading
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """A cached response entry."""
    key: str
    value: Any
    created_at: float
    ttl: float
    hit_count: int = 0

    def is_expired(self) -> bool:
        """Check if the entry has expired."""
        return time.time() - self.created_at > self.ttl

    def age(self) -> float:
        """Get the age of the entry in seconds."""
        return time.time() - self.created_at


class APICache:
    """In-memory cache for API responses.

    Attributes:
        ttl: Default time-to-live for cache entries in seconds.
        max_size: Maximum number of entries to cache.
    """

    def __init__(
        self,
        ttl: float = 300.0,
        max_size: int = 1000,
        persist_path: Optional[Path] = None,
    ) -> None:
        self.ttl = ttl
        self.max_size = max_size
        self._cache: dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._persist_path = persist_path
        if persist_path:
            self._load_from_disk()

    def get(self, key: str) -> Optional[Any]:
        """Get a cached value by key.

        Args:
            key: The cache key.

        Returns:
            The cached value or None if not found or expired.
        """
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                logger.debug(f"Cache miss: {key}")
                return None
            if entry.is_expired():
                logger.debug(f"Cache expired: {key}")
                del self._cache[key]
                return None
            entry.hit_count += 1
            logger.debug(f"Cache hit: {key}")
            return entry.value

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> None:
        """Set a cache entry.

        Args:
            key: The cache key.
            value: The value to cache.
            ttl: Optional custom TTL for this entry.
        """
        with self._lock:
            if len(self._cache) >= self.max_size:
                self._evict_oldest()
            self._cache[key] = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                ttl=ttl or self.ttl,
            )
            logger.debug(f"Cache set: {key}")

    def delete(self, key: str) -> bool:
        """Delete a cache entry.

        Args:
            key: The cache key to delete.

        Returns:
            True if the key was deleted, False if not found.
        """
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                logger.debug(f"Cache deleted: {key}")
                return True
            return False

    def clear(self) -> int:
        """Clear all cache entries.

        Returns:
            Number of entries cleared.
        """
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            logger.debug(f"Cache cleared: {count} entries")
            return count

    def get_or_fetch(
        self,
        func: Callable[..., Any],
        *args: Any,
        cache_key: Optional[str] = None,
        ttl: Optional[float] = None,
        **kwargs: Any,
    ) -> Any:
        """Get from cache or fetch from function.

        Args:
            func: The function to call if cache miss.
            *args: Positional arguments for the function.
            cache_key: Optional custom cache key.
            ttl: Optional custom TTL for this entry.
            **kwargs: Keyword arguments for the function.

        Returns:
            The cached or freshly fetched value.
        """
        if cache_key is None:
            cache_key = self._make_key(func, args, kwargs)
        cached = self.get(cache_key)
        if cached is not None:
            return cached
        result = func(*args, **kwargs)
        self.set(cache_key, result, ttl=ttl)
        return result

    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching a pattern.

        Args:
            pattern: Glob-style pattern (e.g., "user:*").

        Returns:
            Number of keys invalidated.
        """
        import fnmatch
        with self._lock:
            keys = [k for k in self._cache.keys() if fnmatch.fnmatch(k, pattern)]
            for key in keys:
                del self._cache[key]
            logger.debug(f"Cache invalidated {len(keys)} keys matching: {pattern}")
            return len(keys)

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary containing cache stats.
        """
        with self._lock:
            total_hits = sum(e.hit_count for e in self._cache.values())
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "total_hits": total_hits,
                "avg_ttl": self.ttl,
            }

    def _evict_oldest(self) -> None:
        """Evict the oldest cache entry."""
        if not self._cache:
            return
        oldest_key = min(
            self._cache.keys(),
            key=lambda k: self._cache[k].created_at,
        )
        del self._cache[oldest_key]

    def _make_key(
        self,
        func: Callable[..., Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> str:
        """Generate a cache key from function and arguments."""
        key_parts = [
            func.__module__,
            func.__name__,
            str(args),
            str(sorted(kwargs.items())),
        ]
        key_str = "|".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()

    def _load_from_disk(self) -> None:
        """Load cache from disk."""
        if not self._persist_path or not self._persist_path.exists():
            return
        try:
            with open(self._persist_path, "r") as f:
                data = json.load(f)
            with self._lock:
                for key, entry_data in data.items():
                    self._cache[key] = CacheEntry(
                        key=key,
                        value=entry_data["value"],
                        created_at=entry_data["created_at"],
                        ttl=entry_data["ttl"],
                        hit_count=entry_data.get("hit_count", 0),
                    )
            logger.info(f"Loaded {len(self._cache)} entries from disk")
        except Exception as e:
            logger.warning(f"Failed to load cache from disk: {e}")

    def save_to_disk(self) -> None:
        """Save cache to disk."""
        if not self._persist_path:
            return
        try:
            self._persist_path.parent.mkdir(parents=True, exist_ok=True)
            with self._lock:
                data = {
                    key: {
                        "value": entry.value,
                        "created_at": entry.created_at,
                        "ttl": entry.ttl,
                        "hit_count": entry.hit_count,
                    }
                    for key, entry in self._cache.items()
                }
            with open(self._persist_path, "w") as f:
                json.dump(data, f)
            logger.debug(f"Saved {len(data)} entries to disk")
        except Exception as e:
            logger.warning(f"Failed to save cache to disk: {e}")

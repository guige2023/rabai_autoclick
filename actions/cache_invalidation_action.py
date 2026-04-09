"""Cache invalidation action for managing cache invalidation strategies.

Provides TTL-based, tag-based, and pattern-based cache invalidation
with dependency tracking.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class InvalidationStrategy(Enum):
    TTL = "ttl"
    LRU = "lru"
    TAGS = "tags"
    PATTERN = "pattern"
    MANUAL = "manual"


@dataclass
class CacheEntry:
    key: str
    value: Any
    created_at: float
    accessed_at: float
    ttl: Optional[float] = None
    tags: set[str] = field(default_factory=set)
    size_bytes: int = 0
    hit_count: int = 0


@dataclass
class InvalidationEvent:
    strategy: InvalidationStrategy
    keys: list[str]
    timestamp: float = field(default_factory=time.time)
    reason: str = ""


class CacheInvalidationAction:
    """Manage cache invalidation with multiple strategies.

    Args:
        default_ttl: Default TTL in seconds.
        max_entries: Maximum cache entries.
        enable_stats: Enable cache statistics.
    """

    def __init__(
        self,
        default_ttl: float = 3600.0,
        max_entries: int = 10000,
        enable_stats: bool = True,
    ) -> None:
        self._cache: dict[str, CacheEntry] = {}
        self._default_ttl = default_ttl
        self._max_entries = max_entries
        self._enable_stats = enable_stats
        self._stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "invalidations": 0,
        }
        self._invalidation_listeners: list[Callable[[InvalidationEvent], None]] = []
        self._tag_index: dict[str, set[str]] = {}

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        tags: Optional[set[str]] = None,
    ) -> None:
        """Set a cache entry.

        Args:
            key: Cache key.
            value: Value to cache.
            ttl: Time-to-live in seconds.
            tags: Tags for invalidation grouping.
        """
        now = time.time()

        if len(self._cache) >= self._max_entries and key not in self._cache:
            self._evict_lru()

        entry = CacheEntry(
            key=key,
            value=value,
            created_at=now,
            accessed_at=now,
            ttl=ttl or self._default_ttl,
            tags=tags or set(),
            size_bytes=self._estimate_size(value),
        )
        self._cache[key] = entry

        for tag in entry.tags:
            if tag not in self._tag_index:
                self._tag_index[tag] = set()
            self._tag_index[tag].add(key)

        logger.debug(f"Cache set: {key}")

    def get(self, key: str) -> Optional[Any]:
        """Get a cache entry.

        Args:
            key: Cache key.

        Returns:
            Cached value or None.
        """
        entry = self._cache.get(key)
        if not entry:
            self._stats["misses"] += 1
            return None

        if self._is_expired(entry):
            self.invalidate(key)
            self._stats["misses"] += 1
            return None

        entry.accessed_at = time.time()
        entry.hit_count += 1
        self._stats["hits"] += 1
        return entry.value

    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if a cache entry is expired.

        Args:
            entry: Cache entry.

        Returns:
            True if expired.
        """
        if entry.ttl is None:
            return False
        return time.time() > entry.created_at + entry.ttl

    def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if not self._cache:
            return

        lru_key = min(
            self._cache.items(),
            key=lambda x: x[1].accessed_at
        )[0]

        self._evict(lru_key)

    def _evict(self, key: str) -> None:
        """Evict a specific entry.

        Args:
            key: Key to evict.
        """
        if key in self._cache:
            entry = self._cache[key]
            for tag in entry.tags:
                self._tag_index.get(tag, set()).discard(key)

            del self._cache[key]
            self._stats["evictions"] += 1
            logger.debug(f"Evicted: {key}")

    def invalidate(self, key: str) -> bool:
        """Invalidate a cache entry.

        Args:
            key: Key to invalidate.

        Returns:
            True if key was found and invalidated.
        """
        if key not in self._cache:
            return False

        self._evict(key)
        self._stats["invalidations"] += 1
        self._notify_invalidation(InvalidationEvent(
            strategy=InvalidationStrategy.MANUAL,
            keys=[key],
            reason="manual",
        ))
        return True

    def invalidate_by_tag(self, tag: str) -> int:
        """Invalidate all entries with a specific tag.

        Args:
            tag: Tag to invalidate.

        Returns:
            Number of entries invalidated.
        """
        keys = self._tag_index.get(tag, set()).copy()
        count = 0
        for key in keys:
            if self.invalidate(key):
                count += 1

        self._notify_invalidation(InvalidationEvent(
            strategy=InvalidationStrategy.TAGS,
            keys=list(keys),
            reason=f"tag={tag}",
        ))
        return count

    def invalidate_by_pattern(self, pattern: str) -> int:
        """Invalidate entries matching a key pattern.

        Args:
            pattern: Key pattern (supports * wildcard).

        Returns:
            Number of entries invalidated.
        """
        import fnmatch

        keys = [k for k in self._cache.keys() if fnmatch.fnmatch(k, pattern)]
        count = 0
        for key in keys:
            if self.invalidate(key):
                count += 1

        self._notify_invalidation(InvalidationEvent(
            strategy=InvalidationStrategy.PATTERN,
            keys=keys,
            reason=f"pattern={pattern}",
        ))
        return count

    def invalidate_expired(self) -> int:
        """Invalidate all expired entries.

        Returns:
            Number of entries invalidated.
        """
        expired_keys = [
            key for key, entry in self._cache.items()
            if self._is_expired(entry)
        ]

        count = 0
        for key in expired_keys:
            if self.invalidate(key):
                count += 1

        if count > 0:
            self._notify_invalidation(InvalidationEvent(
                strategy=InvalidationStrategy.TTL,
                keys=expired_keys,
                reason="expired",
            ))

        return count

    def clear(self) -> int:
        """Clear all cache entries.

        Returns:
            Number of entries cleared.
        """
        count = len(self._cache)
        self._cache.clear()
        self._tag_index.clear()
        self._stats["invalidations"] += count
        return count

    def _estimate_size(self, value: Any) -> int:
        """Estimate size of a value in bytes.

        Args:
            value: Value to measure.

        Returns:
            Estimated size in bytes.
        """
        try:
            import sys
            return len(str(value))
        except Exception:
            return 0

    def _notify_invalidation(self, event: InvalidationEvent) -> None:
        """Notify listeners of invalidation.

        Args:
            event: Invalidation event.
        """
        for listener in self._invalidation_listeners:
            try:
                listener(event)
            except Exception as e:
                logger.error(f"Invalidation listener error: {e}")

    def register_invalidation_listener(
        self,
        listener: Callable[[InvalidationEvent], None],
    ) -> None:
        """Register a listener for invalidation events.

        Args:
            listener: Callback function.
        """
        self._invalidation_listeners.append(listener)

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dictionary with cache stats.
        """
        total_requests = self._stats["hits"] + self._stats["misses"]
        hit_rate = (
            self._stats["hits"] / total_requests
            if total_requests > 0 else 0.0
        )

        return {
            "entries": len(self._cache),
            "max_entries": self._max_entries,
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "hit_rate": hit_rate,
            "evictions": self._stats["evictions"],
            "invalidations": self._stats["invalidations"],
            "tags": len(self._tag_index),
        }

    def reset_stats(self) -> None:
        """Reset cache statistics."""
        self._stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "invalidations": 0,
        }

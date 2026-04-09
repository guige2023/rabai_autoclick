"""API cache invalidation strategies.

This module provides cache invalidation:
- Time-based expiration
- Tag-based invalidation
- Dependency tracking
- Pattern-based invalidation

Example:
    >>> from actions.api_cache_invalidation_action import CacheInvalidator
    >>> invalidator = CacheInvalidator()
    >>> invalidator.invalidate_by_tag("user:123")
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """A cache entry with invalidation metadata."""
    key: str
    value: Any
    created_at: float
    ttl: Optional[float] = None
    tags: list[str] = field(default_factory=list)
    dependencies: list[str] = field(default_factory=list)


class CacheInvalidator:
    """Manage cache invalidation.

    Example:
        >>> invalidator = CacheInvalidator()
        >>> invalidator.set("user:123", data, tags=["user", "profile"])
        >>> invalidator.invalidate_by_tag("user")
    """

    def __init__(self) -> None:
        self._cache: dict[str, CacheEntry] = {}
        self._tags: dict[str, set[str]] = defaultdict(set)
        self._dependencies: dict[str, set[str]] = defaultdict(set)
        self._lock = threading.RLock()

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        tags: Optional[list[str]] = None,
        dependencies: Optional[list[str]] = None,
    ) -> None:
        """Set a cache entry with invalidation metadata.

        Args:
            key: Cache key.
            value: Value to cache.
            ttl: Time-to-live in seconds.
            tags: Tags for tag-based invalidation.
            dependencies: Keys this entry depends on.
        """
        entry = CacheEntry(
            key=key,
            value=value,
            created_at=time.time(),
            ttl=ttl,
            tags=tags or [],
            dependencies=dependencies or [],
        )
        with self._lock:
            self._cache[key] = entry
            for tag in entry.tags:
                self._tags[tag].add(key)
            for dep in entry.dependencies:
                self._dependencies[dep].add(key)

    def get(self, key: str) -> tuple[Any, bool]:
        """Get a cache entry.

        Args:
            key: Cache key.

        Returns:
            Tuple of (value, found).
        """
        with self._lock:
            entry = self._cache.get(key)
            if not entry:
                return None, False
            if entry.ttl and (time.time() - entry.created_at) > entry.ttl:
                del self._cache[key]
                return None, False
            return entry.value, True

    def invalidate(self, key: str) -> bool:
        """Invalidate a specific key.

        Args:
            key: Key to invalidate.

        Returns:
            True if key was found and invalidated.
        """
        with self._lock:
            if key not in self._cache:
                return False
            entry = self._cache[key]
            for tag in entry.tags:
                self._tags[tag].discard(key)
            del self._cache[key]
            return True

    def invalidate_by_tag(self, tag: str) -> int:
        """Invalidate all entries with a tag.

        Args:
            tag: Tag to match.

        Returns:
            Number of entries invalidated.
        """
        with self._lock:
            keys = list(self._tags.get(tag, set()))
            count = 0
            for key in keys:
                if key in self._cache:
                    entry = self._cache[key]
                    for t in entry.tags:
                        self._tags[t].discard(key)
                    del self._cache[key]
                    count += 1
            return count

    def invalidate_by_pattern(self, pattern: str) -> int:
        """Invalidate keys matching a pattern.

        Args:
            pattern: Glob pattern (e.g., "user:*").

        Returns:
            Number of entries invalidated.
        """
        import fnmatch
        with self._lock:
            keys = [k for k in self._cache.keys() if fnmatch.fnmatch(k, pattern)]
            count = 0
            for key in keys:
                entry = self._cache[key]
                for tag in entry.tags:
                    self._tags[tag].discard(key)
                del self._cache[key]
                count += 1
            return count

    def invalidate_by_dependency(self, dependency: str) -> int:
        """Invalidate entries that depend on a key.

        Args:
            dependency: Dependency key.

        Returns:
            Number of entries invalidated.
        """
        with self._lock:
            keys = list(self._dependencies.get(dependency, set()))
            count = 0
            for key in keys:
                if key in self._cache:
                    entry = self._cache[key]
                    for tag in entry.tags:
                        self._tags[tag].discard(key)
                    del self._cache[key]
                    count += 1
            return count

    def cleanup_expired(self) -> int:
        """Remove all expired entries.

        Returns:
            Number of entries removed.
        """
        with self._lock:
            now = time.time()
            expired = [
                key for key, entry in self._cache.items()
                if entry.ttl and (now - entry.created_at) > entry.ttl
            ]
            for key in expired:
                entry = self._cache[key]
                for tag in entry.tags:
                    self._tags[tag].discard(key)
                del self._cache[key]
            return len(expired)

    def clear(self) -> int:
        """Clear all cache entries.

        Returns:
            Number of entries cleared.
        """
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._tags.clear()
            self._dependencies.clear()
            return count

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            now = time.time()
            expired = sum(
                1 for entry in self._cache.values()
                if entry.ttl and (now - entry.created_at) > entry.ttl
            )
            return {
                "total_entries": len(self._cache),
                "expired_entries": expired,
                "tag_count": len(self._tags),
                "dependency_count": len(self._dependencies),
            }


class InvalidationStrategy:
    """Base class for invalidation strategies."""

    def should_invalidate(self, key: str, metadata: dict[str, Any]) -> bool:
        """Check if key should be invalidated."""
        raise NotImplementedError


class TimeBasedStrategy(InvalidationStrategy):
    """Time-based invalidation."""

    def __init__(self, max_age: float) -> None:
        self.max_age = max_age

    def should_invalidate(self, key: str, metadata: dict[str, Any]) -> bool:
        created_at = metadata.get("created_at", 0)
        return (time.time() - created_at) > self.max_age


class TagBasedStrategy(InvalidationStrategy):
    """Tag-based invalidation."""

    def __init__(self, tags: list[str]) -> None:
        self.tags = set(tags)

    def should_invalidate(self, key: str, metadata: dict[str, Any]) -> bool:
        entry_tags = set(metadata.get("tags", []))
        return bool(self.tags & entry_tags)

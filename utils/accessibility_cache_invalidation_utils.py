"""
Accessibility Cache Invalidation Utilities

Manage cache invalidation for accessibility element lookups,
supporting time-based, event-based, and content-based invalidation strategies.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from typing import Optional, Callable, Any


@dataclass
class CacheEntry:
    """A cache entry with invalidation metadata."""
    key: str
    value: Any
    created_at_ms: float = field(default_factory=lambda: time.time() * 1000)
    last_accessed_ms: float = field(default_factory=lambda: time.time() * 1000)
    access_count: int = 0
    invalidation_hash: Optional[str] = None

    def touch(self) -> None:
        """Update access metadata."""
        self.last_accessed_ms = time.time() * 1000
        self.access_count += 1


class AccessibilityCacheInvalidator:
    """
    Invalidate cached accessibility element data based on
    time, content changes, or explicit invalidation signals.
    """

    def __init__(
        self,
        ttl_ms: float = 5000.0,
        max_entries: int = 500,
        content_hash_enabled: bool = True,
    ):
        self.ttl_ms = ttl_ms
        self.max_entries = max_entries
        self.content_hash_enabled = content_hash_enabled
        self._cache: dict[str, CacheEntry] = {}

    def get(self, key: str) -> Optional[Any]:
        """Get a value from the cache if still valid."""
        entry = self._cache.get(key)
        if not entry:
            return None
        if self._is_expired(entry):
            del self._cache[key]
            return None
        entry.touch()
        return entry.value

    def set(self, key: str, value: Any, content_hash: Optional[str] = None) -> None:
        """Set a value in the cache."""
        if len(self._cache) >= self.max_entries:
            self._evict_lru()

        entry = CacheEntry(
            key=key,
            value=value,
            invalidation_hash=content_hash or self._compute_hash(value),
        )
        self._cache[key] = entry

    def invalidate(self, key: str) -> bool:
        """Explicitly invalidate a cache entry."""
        return self._cache.pop(key, None) is not None

    def invalidate_by_pattern(self, pattern: Callable[[str], bool]) -> int:
        """Invalidate all keys matching a predicate."""
        keys_to_remove = [k for k in self._cache if pattern(k)]
        for k in keys_to_remove:
            del self._cache[k]
        return len(keys_to_remove)

    def invalidate_if_content_changed(self, key: str, new_content_hash: str) -> bool:
        """Invalidate if the content hash has changed."""
        entry = self._cache.get(key)
        if not entry:
            return False
        if entry.invalidation_hash and entry.invalidation_hash != new_content_hash:
            del self._cache[key]
            return True
        return False

    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if a cache entry has expired."""
        now = time.time() * 1000
        return (now - entry.created_at_ms) > self.ttl_ms

    def _evict_lru(self) -> None:
        """Evict the least recently used entry."""
        if not self._cache:
            return
        lru_key = min(self._cache, key=lambda k: self._cache[k].last_accessed_ms)
        del self._cache[lru_key]

    @staticmethod
    def _compute_hash(value: Any) -> str:
        """Compute a content hash for a cache value."""
        try:
            import json
            data = json.dumps(value, sort_keys=True, default=str)
            return hashlib.sha256(data.encode()).hexdigest()[:16]
        except Exception:
            return str(hash(str(value)))[:16]

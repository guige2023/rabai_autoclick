"""
Cache invalidation strategies for persistent storage.

Supports TTL-based, tag-based, and dependency-graph invalidation.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any


class InvalidationStrategy(Enum):
    """How to invalidate cache entries."""

    IMMEDIATE = auto()
    LAZY = auto()
    BACKGROUND = auto()


@dataclass
class CacheEntry:
    """A single cache entry with metadata."""

    key: str
    value: Any
    created_at: float = field(default_factory=time.time)
    last_accessed: float = field(default_factory=time.time)
    tags: set[str] = field(default_factory=set)
    size_bytes: int = 0
    hit_count: int = 0


class CacheInvalidator:
    """
    Manages cache invalidation based on TTL, tags, or dependencies.

    Example:
        invalidator = CacheInvalidator(max_size_mb=100)
        invalidator.set("key", data, tags=["ui", "home"])
        invalidator.invalidate_by_tag("ui")
    """

    def __init__(
        self,
        max_size_mb: float = 50.0,
        default_ttl: float = 300.0,
        strategy: InvalidationStrategy = InvalidationStrategy.LAZY,
    ) -> None:
        self._store: dict[str, CacheEntry] = {}
        self._max_bytes = int(max_size_mb * 1024 * 1024)
        self._default_ttl = default_ttl
        self._strategy = strategy
        self._total_bytes = 0

    def set(
        self,
        key: str,
        value: Any,
        ttl: float | None = None,
        tags: list[str] | None = None,
    ) -> None:
        """Store a value in the cache."""
        serialized = self._serialize(value)
        size = len(serialized)

        if key in self._store:
            self._total_bytes -= self._store[key].size_bytes

        self._store[key] = CacheEntry(
            key=key,
            value=value,
            tags=set(tags) if tags else set(),
            size_bytes=size,
        )
        self._total_bytes += size

        if self._total_bytes > self._max_bytes:
            self._evict_lru()

    def get(self, key: str, ttl: float | None = None) -> Any | None:
        """Retrieve a value if still valid."""
        if key not in self._store:
            return None

        entry = self._store[key]
        entry.last_accessed = time.time()
        entry.hit_count += 1

        effective_ttl = ttl if ttl is not None else self._default_ttl
        if time.time() - entry.created_at > effective_ttl:
            if self._strategy == InvalidationStrategy.IMMEDIATE:
                self.delete(key)
            return None

        return entry.value

    def delete(self, key: str) -> None:
        """Remove a specific key from cache."""
        if key in self._store:
            self._total_bytes -= self._store[key].size_bytes
            del self._store[key]

    def invalidate_by_tag(self, tag: str) -> int:
        """Remove all entries with a specific tag."""
        to_delete = [k for k, e in self._store.items() if tag in e.tags]
        for key in to_delete:
            self.delete(key)
        return len(to_delete)

    def invalidate_prefix(self, prefix: str) -> int:
        """Remove all entries whose key starts with prefix."""
        to_delete = [k for k in self._store if k.startswith(prefix)]
        for key in to_delete:
            self.delete(key)
        return len(to_delete)

    def clear(self) -> None:
        """Clear all cache entries."""
        self._store.clear()
        self._total_bytes = 0

    def stats(self) -> dict[str, Any]:
        """Return cache statistics."""
        return {
            "entries": len(self._store),
            "total_bytes": self._total_bytes,
            "max_bytes": self._max_bytes,
            "utilization": self._total_bytes / self._max_bytes if self._max_bytes else 0,
            "total_hits": sum(e.hit_count for e in self._store.values()),
        }

    def _evict_lru(self) -> None:
        """Evict least recently used entries until within size limit."""
        if not self._store:
            return
        sorted_entries = sorted(self._store.values(), key=lambda e: e.last_accessed)
        to_evict = int(len(sorted_entries) * 0.1) or 1
        for entry in sorted_entries[:to_evict]:
            self.delete(entry.key)

    def _serialize(self, value: Any) -> bytes:
        """Serialize value to bytes for size estimation."""
        try:
            return json.dumps(value).encode()
        except (TypeError, ValueError):
            return str(value).encode()


class DependencyGraphInvalidator:
    """
    Tracks dependencies between cache keys and invalidates cascading.

    Example:
        dep_graph = DependencyGraphInvalidator()
        dep_graph.set("user:1", user_data)
        dep_graph.depends_on("user:1:profile", "user:1")
        dep_graph.invalidate("user:1")  # Also invalidates profile
    """

    def __init__(self, cache: CacheInvalidator | None = None) -> None:
        self._cache = cache or CacheInvalidator()
        self._dependents: dict[str, set[str]] = {}
        self._dependencies: dict[str, set[str]] = {}

    def set(
        self,
        key: str,
        value: Any,
        ttl: float | None = None,
        depends_on: list[str] | None = None,
    ) -> None:
        """Set a key and register its dependencies."""
        if depends_on:
            for dep in depends_on:
                self._add_dependency(key, dep)
        self._cache.set(key, value, ttl)

    def _add_dependency(self, key: str, dependency: str) -> None:
        """Register that key depends on dependency."""
        if dependency not in self._dependents:
            self._dependents[dependency] = set()
        self._dependents[dependency].add(key)

        if key not in self._dependencies:
            self._dependencies[key] = set()
        self._dependencies[key].add(dependency)

    def invalidate(self, key: str) -> int:
        """Invalidate key and all its dependents recursively."""
        invalidated = {key}
        to_process = [key]

        while to_process:
            current = to_process.pop()
            self._cache.delete(current)

            for dependent in self._dependents.get(current, []):
                if dependent not in invalidated:
                    invalidated.add(dependent)
                    to_process.append(dependent)

        return len(invalidated)

    def get(self, key: str, ttl: float | None = None) -> Any | None:
        """Get a cache entry."""
        return self._cache.get(key, ttl)

    def get_stats(self) -> dict[str, Any]:
        """Return cache and dependency stats."""
        stats = self._cache.stats()
        stats["dependency_edges"] = sum(len(d) for d in self._dependents.values())
        return stats

"""Cache Invalidation Action Module.

Provides intelligent cache invalidation strategies including
TTL-based, pattern-based, dependency-graph, and主动式失效。
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class InvalidationStrategy(Enum):
    """Strategy for cache invalidation."""
    TTL = "ttl"
    PATTERN = "pattern"
    DEPENDENCY = "dependency"
    LRU = "lru"
    MANUAL = "manual"
    COMPOSITE = "composite"


@dataclass
class CacheEntry:
    """Single cache entry with metadata."""
    key: str
    value: Any
    created_at: float
    accessed_at: float
    ttl: Optional[float] = None
    tags: Set[str] = field(default_factory=set)
    dependencies: Set[str] = field(default_factory=set)
    size_bytes: int = 0


@dataclass
class InvalidationResult:
    """Result of cache invalidation operation."""
    success: bool
    keys_invalidated: int
    strategy_used: str
    errors: List[str] = field(default_factory=list)
    duration_ms: float = 0.0


class CacheStore:
    """In-memory cache with invalidation support."""

    def __init__(self, max_size_mb: int = 100):
        self._store: Dict[str, CacheEntry] = {}
        self._max_size_bytes = max_size_mb * 1024 * 1024
        self._current_size = 0

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired."""
        entry = self._store.get(key)
        if entry is None:
            return None
        if entry.ttl and (time.time() - entry.created_at) > entry.ttl:
            self._remove(key)
            return None
        entry.accessed_at = time.time()
        return entry.value

    def set(self, key: str, value: Any, ttl: Optional[float] = None,
            tags: Optional[Set[str]] = None,
            dependencies: Optional[Set[str]] = None) -> bool:
        """Set value in cache with optional TTL and tags."""
        import sys
        value_size = len(str(value))
        if self._current_size + value_size > self._max_size_bytes:
            self._evict_lru(value_size)
        entry = CacheEntry(
            key=key,
            value=value,
            created_at=time.time(),
            accessed_at=time.time(),
            ttl=ttl,
            tags=tags or set(),
            dependencies=dependencies or set(),
            size_bytes=value_size
        )
        self._store[key] = entry
        self._current_size += value_size
        return True

    def _remove(self, key: str) -> None:
        """Remove entry from store."""
        if key in self._store:
            self._current_size -= self._store[key].size_bytes
            del self._store[key]

    def _evict_lru(self, needed_size: int) -> None:
        """Evict least recently used entries."""
        if not self._store:
            return
        sorted_entries = sorted(
            self._store.values(),
            key=lambda e: e.accessed_at
        )
        freed = 0
        for entry in sorted_entries:
            if freed >= needed_size:
                break
            self._remove(entry.key)
            freed += entry.size_bytes

    def invalidate_by_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching pattern."""
        import re
        regex = re.compile(pattern)
        keys_to_remove = [k for k in self._store if regex.match(k)]
        for key in keys_to_remove:
            self._remove(key)
        return len(keys_to_remove)

    def invalidate_by_tags(self, tags: Set[str]) -> int:
        """Invalidate all entries with any of the given tags."""
        keys_to_remove = [
            k for k, e in self._store.items()
            if e.tags & tags
        ]
        for key in keys_to_remove:
            self._remove(key)
        return len(keys_to_remove)

    def invalidate_by_dependency(self, dependency_key: str) -> int:
        """Invalidate all entries depending on given key."""
        keys_to_remove = [
            k for k, e in self._store.items()
            if dependency_key in e.dependencies
        ]
        keys_to_remove.append(dependency_key)
        for key in keys_to_remove:
            self._remove(key)
        return len(keys_to_remove)

    def invalidate_all(self) -> int:
        """Clear entire cache."""
        count = len(self._store)
        self._store.clear()
        self._current_size = 0
        return count

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        now = time.time()
        expired = sum(
            1 for e in self._store.values()
            if e.ttl and (now - e.created_at) > e.ttl
        )
        return {
            "total_entries": len(self._store),
            "expired_entries": expired,
            "size_bytes": self._current_size,
            "max_size_bytes": self._max_size_bytes,
            "utilization_pct": (
                self._current_size / self._max_size_bytes * 100
                if self._max_size_bytes > 0 else 0
            )
        }


class CacheInvalidationAction:
    """Intelligent cache invalidation with multiple strategies.

    Example:
        action = CacheInvalidationAction()

        action.set("user:123", data, ttl=3600, tags={"user", "profile"})
        action.invalidate(strategy="pattern", pattern="user:*")
        action.invalidate(strategy="tag", tags={"user"})
    """

    def __init__(self):
        self._cache = CacheStore()

    def get(self, key: str) -> Optional[Any]:
        """Get cached value."""
        return self._cache.get(key)

    def set(self, key: str, value: Any, ttl: Optional[float] = None,
            tags: Optional[List[str]] = None,
            dependencies: Optional[List[str]] = None) -> bool:
        """Set cache value with metadata."""
        return self._cache.set(
            key=key,
            value=value,
            ttl=ttl,
            tags=set(tags) if tags else None,
            dependencies=set(dependencies) if dependencies else None
        )

    def invalidate(self, strategy: str = "manual",
                   pattern: Optional[str] = None,
                   tags: Optional[List[str]] = None,
                   dependency_key: Optional[str] = None) -> InvalidationResult:
        """Invalidate cache using specified strategy.

        Args:
            strategy: One of ttl, pattern, dependency, lru, manual, composite
            pattern: Glob/regex pattern for pattern-based invalidation
            tags: List of tags for tag-based invalidation
            dependency_key: Key whose dependents should be invalidated

        Returns:
            InvalidationResult with count and strategy info
        """
        start = time.time()
        strategy_enum = InvalidationStrategy(strategy.lower())
        count = 0
        errors = []

        try:
            if strategy_enum == InvalidationStrategy.PATTERN:
                if not pattern:
                    errors.append("pattern required for pattern strategy")
                else:
                    count = self._cache.invalidate_by_pattern(pattern)

            elif strategy_enum == InvalidationStrategy.DEPENDENCY:
                if not dependency_key:
                    errors.append("dependency_key required for dependency strategy")
                else:
                    count = self._cache.invalidate_by_dependency(dependency_key)

            elif strategy_enum == InvalidationStrategy.TAG:
                if not tags:
                    errors.append("tags required for tag strategy")
                else:
                    count = self._cache.invalidate_by_tags(set(tags))

            elif strategy_enum == InvalidationStrategy.LRU:
                self._cache._evict_lru(self._cache._current_size)
                count = len(self._cache._store)

            elif strategy_enum == InvalidationStrategy.MANUAL:
                count = self._cache.invalidate_all()

            elif strategy_enum == InvalidationStrategy.COMPOSITE:
                if pattern:
                    count += self._cache.invalidate_by_pattern(pattern)
                if tags:
                    count += self._cache.invalidate_by_tags(set(tags))
                if dependency_key:
                    count += self._cache.invalidate_by_dependency(dependency_key)

        except Exception as e:
            errors.append(str(e))

        duration_ms = (time.time() - start) * 1000

        return InvalidationResult(
            success=len(errors) == 0,
            keys_invalidated=count,
            strategy_used=strategy,
            errors=errors,
            duration_ms=duration_ms
        )

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        return self._cache.stats()


_global_cache_invalidator = CacheInvalidationAction()


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute cache invalidation action.

    Args:
        context: Execution context
        params: Dict with keys:
            - operation: "get", "set", or "invalidate"
            - key: Cache key
            - value: Value to cache (for set)
            - ttl: Time to live in seconds (for set)
            - tags: List of tags (for set/invalidate)
            - dependencies: List of dependency keys (for set)
            - strategy: Invalidation strategy
            - pattern: Pattern for pattern invalidation
            - dependency_key: Key for dependency invalidation

    Returns:
        Dict with success, data, message
    """
    operation = params.get("operation", "get")
    key = params.get("key", "")

    try:
        if operation == "get":
            if not key:
                return {"success": False, "message": "key required for get"}
            value = _global_cache_invalidator.get(key)
            return {
                "success": True,
                "data": value,
                "message": "Cache hit" if value is not None else "Cache miss"
            }

        elif operation == "set":
            if not key:
                return {"success": False, "message": "key required for set"}
            value = params.get("value")
            ttl = params.get("ttl")
            tags = params.get("tags")
            dependencies = params.get("dependencies")
            success = _global_cache_invalidator.set(
                key, value, ttl, tags, dependencies
            )
            return {
                "success": success,
                "message": f"Cached {key}"
            }

        elif operation == "invalidate":
            strategy = params.get("strategy", "manual")
            result = _global_cache_invalidator.invalidate(
                strategy=strategy,
                pattern=params.get("pattern"),
                tags=params.get("tags"),
                dependency_key=params.get("dependency_key")
            )
            return {
                "success": result.success,
                "keys_invalidated": result.keys_invalidated,
                "strategy_used": result.strategy_used,
                "errors": result.errors,
                "duration_ms": result.duration_ms,
                "message": f"Invalidated {result.keys_invalidated} keys"
            }

        elif operation == "stats":
            return {
                "success": True,
                "stats": _global_cache_invalidator.stats(),
                "message": "Stats retrieved"
            }

        else:
            return {
                "success": False,
                "message": f"Unknown operation: {operation}"
            }

    except Exception as e:
        return {
            "success": False,
            "message": f"Cache error: {str(e)}"
        }

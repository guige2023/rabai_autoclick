"""Cache invalidation strategies: TTL, manual, dependency-based, and probabilistic."""

from __future__ import annotations

import hashlib
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, FrozenSet

__all__ = [
    "CacheEntry",
    "CacheInvalidationPolicy",
    "TTLCache",
    "DependencyGraph",
    "ProbabilisticCache",
]


@dataclass
class CacheEntry:
    """A cache entry with invalidation metadata."""
    key: str
    value: Any
    created_at: float = field(default_factory=time.time)
    accessed_at: float = field(default_factory=time.time)
    access_count: int = 0
    ttl: float | None = None
    dependencies: FrozenSet[str] = field(default_factory=frozenset)
    tags: FrozenSet[str] = field(default_factory=frozenset)

    def is_expired(self) -> bool:
        if self.ttl is None:
            return False
        return time.time() > self.created_at + self.ttl

    def touch(self) -> None:
        self.accessed_at = time.time()
        self.access_count += 1


class CacheInvalidationPolicy:
    """Policies for cache invalidation."""

    @staticmethod
    def ttl(entry: CacheEntry) -> bool:
        return entry.is_expired()

    @staticmethod
    def lru(entry: CacheEntry, max_age: float = 300.0) -> bool:
        return time.time() - entry.accessed_at > max_age

    @staticmethod
    def lfu(entry: CacheEntry, min_hits: int = 2) -> bool:
        return entry.access_count < min_hits

    @staticmethod
    def always_false(entry: CacheEntry) -> bool:
        return False


class TTLCache:
    """Thread-safe TTL cache with invalidation policies."""

    def __init__(
        self,
        default_ttl: float = 300.0,
        max_size: int = 1000,
        invalidation_policy: Callable[[CacheEntry], bool] | None = None,
    ) -> None:
        self.default_ttl = default_ttl
        self.max_size = max_size
        self.policy = invalidation_policy or CacheInvalidationPolicy.ttl
        self._store: dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                self._misses += 1
                return default
            if self.policy(entry):
                del self._store[key]
                self._misses += 1
                return default
            entry.touch()
            self._hits += 1
            return entry.value

    def set(
        self,
        key: str,
        value: Any,
        ttl: float | None = None,
        tags: list[str] | None = None,
        dependencies: list[str] | None = None,
    ) -> None:
        with self._lock:
            if len(self._store) >= self.max_size:
                self._evict_one()
            self._store[key] = CacheEntry(
                key=key,
                value=value,
                ttl=ttl if ttl is not None else self.default_ttl,
                tags=frozenset(tags or []),
                dependencies=frozenset(dependencies or []),
            )

    def invalidate(self, key: str) -> bool:
        with self._lock:
            if key in self._store:
                del self._store[key]
                return True
            return False

    def invalidate_by_tags(self, *tags: str) -> int:
        count = 0
        with self._lock:
            to_remove = [
                k for k, e in self._store.items()
                if any(tag in e.tags for tag in tags)
            ]
            for k in to_remove:
                del self._store[k]
                count += 1
        return count

    def invalidate_dependencies(self, *deps: str) -> int:
        count = 0
        with self._lock:
            to_remove = [
                k for k, e in self._store.items()
                if any(dep in e.dependencies for dep in deps)
            ]
            for k in to_remove:
                del self._store[k]
                count += 1
        return count

    def _evict_one(self) -> None:
        if not self._store:
            return
        lru_key = min(
            self._store.keys(),
            key=lambda k: self._store[k].accessed_at,
        )
        del self._store[lru_key]

    def clear(self) -> None:
        with self._lock:
            self._store.clear()

    def stats(self) -> dict[str, Any]:
        with self._lock:
            total = self._hits + self._misses
            return {
                "hits": self._hits,
                "misses": self._misses,
                "hit_rate": self._hits / total if total > 0 else 0.0,
                "size": len(self._store),
                "max_size": self.max_size,
            }


class DependencyGraph:
    """Dependency graph for cache invalidation."""

    def __init__(self) -> None:
        self._graph: dict[str, set[str]] = defaultdict(set)
        self._dependents: dict[str, set[str]] = defaultdict(set)

    def add_dependency(self, key: str, depends_on: str) -> None:
        self._graph[key].add(depends_on)
        self._dependents[depends_on].add(key)

    def get_dependents(self, key: str) -> set[str]:
        """Get all keys that depend on this key (should be invalidated when key changes)."""
        result: set[str] = set()
        stack = list(self._dependents.get(key, set()))
        while stack:
            k = stack.pop()
            if k not in result:
                result.add(k)
                stack.extend(self._dependents.get(k, set()))
        return result

    def invalidate_key(self, key: str) -> list[str]:
        return list(self.get_dependents(key))


class ProbabilisticCache:
    """Cache with probabilistic early expiration (Stale-While-Revalidate)."""

    def __init__(
        self,
        ttl: float = 300.0,
        beta: float = 1.0,
        get_value_fn: Callable[[str], Any] | None = None,
    ) -> None:
        self.ttl = ttl
        self.beta = beta
        self._get_value_fn = get_value_fn
        self._store: dict[str, CacheEntry] = {}
        self._lock = threading.Lock()
        self._refreshes: dict[str, float] = {}
        self._stales: int = 0
        self._fresh: int = 0

    def get(
        self,
        key: str,
        default: Any = None,
        revalidate_fn: Callable[[], Any] | None = None,
    ) -> Any:
        with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return default

            age = time.time() - entry.created_at
            staleness = max(0.0, age - self.ttl)

            if staleness == 0:
                self._fresh += 1
                entry.touch()
                return entry.value

            probability = self._staleness_probability(staleness)
            import random
            should_revalidate = random.random() < probability

            if should_revalidate and revalidate_fn:
                self._stales += 1
                new_value = revalidate_fn()
                self._store[key] = CacheEntry(key=key, value=new_value, ttl=self.ttl)
                return new_value
            else:
                self._fresh += 1
                entry.touch()
                return entry.value

    def _staleness_probability(self, staleness: float) -> float:
        return min(1.0, staleness / (self.ttl * self.beta))

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._store[key] = CacheEntry(key=key, value=value, ttl=self.ttl)

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "fresh": self._fresh,
                "stale_revalidations": self._stales,
                "size": len(self._store),
            }

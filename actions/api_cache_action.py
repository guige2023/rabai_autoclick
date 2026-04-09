"""API Cache Action Module.

Provides multi-level caching for API responses with TTL, LRU/LFU
eviction, cache invalidation patterns, and stale-while-revalidate.
"""

import time
import hashlib
import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    key: str
    value: Any
    created_at: float
    last_accessed: float
    access_count: int = 0
    ttl_sec: float = 3600.0
    tags: List[str] = field(default_factory=list)
    is_stale: bool = False


@dataclass
class CacheConfig:
    max_size: int = 1000
    default_ttl_sec: float = 3600.0
    stale_threshold_sec: float = 300.0
    eviction_policy: str = "lru"
    enable_stats: bool = True


class APICacheAction:
    """Multi-level cache for API responses with configurable eviction."""

    def __init__(self, config: Optional[CacheConfig] = None) -> None:
        self._config = config or CacheConfig()
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._stats = {
            "hits": 0,
            "misses": 0,
            "sets": 0,
            "evictions": 0,
            "invalidations": 0,
        }

    def get(
        self,
        key: str,
        allow_stale: bool = True,
        on_miss: Optional[Callable[[], Any]] = None,
    ) -> Optional[Any]:
        with self._lock:
            entry = self._cache.get(key)
            if not entry:
                self._stats["misses"] += 1
                if on_miss:
                    value = on_miss()
                    if value is not None:
                        self.set(key, value)
                    return value
                return None
            age = time.time() - entry.created_at
            if age > entry.ttl_sec:
                if not (allow_stale and age < entry.ttl_sec + self._config.stale_threshold_sec):
                    self._evict(key)
                    self._stats["misses"] += 1
                    return None
                entry.is_stale = True
            entry.last_accessed = time.time()
            entry.access_count += 1
            self._stats["hits"] += 1
            return entry.value

    def set(
        self,
        key: str,
        value: Any,
        ttl_sec: Optional[float] = None,
        tags: Optional[List[str]] = None,
    ) -> None:
        with self._lock:
            if len(self._cache) >= self._config.max_size and key not in self._cache:
                self._evict_one()
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                last_accessed=time.time(),
                ttl_sec=ttl_sec or self._config.default_ttl_sec,
                tags=tags or [],
            )
            self._cache[key] = entry
            self._stats["sets"] += 1

    def invalidate(self, key: str) -> bool:
        with self._lock:
            if key in self._cache:
                self._evict(key)
                self._stats["invalidations"] += 1
                return True
            return False

    def invalidate_by_tags(self, tags: List[str]) -> int:
        with self._lock:
            to_remove = [
                key
                for key, entry in self._cache.items()
                if any(tag in entry.tags for tag in tags)
            ]
            for key in to_remove:
                self._evict(key)
            self._stats["invalidations"] += len(to_remove)
            return len(to_remove)

    def invalidate_pattern(self, pattern: str) -> int:
        import fnmatch
        with self._lock:
            to_remove = [key for key in self._cache if fnmatch.fnmatch(key, pattern)]
            for key in to_remove:
                self._evict(key)
            self._stats["invalidations"] += len(to_remove)
            return len(to_remove)

    def _evict(self, key: str) -> None:
        if key in self._cache:
            del self._cache[key]
            self._stats["evictions"] += 1

    def _evict_one(self) -> None:
        if not self._cache:
            return
        policy = self._config.eviction_policy
        if policy == "lru":
            key = min(self._cache, key=lambda k: self._cache[k].last_accessed)
        elif policy == "lfu":
            key = min(self._cache, key=lambda k: self._cache[k].access_count)
        else:
            key = next(iter(self._cache))
        self._evict(key)

    def clear(self) -> int:
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            total = self._stats["hits"] + self._stats["misses"]
            return {
                **self._stats,
                "size": len(self._cache),
                "max_size": self._config.max_size,
                "hit_rate": self._stats["hits"] / total if total > 0 else 0,
            }

    def get_keys(self) -> List[str]:
        with self._lock:
            return list(self._cache.keys())

    def has_stale(self) -> bool:
        with self._lock:
            return any(e.is_stale for e in self._cache.values())

    def cleanup_stale(self) -> int:
        with self._lock:
            now = time.time()
            to_remove = []
            for key, entry in self._cache.items():
                if entry.is_stale and now - entry.created_at > entry.ttl_sec + self._config.stale_threshold_sec:
                    to_remove.append(key)
            for key in to_remove:
                self._evict(key)
            return len(to_remove)

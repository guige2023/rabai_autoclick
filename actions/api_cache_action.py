"""API cache action module for RabAI AutoClick.

Provides API caching:
- APICache: Cache API responses
- CacheStrategy: Different caching strategies
- CacheInvalidator: Invalidate cache entries
- CacheMonitor: Monitor cache performance
"""

import time
import hashlib
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CacheStrategy(Enum):
    """Cache strategies."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    TTL = "ttl"
    TIME_BASED = "time_based"


@dataclass
class CacheEntry:
    """Cache entry."""
    key: str
    value: Any
    created_at: float
    accessed_at: float
    access_count: int = 0
    ttl: Optional[float] = None
    size: int = 0


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    size: int = 0
    hit_rate: float = 0.0


class APICache:
    """API response cache."""

    def __init__(
        self,
        strategy: CacheStrategy = CacheStrategy.LRU,
        max_size: int = 1000,
        default_ttl: Optional[float] = None,
    ):
        self.strategy = strategy
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self.stats = CacheStats()

    def get(self, key: str) -> Tuple[Optional[Any], bool]:
        """Get value from cache."""
        with self._lock:
            entry = self._cache.get(key)

            if entry is None:
                self.stats.misses += 1
                self._update_hit_rate()
                return None, False

            if self._is_expired(entry):
                del self._cache[key]
                self.stats.misses += 1
                self._update_hit_rate()
                return None, False

            entry.accessed_at = time.time()
            entry.access_count += 1
            self.stats.hits += 1
            self._update_hit_rate()
            return entry.value, True

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        size: Optional[int] = None,
    ) -> bool:
        """Set value in cache."""
        with self._lock:
            if len(self._cache) >= self.max_size and key not in self._cache:
                self._evict()

            now = time.time()
            entry = CacheEntry(
                key=key,
                value=value,
                created_at=now,
                accessed_at=now,
                ttl=ttl or self.default_ttl,
                size=size or len(str(value)),
            )
            self._cache[key] = entry
            self.stats.size = len(self._cache)
            return True

    def delete(self, key: str) -> bool:
        """Delete from cache."""
        with self._lock:
            if key in self._cache:
                del self._cache[key]
                self.stats.size = len(self._cache)
                return True
            return False

    def clear(self):
        """Clear all cache."""
        with self._lock:
            self._cache.clear()
            self.stats = CacheStats()

    def invalidate(self, pattern: Optional[str] = None) -> int:
        """Invalidate cache entries matching pattern."""
        with self._lock:
            if pattern is None:
                count = len(self._cache)
                self._cache.clear()
                self.stats.size = 0
                return count

            to_delete = [k for k in self._cache.keys() if pattern in k]
            for k in to_delete:
                del self._cache[k]

            self.stats.size = len(self._cache)
            return len(to_delete)

    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if entry is expired."""
        if entry.ttl is None:
            return False
        return time.time() - entry.created_at > entry.ttl

    def _evict(self):
        """Evict entry based on strategy."""
        if not self._cache:
            return

        if self.strategy == CacheStrategy.LRU:
            oldest = min(self._cache.items(), key=lambda x: x[1].accessed_at)
            del self._cache[oldest[0]]
        elif self.strategy == CacheStrategy.LFU:
            lfu = min(self._cache.items(), key=lambda x: x[1].access_count)
            del self._cache[lfu[0]]
        elif self.strategy == CacheStrategy.FIFO:
            oldest = min(self._cache.items(), key=lambda x: x[1].created_at)
            del self._cache[oldest[0]]
        elif self.strategy == CacheStrategy.TTL:
            now = time.time()
            expired = [
                (k, v) for k, v in self._cache.items()
                if v.ttl and now - v.created_at > v.ttl
            ]
            if expired:
                oldest = min(expired, key=lambda x: x[1].created_at)
                del self._cache[oldest[0]]

        self.stats.evictions += 1

    def _update_hit_rate(self):
        """Update hit rate."""
        total = self.stats.hits + self.stats.misses
        if total > 0:
            self.stats.hit_rate = self.stats.hits / total

    def get_stats(self) -> Dict:
        """Get cache statistics."""
        return {
            "hits": self.stats.hits,
            "misses": self.stats.misses,
            "evictions": self.stats.evictions,
            "size": self.stats.size,
            "max_size": self.max_size,
            "hit_rate": self.stats.hit_rate,
            "strategy": self.strategy.value,
        }

    def make_key(self, method: str, url: str, params: Optional[Dict] = None) -> str:
        """Make cache key from request."""
        key_parts = [method.upper(), url]

        if params:
            sorted_params = sorted(params.items())
            param_str = "&".join(f"{k}={v}" for k, v in sorted_params)
            key_parts.append(param_str)

        key_str = "|".join(str(p) for p in key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()


class CacheStrategySelector:
    """Select cache strategy based on request."""

    @staticmethod
    def select(method: str, url: str, params: Optional[Dict] = None) -> CacheStrategy:
        """Select cache strategy."""
        if method.upper() == "GET":
            if "/search" in url or "/query" in url:
                return CacheStrategy.TTL
            return CacheStrategy.LRU
        elif method.upper() in ("POST", "PUT", "PATCH"):
            return CacheStrategy.TIME_BASED
        return CacheStrategy.FIFO


class CacheInvalidator:
    """Invalidate cache entries."""

    def __init__(self, cache: APICache):
        self.cache = cache

    def invalidate_url(self, url: str) -> int:
        """Invalidate all entries for URL."""
        return self.cache.invalidate(pattern=url)

    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate by pattern."""
        return self.cache.invalidate(pattern=pattern)

    def invalidate_method(self, method: str) -> int:
        """Invalidate by HTTP method."""
        return self.cache.invalidate(pattern=method.upper())


class CacheMonitor:
    """Monitor cache performance."""

    def __init__(self, caches: Dict[str, APICache]):
        self.caches = caches

    def get_all_stats(self) -> Dict:
        """Get stats for all caches."""
        result = {}
        for name, cache in self.caches.items():
            result[name] = cache.get_stats()
        return result

    def get_total_hits(self) -> int:
        """Get total hits across all caches."""
        return sum(c.stats.hits for c in self.caches.values())

    def get_total_misses(self) -> int:
        """Get total misses across all caches."""
        return sum(c.stats.misses for c in self.caches.values())


class APICacheAction(BaseAction):
    """API cache action."""
    action_type = "api_cache"
    display_name = "API缓存"
    description = "API响应缓存管理"

    def __init__(self):
        super().__init__()
        self._caches: Dict[str, APICache] = {}

    def _get_or_create_cache(self, name: str, strategy_str: str = "lru") -> APICache:
        """Get or create cache."""
        if name not in self._caches:
            try:
                strategy = CacheStrategy[strategy_str.upper()]
            except KeyError:
                strategy = CacheStrategy.LRU
            self._caches[name] = APICache(strategy=strategy)
        return self._caches[name]

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "get")

            if operation == "create":
                return self._create_cache(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "set":
                return self._set(params)
            elif operation == "delete":
                return self._delete(params)
            elif operation == "clear":
                return self._clear(params)
            elif operation == "invalidate":
                return self._invalidate(params)
            elif operation == "stats":
                return self._get_stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Cache error: {str(e)}")

    def _create_cache(self, params: Dict) -> ActionResult:
        """Create cache."""
        name = params.get("name", "default")
        strategy = params.get("strategy", "lru")
        max_size = params.get("max_size", 1000)
        ttl = params.get("ttl")

        try:
            strat = CacheStrategy[strategy.upper()]
        except KeyError:
            strat = CacheStrategy.LRU

        cache = APICache(strategy=strat, max_size=max_size, default_ttl=ttl)
        self._caches[name] = cache

        return ActionResult(success=True, message=f"Cache '{name}' created with {strategy} strategy")

    def _get(self, params: Dict) -> ActionResult:
        """Get from cache."""
        name = params.get("name", "default")
        key = params.get("key")

        cache = self._get_or_create_cache(name)
        value, hit = cache.get(key)

        return ActionResult(
            success=hit,
            message="Cache hit" if hit else "Cache miss",
            data={"key": key, "hit": hit, "value": value},
        )

    def _set(self, params: Dict) -> ActionResult:
        """Set in cache."""
        name = params.get("name", "default")
        key = params.get("key")
        value = params.get("value")
        ttl = params.get("ttl")

        cache = self._get_or_create_cache(name)
        cache.set(key, value, ttl)

        return ActionResult(success=True, message=f"Cached key '{key}'")

    def _delete(self, params: Dict) -> ActionResult:
        """Delete from cache."""
        name = params.get("name", "default")
        key = params.get("key")

        cache = self._caches.get(name)
        if not cache:
            return ActionResult(success=False, message=f"Cache '{name}' not found")

        deleted = cache.delete(key)
        return ActionResult(success=deleted, message="Deleted" if deleted else "Key not found")

    def _clear(self, params: Dict) -> ActionResult:
        """Clear cache."""
        name = params.get("name", "default")

        cache = self._caches.get(name)
        if not cache:
            return ActionResult(success=False, message=f"Cache '{name}' not found")

        cache.clear()
        return ActionResult(success=True, message=f"Cache '{name}' cleared")

    def _invalidate(self, params: Dict) -> ActionResult:
        """Invalidate cache entries."""
        name = params.get("name", "default")
        pattern = params.get("pattern")

        cache = self._caches.get(name)
        if not cache:
            return ActionResult(success=False, message=f"Cache '{name}' not found")

        count = cache.invalidate(pattern=pattern)
        return ActionResult(success=True, message=f"Invalidated {count} entries")

    def _get_stats(self, params: Dict) -> ActionResult:
        """Get cache stats."""
        name = params.get("name", "default")

        cache = self._caches.get(name)
        if not cache:
            return ActionResult(success=False, message=f"Cache '{name}' not found")

        stats = cache.get_stats()
        return ActionResult(success=True, message="Stats retrieved", data=stats)

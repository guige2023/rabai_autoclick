"""API Response Cache Action Module.

Provides intelligent caching layer for API responses with
TTL, stale-while-revalidate, and invalidation strategies.

Author: RabAi Team
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple
from enum import Enum

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class CacheStrategy(Enum):
    """Cache invalidation strategies."""
    TTL = "ttl"
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    STALE_WHILE_REVALIDATE = "stale_while_revalidate"


class Freshness(Enum):
    """Cache entry freshness status."""
    FRESH = "fresh"
    STALE = "stale"
    EXPIRED = "expired"


@dataclass
class CacheEntry:
    """Cached API response entry."""
    key: str
    value: Any
    created_at: float
    last_accessed: float
    ttl_seconds: int
    access_count: int = 0
    size_bytes: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    etag: Optional[str] = None

    def is_fresh(self) -> bool:
        """Check if entry is still fresh."""
        age = time.time() - self.created_at
        return age < self.ttl_seconds

    def is_stale(self) -> bool:
        """Check if entry is stale but not expired."""
        age = time.time() - self.created_at
        return self.ttl_seconds <= age < self.ttl_seconds * 2

    def get_freshness(self) -> Freshness:
        """Get freshness status."""
        age = time.time() - self.created_at
        if age < self.ttl_seconds:
            return Freshness.FRESH
        elif age < self.ttl_seconds * 2:
            return Freshness.STALE
        else:
            return Freshness.EXPIRED


@dataclass
class CacheConfig:
    """Cache configuration."""
    max_size: int = 1000
    default_ttl_seconds: int = 300
    strategy: CacheStrategy = CacheStrategy.LRU
    enable_stale_revalidate: bool = True
    stale_ttl_multiplier: float = 2.0
    compression_threshold_bytes: int = 1024


class ResponseCache:
    """In-memory response cache with multiple eviction strategies."""

    def __init__(self, config: CacheConfig):
        self.config = config
        self._cache: Dict[str, CacheEntry] = {}
        self._access_order: List[str] = []
        self._hit_count = 0
        self._miss_count = 0

    def _generate_key(
        self,
        url: str,
        method: str,
        params: Optional[Dict[str, Any]] = None
    ) -> str:
        """Generate cache key from request details."""
        components = [method.upper(), url]

        if params:
            sorted_params = sorted(params.items())
            param_str = "&".join(f"{k}={v}" for k, v in sorted_params)
            components.append(param_str)

        key_str = "|".join(str(c) for c in components)
        return hashlib.sha256(key_str.encode()).hexdigest()

    def _evict_if_needed(self) -> None:
        """Evict entries if cache is full."""
        while len(self._cache) >= self.config.max_size:
            if self.config.strategy == CacheStrategy.LRU:
                self._evict_lru()
            elif self.config.strategy == CacheStrategy.LFU:
                self._evict_lfu()
            elif self.config.strategy == CacheStrategy.FIFO:
                self._evict_fifo()
            else:
                self._evict_lru()

    def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if not self._cache:
            return

        oldest_key = min(
            self._cache.keys(),
            key=lambda k: self._cache[k].last_accessed
        )
        del self._cache[oldest_key]

    def _evict_lfu(self) -> None:
        """Evict least frequently used entry."""
        if not self._cache:
            return

        least_used_key = min(
            self._cache.keys(),
            key=lambda k: self._cache[k].access_count
        )
        del self._cache[least_used_key]

    def _evict_fifo(self) -> None:
        """Evict oldest entry by creation time."""
        if not self._cache:
            return

        oldest_key = min(
            self._cache.keys(),
            key=lambda k: self._cache[k].created_at
        )
        del self._cache[oldest_key]

    def get(
        self,
        url: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None
    ) -> Tuple[Optional[Any], Freshness]:
        """Get cached response."""
        key = self._generate_key(url, method, params)

        if key not in self._cache:
            self._miss_count += 1
            return None, Freshness.EXPIRED

        entry = self._cache[key]
        entry.last_accessed = time.time()
        entry.access_count += 1

        freshness = entry.get_freshness()

        if freshness == Freshness.FRESH:
            self._hit_count += 1
        elif freshness == Freshness.STALE:
            pass
        else:
            self._miss_count += 1

        return entry.value, freshness

    def set(
        self,
        url: str,
        value: Any,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        ttl_seconds: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> None:
        """Cache a response."""
        key = self._generate_key(url, method, params)

        self._evict_if_needed()

        entry = CacheEntry(
            key=key,
            value=value,
            created_at=time.time(),
            last_accessed=time.time(),
            ttl_seconds=ttl_seconds or self.config.default_ttl_seconds,
            metadata=metadata or {}
        )

        self._cache[key] = entry

    def invalidate(
        self,
        url: Optional[str] = None,
        pattern: Optional[str] = None
    ) -> int:
        """Invalidate cached entries."""
        removed = 0

        if url:
            for key in list(self._cache.keys()):
                if url in key:
                    del self._cache[key]
                    removed += 1

        elif pattern:
            import re
            regex = re.compile(pattern)
            for key in list(self._cache.keys()):
                if regex.search(key):
                    del self._cache[key]
                    removed += 1

        else:
            removed = len(self._cache)
            self._cache.clear()

        return removed

    def get_or_compute(
        self,
        url: str,
        compute_fn: Callable[[], Any],
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        ttl_seconds: Optional[int] = None
    ) -> Any:
        """Get from cache or compute and cache."""
        cached, freshness = self.get(url, method, params)

        if cached is not None:
            if freshness == Freshness.FRESH:
                return cached
            elif (freshness == Freshness.STALE and
                  self.config.enable_stale_revalidate):
                return cached

        result = compute_fn()
        self.set(url, result, method, params, ttl_seconds)
        return result

    def get_statistics(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_requests = self._hit_count + self._miss_count
        hit_rate = self._hit_count / total_requests if total_requests > 0 else 0.0

        fresh_count = sum(
            1 for e in self._cache.values()
            if e.get_freshness() == Freshness.FRESH
        )
        stale_count = sum(
            1 for e in self._cache.values()
            if e.get_freshness() == Freshness.STALE
        )

        return {
            "size": len(self._cache),
            "max_size": self.config.max_size,
            "hit_count": self._hit_count,
            "miss_count": self._miss_count,
            "hit_rate": hit_rate,
            "fresh_entries": fresh_count,
            "stale_entries": stale_count,
            "strategy": self.config.strategy.value,
            "default_ttl_seconds": self.config.default_ttl_seconds
        }

    def clear(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()
        self._hit_count = 0
        self._miss_count = 0


class APIResponseCacheAction(BaseAction):
    """Action for API response caching operations."""

    def __init__(self):
        super().__init__("api_response_cache")
        self._cache = ResponseCache(CacheConfig())

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute cache action."""
        try:
            operation = params.get("operation", "get")

            if operation == "get":
                return self._get(params)
            elif operation == "set":
                return self._set(params)
            elif operation == "invalidate":
                return self._invalidate(params)
            elif operation == "stats":
                return self._get_stats(params)
            elif operation == "clear":
                return self._clear(params)
            elif operation == "configure":
                return self._configure(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get cached response."""
        url = params.get("url", "")
        method = params.get("method", "GET")
        params_dict = params.get("params")

        cached, freshness = self._cache.get(url, method, params_dict)

        return ActionResult(
            success=cached is not None,
            data={
                "cached": cached is not None,
                "freshness": freshness.value,
                "value": cached
            }
        )

    def _set(self, params: Dict[str, Any]) -> ActionResult:
        """Cache a response."""
        url = params.get("url", "")
        value = params.get("value")
        method = params.get("method", "GET")
        params_dict = params.get("params")
        ttl = params.get("ttl_seconds")

        self._cache.set(url, value, method, params_dict, ttl)

        return ActionResult(success=True)

    def _invalidate(self, params: Dict[str, Any]) -> ActionResult:
        """Invalidate cached entries."""
        url = params.get("url")
        pattern = params.get("pattern")

        removed = self._cache.invalidate(url, pattern)

        return ActionResult(
            success=True,
            data={"removed": removed}
        )

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get cache statistics."""
        stats = self._cache.get_statistics()
        return ActionResult(success=True, data=stats)

    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        """Clear cache."""
        self._cache.clear()
        return ActionResult(success=True, message="Cache cleared")

    def _configure(self, params: Dict[str, Any]) -> ActionResult:
        """Configure cache settings."""
        config = CacheConfig(
            max_size=params.get("max_size", 1000),
            default_ttl_seconds=params.get("default_ttl_seconds", 300),
            strategy=CacheStrategy(params.get("strategy", "lru")),
            enable_stale_revalidate=params.get(
                "enable_stale_revalidate", True
            )
        )

        self._cache = ResponseCache(config)

        return ActionResult(success=True, message="Cache configured")

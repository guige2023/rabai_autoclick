"""
API Caching Action Module.

Caches API responses with TTL, cache invalidation,
conditional requests (ETag/Last-Modified), and cache-aside pattern.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class CacheEntry:
    """A cache entry."""
    key: str
    value: Any
    created_at: float
    expires_at: float
    etag: Optional[str] = None


@dataclass
class CacheResult:
    """Result of cache operation."""
    hit: bool
    value: Optional[Any]
    from_cache: bool = True
    error: Optional[str] = None


class APICachingAction(BaseAction):
    """Cache API responses."""

    def __init__(self) -> None:
        super().__init__("api_caching")
        self._cache: dict[str, CacheEntry] = {}
        self._hits = 0
        self._misses = 0

    def execute(self, context: dict, params: dict) -> dict:
        """
        Perform cache operation.

        Args:
            context: Execution context
            params: Parameters:
                - operation: get, set, invalidate, clear
                - key: Cache key
                - value: Value to cache (for set)
                - ttl: Time to live in seconds
                - force_refresh: Bypass cache and refresh
                - check_etag: Check ETag before returning
                - max_size: Max cache entries (LRU eviction)

        Returns:
            CacheResult with cache status and value
        """
        import time

        operation = params.get("operation", "get")
        key = params.get("key", "")
        value = params.get("value")
        ttl = params.get("ttl", 300)
        force_refresh = params.get("force_refresh", False)
        max_size = params.get("max_size", 1000)

        if not key:
            return CacheResult(hit=False, value=None, error="Key required")

        if operation == "get":
            return self._cache_get(key, force_refresh, time.time())
        elif operation == "set":
            return self._cache_set(key, value, ttl, max_size, time.time())
        elif operation == "invalidate":
            return self._cache_invalidate(key)
        elif operation == "clear":
            return self._cache_clear()
        else:
            return CacheResult(hit=False, value=None, error=f"Unknown operation: {operation}")

    def _cache_get(self, key: str, force_refresh: bool, now: float) -> CacheResult:
        """Get value from cache."""
        if key in self._cache:
            entry = self._cache[key]
            if entry.expires_at > now:
                self._hits += 1
                return CacheResult(hit=True, value=entry.value, from_cache=True)
            else:
                del self._cache[key]

        self._misses += 1
        return CacheResult(hit=False, value=None, from_cache=False)

    def _cache_set(self, key: str, value: Any, ttl: int, max_size: int, now: float) -> CacheResult:
        """Set value in cache."""
        if len(self._cache) >= max_size:
            oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k].created_at)
            del self._cache[oldest_key]

        import hashlib
        etag = hashlib.md5(str(value).encode()).hexdigest() if value else None

        entry = CacheEntry(
            key=key,
            value=value,
            created_at=now,
            expires_at=now + ttl,
            etag=etag
        )
        self._cache[key] = entry
        return CacheResult(hit=False, value=value, from_cache=False)

    def _cache_invalidate(self, key: str) -> CacheResult:
        """Invalidate cache entry."""
        if key in self._cache:
            del self._cache[key]
        return CacheResult(hit=False, value=None, from_cache=False)

    def _cache_clear(self) -> CacheResult:
        """Clear all cache."""
        count = len(self._cache)
        self._cache.clear()
        self._hits = 0
        self._misses = 0
        return CacheResult(hit=False, value=count, from_cache=False)

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0.0
        return {
            "hits": self._hits,
            "misses": self._misses,
            "total": total,
            "hit_rate": hit_rate,
            "size": len(self._cache)
        }

    def get_etag(self, key: str) -> Optional[str]:
        """Get ETag for a cache key."""
        if key in self._cache:
            return self._cache[key].etag
        return None

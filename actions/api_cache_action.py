"""
API Cache Action Module.

Provides caching capabilities for API responses with
TTL, invalidation, and cache warming.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import hashlib
import json
import logging
import time

logger = logging.getLogger(__name__)


class CacheStrategy(Enum):
    """Cache strategies."""
    LRU = "lru"
    LFU = "lfu"
    FIFO = "fifo"
    TTL = "ttl"


@dataclass
class CacheEntry:
    """Cache entry."""
    key: str
    value: Any
    created_at: datetime
    accessed_at: datetime
    ttl: Optional[int] = None
    access_count: int = 0
    size_bytes: int = 0

    @property
    def is_expired(self) -> bool:
        """Check if entry is expired."""
        if self.ttl is None:
            return False
        age = (datetime.now() - self.created_at).total_seconds()
        return age > self.ttl


class CacheStats:
    """Cache statistics."""

    def __init__(self):
        self.hits: int = 0
        self.misses: int = 0
        self.evictions: int = 0
        self.expirations: int = 0

    @property
    def hit_rate(self) -> float:
        """Get hit rate."""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


class APICache:
    """API response cache."""

    def __init__(
        self,
        strategy: CacheStrategy = CacheStrategy.LRU,
        max_size: int = 1000,
        default_ttl: Optional[int] = 300
    ):
        self.strategy = strategy
        self.max_size = max_size
        self.default_ttl = default_ttl
        self._cache: Dict[str, CacheEntry] = {}
        self._stats = CacheStats()
        self._lock = asyncio.Lock()

    def _make_key(self, method: str, path: str, params: Optional[Dict] = None) -> str:
        """Generate cache key."""
        key_data = f"{method}:{path}"
        if params:
            key_data += f":{json.dumps(params, sort_keys=True)}"
        return hashlib.sha256(key_data.encode()).hexdigest()

    async def get(
        self,
        method: str,
        path: str,
        params: Optional[Dict] = None
    ) -> Optional[Any]:
        """Get cached response."""
        key = self._make_key(method, path, params)

        async with self._lock:
            entry = self._cache.get(key)

            if entry is None:
                self._stats.misses += 1
                return None

            if entry.is_expired:
                del self._cache[key]
                self._stats.expirations += 1
                return None

            entry.accessed_at = datetime.now()
            entry.access_count += 1
            self._stats.hits += 1

            return entry.value

    async def set(
        self,
        method: str,
        path: str,
        value: Any,
        params: Optional[Dict] = None,
        ttl: Optional[int] = None
    ):
        """Set cached response."""
        key = self._make_key(method, path, params)

        async with self._lock:
            if len(self._cache) >= self.max_size:
                await self._evict()

            entry = CacheEntry(
                key=key,
                value=value,
                created_at=datetime.now(),
                accessed_at=datetime.now(),
                ttl=ttl or self.default_ttl,
                size_bytes=len(json.dumps(value)) if isinstance(value, (dict, list)) else 0
            )

            self._cache[key] = entry

    async def invalidate(self, path_pattern: Optional[str] = None):
        """Invalidate cache entries."""
        async with self._lock:
            if path_pattern is None:
                self._cache.clear()
            else:
                keys_to_delete = [
                    k for k, v in self._cache.items()
                    if path_pattern in k
                ]
                for key in keys_to_delete:
                    del self._cache[key]

    async def _evict(self):
        """Evict entry based on strategy."""
        if not self._cache:
            return

        if self.strategy == CacheStrategy.LRU:
            lru_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].accessed_at
            )
        elif self.strategy == CacheStrategy.LFU:
            lfu_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].access_count
            )
            lru_key = lfu_key
        elif self.strategy == CacheStrategy.FIFO:
            fifo_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].created_at
            )
            lru_key = fifo_key
        else:
            lru_key = list(self._cache.keys())[0]

        del self._cache[lru_key]
        self._stats.evictions += 1

    async def cleanup_expired(self):
        """Remove expired entries."""
        async with self._lock:
            expired_keys = [
                k for k, v in self._cache.items()
                if v.is_expired
            ]
            for key in expired_keys:
                del self._cache[key]
                self._stats.expirations += 1

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        return self._stats

    async def get_size(self) -> int:
        """Get current cache size."""
        async with self._lock:
            return len(self._cache)


class CacheWarmer:
    """Warms cache with frequently accessed data."""

    def __init__(self, cache: APICache):
        self.cache = cache
        self._warming = False

    async def warm(
        self,
        endpoints: List[Tuple[str, str, Optional[Dict]]],
        fetcher: Callable
    ):
        """Warm cache with endpoint data."""
        if self._warming:
            return

        self._warming = True

        try:
            for method, path, params in endpoints:
                try:
                    data = await fetcher(method, path, params)
                    await self.cache.set(method, path, data, params)
                except Exception as e:
                    logger.error(f"Cache warming failed for {method} {path}: {e}")

        finally:
            self._warming = False


class CacheInvalidator:
    """Handles cache invalidation patterns."""

    def __init__(self, cache: APICache):
        self.cache = cache
        self._patterns: List[str] = []

    def add_pattern(self, pattern: str):
        """Add invalidation pattern."""
        self._patterns.append(pattern)

    async def invalidate_related(self, changed_path: str):
        """Invalidate all cache entries related to changed path."""
        for pattern in self._patterns:
            if pattern in changed_path:
                await self.cache.invalidate(pattern)


async def main():
    """Demonstrate API cache."""
    cache = APICache(strategy=CacheStrategy.LRU, max_size=100)

    await cache.set("GET", "/api/users", {"users": ["Alice", "Bob"]})
    result = await cache.get("GET", "/api/users")

    print(f"Cache hit: {result}")
    print(f"Stats: {cache.get_stats()}")


if __name__ == "__main__":
    asyncio.run(main())

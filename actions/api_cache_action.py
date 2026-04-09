"""
API Cache Action Module

Multi-layer caching with in-memory, Redis, and TTL support.
Cache invalidation, compression, and statistics tracking.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class CacheLevel(Enum):
    """Cache storage levels."""

    MEMORY = "memory"
    REDIS = "redis"
    DISK = "disk"


class CachePolicy(Enum):
    """Cache policies."""

    STORE_THROUGH = "store_through"
    STORE_AROUND = "store_around"
    CACHE_ASIDE = "cache_aside"


@dataclass
class CacheEntry:
    """A single cache entry."""

    key: str
    value: Any
    created_at: float = field(default_factory=time.time)
    accessed_at: float = field(default_factory=time.time)
    access_count: int = 0
    ttl_seconds: float = 0.0
    compressed: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_expired(self) -> bool:
        """Check if entry is expired."""
        if self.ttl_seconds <= 0:
            return False
        return time.time() - self.created_at > self.ttl_seconds

    def touch(self) -> None:
        """Update access time and count."""
        self.accessed_at = time.time()
        self.access_count += 1

    def get_age_seconds(self) -> float:
        """Get age of entry in seconds."""
        return time.time() - self.created_at


@dataclass
class CacheStats:
    """Cache statistics."""

    hits: int = 0
    misses: int = 0
    sets: int = 0
    deletes: int = 0
    expirations: int = 0
    evictions: int = 0
    bytes_stored: int = 0
    bytes_saved: int = 0


class MemoryCache:
    """
    In-memory cache with LRU eviction.
    """

    def __init__(self, max_size: int = 10000, max_memory_mb: float = 100.0):
        self.max_size = max_size
        self.max_memory_bytes = int(max_memory_mb * 1024 * 1024)
        self._cache: Dict[str, CacheEntry] = {}
        self._access_order: List[str] = []
        self._lock = asyncio.Lock()

    def _generate_key(self, key: str) -> str:
        """Generate cache key hash."""
        return hashlib.md5(key.encode()).hexdigest()

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None

            if entry.is_expired():
                await self.delete(key)
                return None

            entry.touch()
            self._update_access_order(key)
            return entry.value

    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: float = 0.0,
        compress: bool = False,
    ) -> None:
        """Set value in cache."""
        async with self._lock:
            # Compress if needed
            stored_value = value
            if compress:
                try:
                    serialized = json.dumps(value)
                    compressed = zlib.compress(serialized.encode())
                    if len(compressed) < len(serialized):
                        stored_value = compressed
                        compress = True
                except Exception:
                    pass

            # Check size limit
            if len(self._cache) >= self.max_size:
                await self._evict_lru()

            entry = CacheEntry(
                key=key,
                value=stored_value,
                ttl_seconds=ttl_seconds,
                compressed=compress,
            )
            self._cache[key] = entry
            self._update_access_order(key)

    async def delete(self, key: str) -> bool:
        """Delete value from cache."""
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                if key in self._access_order:
                    self._access_order.remove(key)
                return True
            return False

    async def clear(self) -> None:
        """Clear all cache entries."""
        async with self._lock:
            self._cache.clear()
            self._access_order.clear()

    async def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if self._access_order:
            lru_key = self._access_order.pop(0)
            if lru_key in self._cache:
                del self._cache[lru_key]

    def _update_access_order(self, key: str) -> None:
        """Update access order for LRU."""
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    async def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        async with self._lock:
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "max_memory_mb": self.max_memory_bytes / (1024 * 1024),
            }


class RedisCache:
    """
    Redis-backed cache with async support.
    """

    def __init__(
        self,
        redis_client: Any,
        prefix: str = "cache:",
        default_ttl: int = 3600,
    ):
        self.redis = redis_client
        self.prefix = prefix
        self.default_ttl = default_ttl

    def _make_key(self, key: str) -> str:
        """Make Redis key with prefix."""
        return f"{self.prefix}{key}"

    async def get(self, key: str) -> Optional[Any]:
        """Get value from Redis."""
        try:
            redis_key = self._make_key(key)
            value = await self.redis.get(redis_key)
            if value is None:
                return None

            # Try to decompress
            try:
                return json.loads(zlib.decompress(value))
            except Exception:
                return json.loads(value)

        except Exception as e:
            logger.error(f"Redis get error: {e}")
            return None

    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
        compress: bool = True,
    ) -> None:
        """Set value in Redis."""
        try:
            redis_key = self._make_key(key)
            serialized = json.dumps(value)

            if compress:
                serialized = zlib.compress(serialized.encode())

            await self.redis.set(
                redis_key,
                serialized,
                ex=ttl_seconds or self.default_ttl,
            )

        except Exception as e:
            logger.error(f"Redis set error: {e}")

    async def delete(self, key: str) -> bool:
        """Delete value from Redis."""
        try:
            redis_key = self._make_key(key)
            result = await self.redis.delete(redis_key)
            return result > 0
        except Exception as e:
            logger.error(f"Redis delete error: {e}")
            return False

    async def clear(self) -> None:
        """Clear all cache entries with prefix."""
        try:
            cursor = 0
            while True:
                cursor, keys = await self.redis.scan(
                    cursor,
                    match=f"{self.prefix}*",
                    count=100,
                )
                if keys:
                    await self.redis.delete(*keys)
                if cursor == 0:
                    break
        except Exception as e:
            logger.error(f"Redis clear error: {e}")


class APICacheAction:
    """
    Main action class for API caching.

    Features:
    - Multi-layer caching (memory, Redis)
    - TTL support
    - Compression
    - LRU eviction
    - Cache statistics
    - Cache invalidation patterns

    Usage:
        cache = APICacheAction()
        await cache.set("key", data, ttl=3600)
        value = await cache.get("key")
    """

    def __init__(
        self,
        memory_cache: Optional[MemoryCache] = None,
        redis_cache: Optional[RedisCache] = None,
        default_ttl: int = 3600,
    ):
        self.memory_cache = memory_cache or MemoryCache()
        self.redis_cache = redis_cache
        self.default_ttl = default_ttl
        self._stats = CacheStats()

    def _generate_key(self, *args: Any, **kwargs: Any) -> str:
        """Generate cache key from args."""
        key_parts = [str(arg) for arg in args]
        key_parts.extend(f"{k}={v}" for k, v in sorted(kwargs.items()))
        key_data = "|".join(key_parts)
        return hashlib.sha256(key_data.encode()).hexdigest()

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache (multi-layer)."""
        # Try memory first
        value = await self.memory_cache.get(key)
        if value is not None:
            self._stats.hits += 1
            return value

        # Try Redis
        if self.redis_cache:
            value = await self.redis_cache.get(key)
            if value is not None:
                self._stats.hits += 1
                # Populate memory cache
                await self.memory_cache.set(key, value, self.default_ttl)
                return value

        self._stats.misses += 1
        return None

    async def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[int] = None,
        compress: bool = True,
    ) -> None:
        """Set value in cache (multi-layer)."""
        ttl = ttl_seconds or self.default_ttl

        # Set in memory
        await self.memory_cache.set(key, value, ttl, compress)

        # Set in Redis
        if self.redis_cache:
            await self.redis_cache.set(key, value, ttl, compress)

        self._stats.sets += 1

    async def delete(self, key: str) -> bool:
        """Delete value from cache."""
        deleted = await self.memory_cache.delete(key)

        if self.redis_cache:
            deleted = await self.redis_cache.delete(key) or deleted

        if deleted:
            self._stats.deletes += 1

        return deleted

    async def clear(self) -> None:
        """Clear all caches."""
        await self.memory_cache.clear()
        if self.redis_cache:
            await self.redis_cache.clear()

    async def cached_call(
        self,
        key: str,
        func: Callable[..., Any],
        ttl_seconds: Optional[int] = None,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """Execute function with cache-aside pattern."""
        # Try cache first
        cached = await self.get(key)
        if cached is not None:
            return cached

        # Call function
        if asyncio.iscoroutinefunction(func):
            result = await func(*args, **kwargs)
        else:
            result = func(*args, **kwargs)

        # Cache result
        await self.set(key, result, ttl_seconds)
        return result

    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate keys matching pattern."""
        count = 0

        # Invalidate from memory
        keys_to_delete = [
            k for k in (await self.memory_cache.get_stats()).keys()
            if pattern in k
        ]
        for key in keys_to_delete:
            if await self.memory_cache.delete(key):
                count += 1

        # Invalidate from Redis
        if self.redis_cache:
            # Note: Redis pattern invalidation would need redis_cache support
            pass

        return count

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        hit_rate = 0.0
        total = self._stats.hits + self._stats.misses
        if total > 0:
            hit_rate = (self._stats.hits / total) * 100

        return {
            "hits": self._stats.hits,
            "misses": self._stats.misses,
            "hit_rate_percent": hit_rate,
            "sets": self._stats.sets,
            "deletes": self._stats.deletes,
            "expirations": self._stats.expirations,
            "evictions": self._stats.evictions,
        }

    def reset_stats(self) -> None:
        """Reset statistics."""
        self._stats = CacheStats()


async def demo_cache():
    """Demonstrate cache usage."""
    cache = APICacheAction()

    # Set value
    await cache.set("user:123", {"name": "Alice", "email": "alice@example.com"}, ttl=3600)

    # Get value
    value = await cache.get("user:123")
    print(f"Cached value: {value}")

    # Cached call
    async def fetch_user(user_id: int):
        await asyncio.sleep(0.1)  # Simulate API call
        return {"name": "Bob", "email": "bob@example.com"}

    result = await cache.cached_call("user:456", fetch_user, 456)
    print(f"Result: {result}")

    print(f"Stats: {cache.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_cache())

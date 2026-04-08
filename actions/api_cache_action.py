"""API Cache Action Module.

Provides multi-layer API response caching with TTL, ETag,
conditional requests, and cache invalidation strategies.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
import logging

logger = logging.getLogger(__name__)


class CacheStrategy(Enum):
    """Cache strategy."""
    TTL = "ttl"
    ETag = "etag"
    LastModified = "last_modified"
    Conditional = "conditional"


@dataclass
class CacheEntry:
    """Cache entry."""
    key: str
    value: Any
    created_at: float
    ttl: float
    etag: Optional[str] = None
    last_modified: Optional[str] = None


@dataclass
class CacheConfig:
    """Cache configuration."""
    strategy: CacheStrategy = CacheStrategy.TTL
    default_ttl: float = 300.0
    max_entries: int = 1000
    stale_while_revalidate: bool = True
    stale_ttl: Optional[float] = None


class APICacheAction:
    """Multi-layer API response cache.

    Example:
        cache = APICacheAction(CacheConfig(default_ttl=300))

        result = await cache.get_or_fetch(
            "user_123",
            lambda: api.get_user("123")
        )

        cache.invalidate("user_123")
    """

    def __init__(self, config: Optional[CacheConfig] = None) -> None:
        self.config = config or CacheConfig()
        self._cache: Dict[str, CacheEntry] = {}
        self._access_order: List[str] = []
        self._lock = asyncio.Lock()
        self._pending: Dict[str, asyncio.Task] = {}
        self._revalidation_callbacks: Dict[str, Callable] = {}

    def make_key(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Generate cache key from endpoint and params.

        Args:
            endpoint: API endpoint path
            params: Query parameters

        Returns:
            Cache key string
        """
        content = endpoint
        if params:
            sorted_params = sorted(params.items())
            param_str = "&".join(f"{k}={v}" for k, v in sorted_params)
            content = f"{endpoint}?{param_str}"

        return hashlib.sha256(content.encode()).hexdigest()

    async def get_or_fetch(
        self,
        key: str,
        fetch_fn: Callable[[], Any],
        ttl: Optional[float] = None,
        etag: Optional[str] = None,
    ) -> Any:
        """Get cached value or fetch if not present.

        Args:
            key: Cache key
            fetch_fn: Async function to fetch data
            ttl: Time-to-live in seconds
            etag: Optional ETag for conditional requests

        Returns:
            Cached or fetched value
        """
        cached = await self.get(key)
        if cached is not None:
            return cached

        if key in self._pending:
            return await self._pending[key]

        task = asyncio.create_task(self._fetch_and_cache(key, fetch_fn, ttl, etag))
        self._pending[key] = task

        try:
            return await task
        finally:
            self._pending.pop(key, None)

    async def _fetch_and_cache(
        self,
        key: str,
        fetch_fn: Callable[[], Any],
        ttl: Optional[float],
        etag: Optional[str],
    ) -> Any:
        """Fetch data and cache it."""
        async with self._lock:
            if key in self._cache:
                return self._cache[key].value

        if asyncio.iscoroutinefunction(fetch_fn):
            value = await fetch_fn()
        else:
            value = fetch_fn()

        await self.set(key, value, ttl=ttl, etag=etag)
        return value

    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache.

        Args:
            key: Cache key

        Returns:
            Cached value or None
        """
        async with self._lock:
            if key not in self._cache:
                return None

            entry = self._cache[key]
            age = time.time() - entry.created_at

            if age > entry.ttl:
                if self.config.stale_while_revalidate and self.config.stale_ttl:
                    if age < entry.ttl + self.config.stale_ttl:
                        asyncio.create_task(self._revalidate(key))
                        return entry.value

                del self._cache[key]
                self._access_order.remove(key)
                return None

            self._touch_access(key)
            return entry.value

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
    ) -> None:
        """Set value in cache.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds
            etag: Optional ETag
            last_modified: Optional Last-Modified header
        """
        async with self._lock:
            if len(self._cache) >= self.config.max_entries:
                await self._evict_oldest()

            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                ttl=ttl or self.config.default_ttl,
                etag=etag,
                last_modified=last_modified,
            )

            self._cache[key] = entry
            self._touch_access(key)

    async def invalidate(self, key: str) -> None:
        """Invalidate cache entry.

        Args:
            key: Cache key to invalidate
        """
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                if key in self._access_order:
                    self._access_order.remove(key)

    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching pattern.

        Args:
            pattern: Key pattern (simple substring match)

        Returns:
            Number of invalidated keys
        """
        async with self._lock:
            keys_to_remove = [
                k for k in self._cache.keys()
                if pattern in k
            ]

            for key in keys_to_remove:
                del self._cache[key]
                if key in self._access_order:
                    self._access_order.remove(key)

            return len(keys_to_remove)

    async def invalidate_prefix(self, prefix: str) -> int:
        """Invalidate all keys with given prefix.

        Args:
            prefix: Key prefix

        Returns:
            Number of invalidated keys
        """
        return await self.invalidate_pattern(prefix)

    async def clear(self) -> None:
        """Clear all cache entries."""
        async with self._lock:
            self._cache.clear()
            self._access_order.clear()

    def register_revalidation_callback(
        self,
        key: str,
        callback: Callable[[Any], Any],
    ) -> None:
        """Register callback for stale-while-revalidate.

        Args:
            key: Cache key
            callback: Revalidation function
        """
        self._revalidation_callbacks[key] = callback

    async def _revalidate(self, key: str) -> None:
        """Revalidate stale cache entry."""
        if key not in self._cache:
            return

        if key in self._revalidation_callbacks:
            try:
                new_value = self._revalidation_callbacks[key](self._cache[key].value)
                if asyncio.iscoroutine(new_value):
                    new_value = await new_value
                await self.set(key, new_value, etag=self._cache[key].etag)
            except Exception as e:
                logger.error(f"Revalidation failed for {key}: {e}")

    def _touch_access(self, key: str) -> None:
        """Update access order."""
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    async def _evict_oldest(self) -> None:
        """Evict least recently used entry."""
        if self._access_order:
            oldest = self._access_order.pop(0)
            self._cache.pop(oldest, None)

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        total_entries = len(self._cache)
        total_size = sum(
            len(str(entry.value)) for entry in self._cache.values()
        )

        return {
            "entries": total_entries,
            "max_entries": self.config.max_entries,
            "total_size_bytes": total_size,
            "strategy": self.config.strategy.value,
        }

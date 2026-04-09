"""API Response Cache Action module.

Provides intelligent caching for API responses with TTL,
stale-while-revalidate, cache invalidation, and compression.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import zlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

import aiohttp


class CacheStrategy(Enum):
    """Cache strategies."""

    CACHE_FIRST = "cache_first"
    NETWORK_FIRST = "network_first"
    STALE_WHILE_REVALIDATE = "stale_while_revalidate"
    CACHE_ONLY = "cache_only"
    NETWORK_ONLY = "network_only"


@dataclass
class CacheEntry:
    """A cached response entry."""

    key: str
    data: bytes
    headers: dict[str, str]
    status: int
    created_at: float
    accessed_at: float
    expires_at: float
    ttl: float
    compress: bool
    hit_count: int = 0
    size_bytes: int = 0

    @property
    def is_expired(self) -> bool:
        """Check if entry has expired."""
        return time.time() > self.expires_at

    @property
    def is_stale(self) -> bool:
        """Check if entry is stale but not yet expired."""
        if self.is_expired:
            return False
        stale_threshold = self.created_at + self.ttl * 0.8
        return time.time() > stale_threshold

    def touch(self) -> None:
        """Update access time."""
        self.accessed_at = time.time()
        self.hit_count += 1


@dataclass
class CacheConfig:
    """Configuration for response cache."""

    max_size_mb: float = 100.0
    default_ttl: float = 300.0
    compression_threshold: int = 1024
    enable_stale_while_revalidate: bool = True
    max_stale_age: float = 3600.0
    cache_empty_responses: bool = False


class ResponseCache:
    """In-memory response cache with eviction."""

    def __init__(self, config: Optional[CacheConfig] = None):
        self.config = config or CacheConfig()
        self._cache: dict[str, CacheEntry] = {}
        self._lock = asyncio.Lock()
        self._total_size = 0
        self._hits = 0
        self._misses = 0
        self._revalidations = 0

    def _make_key(self, url: str, method: str, params: Optional[dict]) -> str:
        """Generate cache key."""
        key_parts = [method.upper(), url]
        if params:
            key_parts.append(json.dumps(params, sort_keys=True))
        key_str = "|".join(key_parts)
        return hashlib.sha256(key_str.encode()).hexdigest()

    def _compress(self, data: bytes) -> bytes:
        """Compress data if beneficial."""
        if len(data) < self.config.compression_threshold:
            return data
        return zlib.compress(data)

    def _decompress(self, data: bytes, is_compressed: bool) -> bytes:
        """Decompress data if needed."""
        if is_compressed:
            return zlib.decompress(data)
        return data

    async def get(
        self,
        url: str,
        method: str = "GET",
        params: Optional[dict] = None,
    ) -> Optional[tuple[bytes, dict[str, str], int]]:
        """Get cached response if available.

        Returns:
            Tuple of (data, headers, status) or None
        """
        key = self._make_key(url, method, params)

        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._misses += 1
                return None

            if entry.is_expired:
                self._misses += 1
                del self._cache[key]
                self._total_size -= entry.size_bytes
                return None

            entry.touch()
            self._hits += 1
            data = self._decompress(entry.data, entry.compress)
            return data, entry.headers, entry.status

    async def set(
        self,
        url: str,
        data: bytes,
        headers: dict[str, str],
        status: int,
        method: str = "GET",
        params: Optional[dict] = None,
        ttl: Optional[float] = None,
    ) -> None:
        """Cache a response."""
        if status >= 400 and not self.config.cache_empty_responses:
            return

        key = self._make_key(url, method, params)
        compress = len(data) >= self.config.compression_threshold
        data_to_store = self._compress(data) if compress else data

        entry = CacheEntry(
            key=key,
            data=data_to_store,
            headers=headers,
            status=status,
            created_at=time.time(),
            accessed_at=time.time(),
            expires_at=time.time() + (ttl or self.config.default_ttl),
            ttl=ttl or self.config.default_ttl,
            compress=compress,
            size_bytes=len(data_to_store),
        )

        async with self._lock:
            old_entry = self._cache.get(key)
            if old_entry:
                self._total_size -= old_entry.size_bytes

            self._cache[key] = entry
            self._total_size += entry.size_bytes

            await self._evict_if_needed()

    async def _evict_if_needed(self) -> None:
        """Evict entries if cache exceeds max size."""
        max_bytes = self.config.max_size_mb * 1024 * 1024

        while self._total_size > max_bytes and self._cache:
            oldest = min(self._cache.values(), key=lambda e: e.accessed_at)
            del self._cache[oldest.key]
            self._total_size -= oldest.size_bytes

    async def invalidate(self, pattern: Optional[str] = None) -> int:
        """Invalidate cache entries matching pattern.

        Args:
            pattern: Optional URL pattern to match

        Returns:
            Number of entries invalidated
        """
        count = 0
        async with self._lock:
            if pattern is None:
                count = len(self._cache)
                self._cache.clear()
                self._total_size = 0
            else:
                keys_to_delete = [
                    k for k, v in self._cache.items() if pattern in k
                ]
                for key in keys_to_delete:
                    self._total_size -= self._cache[key].size_bytes
                    del self._cache[key]
                    count += 1
        return count

    async def get_stale(
        self,
        url: str,
        method: str = "GET",
        params: Optional[dict] = None,
    ) -> Optional[tuple[bytes, dict[str, str], int]]:
        """Get cached response even if stale (for stale-while-revalidate)."""
        key = self._make_key(url, method, params)

        async with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return None

            max_stale = entry.created_at + self.config.max_stale_age
            if time.time() > max_stale:
                return None

            if not entry.is_expired:
                return None

            entry.touch()
            self._revalidations += 1
            data = self._decompress(entry.data, entry.compress)
            return data, entry.headers, entry.status

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0.0

        return {
            "entries": len(self._cache),
            "size_mb": self._total_size / (1024 * 1024),
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": hit_rate,
            "revalidations": self._revalidations,
        }


class CachedApiClient:
    """API client with integrated caching."""

    def __init__(
        self,
        cache: Optional[ResponseCache] = None,
        cache_strategy: CacheStrategy = CacheStrategy.STALE_WHILE_REVALIDATE,
    ):
        self.cache = cache or ResponseCache()
        self.strategy = cache_strategy
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def request(
        self,
        method: str,
        url: str,
        params: Optional[dict] = None,
        ttl: Optional[float] = None,
        **kwargs: Any,
    ) -> tuple[bytes, aiohttp.ClientResponse]:
        """Make cached API request.

        Args:
            method: HTTP method
            url: Request URL
            params: Query parameters
            ttl: Cache TTL in seconds
            **kwargs: Additional request options

        Returns:
            Tuple of (response_data, response)
        """
        cache_key = self._make_key(url, method, params)

        if self.strategy in (
            CacheStrategy.CACHE_FIRST,
            CacheStrategy.STALE_WHILE_REVALIDATE,
        ):
            cached = await self.cache.get(url, method, params)
            if cached:
                data, headers, status = cached
                if self.strategy == CacheStrategy.STALE_WHILE_REVALIDATE:
                    asyncio.create_task(
                        self._revalidate(method, url, params, ttl, **kwargs)
                    )
                return data, self._make_response(status, headers)

        if self.strategy == CacheStrategy.CACHE_ONLY:
            cached = await self.cache.get(url, method, params)
            if cached:
                data, headers, status = cached
                return data, self._make_response(status, headers)
            raise ValueError("No cached response available")

        session = await self._get_session()
        async with session.request(
            method,
            url,
            params=params,
            **kwargs,
        ) as response:
            data = await response.read()
            await self.cache.set(
                url, data, dict(response.headers),
                response.status, method, params, ttl,
            )
            return data, response

    async def _revalidate(
        self,
        method: str,
        url: str,
        params: Optional[dict],
        ttl: Optional[float],
        **kwargs: Any,
    ) -> None:
        """Revalidate a stale cache entry."""
        try:
            stale = await self.cache.get_stale(url, method, params)
            if stale is None:
                return

            session = await self._get_session()
            async with session.request(method, url, params=params, **kwargs) as resp:
                data = await resp.read()
                if resp.status < 400:
                    await self.cache.set(
                        url, data, dict(resp.headers),
                        resp.status, method, params, ttl,
                    )
        except Exception:
            pass

    def _make_key(self, url: str, method: str, params: Optional[dict]) -> str:
        """Generate cache key."""
        key_parts = [method.upper(), url]
        if params:
            key_parts.append(json.dumps(params, sort_keys=True))
        return hashlib.sha256("|".join(key_parts).encode()).hexdigest()

    def _make_response(self, status: int, headers: dict[str, str]) -> Any:
        """Create a mock response object."""
        class MockResponse:
            status = status
            headers = headers
            def __init__(self, s, h):
                self.status = s
                self.headers = h
        return MockResponse(status, headers)

    async def close(self) -> None:
        """Close the client session."""
        if self._session and not self._session.closed:
            await self._session.close()

"""
API response caching module.

Provides intelligent response caching with TTL, invalidation,
and cache-aside patterns for API requests.

Author: Aito Auto Agent
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class CacheStrategy(Enum):
    """Caching strategy types."""
    CACHE_ASIDE = auto()
    READ_THROUGH = auto()
    WRITE_THROUGH = auto()
    WRITE_BACK = auto()


class InvalidationType(Enum):
    """Cache invalidation types."""
    TTL = auto()
    LRU = auto()
    LFU = auto()
    MANUAL = auto()


@dataclass
class CacheEntry:
    """Single cache entry with metadata."""
    key: str
    value: Any
    created_at: float
    last_accessed: float
    access_count: int = 0
    ttl_seconds: Optional[float] = None
    tags: list[str] = field(default_factory=list)

    def is_expired(self) -> bool:
        """Check if entry has expired."""
        if self.ttl_seconds is None:
            return False
        return (time.time() - self.created_at) > self.ttl_seconds

    def is_stale(self, max_age: float) -> bool:
        """Check if entry is older than max_age seconds."""
        return (time.time() - self.created_at) > max_age


@dataclass
class CacheStats:
    """Cache statistics."""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    expirations: int = 0
    writes: int = 0
    deletes: int = 0

    @property
    def total_requests(self) -> int:
        return self.hits + self.misses

    @property
    def hit_rate(self) -> float:
        return self.hits / self.total_requests if self.total_requests > 0 else 0.0


class ApiResponseCache:
    """
    API response caching with multiple strategies.

    Example:
        cache = ApiResponseCache(max_size=1000, ttl_seconds=300)

        # Get cached response
        response = cache.get("api/users/123")

        # Set cache
        cache.set("api/users/123", response_data)

        # Invalidate by tag
        cache.invalidate_by_tag("user_123")
    """

    def __init__(
        self,
        max_size: int = 1000,
        ttl_seconds: Optional[float] = None,
        strategy: CacheStrategy = CacheStrategy.CACHE_ASIDE,
        invalidation: InvalidationType = InvalidationType.TTL,
        lock_threshold: int = 100
    ):
        self._max_size = max_size
        self._default_ttl = ttl_seconds
        self._strategy = strategy
        self._invalidation = invalidation
        self._lock = threading.RLock() if max_size > lock_threshold else None

        self._cache: dict[str, CacheEntry] = {}
        self._tag_index: dict[str, set[str]] = {}
        self._stats = CacheStats()

    def _acquire(self):
        if self._lock:
            self._lock.acquire()

    def _release(self):
        if self._lock:
            self._lock.release()

    def _generate_key(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[dict] = None,
        headers: Optional[dict] = None
    ) -> str:
        """Generate cache key from request parameters."""
        key_parts = [method.upper(), endpoint]

        if params:
            sorted_params = json.dumps(params, sort_keys=True)
            key_parts.append(sorted_params)

        if headers:
            sorted_headers = json.dumps(
                {k: v for k, v in sorted(headers.items()) if k.lower().startswith("accept")},
                sort_keys=True
            )
            key_parts.append(sorted_headers)

        key_string = "|".join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()

    def get(
        self,
        key: str,
        refresh_ttl: bool = True
    ) -> Optional[Any]:
        """
        Get cached value by key.

        Args:
            key: Cache key
            refresh_ttl: Whether to refresh TTL on access

        Returns:
            Cached value or None if not found/expired
        """
        with self._acquire():
            entry = self._cache.get(key)

            if entry is None:
                self._stats.misses += 1
                return None

            if entry.is_expired():
                self._delete_entry(key)
                self._stats.misses += 1
                self._stats.expirations += 1
                return None

            entry.last_accessed = time.time()
            entry.access_count += 1
            self._stats.hits += 1

            if refresh_ttl and entry.ttl_seconds:
                entry.created_at = time.time()

            return entry.value

    def set(
        self,
        key: str,
        value: Any,
        ttl_seconds: Optional[float] = None,
        tags: Optional[list[str]] = None
    ) -> None:
        """
        Set cached value.

        Args:
            key: Cache key
            value: Value to cache
            ttl_seconds: Time-to-live in seconds
            tags: Tags for invalidation
        """
        with self._acquire():
            if len(self._cache) >= self._max_size:
                self._evict()

            ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl

            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                last_accessed=time.time(),
                ttl_seconds=ttl,
                tags=tags or []
            )

            self._cache[key] = entry
            self._stats.writes += 1

            for tag in entry.tags:
                if tag not in self._tag_index:
                    self._tag_index[tag] = set()
                self._tag_index[tag].add(key)

    def _evict(self) -> None:
        """Evict entry based on invalidation strategy."""
        if not self._cache:
            return

        if self._invalidation == InvalidationType.LRU:
            lru_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].last_accessed
            )
            self._delete_entry(lru_key)

        elif self._invalidation == InvalidationType.LFU:
            lfu_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].access_count
            )
            self._delete_entry(lfu_key)

        else:
            oldest_key = min(
                self._cache.keys(),
                key=lambda k: self._cache[k].created_at
            )
            self._delete_entry(oldest_key)

        self._stats.evictions += 1

    def _delete_entry(self, key: str) -> None:
        """Delete entry and clean up tag index."""
        if key in self._cache:
            entry = self._cache[key]
            for tag in entry.tags:
                if tag in self._tag_index:
                    self._tag_index[tag].discard(key)

            del self._cache[key]
            self._stats.deletes += 1

    def delete(self, key: str) -> bool:
        """
        Delete cached entry by key.

        Returns:
            True if entry was deleted
        """
        with self._acquire():
            if key in self._cache:
                self._delete_entry(key)
                return True
            return False

    def invalidate_by_tag(self, tag: str) -> int:
        """
        Invalidate all entries with a specific tag.

        Args:
            tag: Tag to match

        Returns:
            Number of entries invalidated
        """
        with self._acquire():
            keys = self._tag_index.get(tag, set()).copy()
            for key in keys:
                self._delete_entry(key)
            return len(keys)

    def invalidate_by_pattern(self, pattern: str) -> int:
        """
        Invalidate entries whose keys match pattern.

        Args:
            pattern: Pattern to match (supports * wildcards)

        Returns:
            Number of entries invalidated
        """
        with self._acquire():
            import fnmatch

            matching_keys = [
                k for k in self._cache.keys()
                if fnmatch.fnmatch(k, pattern)
            ]

            for key in matching_keys:
                self._delete_entry(key)

            return len(matching_keys)

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._acquire():
            self._cache.clear()
            self._tag_index.clear()

    def cleanup_expired(self) -> int:
        """
        Remove all expired entries.

        Returns:
            Number of entries removed
        """
        with self._acquire():
            expired_keys = [
                k for k, v in self._cache.items()
                if v.is_expired()
            ]

            for key in expired_keys:
                self._delete_entry(key)
                self._stats.expirations += 1

            return len(expired_keys)

    def get_stats(self) -> CacheStats:
        """Get cache statistics."""
        return self._stats

    def get_size(self) -> int:
        """Get current cache size."""
        return len(self._cache)

    def contains(self, key: str) -> bool:
        """Check if key exists and is not expired."""
        entry = self._cache.get(key)
        if entry is None:
            return False
        if entry.is_expired():
            self._delete_entry(key)
            return False
        return True


class CachedApiClient:
    """
    API client with built-in caching.

    Example:
        client = CachedApiClient(
            base_url="https://api.example.com",
            cache=ApiResponseCache(ttl_seconds=60)
        )

        # Requests are automatically cached
        response = client.get("/users/123")
    """

    def __init__(
        self,
        base_url: str = "",
        cache: Optional[ApiResponseCache] = None,
        session: Optional[Any] = None
    ):
        self._base_url = base_url.rstrip("/")
        self._cache = cache or ApiResponseCache()
        self._session = session

    def get(
        self,
        endpoint: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        use_cache: bool = True,
        cache_ttl: Optional[float] = None,
        **kwargs
    ) -> Any:
        """Make GET request with caching."""
        key = self._cache._generate_key(endpoint, "GET", params, headers)

        if use_cache:
            cached = self._cache.get(key)
            if cached is not None:
                return cached

        response = self._make_request("GET", endpoint, params=params, headers=headers, **kwargs)

        self._cache.set(key, response, ttl_seconds=cache_ttl)
        return response

    def post(
        self,
        endpoint: str,
        data: Optional[Any] = None,
        headers: Optional[dict] = None,
        invalidate_cache: bool = False,
        **kwargs
    ) -> Any:
        """Make POST request."""
        response = self._make_request("POST", endpoint, data=data, headers=headers, **kwargs)

        if invalidate_cache:
            self._cache.clear()

        return response

    def _make_request(
        self,
        method: str,
        endpoint: str,
        **kwargs
    ) -> Any:
        """Make actual HTTP request."""
        import requests

        url = f"{self._base_url}/{endpoint.lstrip('/')}"

        if self._session:
            response = self._session.request(method, url, **kwargs)
        else:
            response = requests.request(method, url, **kwargs)

        return response.json() if response.content else None


def create_cache(
    max_size: int = 1000,
    ttl_seconds: Optional[float] = None,
    strategy: CacheStrategy = CacheStrategy.CACHE_ASIDE
) -> ApiResponseCache:
    """Factory to create an ApiResponseCache."""
    return ApiResponseCache(
        max_size=max_size,
        ttl_seconds=ttl_seconds,
        strategy=strategy
    )


def create_cached_client(
    base_url: str,
    cache: Optional[ApiResponseCache] = None
) -> CachedApiClient:
    """Factory to create a CachedApiClient."""
    return CachedApiClient(base_url=base_url, cache=cache)

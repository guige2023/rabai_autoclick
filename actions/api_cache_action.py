"""API response caching action module.

Provides intelligent caching for API responses with TTL, invalidation,
and conditional request support (ETag/Last-Modified).
"""

from __future__ import annotations

import time
import hashlib
import json
import logging
import fnmatch
from typing import Optional, Dict, Any, Callable
from dataclasses import dataclass, field
from functools import wraps

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """A cached API response."""
    key: str
    data: Any
    status_code: int
    headers: Dict[str, str]
    created_at: float = field(default_factory=lambda: time.time())
    expires_at: Optional[float] = None
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    hit_count: int = 0

    @property
    def is_expired(self) -> bool:
        """Check if the cache entry has expired."""
        if self.expires_at is None:
            return False
        return time.time() >= self.expires_at

    def to_dict(self) -> Dict[str, Any]:
        """Serialize cache entry to dict."""
        return {
            "key": self.key,
            "status_code": self.status_code,
            "headers": self.headers,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "etag": self.etag,
            "last_modified": self.last_modified,
            "hit_count": self.hit_count,
        }


class APICache:
    """In-memory cache for API responses with TTL and LRU eviction.

    Supports conditional requests via ETag/Last-Modified headers.
    """

    def __init__(
        self,
        max_entries: int = 1000,
        default_ttl: int = 300,
        storage_path: Optional[str] = None,
    ) -> None:
        """Initialize API cache.

        Args:
            max_entries: Maximum number of cached entries.
            default_ttl: Default time-to-live in seconds.
            storage_path: Optional path to persist cache to disk.
        """
        self.max_entries = max_entries
        self.default_ttl = default_ttl
        self.storage_path = storage_path
        self._cache: Dict[str, CacheEntry] = {}
        self._access_order: list = []
        self._load()

    def get(self, key: str) -> Optional[CacheEntry]:
        """Get a cached entry by key.

        Args:
            key: Cache key (typically URL + query params hash).

        Returns:
            CacheEntry if found and valid, None otherwise.
        """
        entry = self._cache.get(key)
        if entry is None:
            return None

        if entry.is_expired:
            self._evict(key)
            return None

        entry.hit_count += 1
        self._touch(key)
        return entry

    def set(
        self,
        key: str,
        data: Any,
        status_code: int = 200,
        headers: Optional[Dict[str, str]] = None,
        ttl: Optional[int] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
    ) -> None:
        """Store an API response in cache.

        Args:
            key: Cache key.
            data: Response body.
            status_code: HTTP status code.
            headers: Response headers.
            ttl: Time-to-live in seconds (None = use default).
            etag: ETag header value for conditional requests.
            last_modified: Last-Modified header value.
        """
        if len(self._cache) >= self.max_entries:
            self._evict_lru()

        ttl = ttl if ttl is not None else self.default_ttl
        entry = CacheEntry(
            key=key,
            data=data,
            status_code=status_code,
            headers=headers or {},
            expires_at=(time.time() + ttl) if ttl > 0 else None,
            etag=etag,
            last_modified=last_modified,
        )
        self._cache[key] = entry
        self._touch(key)
        self._save()

    def invalidate(self, key: str) -> bool:
        """Remove a specific entry from cache.

        Args:
            key: Cache key to remove.

        Returns:
            True if removed, False if not found.
        """
        if key in self._cache:
            self._evict(key)
            self._save()
            return True
        return False

    def invalidate_pattern(self, pattern: str) -> int:
        """Remove all entries matching a URL pattern.

        Args:
            pattern: Glob pattern (e.g., "/api/users/*").

        Returns:
            Number of entries removed.
        """
        to_remove = [k for k in self._cache if fnmatch.fnmatch(k, pattern)]
        for key in to_remove:
            self._evict(key)
        if to_remove:
            self._save()
        return len(to_remove)

    def clear(self) -> int:
        """Clear all cache entries.

        Returns:
            Number of entries cleared.
        """
        count = len(self._cache)
        self._cache.clear()
        self._access_order.clear()
        self._save()
        return count

    def stats(self) -> Dict[str, Any]:
        """Get cache statistics.

        Returns:
            Dict with size, hit counts, and entry details.
        """
        total_hits = sum(e.hit_count for e in self._cache.values())
        return {
            "size": len(self._cache),
            "max_entries": self.max_entries,
            "total_hits": total_hits,
            "entries": [e.to_dict() for e in list(self._cache.values())[:10]],
        }

    def conditional_headers(self, key: str) -> Optional[Dict[str, str]]:
        """Get conditional request headers for a cached entry.

        Args:
            key: Cache key.

        Returns:
            Dict with If-None-Match and/or If-Modified-Since or None.
        """
        entry = self._cache.get(key)
        if entry is None:
            return None

        headers = {}
        if entry.etag:
            headers["If-None-Match"] = entry.etag
        if entry.last_modified:
            headers["If-Modified-Since"] = entry.last_modified
        return headers if headers else None

    def _touch(self, key: str) -> None:
        """Update access order for LRU tracking."""
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    def _evict(self, key: str) -> None:
        """Remove a key from cache."""
        self._cache.pop(key, None)
        if key in self._access_order:
            self._access_order.remove(key)

    def _evict_lru(self) -> None:
        """Evict the least recently used entry."""
        if self._access_order:
            self._evict(self._access_order[0])

    def _load(self) -> None:
        """Load cache from disk."""
        if not self.storage_path:
            return
        try:
            with open(self.storage_path, "r") as f:
                data = json.load(f)
                for item in data:
                    entry = CacheEntry(**item)
                    if not entry.is_expired:
                        self._cache[entry.key] = entry
                        self._access_order.append(entry.key)
        except (FileNotFoundError, json.JSONDecodeError):
            pass

    def _save(self) -> None:
        """Persist cache to disk."""
        if not self.storage_path:
            return
        try:
            with open(self.storage_path, "w") as f:
                json.dump([e.to_dict() for e in self._cache.values()], f, indent=2)
        except IOError as e:
            logger.error("Failed to save API cache: %s", e)


def cached_request(
    cache: APICache,
    key_func: Optional[Callable[..., str]] = None,
    ttl: int = 300,
):
    """Decorator to cache HTTP request responses.

    Args:
        cache: APICache instance.
        key_func: Function to generate cache key from args/kwargs.
        ttl: Time-to-live in seconds.

    Example:
        cache = APICache()
        @cached_request(cache, ttl=600)
        def fetch_user(user_id):
            return requests.get(f"/api/users/{user_id}").json()
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            cache_key = key_func(*args, **kwargs) if key_func else (
                f"{func.__name__}:{hashlib.sha256(json.dumps({'args': args, 'kwargs': kwargs}, sort_keys=True).encode()).hexdigest()}"
            )

            cached = cache.get(cache_key)
            if cached is not None:
                logger.debug("Cache hit for %s", cache_key)
                return cached.data

            result = func(*args, **kwargs)
            cache.set(cache_key, result, ttl=ttl)
            return result

        return wrapper
    return decorator


class APICacheAction:
    """High-level API caching action.

    Example:
        cache = APICache(default_ttl=600, storage_path="/tmp/api_cache.json")
        action = APICacheAction(cache)

        action.set("user_123", user_data, etag='"abc123"')
        entry = action.get("user_123")
    """

    def __init__(self, cache: Optional[APICache] = None) -> None:
        """Initialize API cache action.

        Args:
            cache: APICache instance. Creates default if None.
        """
        self.cache = cache or APICache()

    def get(self, key: str) -> Optional[Any]:
        """Get cached response data."""
        entry = self.cache.get(key)
        return entry.data if entry else None

    def set(
        self,
        key: str,
        data: Any,
        ttl: int = 300,
        **kwargs,
    ) -> None:
        """Cache a response."""
        self.cache.set(key, data, ttl=ttl, **kwargs)

    def get_or_fetch(
        self,
        key: str,
        fetch_fn: Callable[[], Any],
        ttl: Optional[int] = None,
    ) -> Any:
        """Get from cache or fetch and cache.

        Args:
            key: Cache key.
            fetch_fn: Function to call if cache miss.
            ttl: Optional TTL override.

        Returns:
            Cached or freshly fetched data.
        """
        entry = self.cache.get(key)
        if entry is not None:
            return entry.data

        data = fetch_fn()
        ttl_val = ttl if ttl is not None else self.cache.default_ttl
        self.cache.set(key, data, ttl=ttl_val)
        return data

    def invalidate(self, key: str) -> bool:
        """Invalidate a specific key."""
        return self.cache.invalidate(key)

    def invalidate_prefix(self, prefix: str) -> int:
        """Invalidate all keys starting with prefix."""
        to_remove = [k for k in self.cache._cache if k.startswith(prefix)]
        for k in to_remove:
            self.cache._evict(k)
        return len(to_remove)

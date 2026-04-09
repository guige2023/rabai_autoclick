"""
API Response Cache Action Module.

Multi-layer response caching with TTL, tags,
invalidation patterns, and stale-while-revalidate.
"""

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class CacheEntry:
    """
    Cached response entry.

    Attributes:
        key: Cache key.
        value: Cached response data.
        created_at: Timestamp when entry was created.
        ttl: Time-to-live in seconds.
        tags: Set of tag identifiers.
        stale_at: Timestamp when entry becomes stale.
        hit_count: Number of cache hits.
    """
    key: str
    value: Any
    created_at: float
    ttl: float
    tags: set = field(default_factory=set)
    stale_at: float = field(default=0.0, init=False)
    hit_count: int = field(default=0, init=False)
    is_stale: bool = field(default=False, init=False)

    def __post_init__(self):
        """Calculate stale timestamp."""
        self.stale_at = self.created_at + self.ttl


@dataclass
class CacheConfig:
    """Configuration for response cache."""
    default_ttl: float = 300.0
    max_entries: int = 10000
    stale_while_revalidate: bool = True
    stale_ttl: float = 60.0
    eviction_policy: str = "lru"


class APIResponseCacheAction:
    """
    Multi-layer API response caching with advanced features.

    Example:
        cache = APIResponseCacheAction()
        cache.configure(default_ttl=600, max_entries=5000)

        # Check cache first
        result = cache.get("user:123")
        if result is None:
            result = await fetch_user(123)
            cache.set("user:123", result, tags=["user"])
    """

    def __init__(self, config: Optional[CacheConfig] = None):
        """
        Initialize API response cache.

        Args:
            config: Cache configuration. Uses defaults if None.
        """
        self.config = config or CacheConfig()
        self._cache: dict[str, CacheEntry] = {}
        self._access_order: list[str] = []
        self._tag_index: dict[str, set[str]] = {}
        self._revalidate_tasks: dict[str, asyncio.Task] = {}
        self._hits = 0
        self._misses = 0

    def configure(
        self,
        default_ttl: Optional[float] = None,
        max_entries: Optional[int] = None,
        stale_while_revalidate: Optional[bool] = None
    ) -> None:
        """Update cache configuration."""
        if default_ttl is not None:
            self.config.default_ttl = default_ttl
        if max_entries is not None:
            self.config.max_entries = max_entries
        if stale_while_revalidate is not None:
            self.config.stale_while_revalidate = stale_while_revalidate

    def _generate_key(self, prefix: str, *args: Any, **kwargs: Any) -> str:
        """Generate cache key from arguments."""
        key_data = f"{prefix}:{args}:{sorted(kwargs.items())}"
        return hashlib.sha256(key_data.encode()).hexdigest()[:32]

    def get(self, key: str) -> Optional[Any]:
        """
        Get cached value by key.

        Args:
            key: Cache key.

        Returns:
            Cached value or None if not found/expired.
        """
        if key not in self._cache:
            self._misses += 1
            return None

        entry = self._cache[key]
        now = time.time()

        if now > entry.stale_at + self.config.stale_ttl:
            self._evict(key)
            self._misses += 1
            return None

        if now > entry.stale_at:
            entry.is_stale = True
            if self.config.stale_while_revalidate and key not in self._revalidate_tasks:
                logger.debug(f"Cache stale for key: {key}")
        else:
            entry.hit_count += 1

        self._hits += 1
        self._update_access_order(key)

        return entry.value

    def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
        tags: Optional[list[str]] = None
    ) -> None:
        """
        Set cache value.

        Args:
            key: Cache key.
            value: Value to cache.
            ttl: Time-to-live in seconds.
            tags: List of tags for invalidation.
        """
        if len(self._cache) >= self.config.max_entries:
            self._evict_lru()

        entry = CacheEntry(
            key=key,
            value=value,
            created_at=time.time(),
            ttl=ttl or self.config.default_ttl,
            tags=set(tags) if tags else set()
        )

        self._cache[key] = entry

        for tag in entry.tags:
            if tag not in self._tag_index:
                self._tag_index[tag] = set()
            self._tag_index[tag].add(key)

        self._update_access_order(key)

        logger.debug(f"Cached key: {key} (TTL: {entry.ttl}s, tags: {entry.tags})")

    def invalidate(self, key: str) -> bool:
        """
        Invalidate a single cache entry.

        Args:
            key: Cache key to invalidate.

        Returns:
            True if key was found and removed.
        """
        if key in self._cache:
            self._evict(key)
            return True
        return False

    def invalidate_by_tags(self, *tags: str) -> int:
        """
        Invalidate all entries with given tags.

        Args:
            *tags: Tag names to match.

        Returns:
            Number of entries invalidated.
        """
        keys_to_evict: set[str] = set()

        for tag in tags:
            if tag in self._tag_index:
                keys_to_evict.update(self._tag_index[tag])

        for key in keys_to_evict:
            self._evict(key)

        logger.info(f"Invalidated {len(keys_to_evict)} entries by tags: {tags}")
        return len(keys_to_evict)

    def invalidate_pattern(self, pattern: str) -> int:
        """
        Invalidate entries matching key pattern.

        Args:
            pattern: Key pattern (supports * wildcard).

        Returns:
            Number of entries invalidated.
        """
        import fnmatch

        keys_to_evict = [
            key for key in self._cache.keys()
            if fnmatch.fnmatch(key, pattern)
        ]

        for key in keys_to_evict:
            self._evict(key)

        logger.info(f"Invalidated {len(keys_to_evict)} entries matching pattern: {pattern}")
        return len(keys_to_evict)

    def _evict(self, key: str) -> None:
        """Remove entry from cache."""
        if key not in self._cache:
            return

        entry = self._cache[key]

        for tag in entry.tags:
            if tag in self._tag_index:
                self._tag_index[tag].discard(key)

        if key in self._revalidate_tasks:
            self._revalidate_tasks[key].cancel()
            del self._revalidate_tasks[key]

        del self._cache[key]

        if key in self._access_order:
            self._access_order.remove(key)

    def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if not self._access_order:
            return

        lru_key = self._access_order.pop(0)
        self._evict(lru_key)
        logger.debug(f"Evicted LRU key: {lru_key}")

    def _update_access_order(self, key: str) -> None:
        """Update LRU access order."""
        if key in self._access_order:
            self._access_order.remove(key)
        self._access_order.append(key)

    async def get_or_fetch(
        self,
        key: str,
        fetch_func: Callable,
        *args: Any,
        ttl: Optional[float] = None,
        tags: Optional[list[str]] = None,
        **kwargs: Any
    ) -> Any:
        """
        Get from cache or fetch if missing.

        Args:
            key: Cache key.
            fetch_func: Async function to fetch data.
            *args: Positional args for fetch_func.
            ttl: Cache TTL.
            tags: Cache tags.
            **kwargs: Keyword args for fetch_func.

        Returns:
            Cached or freshly fetched value.
        """
        cached = self.get(key)

        if cached is not None:
            entry = self._cache.get(key)
            if entry and entry.is_stale and self.config.stale_while_revalidate:
                if key not in self._revalidate_tasks:
                    self._revalidate_tasks[key] = asyncio.create_task(
                        self._revalidate(key, fetch_func, *args, ttl=ttl, tags=tags, **kwargs)
                    )
            return cached

        if asyncio.iscoroutinefunction(fetch_func):
            value = await fetch_func(*args, **kwargs)
        else:
            value = fetch_func(*args, **kwargs)

        self.set(key, value, ttl=ttl, tags=tags)
        return value

    async def _revalidate(
        self,
        key: str,
        fetch_func: Callable,
        *args: Any,
        ttl: Optional[float] = None,
        tags: Optional[list[str]] = None,
        **kwargs: Any
    ) -> None:
        """Revalidate stale cache entry."""
        try:
            if asyncio.iscoroutinefunction(fetch_func):
                new_value = await fetch_func(*args, **kwargs)
            else:
                new_value = fetch_func(*args, **kwargs)

            self.set(key, new_value, ttl=ttl, tags=tags)
            logger.debug(f"Revalidated cache key: {key}")

        except Exception as e:
            logger.error(f"Revalidation failed for {key}: {e}")
        finally:
            if key in self._revalidate_tasks:
                del self._revalidate_tasks[key]

    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()
        self._access_order.clear()
        self._tag_index.clear()
        self._revalidate_tasks.clear()
        self._hits = 0
        self._misses = 0

    def get_stats(self) -> dict:
        """Get cache statistics."""
        total = self._hits + self._misses
        hit_rate = self._hits / total if total > 0 else 0.0

        return {
            "entries": len(self._cache),
            "max_entries": self.config.max_entries,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": f"{hit_rate:.2%}",
            "tags": len(self._tag_index),
            "revalidating": len(self._revalidate_tasks)
        }

    def get_all_tags(self) -> list[str]:
        """Get list of all cache tags."""
        return list(self._tag_index.keys())

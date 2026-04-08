"""Data Fetcher Action Module.

Provides data fetching with caching, conditional requests,
pagination, and error handling.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypeVar
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class FetchConfig:
    """Fetch configuration."""
    cache_ttl: float = 300.0
    max_retries: int = 3
    timeout: float = 30.0
    retry_delay: float = 1.0
    use_cache: bool = True


class DataFetcherAction:
    """Data fetcher with caching and retries.

    Example:
        fetcher = DataFetcherAction()

        data = await fetcher.fetch(
            lambda: api.get_data(),
            cache_key="user_data"
        )

        if fetcher.is_stale("user_data"):
            asyncio.create_task(fetcher.refresh("user_data"))
    """

    def __init__(self, config: Optional[FetchConfig] = None) -> None:
        self.config = config or FetchConfig()
        self._cache: Dict[str, Dict] = {}
        self._fetch_functions: Dict[str, Callable] = {}

    async def fetch(
        self,
        fetch_fn: Callable[[], T],
        cache_key: str,
        force_refresh: bool = False,
    ) -> T:
        """Fetch data with caching.

        Args:
            fetch_fn: Function to fetch data
            cache_key: Cache key for storage
            force_refresh: Skip cache and fetch fresh

        Returns:
            Fetched data
        """
        if not force_refresh and self._is_cached(cache_key):
            cached = self._cache[cache_key]
            if not self._is_stale(cache_key):
                logger.debug(f"Cache hit for {cache_key}")
                return cached["data"]

        self._fetch_functions[cache_key] = fetch_fn

        for attempt in range(self.config.max_retries):
            try:
                if asyncio.iscoroutinefunction(fetch_fn):
                    data = await asyncio.wait_for(
                        fetch_fn(),
                        timeout=self.config.timeout
                    )
                else:
                    data = fetch_fn()

                self._cache[cache_key] = {
                    "data": data,
                    "fetched_at": time.time(),
                    "error": None,
                }

                return data

            except Exception as e:
                logger.warning(
                    f"Fetch attempt {attempt + 1} failed for {cache_key}: {e}"
                )

                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(
                        self.config.retry_delay * (attempt + 1)
                    )

        if cache_key in self._cache:
            return self._cache[cache_key]["data"]

        raise ValueError(f"Failed to fetch {cache_key}")

    def _is_cached(self, cache_key: str) -> bool:
        """Check if key is in cache."""
        return (
            self.config.use_cache
            and cache_key in self._cache
            and self._cache[cache_key].get("data") is not None
        )

    def _is_stale(self, cache_key: str) -> bool:
        """Check if cached data is stale."""
        if cache_key not in self._cache:
            return True

        cached = self._cache[cache_key]
        age = time.time() - cached.get("fetched_at", 0)

        return age > self.config.cache_ttl

    async def refresh(self, cache_key: str) -> Optional[T]:
        """Refresh cached data.

        Args:
            cache_key: Cache key to refresh

        Returns:
            Refreshed data or None
        """
        if cache_key not in self._fetch_functions:
            logger.warning(f"No fetch function for {cache_key}")
            return None

        return await self.fetch(
            self._fetch_functions[cache_key],
            cache_key,
            force_refresh=True,
        )

    async def refresh_all(self) -> None:
        """Refresh all cached items."""
        for cache_key in list(self._fetch_functions.keys()):
            try:
                await self.refresh(cache_key)
            except Exception as e:
                logger.error(f"Failed to refresh {cache_key}: {e}")

    def invalidate(self, cache_key: str) -> None:
        """Invalidate cache entry.

        Args:
            cache_key: Cache key to invalidate
        """
        if cache_key in self._cache:
            del self._cache[cache_key]

    def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate all keys matching pattern.

        Args:
            pattern: Pattern to match

        Returns:
            Number of invalidated keys
        """
        keys_to_remove = [
            k for k in self._cache.keys()
            if pattern in k
        ]

        for key in keys_to_remove:
            del self._cache[key]

        return len(keys_to_remove)

    def get_cached(self, cache_key: str) -> Optional[Any]:
        """Get cached data without fetching.

        Args:
            cache_key: Cache key

        Returns:
            Cached data or None
        """
        if cache_key in self._cache:
            return self._cache[cache_key]["data"]
        return None

    def get_cache_metadata(self, cache_key: str) -> Optional[Dict]:
        """Get cache metadata for key.

        Args:
            cache_key: Cache key

        Returns:
            Metadata dict or None
        """
        if cache_key not in self._cache:
            return None

        cached = self._cache[cache_key]
        return {
            "fetched_at": cached.get("fetched_at"),
            "age_seconds": time.time() - cached.get("fetched_at", 0),
            "is_stale": self._is_stale(cache_key),
            "has_error": cached.get("error") is not None,
        }

    def clear(self) -> None:
        """Clear all cache."""
        self._cache.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        stale_count = sum(1 for k in self._cache if self._is_stale(k))

        return {
            "total_entries": len(self._cache),
            "stale_entries": stale_count,
            "fresh_entries": len(self._cache) - stale_count,
        }

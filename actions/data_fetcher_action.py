"""
Data Fetcher Action Module.

Fetches data from multiple sources with automatic retries,
 response caching, and error handling.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)


@dataclass
class FetchConfig:
    """Configuration for fetch operations."""
    timeout: float = 30.0
    max_retries: int = 3
    retry_delay: float = 1.0
    backoff_factor: float = 2.0
    cache_ttl: float = 300.0


@dataclass
class FetchResult:
    """Result of a fetch operation."""
    success: bool
    data: Any = None
    source: str = ""
    latency_ms: float = 0.0
    attempts: int = 0
    error: Optional[str] = None
    cached: bool = False


class DataFetcherAction:
    """
    Multi-source data fetching with caching and retry logic.

    Fetches data from APIs, databases, or files with automatic
    retries, caching, and error handling.

    Example:
        fetcher = DataFetcherAction()
        fetcher.register_source("users", fetch_users_from_api)
        result = await fetcher.fetch("users", user_id="123")
    """

    def __init__(
        self,
        config: Optional[FetchConfig] = None,
    ) -> None:
        self.config = config or FetchConfig()
        self._sources: dict[str, Callable] = {}
        self._cache: dict[str, tuple[Any, float]] = {}

    def register_source(
        self,
        name: str,
        fetch_func: Callable,
    ) -> "DataFetcherAction":
        """Register a data source."""
        self._sources[name] = fetch_func
        return self

    async def fetch(
        self,
        source_name: str,
        use_cache: bool = True,
        cache_ttl: Optional[float] = None,
        **kwargs: Any,
    ) -> FetchResult:
        """Fetch data from a registered source."""
        start_time = time.monotonic()
        cache_key = f"{source_name}:{str(kwargs)}"

        if use_cache:
            cached = self._get_from_cache(cache_key)
            if cached is not None:
                return FetchResult(
                    success=True,
                    data=cached,
                    source=source_name,
                    latency_ms=(time.monotonic() - start_time) * 1000,
                    cached=True,
                )

        fetch_func = self._sources.get(source_name)
        if not fetch_func:
            return FetchResult(
                success=False,
                source=source_name,
                latency_ms=(time.monotonic() - start_time) * 1000,
                error=f"Source '{source_name}' not registered",
            )

        delay = self.config.retry_delay
        for attempt in range(1, self.config.max_retries + 1):
            try:
                if asyncio.iscoroutinefunction(fetch_func):
                    data = await asyncio.wait_for(
                        fetch_func(**kwargs),
                        timeout=self.config.timeout,
                    )
                else:
                    data = fetch_func(**kwargs)

                if use_cache:
                    self._set_cache(cache_key, data, cache_ttl)

                return FetchResult(
                    success=True,
                    data=data,
                    source=source_name,
                    latency_ms=(time.monotonic() - start_time) * 1000,
                    attempts=attempt,
                )

            except asyncio.TimeoutError:
                error = f"Timeout after {self.config.timeout}s"

            except Exception as e:
                error = str(e)

            if attempt < self.config.max_retries:
                await asyncio.sleep(delay)
                delay *= self.config.backoff_factor

        return FetchResult(
            success=False,
            source=source_name,
            latency_ms=(time.monotonic() - start_time) * 1000,
            attempts=self.config.max_retries,
            error=error,
        )

    async def fetch_batch(
        self,
        requests: list[tuple[str, dict[str, Any]]],
    ) -> list[FetchResult]:
        """Fetch from multiple sources in parallel."""
        tasks = [
            self.fetch(source_name, **kwargs)
            for source_name, kwargs in requests
        ]
        return await asyncio.gather(*tasks)

    def _get_from_cache(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired."""
        if key in self._cache:
            value, timestamp = self._cache[key]
            if time.time() - timestamp < self.config.cache_ttl:
                return value
            del self._cache[key]
        return None

    def _set_cache(
        self,
        key: str,
        value: Any,
        ttl: Optional[float] = None,
    ) -> None:
        """Set value in cache."""
        ttl = ttl or self.config.cache_ttl
        self._cache[key] = (value, time.time())

    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._cache.clear()

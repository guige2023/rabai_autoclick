"""API client action with rate limiting and caching.

This module provides REST API client capabilities with
automatic rate limiting, response caching, and error handling.

Example:
    >>> action = APIClientAction()
    >>> result = action.execute(endpoint="/users", cache=True)
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class RateLimitConfig:
    """Rate limiting configuration."""
    max_requests: int = 60
    per_seconds: float = 60.0
    burst: int = 10


@dataclass
class CacheEntry:
    """Cache entry with TTL."""
    value: Any
    timestamp: float
    ttl: float


class APIClientAction:
    """API client action with rate limiting and caching.

    Provides intelligent API client with automatic rate limiting,
    response caching, and retry logic.

    Example:
        >>> action = APIClientAction(base_url="https://api.example.com")
        >>> result = action.execute(
        ...     endpoint="/data",
        ...     method="GET"
        ... )
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        rate_limit: Optional[RateLimitConfig] = None,
    ) -> None:
        """Initialize API client.

        Args:
            base_url: Base URL for API endpoints.
            api_key: Optional API key for authentication.
            rate_limit: Optional rate limiting configuration.
        """
        self.base_url = base_url or ""
        self.api_key = api_key
        self.rate_limit = rate_limit or RateLimitConfig()
        self._cache: dict[str, CacheEntry] = {}
        self._request_times: list[float] = []

    def execute(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[dict] = None,
        data: Optional[dict] = None,
        headers: Optional[dict] = None,
        cache: bool = False,
        cache_ttl: float = 300.0,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute API request.

        Args:
            endpoint: API endpoint path.
            method: HTTP method.
            params: Query parameters.
            data: Request body data.
            headers: Custom headers.
            cache: Whether to use cached response.
            cache_ttl: Cache time-to-live in seconds.
            **kwargs: Additional parameters.

        Returns:
            API response dictionary.

        Raises:
            ValueError: If endpoint is invalid.
        """
        import requests

        if not endpoint:
            raise ValueError("Endpoint is required")

        url = f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"
        result: dict[str, Any] = {"endpoint": endpoint, "method": method}

        # Check cache
        cache_key = self._get_cache_key(method, url, params, data)
        if cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                result["cached"] = True
                result["data"] = cached
                return result

        # Rate limiting
        self._enforce_rate_limit()

        # Build headers
        req_headers = dict(headers) if headers else {}
        if self.api_key:
            req_headers["Authorization"] = f"Bearer {self.api_key}"
        req_headers["Content-Type"] = "application/json"

        try:
            start_time = time.time()
            response = requests.request(
                method=method.upper(),
                url=url,
                params=params,
                json=data,
                headers=req_headers,
                timeout=kwargs.get("timeout", 30),
            )
            elapsed_ms = (time.time() - start_time) * 1000

            result["elapsed_ms"] = elapsed_ms
            result["status_code"] = response.status_code

            # Parse response
            try:
                result["data"] = response.json()
            except (json.JSONDecodeError, ValueError):
                result["data"] = response.text

            # Cache if successful
            if cache and response.status_code == 200:
                self._set_cache(cache_key, result["data"], cache_ttl)

            result["success"] = response.ok

        except requests.RequestException as e:
            result["success"] = False
            result["error"] = str(e)

        return result

    def _enforce_rate_limit(self) -> None:
        """Enforce rate limiting by sleeping if necessary."""
        now = time.time()
        window = self.rate_limit.per_seconds

        # Remove old requests outside window
        self._request_times = [t for t in self._request_times if now - t < window]

        if len(self._request_times) >= self.rate_limit.max_requests:
            oldest = self._request_times[0]
            sleep_time = window - (now - oldest)
            if sleep_time > 0:
                time.sleep(sleep_time)
                self._request_times = [t for t in self._request_times if time.time() - t < window]

        self._request_times.append(now)

    def _get_cache_key(
        self,
        method: str,
        url: str,
        params: Optional[dict],
        data: Optional[dict],
    ) -> str:
        """Generate cache key for request.

        Args:
            method: HTTP method.
            url: Request URL.
            params: Query parameters.
            data: Request body.

        Returns:
            Cache key string.
        """
        key_parts = [method, url]
        if params:
            key_parts.append(json.dumps(params, sort_keys=True))
        if data:
            key_parts.append(json.dumps(data, sort_keys=True))
        key_str = "|".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()

    def _get_cached(self, key: str) -> Optional[Any]:
        """Get cached value if not expired.

        Args:
            key: Cache key.

        Returns:
            Cached value or None if expired/missing.
        """
        if key not in self._cache:
            return None
        entry = self._cache[key]
        if time.time() - entry.timestamp > entry.ttl:
            del self._cache[key]
            return None
        return entry.value

    def _set_cache(self, key: str, value: Any, ttl: float) -> None:
        """Set cache entry with TTL.

        Args:
            key: Cache key.
            value: Value to cache.
            ttl: Time-to-live in seconds.
        """
        self._cache[key] = CacheEntry(
            value=value,
            timestamp=time.time(),
            ttl=ttl,
        )

    def clear_cache(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()

    def batch_request(
        self,
        requests: list[dict[str, Any]],
        concurrency: int = 5,
    ) -> list[dict[str, Any]]:
        """Execute multiple API requests with concurrency limit.

        Args:
            requests: List of request specifications.
            concurrency: Maximum concurrent requests.

        Returns:
            List of response dictionaries.
        """
        import concurrent.futures

        results: list[dict[str, Any]] = []

        def make_request(req_spec: dict[str, Any]) -> dict[str, Any]:
            return self.execute(**req_spec)

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [executor.submit(make_request, req) for req in requests]
            for future in concurrent.futures.as_completed(futures):
                results.append(future.result())

        return results

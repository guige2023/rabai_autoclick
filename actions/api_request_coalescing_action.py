"""
API Request Coalescing Action Module.

Batches multiple concurrent requests for the same resource
into a single upstream request to reduce load.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class CoalescingKey:
    """Key used for request coalescing."""

    endpoint: str
    params_hash: str

    def __hash__(self) -> int:
        return hash((self.endpoint, self.params_hash))


@dataclass
class PendingRequest:
    """Represents a pending coalesced request."""

    future: asyncio.Future
    created_at: float = field(default_factory=time.time)
    result: Optional[Any] = None


class APIRequestCoalescingAction:
    """
    Coalesces duplicate concurrent requests.

    When multiple identical requests arrive within a short window,
    they are batched into a single upstream request.

    Example:
        coalescer = APIRequestCoalescingAction()
        results = await asyncio.gather(
            coalescer.request("GET", "/api/data"),
            coalescer.request("GET", "/api/data"),
        )
    """

    def __init__(
        self,
        window_ms: float = 50.0,
        max_batch_size: int = 100,
        ttl_seconds: float = 300.0,
    ) -> None:
        """
        Initialize request coalescer.

        Args:
            window_ms: Time window to coalesce requests (ms).
            max_batch_size: Maximum requests to batch together.
            ttl_seconds: TTL for cache entries.
        """
        self.window_ms = window_ms
        self.max_batch_size = max_batch_size
        self.ttl_seconds = ttl_seconds
        self._pending: dict[CoalescingKey, PendingRequest] = {}
        self._cache: dict[CoalescingKey, tuple[Any, float]] = {}
        self._lock = asyncio.Lock()
        self._stats = {
            "coalesced": 0,
            "direct": 0,
            "cache_hits": 0,
        }

    async def request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        executor: Optional[Callable] = None,
    ) -> Any:
        """
        Make a coalesced request.

        Args:
            method: HTTP method.
            endpoint: API endpoint.
            params: Query/body parameters.
            executor: Function to execute actual request.

        Returns:
            Response data.
        """
        params_str = self._hash_params(params)
        key = CoalescingKey(endpoint=endpoint, params_hash=params_str)

        cached = self._get_cached(key)
        if cached is not None:
            self._stats["cache_hits"] += 1
            return cached

        async with self._lock:
            if key in self._pending:
                self._stats["coalesced"] += 1
                future = self._pending[key].future
            else:
                self._stats["direct"] += 1
                future = asyncio.Future()
                self._pending[key] = PendingRequest(future=future)

        if not future.done():
            if executor:
                try:
                    result = await asyncio.wait_for(
                        executor(method, endpoint, params),
                        timeout=30.0,
                    )
                    future.set_result(result)
                except Exception as e:
                    future.set_exception(e)
                finally:
                    await self._complete_request(key, future)
        else:
            await self._complete_request(key, future)

        return await future

    def _hash_params(self, params: Optional[dict[str, Any]]) -> str:
        """Create a hash string from parameters."""
        if not params:
            return ""
        import json
        return json.dumps(params, sort_keys=True, default=str)

    def _get_cached(self, key: CoalescingKey) -> Optional[Any]:
        """Get cached result if still valid."""
        if key in self._cache:
            result, timestamp = self._cache[key]
            if time.time() - timestamp < self.ttl_seconds:
                return result
            del self._cache[key]
        return None

    async def _complete_request(self, key: CoalescingKey, future: asyncio.Future) -> None:
        """Complete pending request and cleanup."""
        await asyncio.sleep(self.window_ms / 1000.0)

        async with self._lock:
            if key in self._pending and self._pending[key].future is future:
                del self._pending[key]

        if not future.cancelled() and not future.exception():
            self._cache[key] = (future.result(), time.time())

    async def invalidate(self, endpoint: str) -> int:
        """
        Invalidate all cached entries for an endpoint.

        Args:
            endpoint: API endpoint to invalidate.

        Returns:
            Number of entries invalidated.
        """
        async with self._lock:
            to_remove = [k for k in self._cache if k.endpoint == endpoint]
            for key in to_remove:
                del self._cache[key]
        return len(to_remove)

    def get_stats(self) -> dict[str, Any]:
        """Get coalescing statistics."""
        return {
            **self._stats,
            "pending_requests": len(self._pending),
            "cached_entries": len(self._cache),
        }

    def clear_cache(self) -> None:
        """Clear all cached entries."""
        self._cache.clear()
        logger.info("Coalescing cache cleared")

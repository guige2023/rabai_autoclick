"""API Request Deduplication Action module.

Deduplicates API requests using content hashing and
request collapsing to reduce redundant API calls.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


@dataclass
class DedupeKey:
    """Key for request deduplication."""

    content_hash: str
    created_at: float = field(default_factory=time.monotonic)
    hit_count: int = 0


class RequestDeduplicator:
    """Deduplicates identical API requests.

    When the same request is made multiple times within
    a time window, only one actual call is made and
    subsequent requests wait for and return the same result.
    """

    def __init__(
        self,
        window_seconds: float = 60.0,
        max_size: int = 10000,
    ):
        self.window_seconds = window_seconds
        self.max_size = max_size

        self._pending: dict[str, asyncio.Future] = {}
        self._results: dict[str, tuple[Any, float]] = {}
        self._keys: dict[str, DedupeKey] = {}
        self._lock = asyncio.Lock()

    def _make_key(self, request: Any) -> str:
        """Create deduplication key from request.

        Args:
            request: Request data

        Returns:
            Hash string
        """
        import json

        request_str = json.dumps(request, sort_keys=True, default=str)
        return hashlib.sha256(request_str.encode()).hexdigest()

    async def execute(
        self,
        func: Callable[..., T],
        request: Any,
        ttl: float | None = None,
    ) -> T:
        """Execute function with request deduplication.

        Args:
            func: Function to call
            request: Request data for deduplication
            ttl: Time to cache result (default: window_seconds)

        Returns:
            Function result
        """
        key = self._make_key(request)
        ttl = ttl or self.window_seconds

        async with self._lock:
            now = time.monotonic()

            if key in self._results:
                result, cached_at = self._results[key]
                if now - cached_at < ttl:
                    dedupe_key = self._keys.get(key)
                    if dedupe_key:
                        dedupe_key.hit_count += 1
                    return result

            if key in self._pending:
                future = self._pending[key]
            else:
                future = asyncio.get_running_loop().create_future()
                self._pending[key] = future

        try:
            result = await func(request)

            async with self._lock:
                self._results[key] = (result, now)
                self._keys[key] = DedupeKey(
                    content_hash=key,
                    hit_count=1,
                )

                if future not in self._pending.values():
                    pass
                else:
                    for k, f in list(self._pending.items()):
                        if f is future:
                            del self._pending[k]
                            break

                future.set_result(result)

                self._cleanup_expired(now)

                if len(self._results) > self.max_size:
                    self._evict_oldest()

            return result

        except Exception as e:
            async with self._lock:
                if key in self._pending:
                    for k, f in list(self._pending.items()):
                        if f is future:
                            del self._pending[k]
                            break
                    future.set_exception(e)
            raise

    def _cleanup_expired(self, now: float) -> None:
        """Remove expired entries."""
        expired_keys = [
            key for key, (_, cached_at) in self._results.items()
            if now - cached_at >= self.window_seconds
        ]
        for key in expired_keys:
            del self._results[key]
            self._keys.pop(key, None)

    def _evict_oldest(self) -> None:
        """Evict oldest entries when max size reached."""
        if not self._results:
            return

        sorted_items = sorted(
            self._results.items(),
            key=lambda x: x[1][1],
        )

        for key, _ in sorted_items[: len(self._results) // 2]:
            del self._results[key]
            self._keys.pop(key, None)

    def clear(self) -> None:
        """Clear all cached results."""
        self._results.clear()
        self._keys.clear()
        self._pending.clear()

    def get_stats(self) -> dict[str, Any]:
        """Get deduplication statistics."""
        total_hits = sum(k.hit_count for k in self._keys.values())
        return {
            "cached_requests": len(self._results),
            "pending_requests": len(self._pending),
            "total_deduplications": total_hits,
            "total_savings": total_hits - len(self._results) if total_hits > 0 else 0,
        }


class RequestCollapser:
    """Collapses multiple similar requests into batched calls.

    Groups requests by a key function and executes them
    together when a batch is full or timeout expires.
    """

    def __init__(
        self,
        batch_size: int = 50,
        flush_interval: float = 0.1,
        key_func: Callable[[Any], str] | None = None,
    ):
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.key_func = key_func or (lambda x: "default")

        self._batches: dict[str, list[tuple[asyncio.Future, Any]]] = defaultdict(list)
        self._scheduled_flushes: dict[str, asyncio.TimerHandle] = {}
        self._lock = asyncio.Lock()

    async def execute(
        self,
        func: Callable[[list[Any]], list[Any]],
        request: Any,
    ) -> Any:
        """Execute request through batch.

        Args:
            func: Batch function that takes list of requests
            request: Individual request

        Returns:
            Result for this request
        """
        batch_key = self.key_func(request)
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        async with self._lock:
            self._batches[batch_key].append((future, request))

            if len(self._batches[batch_key]) >= self.batch_size:
                await self._flush_batch(batch_key, func)
            else:
                self._schedule_flush(batch_key)

        return await future

    def _schedule_flush(self, batch_key: str) -> None:
        """Schedule a batch flush."""
        if batch_key in self._scheduled_flushes:
            return

        def flush() -> None:
            asyncio.create_task(self._flush_batch(batch_key, self._current_func))

        loop = asyncio.get_running_loop()
        handle = loop.call_later(self.flush_interval, flush)
        self._scheduled_flushes[batch_key] = handle

    async def _flush_batch(
        self,
        batch_key: str,
        func: Callable[[list[Any]], list[Any]],
    ) -> None:
        """Flush a batch of requests."""
        self._scheduled_flushes.pop(batch_key, None)

        async with self._lock:
            if batch_key not in self._batches:
                return
            batch = self._batches.pop(batch_key)

        if not batch:
            return

        futures = [item[0] for item in batch]
        requests = [item[1] for item in batch]

        try:
            results = await func(requests)
            for future, result in zip(futures, results):
                if not future.done():
                    future.set_result(result)
        except Exception as e:
            for future in futures:
                if not future.done():
                    future.set_exception(e)

    def _current_func(self, requests: list[Any]) -> list[Any]:
        """Placeholder for current batch function."""
        return [None] * len(requests)


class IdempotencyKeyManager:
    """Manages idempotency keys for API requests.

    Ensures retry-safe requests by associating each
    request with a unique idempotency key.
    """

    def __init__(self, ttl_seconds: float = 86400):
        self.ttl_seconds = ttl_seconds
        self._store: dict[str, tuple[Any, float]] = {}
        self._lock = asyncio.Lock()

    def generate_key(
        self,
        endpoint: str,
        method: str,
        params: dict[str, Any],
    ) -> str:
        """Generate idempotency key.

        Args:
            endpoint: API endpoint
            method: HTTP method
            params: Request parameters

        Returns:
            Idempotency key string
        """
        import json

        content = {
            "endpoint": endpoint,
            "method": method,
            "params": params,
        }
        key_str = json.dumps(content, sort_keys=True)
        return hashlib.sha256(key_str.encode()).hexdigest()[:32]

    async def get_cached(
        self,
        key: str,
    ) -> tuple[bool, Any | None]:
        """Check if idempotency key has cached result.

        Returns:
            Tuple of (has_cached, cached_result)
        """
        async with self._lock:
            if key in self._store:
                result, cached_at = self._store[key]
                if time.monotonic() - cached_at < self.ttl_seconds:
                    return True, result
                else:
                    del self._store[key]
            return False, None

    async def store_result(
        self,
        key: str,
        result: Any,
    ) -> None:
        """Store result for idempotency key."""
        async with self._lock:
            self._store[key] = (result, time.monotonic())

    async def clear_expired(self) -> int:
        """Clear expired entries.

        Returns:
            Number of entries cleared
        """
        now = time.monotonic()
        expired = [
            k for k, (_, cached_at) in self._store.items()
            if now - cached_at >= self.ttl_seconds
        ]

        for key in expired:
            del self._store[key]

        return len(expired)

"""API Request Coalescing Action module.

Batches multiple rapid API requests into single calls using
coalescing and request collapsing patterns.
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
class CoalescedRequest:
    """A coalesced API request."""

    key: str
    created_at: float
    futures: list[asyncio.Future] = field(default_factory=list)
    args: tuple = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)


class RequestCoalescer:
    """Coalesces multiple identical requests into one.

    When multiple requests for the same resource arrive within
    a time window, only one actual API call is made and all
    waiters receive the same result.
    """

    def __init__(
        self,
        window_seconds: float = 0.1,
        max_batch_size: int = 100,
    ):
        self.window_seconds = window_seconds
        self.max_batch_size = max_batch_size
        self._pending: dict[str, CoalescedRequest] = {}
        self._scheduled_flushes: dict[str, asyncio.TimerHandle] = {}

    def _make_key(self, func_name: str, args: tuple, kwargs: dict) -> str:
        """Create a cache key for the request."""
        key_parts = [func_name]
        key_parts.append(str(args))
        key_parts.append(str(sorted(kwargs.items())))
        key_str = "|".join(key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()[:16]

    async def execute(
        self,
        func: Callable[..., Any],
        args: tuple = (),
        kwargs: Optional[dict] = None,
    ) -> Any:
        """Execute function with request coalescing."""
        if kwargs is None:
            kwargs = {}

        key = self._make_key(func.__name__, args, kwargs)
        loop = asyncio.get_running_loop()

        existing = self._pending.get(key)
        if existing is not None:
            future = loop.create_future()
            existing.futures.append(future)
            try:
                return await future
            except asyncio.CancelledError:
                future.cancel()
                raise

        future = loop.create_future()
        coalesced = CoalescedRequest(
            key=key,
            created_at=time.monotonic(),
            futures=[future],
            args=args,
            kwargs=kwargs,
        )
        self._pending[key] = coalesced

        self._schedule_flush(key)

        result = await func(*args, **kwargs)

        for f in coalesced.futures:
            if not f.done():
                f.set_result(result)

        del self._pending[key]
        self._scheduled_flushes.pop(key, None)

        return result

    def _schedule_flush(self, key: str) -> None:
        """Schedule a flush of pending requests."""
        loop = asyncio.get_running_loop()

        if key in self._scheduled_flushes:
            return

        def flush() -> None:
            asyncio.create_task(self._flush_key(key))

        handle = loop.call_later(self.window_seconds, flush)
        self._scheduled_flushes[key] = handle

    async def _flush_key(self, key: str) -> None:
        """Flush pending requests for a key."""
        self._scheduled_flushes.pop(key, None)

        coalesced = self._pending.pop(key, None)
        if coalesced is None:
            return


class BatchRequestCoalescer:
    """Batches multiple requests into a single call.

    Accumulates requests and dispatches them together when
    the batch is full or the timeout expires.
    """

    def __init__(
        self,
        max_batch_size: int = 50,
        flush_interval: float = 0.05,
        batch_func: Callable[[list[tuple]], list[Any]] | None = None,
    ):
        self.max_batch_size = max_batch_size
        self.flush_interval = flush_interval
        self._batch_func = batch_func
        self._pending: list[tuple[asyncio.Future, tuple, dict]] = []
        self._pending_lock = asyncio.Lock()
        self._flush_handle: Optional[asyncio.TimerHandle] = None

    async def add(
        self,
        func: Callable[..., Any],
        args: tuple = (),
        kwargs: Optional[dict] = None,
    ) -> Any:
        """Add a request to the batch."""
        if kwargs is None:
            kwargs = {}
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        async with self._pending_lock:
            self._pending.append((future, (func, args, kwargs), {}))
            if len(self._pending) >= self.max_batch_size:
                await self._flush_batch()

        return await future

    async def _flush_batch(self) -> None:
        """Flush pending requests as a batch."""
        async with self._pending_lock:
            if not self._pending:
                return
            pending = self._pending
            self._pending = []

        results = []
        if self._batch_func:
            batch_args = [(p[1][1], p[1][2]) for p in pending]
            results = await self._batch_func(batch_args)
        else:
            for _, (func, args, kwargs), _ in pending:
                try:
                    result = await func(*args, **kwargs)
                    results.append(result)
                except Exception as e:
                    results.append(e)

        for i, (future, _, _) in enumerate(pending):
            if i < len(results):
                result = results[i]
                if isinstance(result, Exception):
                    future.set_exception(result)
                else:
                    future.set_result(result)


@dataclass
class AdaptiveBatcherConfig:
    """Configuration for adaptive batching."""

    min_batch_size: int = 5
    max_batch_size: int = 50
    target_latency_ms: float = 50.0
    max_latency_ms: float = 200.0


class AdaptiveRequestBatcher:
    """Adaptively adjusts batch size based on latency.

    Monitors request latency and dynamically adjusts
    batch sizes to meet latency targets.
    """

    def __init__(self, config: Optional[AdaptiveBatcherConfig] = None):
        self.config = config or AdaptiveBatcherConfig()
        self._current_batch_size = self.config.min_batch_size
        self._latency_history: list[float] = []
        self._history_window = 100

    def record_latency(self, latency_ms: float) -> None:
        """Record a batch latency measurement."""
        self._latency_history.append(latency_ms)
        if len(self._latency_history) > self._history_window:
            self._latency_history.pop(0)
        self._adjust_batch_size()

    def _adjust_batch_size(self) -> None:
        """Adjust batch size based on latency."""
        if len(self._latency_history) < 10:
            return

        recent = self._latency_history[-10:]
        avg_latency = sum(recent) / len(recent)

        if avg_latency > self.config.max_latency_ms:
            self._current_batch_size = max(
                self.config.min_batch_size,
                self._current_batch_size // 2,
            )
        elif avg_latency < self.config.target_latency_ms:
            self._current_batch_size = min(
                self.config.max_batch_size,
                int(self._current_batch_size * 1.5),
            )

    @property
    def batch_size(self) -> int:
        """Get current batch size."""
        return self._current_batch_size

    def get_stats(self) -> dict[str, Any]:
        """Get batcher statistics."""
        return {
            "current_batch_size": self._current_batch_size,
            "avg_recent_latency": (
                sum(self._latency_history[-10:]) / len(self._latency_history[-10:])
                if self._latency_history
                else 0
            ),
            "history_size": len(self._latency_history),
        }

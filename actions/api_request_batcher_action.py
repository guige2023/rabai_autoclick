"""
API Request Batcher Action Module.

Batches multiple API requests into single operations with
automatic batching, queue management, and response routing.
"""

import asyncio
import hashlib
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional, TypeVar

T = TypeVar("T")


@dataclass
class BatchRequest:
    """A single request within a batch."""

    request_id: str
    key: str
    payload: Any
    future: asyncio.Future
    created_at: float
    priority: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchResponse:
    """Response for a batched request."""

    request_id: str
    success: bool
    data: Any = None
    error: Optional[str] = None
    processing_time: float = 0.0


@dataclass
class BatchStats:
    """Statistics for batching operations."""

    total_batches: int = 0
    total_requests: int = 0
    avg_batch_size: float = 0.0
    total_wait_time: float = 0.0
    deduplicated_requests: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Export stats as dictionary."""
        return {
            "total_batches": self.total_batches,
            "total_requests": self.total_requests,
            "avg_batch_size": round(self.avg_batch_size, 2),
            "total_wait_time": round(self.total_wait_time, 2),
            "deduplicated_requests": self.deduplicated_requests,
        }


class APIBatcher:
    """
    Batches multiple API requests into optimized single operations.

    Supports request deduplication, priority queuing, configurable
    batch size and timing thresholds.
    """

    def __init__(
        self,
        batch_size: int = 100,
        batch_timeout: float = 0.1,
        max_queue_size: int = 10000,
        enable_dedup: bool = True,
        dedup_window: float = 1.0,
    ) -> None:
        """
        Initialize the API batcher.

        Args:
            batch_size: Maximum requests per batch.
            batch_timeout: Max seconds to wait before flushing batch.
            max_queue_size: Maximum queued requests.
            enable_dedup: Enable request deduplication.
            dedup_window: Time window for deduplication.
        """
        self._batch_size = batch_size
        self._batch_timeout = batch_timeout
        self._max_queue_size = max_queue_size
        self._enable_dedup = enable_dedup
        self._dedup_window = dedup_window
        self._queue: list[BatchRequest] = []
        self._pending: dict[str, BatchRequest] = {}
        self._stats = BatchStats()
        self._lock = asyncio.Lock()
        self._batch_task: Optional[asyncio.Task] = None
        self._running = False
        self._last_flush = time.time()
        self._dedup_cache: dict[str, float] = {}

    def _make_request_key(self, key: str, payload: Any) -> str:
        """Generate deduplication key from request."""
        data = f"{key}:{str(payload)}"
        return hashlib.sha256(data.encode()).hexdigest()[:24]

    def _cleanup_dedup_cache(self) -> None:
        """Remove stale entries from deduplication cache."""
        now = time.time()
        expired = [k for k, v in self._dedup_cache.items() if now - v > self._dedup_window]
        for k in expired:
            del self._dedup_cache[k]

    async def enqueue(
        self,
        key: str,
        payload: Any,
        priority: int = 0,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Any:
        """
        Enqueue a request for batching.

        Args:
            key: Batch operation key (e.g., "user_lookup").
            payload: Request payload.
            priority: Higher priority requests processed first.
            metadata: Optional request metadata.

        Returns:
            Response data when request completes.
        """
        async with self._lock:
            self._cleanup_dedup_cache()

            dedup_key = self._make_request_key(key, payload)
            if self._enable_dedup and dedup_key in self._dedup_cache:
                existing_id = self._dedup_cache[dedup_key]
                if existing_id in self._pending:
                    self._stats.deduplicated_requests += 1
                    return await self._pending[existing_id].future

            request_id = hashlib.sha256(f"{time.time()}{dedup_key}".encode()).hexdigest()[:16]
            loop = asyncio.get_running_loop()
            future = loop.create_future()

            request = BatchRequest(
                request_id=request_id,
                key=key,
                payload=payload,
                future=future,
                created_at=time.time(),
                priority=priority,
                metadata=metadata or {},
            )

            self._queue.append(request)
            self._pending[request_id] = request
            self._dedup_cache[dedup_key] = request_id

            if len(self._queue) >= self._batch_size:
                await self._flush()

            return await future

    async def _flush(self) -> None:
        """Flush all queued requests as a batch."""
        if not self._queue:
            return

        wait_time = time.time() - self._last_flush
        self._stats.total_wait_time += wait_time

        batch = sorted(self._queue, key=lambda r: r.priority, reverse=True)
        self._queue.clear()
        self._last_flush = time.time()
        self._stats.total_batches += 1
        self._stats.total_requests += len(batch)

        if self._stats.total_batches > 0:
            self._stats.avg_batch_size = self._stats.total_requests / self._stats.total_batches

        grouped: dict[str, list[BatchRequest]] = defaultdict(list)
        for req in batch:
            grouped[req.key].append(req)

        for key, requests in grouped.items():
            payloads = [r.payload for r in requests]
            try:
                results = await self._execute_batch(key, payloads)
                for req, result in zip(requests, results):
                    if not req.future.done():
                        req.future.set_result(result)
            except Exception as e:
                for req in requests:
                    if not req.future.done():
                        req.future.set_exception(e)

        for req in batch:
            if req.request_id in self._pending:
                del self._pending[req.request_id]

    async def _execute_batch(
        self,
        key: str,
        payloads: list[Any],
    ) -> list[Any]:
        """
        Execute a batch of requests. Override this to implement actual batching.

        Args:
            key: Batch operation key.
            payloads: List of payloads for this batch.

        Returns:
            List of results in same order as payloads.
        """
        return payloads

    async def _batch_worker(self) -> None:
        """Background worker that flushes batches on timeout."""
        while self._running:
            try:
                await asyncio.sleep(self._batch_timeout)
                async with self._lock:
                    if self._queue and time.time() - self._last_flush >= self._batch_timeout:
                        await self._flush()
            except asyncio.CancelledError:
                break
            except Exception:
                pass

    async def start(self) -> None:
        """Start the background batch worker."""
        if self._running:
            return
        self._running = True
        self._batch_task = asyncio.create_task(self._batch_worker())

    async def stop(self) -> None:
        """Stop the worker and flush remaining requests."""
        self._running = False
        if self._batch_task:
            self._batch_task.cancel()
            try:
                await self._batch_task
            except asyncio.CancelledError:
                pass
        async with self._lock:
            await self._flush()

    def stats(self) -> BatchStats:
        """Return current batching statistics."""
        return self._stats

    def queue_size(self) -> int:
        """Return current queue size."""
        return len(self._queue)

    def pending_count(self) -> int:
        """Return number of pending requests awaiting response."""
        return len(self._pending)


class SmartBatcher(APIBatcher):
    """
    Enhanced batcher with adaptive batching based on request patterns.

    Learns optimal batch sizes and timing based on throughput.
    """

    def __init__(
        self,
        initial_batch_size: int = 50,
        initial_timeout: float = 0.05,
        min_batch_size: int = 10,
        max_batch_size: int = 500,
        target_latency: float = 0.1,
    ) -> None:
        """
        Initialize the smart batcher.

        Args:
            initial_batch_size: Starting batch size.
            initial_timeout: Starting timeout.
            min_batch_size: Minimum batch size.
            max_batch_size: Maximum batch size.
            target_latency: Target end-to-end latency.
        """
        super().__init__(
            batch_size=initial_batch_size,
            batch_timeout=initial_timeout,
        )
        self._min_batch_size = min_batch_size
        self._max_batch_size = max_batch_size
        self._target_latency = target_latency
        self._observed_latencies: list[float] = []

    async def _flush(self) -> None:
        """Flush with latency tracking for adaptation."""
        start = time.time()
        await super()._flush()
        latency = time.time() - start
        self._observed_latencies.append(latency)
        if len(self._observed_latencies) > 100:
            self._observed_latencies = self._observed_latencies[-100:]
        self._adapt_parameters()

    def _adapt_parameters(self) -> None:
        """Adapt batch size and timeout based on observed latency."""
        if not self._observed_latencies:
            return
        avg_latency = sum(self._observed_latencies) / len(self._observed_latencies)

        if avg_latency > self._target_latency * 1.5:
            self._batch_size = max(self._min_batch_size, self._batch_size // 2)
            self._batch_timeout = max(0.001, self._batch_timeout * 0.5)
        elif avg_latency < self._target_latency * 0.5:
            self._batch_size = min(self._max_batch_size, int(self._batch_size * 1.5))
            self._batch_timeout = min(1.0, self._batch_timeout * 1.5)


def create_batcher(
    batch_size: int = 100,
    batch_timeout: float = 0.1,
) -> APIBatcher:
    """
    Factory function to create an API batcher.

    Args:
        batch_size: Maximum requests per batch.
        batch_timeout: Max seconds to wait before flushing.

    Returns:
        Configured APIBatcher instance.
    """
    return APIBatcher(batch_size=batch_size, batch_timeout=batch_timeout)

"""API request batcher action for combining multiple requests.

Batches multiple API requests together to reduce network
overhead and improve throughput with configurable batching.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class BatchRequest:
    """A single request in a batch."""
    id: str
    params: dict[str, Any]
    callback: Optional[Callable[[Any], None]] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class BatchResponse:
    """Response for a batched request."""
    request_id: str
    success: bool
    data: Any
    error: Optional[str] = None
    latency_ms: float = 0.0


@dataclass
class BatchStats:
    """Statistics for batch operations."""
    requests_received: int = 0
    requests_batched: int = 0
    requests_failed: int = 0
    avg_batch_size: float = 0.0
    total_latency_ms: float = 0.0


class APIRequestBatcherAction:
    """Batch multiple API requests for efficient processing.

    Args:
        batch_size: Maximum requests per batch.
        batch_timeout_ms: Max wait time before flushing batch.

    Example:
        >>> batcher = APIRequestBatcherAction(batch_size=10)
        >>> batcher.add_request("req1", {"url": "/api/data"})
        >>> await batcher.flush()
    """

    def __init__(
        self,
        batch_size: int = 10,
        batch_timeout_ms: float = 100.0,
        executor_fn: Optional[Callable[[list[BatchRequest]], Any]] = None,
    ) -> None:
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms
        self.executor_fn = executor_fn or self._default_executor
        self._pending: list[BatchRequest] = []
        self._lock = asyncio.Lock()
        self._stats = BatchStats()
        self._running = False

    async def add_request(
        self,
        request_id: str,
        params: dict[str, Any],
        callback: Optional[Callable[[Any], None]] = None,
    ) -> None:
        """Add a request to the batch queue.

        Args:
            request_id: Unique identifier for this request.
            params: Request parameters.
            callback: Optional callback for results.
        """
        request = BatchRequest(request_id, params, callback)
        self._stats.requests_received += 1

        async with self._lock:
            self._pending.append(request)

            if len(self._pending) >= self.batch_size:
                await self._flush_batch()

    async def _flush_batch(self) -> None:
        """Flush current pending requests as a batch."""
        if not self._pending:
            return

        batch = self._pending.copy()
        self._pending.clear()

        logger.debug(f"Flushing batch of {len(batch)} requests")
        self._stats.requests_batched += len(batch)

        try:
            results = await self._execute_batch(batch)
            for response in results:
                if response.callback:
                    response.callback(response.data)
        except Exception as e:
            logger.error(f"Batch execution failed: {e}")
            for request in batch:
                self._stats.requests_failed += 1
                if request.callback:
                    request.callback(None)

    async def flush(self) -> None:
        """Manually flush all pending requests."""
        async with self._lock:
            await self._flush_batch()

    async def _execute_batch(
        self,
        batch: list[BatchRequest],
    ) -> list[BatchResponse]:
        """Execute a batch of requests.

        Args:
            batch: List of requests to execute.

        Returns:
            List of responses.
        """
        start_time = time.time()

        try:
            raw_results = await asyncio.wait_for(
                self.executor_fn(batch),
                timeout=30.0,
            )

            results = []
            for i, request in enumerate(batch):
                data = None
                if isinstance(raw_results, list) and i < len(raw_results):
                    data = raw_results[i]
                elif isinstance(raw_results, dict):
                    data = raw_results.get(request.id)

                results.append(BatchResponse(
                    request_id=request.id,
                    success=True,
                    data=data,
                    latency_ms=(time.time() - start_time) * 1000,
                ))

            return results

        except asyncio.TimeoutError:
            logger.error(f"Batch execution timed out for {len(batch)} requests")
            return [
                BatchResponse(
                    request_id=r.id,
                    success=False,
                    data=None,
                    error="Batch execution timeout",
                    latency_ms=(time.time() - start_time) * 1000,
                )
                for r in batch
            ]
        except Exception as e:
            logger.error(f"Batch execution error: {e}")
            return [
                BatchResponse(
                    request_id=r.id,
                    success=False,
                    data=None,
                    error=str(e),
                    latency_ms=(time.time() - start_time) * 1000,
                )
                for r in batch
            ]

    async def _default_executor(
        self,
        batch: list[BatchRequest],
    ) -> list[Any]:
        """Default batch executor.

        Args:
            batch: Requests to execute.

        Returns:
            List of results.
        """
        await asyncio.sleep(0.01)
        return [None] * len(batch)

    async def start_auto_flush(self) -> None:
        """Start automatic batch flushing loop."""
        self._running = True

        while self._running:
            await asyncio.sleep(self.batch_timeout_ms / 1000.0)
            async with self._lock:
                if self._pending:
                    await self._flush_batch()

    def stop_auto_flush(self) -> None:
        """Stop automatic batch flushing."""
        self._running = False

    def get_stats(self) -> BatchStats:
        """Get batch statistics.

        Returns:
            Current statistics.
        """
        total = self._stats.requests_batched
        if total > 0:
            self._stats.avg_batch_size = self._stats.requests_batched / total
        return self._stats

    def pending_count(self) -> int:
        """Get number of pending requests.

        Returns:
            Count of pending requests.
        """
        return len(self._pending)

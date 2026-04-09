"""Request Batcher Action Module.

Batch multiple requests into single operations for efficiency.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class BatchedRequest(Generic[T]):
    """Single request in a batch."""
    request_id: str
    data: T
    future: asyncio.Future
    created_at: float


@dataclass
class BatchConfig:
    """Batch configuration."""
    max_batch_size: int = 100
    max_wait_time: float = 0.05
    min_batch_size: int = 1


class RequestBatcher(Generic[T, R]):
    """Batch multiple requests for efficient processing."""

    def __init__(self, processor: Callable[[list[T]], list[R]], config: BatchConfig | None = None) -> None:
        self.processor = processor
        self.config = config or BatchConfig()
        self._pending: deque[BatchedRequest[T]] = deque()
        self._lock = asyncio.Lock()
        self._running = False
        self._batch_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the batcher."""
        self._running = True
        self._batch_task = asyncio.create_task(self._process_loop())

    async def stop(self) -> None:
        """Stop the batcher."""
        self._running = False
        if self._batch_task:
            self._batch_task.cancel()
            try:
                await self._batch_task
            except asyncio.CancelledError:
                pass

    async def submit(self, data: T, request_id: str | None = None) -> R:
        """Submit a request to the batcher."""
        future = asyncio.Future()
        request = BatchedRequest(
            request_id=request_id or str(id(data)),
            data=data,
            future=future,
            created_at=time.monotonic()
        )
        async with self._lock:
            self._pending.append(request)
        return await future

    async def _process_loop(self) -> None:
        """Main batch processing loop."""
        while self._running:
            await asyncio.sleep(self.config.max_wait_time)
            batch = await self._get_batch()
            if batch:
                asyncio.create_task(self._process_batch(batch))

    async def _get_batch(self) -> list[BatchedRequest[T]]:
        """Get next batch of requests."""
        async with self._lock:
            if not self._pending:
                return []
            now = time.monotonic()
            batch = []
            while self._pending and len(batch) < self.config.max_batch_size:
                oldest = self._pending[0]
                age = now - oldest.created_at
                if batch and age > self.config.max_wait_time:
                    break
                batch.append(self._pending.popleft())
            return batch

    async def _process_batch(self, batch: list[BatchedRequest[T]]) -> None:
        """Process a batch of requests."""
        try:
            data_list = [r.data for r in batch]
            results = await asyncio.to_thread(self.processor, data_list)
            for request, result in zip(batch, results):
                if not request.future.done():
                    request.future.set_result(result)
        except Exception as e:
            for request in batch:
                if not request.future.done():
                    request.future.set_exception(e)


class AdaptiveBatcher(Generic[T, R]):
    """Adaptive batcher that adjusts batch size based on throughput."""

    def __init__(
        self,
        processor: Callable[[list[T]], list[R]],
        initial_batch_size: int = 10,
        max_batch_size: int = 100
    ) -> None:
        self.processor = processor
        self.base_batcher = RequestBatcher(processor)
        self.current_batch_size = initial_batch_size
        self.max_batch_size = max_batch_size
        self._throughput_history: deque[float] = deque(maxlen=100)

    async def start(self) -> None:
        """Start the adaptive batcher."""
        await self.base_batcher.start()

    async def stop(self) -> None:
        """Stop the adaptive batcher."""
        await self.base_batcher.stop()

    async def submit(self, data: T, request_id: str | None = None) -> R:
        """Submit request with adaptive batching."""
        start = time.monotonic()
        result = await self.base_batcher.submit(data, request_id)
        elapsed = time.monotonic() - start
        throughput = 1 / elapsed if elapsed > 0 else 0
        self._throughput_history.append(throughput)
        self._adjust_batch_size()
        return result

    def _adjust_batch_size(self) -> None:
        """Adjust batch size based on recent throughput."""
        if len(self._throughput_history) < 10:
            return
        recent = list(self._throughput_history)[-10:]
        avg_throughput = sum(recent) / len(recent)
        if avg_throughput > 100:
            self.current_batch_size = min(self.current_batch_size + 5, self.max_batch_size)
        elif avg_throughput < 10:
            self.current_batch_size = max(self.current_batch_size - 5, 1)

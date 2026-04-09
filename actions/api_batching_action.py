"""
API Batching Action Module

Provides intelligent request batching for API calls with dynamic batch sizing,
concurrency control, and automatic flushing strategies.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class BatchStrategy(Enum):
    """Batch formation strategies."""

    FIXED_SIZE = "fixed_size"
    TIME_BASED = "time_based"
    ADAPTIVE = "adaptive"
    HYBRID = "hybrid"


@dataclass
class BatchItem:
    """An item in a batch."""

    item_id: str
    request: Any
    callback: Optional[Callable[[Any], None]] = None
    priority: int = 0
    added_at: float = field(default_factory=time.time)


@dataclass
class Batch:
    """A batch of requests."""

    batch_id: str
    items: List[BatchItem]
    created_at: float
    flush_reason: str


@dataclass
class BatchResult:
    """Result of batch processing."""

    batch_id: str
    total_items: int
    successful: int
    failed: int
    results: List[Any]
    duration_ms: float = 0.0


@dataclass
class BatchConfig:
    """Configuration for batching."""

    max_batch_size: int = 100
    min_batch_size: int = 1
    flush_interval_seconds: float = 0.1
    max_wait_seconds: float = 5.0
    strategy: BatchStrategy = BatchStrategy.HYBRID
    enable_deduplication: bool = True
    timeout_seconds: float = 30.0


class BatchManager:
    """Manages batch formation and flushing."""

    def __init__(self, config: BatchConfig):
        self.config = config
        self._items: List[BatchItem] = []
        self._last_flush_time = time.time()

    def add_item(self, item: BatchItem) -> None:
        """Add an item to the batch."""
        self._items.append(item)

    def should_flush(self) -> tuple[bool, str]:
        """Determine if batch should be flushed."""
        now = time.time()
        elapsed = now - self._last_flush_time

        if len(self._items) >= self.config.max_batch_size:
            return True, "max_size_reached"

        if self.config.strategy == BatchStrategy.TIME_BASED and elapsed >= self.config.flush_interval_seconds:
            return True, "time_elapsed"

        if self.config.strategy == BatchStrategy.FIXED_SIZE and len(self._items) >= self.config.max_batch_size:
            return True, "max_size_reached"

        if elapsed >= self.config.max_wait_seconds and len(self._items) >= self.config.min_batch_size:
            return True, "max_wait"

        return False, ""

    def flush(self, reason: str) -> Batch:
        """Flush current items into a batch."""
        batch = Batch(
            batch_id=f"batch_{uuid.uuid4().hex[:12]}",
            items=self._items.copy(),
            created_at=time.time(),
            flush_reason=reason,
        )
        self._items.clear()
        self._last_flush_time = time.time()
        return batch


class APIBatchingAction:
    """
    API batching action for request coalescing.

    Features:
    - Multiple batch strategies (fixed, time-based, adaptive, hybrid)
    - Automatic batch flushing
    - Request deduplication
    - Priority-based ordering
    - Callback support for results
    - Batch result aggregation

    Usage:
        batcher = APIBatchingAction(config)
        
        async def api_call(items):
            return [process(item) for item in items]
        
        batcher.set_processor(api_call)
        
        result = await batcher.add("request-data")
    """

    def __init__(self, config: Optional[BatchConfig] = None):
        self.config = config or BatchConfig()
        self._manager = BatchManager(self.config)
        self._processor: Optional[Callable[[List[BatchItem]], Any]] = None
        self._pending_futures: Dict[str, asyncio.Future] = {}
        self._running = False
        self._lock = asyncio.Lock()
        self._stats = {
            "items_added": 0,
            "batches_flushed": 0,
            "total_items_processed": 0,
            "items_failed": 0,
        }

    def set_processor(self, processor: Callable[[List[BatchItem]], Any]) -> None:
        """Set the batch processor function."""
        self._processor = processor

    async def add(
        self,
        request: Any,
        callback: Optional[Callable[[Any], None]] = None,
        priority: int = 0,
        timeout: Optional[float] = None,
    ) -> Any:
        """Add a request to the batch."""
        if self._processor is None:
            raise ValueError("No processor set. Call set_processor() first.")

        item = BatchItem(
            item_id=f"item_{uuid.uuid4().hex[:8]}",
            request=request,
            callback=callback,
            priority=priority,
        )

        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_futures[item.item_id] = future

        async with self._lock:
            self._manager.add_item(item)
            self._stats["items_added"] += 1

        should_flush, reason = self._manager.should_flush()

        if should_flush:
            asyncio.create_task(self._flush_and_process())

        timeout = timeout or self.config.timeout_seconds

        try:
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            future.cancel()
            self._stats["items_failed"] += 1
            raise

    async def _flush_and_process(self) -> None:
        """Flush pending items and process the batch."""
        async with self._lock:
            should_flush, reason = self._manager.should_flush()
            if not should_flush or not self._pending_futures:
                return

            batch = self._manager.flush(reason)
            item_ids = {item.item_id for item in batch.items}

            futures_to_resolve = {
                item_id: self._pending_futures[item_id]
                for item_id in item_ids
                if item_id in self._pending_futures
            }

        if not batch.items or not self._processor:
            return

        start_time = time.time()
        self._stats["batches_flushed"] += 1

        try:
            results = await self._processor(batch.items)

            for item, result in zip(batch.items, results):
                if item.item_id in futures_to_resolve:
                    future = futures_to_resolve[item.item_id]
                    if not future.done():
                        future.set_result(result)
                    if item.callback:
                        item.callback(result)

        except Exception as e:
            logger.error(f"Batch processing error: {e}")
            for item in batch.items:
                if item.item_id in futures_to_resolve:
                    future = futures_to_resolve[item.item_id]
                    if not future.done():
                        future.set_exception(e)

        duration = (time.time() - start_time) * 1000
        self._stats["total_items_processed"] += len(batch.items)

    async def flush_pending(self) -> None:
        """Manually flush all pending items."""
        async with self._lock:
            batch = self._manager.flush("manual")
            if batch.items:
                await self._flush_and_process()

    def get_pending_count(self) -> int:
        """Get number of pending items."""
        return len(self._pending_futures)

    def get_stats(self) -> Dict[str, Any]:
        """Get batching statistics."""
        return {
            **self._stats.copy(),
            "pending_items": len(self._pending_futures),
            "strategy": self.config.strategy.value,
        }


async def demo_batching():
    """Demonstrate API batching."""

    async def processor(items: List[BatchItem]) -> List[Dict]:
        await asyncio.sleep(0.05)
        return [{"result": f"processed-{item.item_id}"} for item in items]

    config = BatchConfig(max_batch_size=10, strategy=BatchStrategy.FIXED_SIZE)
    batcher = APIBatchingAction(config)
    batcher.set_processor(processor)

    results = []
    for i in range(5):
        result = await batcher.add({"request_id": i})
        results.append(result)

    await batcher.flush_pending()

    print(f"Results: {len(results)}")
    print(f"Stats: {batcher.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_batching())

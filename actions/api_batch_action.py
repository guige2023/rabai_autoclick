"""API Batch Action Module.

Provides batch request processing with grouping,
parallel execution, and result aggregation.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class BatchItem:
    """Single batch item."""
    id: str
    data: Any
    priority: int = 0


@dataclass
class BatchResult:
    """Batch operation result."""
    item_id: str
    success: bool
    result: Any = None
    error: Optional[str] = None


class APIBatchAction:
    """Batch request processor.

    Example:
        batcher = APIBatchAction()

        batcher.register_handler(
            lambda items: [api.process(item) for item in items]
        )

        await batcher.add("id1", {"data": "value1"})
        await batcher.add("id2", {"data": "value2"})

        results = await batcher.flush()
    """

    def __init__(
        self,
        max_batch_size: int = 100,
        max_wait_ms: float = 100.0,
        max_concurrency: int = 5,
    ) -> None:
        self.max_batch_size = max_batch_size
        self.max_wait_ms = max_wait_ms
        self.max_concurrency = max_concurrency

        self._items: List[BatchItem] = []
        self._handler: Optional[Callable] = None
        self._lock = asyncio.Lock()
        self._results: Dict[str, BatchResult] = {}
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False

    def register_handler(
        self,
        handler: Callable[[List[BatchItem]], List[Any]],
    ) -> None:
        """Register batch processing handler.

        Args:
            handler: Function that receives batch and returns list of results
        """
        self._handler = handler

    async def add(
        self,
        item_id: str,
        data: Any,
        priority: int = 0,
    ) -> None:
        """Add item to batch.

        Args:
            item_id: Unique identifier for item
            data: Item data
            priority: Higher = processed sooner
        """
        async with self._lock:
            self._items.append(BatchItem(
                id=item_id,
                data=data,
                priority=priority,
            ))

            if len(self._items) >= self.max_batch_size:
                await self._execute_batch()

    async def add_many(
        self,
        items: List[Dict[str, Any]],
    ) -> None:
        """Add multiple items to batch.

        Args:
            items: List of dicts with 'id' and 'data' keys
        """
        for item in items:
            await self.add(
                item["id"],
                item.get("data"),
                item.get("priority", 0)
            )

    async def flush(self) -> List[BatchResult]:
        """Flush current batch and return results.

        Returns:
            List of BatchResults
        """
        async with self._lock:
            return await self._execute_batch()

    async def _execute_batch(self) -> List[BatchResult]:
        """Execute current batch."""
        if not self._items:
            return []

        if not self._handler:
            logger.error("No handler registered for batch processing")
            return []

        batch = self._items[:]
        self._items.clear()

        sorted_batch = sorted(batch, key=lambda x: -x.priority)

        results: List[BatchResult] = []

        try:
            if asyncio.iscoroutinefunction(self._handler):
                raw_results = await self._handler(sorted_batch)
            else:
                raw_results = self._handler(sorted_batch)

            for item, result in zip(sorted_batch, raw_results):
                batch_result = BatchResult(
                    item_id=item.id,
                    success=True,
                    result=result,
                )
                results.append(batch_result)
                self._results[item.id] = batch_result

        except Exception as e:
            logger.error(f"Batch processing error: {e}")

            for item in sorted_batch:
                batch_result = BatchResult(
                    item_id=item.id,
                    success=False,
                    error=str(e),
                )
                results.append(batch_result)
                self._results[item.id] = batch_result

        return results

    async def start(self) -> None:
        """Start background batch processing."""
        if self._running:
            return

        self._running = True
        self._flush_task = asyncio.create_task(self._background_flush())

    async def stop(self) -> None:
        """Stop background batch processing."""
        self._running = False

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        await self.flush()

    async def _background_flush(self) -> None:
        """Background flush loop."""
        while self._running:
            try:
                await asyncio.sleep(self.max_wait_ms / 1000.0)

                async with self._lock:
                    if self._items:
                        await self._execute_batch()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Background flush error: {e}")

    def get_result(self, item_id: str) -> Optional[BatchResult]:
        """Get result for specific item."""
        return self._results.get(item_id)

    def get_stats(self) -> Dict[str, Any]:
        """Get batch statistics."""
        return {
            "pending_items": len(self._items),
            "total_results": len(self._results),
            "successful": sum(
                1 for r in self._results.values() if r.success
            ),
            "failed": sum(
                1 for r in self._results.values() if not r.success
            ),
        }

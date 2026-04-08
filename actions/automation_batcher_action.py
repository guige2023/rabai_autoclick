"""Automation Batcher Action Module.

Provides intelligent batching of operations with size limits,
time windows, and flush triggers.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypeVar
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class BatchConfig:
    """Batch configuration."""
    max_size: int = 100
    max_wait_seconds: float = 5.0
    flush_on_size: bool = True
    flush_on_time: bool = True
    flush_on_empty: bool = False


class AutomationBatcherAction:
    """Intelligent operation batcher.

    Example:
        batcher = AutomationBatcherAction(
            BatchConfig(max_size=50, max_wait_seconds=2.0)
        )

        batcher.register_handler(
            lambda batch: api.bulk_insert(batch)
        )

        await batcher.add({"id": 1, "data": "item1"})
        await batcher.add({"id": 2, "data": "item2"})
        await batcher.flush()
    """

    def __init__(self, config: Optional[BatchConfig] = None) -> None:
        self.config = config or BatchConfig()
        self._batch: List[Any] = []
        self._handler: Optional[Callable[[List[Any]], Any]] = None
        self._lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None
        self._last_flush_time: float = time.time()
        self._total_batches: int = 0
        self._total_items: int = 0

    def register_handler(
        self,
        handler: Callable[[List[Any]], Any],
    ) -> None:
        """Register batch processing handler.

        Args:
            handler: Function that receives batches
        """
        self._handler = handler

    async def add(self, item: T) -> None:
        """Add item to batch.

        Args:
            item: Item to add
        """
        async with self._lock:
            self._batch.append(item)
            self._total_items += 1

            if self.config.flush_on_size and len(self._batch) >= self.config.max_size:
                await self._flush()

    async def add_batch(self, items: List[T]) -> None:
        """Add multiple items to batch.

        Args:
            items: Items to add
        """
        for item in items:
            await self.add(item)

    async def flush(self) -> Optional[List[Any]]:
        """Manually flush current batch.

        Returns:
            Flushed items if handler registered
        """
        async with self._lock:
            return await self._flush()

    async def _flush(self) -> Optional[List[Any]]:
        """Internal flush implementation."""
        if not self._batch:
            return None

        batch = self._batch[:]
        self._batch.clear()
        self._last_flush_time = time.time()
        self._total_batches += 1

        logger.debug(f"Flushing batch of {len(batch)} items")

        if self._handler:
            try:
                result = self._handler(batch)
                if asyncio.iscoroutine(result):
                    await result
                return batch
            except Exception as e:
                logger.error(f"Batch handler error: {e}")
                raise

        return batch

    async def start_auto_flush(self) -> None:
        """Start automatic time-based flushing."""
        if self._flush_task:
            return

        self._flush_task = asyncio.create_task(self._auto_flush_loop())

    async def stop_auto_flush(self) -> None:
        """Stop automatic flushing."""
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None

    async def _auto_flush_loop(self) -> None:
        """Automatic flush loop."""
        while True:
            try:
                await asyncio.sleep(0.5)

                async with self._lock:
                    should_flush = (
                        self.config.flush_on_time and
                        time.time() - self._last_flush_time >= self.config.max_wait_seconds and
                        len(self._batch) > 0
                    )

                if should_flush:
                    await self.flush()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Auto-flush error: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get batcher statistics."""
        return {
            "pending_items": len(self._batch),
            "total_batches": self._total_batches,
            "total_items": self._total_items,
            "avg_batch_size": self._total_batches and self._total_items / self._total_batches or 0,
        }

    async def __aenter__(self) -> "AutomationBatcherAction":
        """Async context manager entry."""
        await self.start_auto_flush()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.flush()
        await self.stop_auto_flush()

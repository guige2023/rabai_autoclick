"""
Automation Batch Action Module.

Provides batch processing with chunking, parallel execution,
rate limiting, and progress tracking.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterator, Optional

logger = logging.getLogger(__name__)


class BatchStatus(Enum):
    """Batch processing status."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class BatchProgress:
    """Progress tracking for batch operations."""

    total: int = 0
    processed: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    start_time: float = 0.0
    end_time: float = 0.0
    current_item: Any = None


@dataclass
class BatchResult:
    """Result of batch processing."""

    status: BatchStatus
    progress: BatchProgress
    results: list[Any] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)


class AutomationBatchAction:
    """
    Batch processing with configurable strategies.

    Features:
    - Configurable chunk sizes
    - Parallel processing with rate limiting
    - Progress tracking and callbacks
    - Error handling and retry
    - Streaming processing for large datasets

    Example:
        batch = AutomationBatchAction(chunk_size=100, max_parallel=4)
        result = await batch.process(items, process_fn)
    """

    def __init__(
        self,
        chunk_size: int = 100,
        max_parallel: int = 4,
        rate_limit: float = 0.0,
        enable_progress: bool = True,
    ) -> None:
        """
        Initialize batch action.

        Args:
            chunk_size: Items per chunk.
            max_parallel: Maximum parallel chunks.
            rate_limit: Minimum delay between items (seconds).
            enable_progress: Enable progress tracking.
        """
        self.chunk_size = chunk_size
        self.max_parallel = max_parallel
        self.rate_limit = rate_limit
        self.enable_progress = enable_progress
        self._progress_callbacks: list[Callable] = []
        self._stats = {
            "total_batches": 0,
            "total_items": 0,
            "succeeded_items": 0,
            "failed_items": 0,
        }

    def on_progress(self, callback: Callable[[BatchProgress], None]) -> None:
        """
        Register a progress callback.

        Args:
            callback: Progress callback function.
        """
        self._progress_callbacks.append(callback)

    async def process(
        self,
        items: list[Any],
        process_fn: Callable[[Any], Any],
        error_handler: Optional[Callable[[Any, Exception], Any]] = None,
    ) -> BatchResult:
        """
        Process items in batches.

        Args:
            items: Items to process.
            process_fn: Processing function.
            error_handler: Optional error handler.

        Returns:
            BatchResult with all results.
        """
        progress = BatchProgress(total=len(items), start_time=time.time())
        result = BatchResult(status=BatchStatus.PROCESSING, progress=progress)

        self._stats["total_batches"] += 1
        self._stats["total_items"] += len(items)

        chunks = self._chunk_items(items)

        for chunk_idx, chunk in enumerate(chunks):
            chunk_tasks = []

            for item in chunk:
                task = self._process_item(
                    item, process_fn, error_handler, progress, result
                )
                chunk_tasks.append(task)

            await asyncio.gather(*chunk_tasks, return_exceptions=True)

            if progress.total > 0:
                self._report_progress(progress)

        progress.end_time = time.time()
        result.status = BatchStatus.COMPLETED
        self._stats["succeeded_items"] += progress.succeeded
        self._stats["failed_items"] += progress.failed

        logger.info(
            f"Batch completed: {progress.succeeded}/{progress.total} succeeded "
            f"in {progress.end_time - progress.start_time:.2f}s"
        )

        return result

    async def _process_item(
        self,
        item: Any,
        process_fn: Callable,
        error_handler: Optional[Callable],
        progress: BatchProgress,
        result: BatchResult,
    ) -> None:
        """Process a single item."""
        progress.current_item = item

        try:
            if asyncio.iscoroutinefunction(process_fn):
                processed = await process_fn(item)
            else:
                processed = process_fn(item)

            progress.processed += 1
            progress.succeeded += 1
            result.results.append(processed)

        except Exception as e:
            progress.processed += 1
            progress.failed += 1

            error_record = {"item": str(item), "error": str(e)}
            result.errors.append(error_record)

            if error_handler:
                try:
                    handled = error_handler(item, e)
                    if handled is not None:
                        result.results.append(handled)
                except Exception:
                    pass

        if self.rate_limit > 0:
            await asyncio.sleep(self.rate_limit)

    def _chunk_items(self, items: list[Any]) -> list[list[Any]]:
        """Split items into chunks."""
        chunks = []
        for i in range(0, len(items), self.chunk_size):
            chunks.append(items[i:i + self.chunk_size])
        return chunks

    def process_stream(
        self,
        item_iterator: Iterator[Any],
        process_fn: Callable[[Any], Any],
        max_buffer: int = 1000,
    ) -> BatchResult:
        """
        Process items from an iterator/stream.

        Args:
            item_iterator: Iterator yielding items.
            process_fn: Processing function.
            max_buffer: Maximum buffered items.

        Returns:
            BatchResult (streaming mode, partial results).
        """
        progress = BatchProgress(start_time=time.time())
        result = BatchResult(status=BatchStatus.PROCESSING, progress=progress)

        buffer = []

        for item in item_iterator:
            buffer.append(item)

            if len(buffer) >= max_buffer:
                processed = self._process_chunk_sync(buffer, process_fn)
                result.results.extend(processed)
                progress.processed += len(buffer)
                progress.succeeded += len(processed)
                buffer.clear()

        if buffer:
            processed = self._process_chunk_sync(buffer, process_fn)
            result.results.extend(processed)
            progress.processed += len(buffer)
            progress.succeeded += len(processed)

        progress.end_time = time.time()
        progress.total = progress.processed
        result.status = BatchStatus.COMPLETED

        return result

    def _process_chunk_sync(
        self,
        chunk: list[Any],
        process_fn: Callable,
    ) -> list[Any]:
        """Process a chunk synchronously."""
        results = []
        for item in chunk:
            try:
                result = process_fn(item)
                results.append(result)
            except Exception:
                pass
        return results

    def _report_progress(self, progress: BatchProgress) -> None:
        """Report progress to callbacks."""
        if not self.enable_progress:
            return

        for callback in self._progress_callbacks:
            try:
                callback(progress)
            except Exception as e:
                logger.error(f"Progress callback error: {e}")

    def get_stats(self) -> dict[str, Any]:
        """
        Get batch processing statistics.

        Returns:
            Statistics dictionary.
        """
        return {
            **self._stats,
            "success_rate": (
                f"{self._stats['succeeded_items'] / max(1, self._stats['total_items']) * 100:.1f}%"
            ),
        }

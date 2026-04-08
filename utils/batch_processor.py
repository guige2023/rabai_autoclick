"""
Batch Processor Utility

Processes items in batches with parallelism control.
Supports batching, chunking, and concurrent execution.

Example:
    >>> processor = BatchProcessor(batch_size=10, max_workers=4)
    >>> results = processor.process(items, process_fn)
"""

from __future__ import annotations

import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, Future
from dataclasses import dataclass
from typing import Any, Callable, Optional, TypeVar


T = TypeVar("T")
R = TypeVar("R")


@dataclass
class BatchResult:
    """Result of a batch processing operation."""
    total_items: int
    successful: int
    failed: int
    duration: float
    results: list[R]
    errors: list[tuple[Any, str]]


class BatchProcessor:
    """
    Processes items in parallel batches.

    Args:
        batch_size: Number of items per batch.
        max_workers: Maximum parallel workers.
    """

    def __init__(
        self,
        batch_size: int = 10,
        max_workers: int = 4,
    ) -> None:
        self.batch_size = batch_size
        self.max_workers = max_workers
        self._executor: Optional[ThreadPoolExecutor] = None

    def process(
        self,
        items: list[T],
        process_fn: Callable[[T], R],
        error_handler: Optional[Callable[[T, Exception], R]] = None,
    ) -> BatchResult:
        """
        Process items in batches.

        Args:
            items: List of items to process.
            process_fn: Function to apply to each item.
            error_handler: Optional error handler (item, error) -> recovery value.

        Returns:
            BatchResult with all results and statistics.
        """
        start_time = time.time()
        results: list[R] = []
        errors: list[tuple[Any, str]] = []
        successful = 0
        failed = 0

        batches = self._chunk(items, self.batch_size)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures: list[Future] = []

            for batch in batches:
                for item in batch:
                    future = executor.submit(self._process_item, item, process_fn, error_handler)
                    futures.append(future)

            for future in futures:
                try:
                    result = future.result(timeout=30.0)
                    if result is not None:
                        results.append(result)
                        successful += 1
                except Exception as e:
                    errors.append((None, str(e)))
                    failed += 1

        return BatchResult(
            total_items=len(items),
            successful=successful,
            failed=failed,
            duration=time.time() - start_time,
            results=results,
            errors=errors,
        )

    def _process_item(
        self,
        item: T,
        process_fn: Callable[[T], R],
        error_handler: Optional[Callable[[T, Exception], R]],
    ) -> R:
        """Process a single item with error handling."""
        try:
            return process_fn(item)
        except Exception as e:
            if error_handler:
                return error_handler(item, e)
            raise

    def _chunk(self, items: list[T], size: int) -> list[list[T]]:
        """Split items into chunks of given size."""
        chunks: list[list[T]] = []
        for i in range(0, len(items), size):
            chunks.append(items[i:i + size])
        return chunks

    def process_async(
        self,
        items: list[T],
        process_fn: Callable[[T], R],
        callback: Optional[Callable[[BatchResult], None]] = None,
    ) -> Future:
        """
        Process items asynchronously.

        Args:
            items: List of items to process.
            process_fn: Function to apply to each item.
            callback: Optional callback when complete.

        Returns:
            Future that will contain the BatchResult.
        """
        def run():
            result = self.process(items, process_fn)
            if callback:
                callback(result)
            return result

        executor = ThreadPoolExecutor(max_workers=1)
        future = executor.submit(run)
        executor.shutdown(wait=False)
        return future


class StreamingBatchProcessor:
    """
    Processes items in a streaming fashion with batching.

    Useful for processing large datasets without loading all into memory.
    """

    def __init__(
        self,
        batch_size: int = 100,
        max_workers: int = 4,
    ) -> None:
        self.batch_size = batch_size
        self.max_workers = max_workers
        self._queue: deque = deque()
        self._lock = threading.Lock()
        self._executor: Optional[ThreadPoolExecutor] = None

    def add(self, item: T) -> None:
        """Add an item to the processing queue."""
        with self._lock:
            self._queue.append(item)

    def add_batch(self, items: list[T]) -> None:
        """Add multiple items to the queue."""
        with self._lock:
            self._queue.extend(items)

    def process(
        self,
        process_fn: Callable[[T], R],
        min_batch: int = 1,
    ) -> list[R]:
        """
        Process queued items in batches.

        Args:
            process_fn: Function to apply to each item.
            min_batch: Minimum batch size to trigger processing.

        Returns:
            List of results.
        """
        results: list[R] = []

        with self._lock:
            if len(self._queue) < min_batch:
                return results

            batch = [self._queue.popleft() for _ in range(min(len(self._queue), self.batch_size))]

        processor = BatchProcessor(batch_size=self.batch_size, max_workers=self.max_workers)
        result = processor.process(batch, process_fn)
        results.extend(result.results)

        return results

    def drain_and_process(
        self,
        process_fn: Callable[[T], R],
    ) -> list[R]:
        """
        Drain all queued items and process them.

        Returns:
            List of all results.
        """
        with self._lock:
            items = list(self._queue)
            self._queue.clear()

        if not items:
            return []

        processor = BatchProcessor(batch_size=self.batch_size, max_workers=self.max_workers)
        result = processor.process(items, process_fn)
        return result.results

    def __len__(self) -> int:
        """Return number of items in queue."""
        with self._lock:
            return len(self._queue)

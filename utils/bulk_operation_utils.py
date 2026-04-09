"""Bulk operation utilities for batch processing automation actions.

Provides batching, chunking, parallel execution, and
result aggregation for large-scale automation operations.

Example:
    >>> from utils.bulk_operation_utils import BatchProcessor, chunk
    >>> processor = BatchProcessor(batch_size=50, max_workers=4)
    >>> results = processor.process(large_element_set, action_fn)
"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Iterator,
    List,
    Optional,
    TypeVar,
    Generic,
)

T = TypeVar("T")
U = TypeVar("U")


@dataclass
class BatchResult(Generic[T]):
    """Result of a batch operation."""
    total: int
    succeeded: int
    failed: int
    results: List[T]
    errors: List[tuple[int, Exception]]
    duration_seconds: float


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    batch_size: int = 50
    max_workers: int = 4
    stop_on_error: bool = False
    progress_callback: Optional[Callable[[int, int], None]] = None


def chunk(items: List[T], size: int) -> Iterator[List[T]]:
    """Split items into chunks of specified size.

    Args:
        items: List of items to chunk.
        size: Maximum chunk size.

    Yields:
        Chunks of items.

    Example:
        >>> list(chunk([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]
    """
    for i in range(0, len(items), size):
        yield items[i : i + size]


class BatchProcessor(Generic[T]):
    """Batch processor for automating large sets of operations.

    Supports sequential and parallel batch processing with
    configurable error handling and progress reporting.

    Example:
        >>> processor = BatchProcessor(batch_size=20, max_workers=8)
        >>> result = processor.process(elements, click_action)
        >>> print(f"Succeeded: {result.succeeded}/{result.total}")
    """

    def __init__(
        self,
        batch_size: int = 50,
        max_workers: int = 4,
        stop_on_error: bool = False,
    ) -> None:
        """Initialize batch processor.

        Args:
            batch_size: Items per batch.
            max_workers: Parallel worker threads.
            stop_on_error: Stop processing on first error.
        """
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.stop_on_error = stop_on_error

    def process(
        self,
        items: List[T],
        action: Callable[[T], U],
        *,
        sequential: bool = False,
    ) -> BatchResult[U]:
        """Process items in batches.

        Args:
            items: Items to process.
            action: Action to apply to each item.
            sequential: If True, process in main thread sequentially.

        Returns:
            BatchResult with all results and statistics.
        """
        import time
        start = time.monotonic()

        results: List[U] = []
        errors: List[tuple[int, Exception]] = []
        succeeded = 0
        failed = 0

        batches = list(chunk(items, self.batch_size))

        if sequential:
            for batch_idx, batch in enumerate(batches):
                for item_idx, item in enumerate(batch):
                    global_idx = batch_idx * self.batch_size + item_idx
                    try:
                        result = action(item)
                        results.append(result)
                        succeeded += 1
                    except Exception as e:
                        errors.append((global_idx, e))
                        failed += 1
                        if self.stop_on_error:
                            break
        else:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {}
                for batch_idx, batch in enumerate(batches):
                    for item_idx, item in enumerate(batch):
                        global_idx = batch_idx * self.batch_size + item_idx
                        future = executor.submit(_safe_call, action, item)
                        futures[future] = global_idx

                for future in as_completed(futures):
                    global_idx = futures[future]
                    try:
                        result = future.result()
                        results.append(result)
                        succeeded += 1
                    except Exception as e:
                        errors.append((global_idx, e))
                        failed += 1
                        if self.stop_on_error:
                            for f in futures:
                                f.cancel()

        duration = time.monotonic() - start
        return BatchResult(
            total=len(items),
            succeeded=succeeded,
            failed=failed,
            results=results,
            errors=errors,
            duration_seconds=duration,
        )

    def process_with_retry(
        self,
        items: List[T],
        action: Callable[[T], U],
        max_retries: int = 2,
    ) -> BatchResult[U]:
        """Process items with per-item retry logic.

        Args:
            items: Items to process.
            action: Action to apply to each item.
            max_retries: Maximum retries per item.

        Returns:
            BatchResult with all results.
        """
        import time
        start = time.monotonic()

        results: List[U] = []
        errors: List[tuple[int, Exception]] = []
        succeeded = 0
        failed = 0

        for idx, item in enumerate(items):
            last_error: Optional[Exception] = None
            for attempt in range(max_retries + 1):
                try:
                    result = action(item)
                    results.append(result)
                    succeeded += 1
                    break
                except Exception as e:
                    last_error = e
            else:
                errors.append((idx, last_error or Exception("Unknown")))
                failed += 1

        duration = time.monotonic() - start
        return BatchResult(
            total=len(items),
            succeeded=succeeded,
            failed=failed,
            results=results,
            errors=errors,
            duration_seconds=duration,
        )


def _safe_call(fn: Callable[[T], U], arg: T) -> U:
    """Safely call a function and return result."""
    return fn(arg)


class BulkOperationTracker:
    """Track progress of bulk operations with checkpoints.

    Example:
        >>> tracker = BulkOperationTracker(total=1000)
        >>> tracker.checkpoint("started")
        >>> # ... do work ...
        >>> tracker.checkpoint("halfway")
        >>> print(tracker.summary())
    """

    def __init__(self, total: int) -> None:
        self.total = total
        self._completed = 0
        self._checkpoints: dict[str, int] = {}
        self._lock = threading.Lock()

    def increment(self, count: int = 1) -> None:
        """Increment completed count."""
        with self._lock:
            self._completed = min(self.total, self._completed + count)

    def checkpoint(self, name: str) -> None:
        """Record a named checkpoint with current progress."""
        with self._lock:
            self._checkpoints[name] = self._completed

    @property
    def completed(self) -> int:
        return self._completed

    @property
    def progress_percent(self) -> float:
        if self.total == 0:
            return 100.0
        return (self._completed / self.total) * 100

    def summary(self) -> dict[str, Any]:
        """Get summary of operation progress."""
        return {
            "total": self.total,
            "completed": self._completed,
            "percent": self.progress_percent,
            "checkpoints": dict(self._checkpoints),
        }

"""Batch processing utilities for RabAI AutoClick.

Provides:
- Batch processing helpers
- Parallel execution
"""

import multiprocessing
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Generator, List, Optional, TypeVar


T = TypeVar("T")
R = TypeVar("R")


@dataclass
class BatchResult:
    """Result of batch processing."""
    total: int
    successful: int
    failed: int
    results: List[Any]
    errors: List[Exception]


def batch_process(
    items: List[T],
    processor: Callable[[T], R],
    batch_size: int = 10,
    max_workers: int = 4,
) -> BatchResult:
    """Process items in batches.

    Args:
        items: Items to process.
        processor: Function to process each item.
        batch_size: Items per batch.
        max_workers: Max parallel workers.

    Returns:
        BatchResult with processing results.
    """
    results = []
    errors = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []

        for item in items:
            future = executor.submit(processor, item)
            futures.append(future)

        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                errors.append(e)

    return BatchResult(
        total=len(items),
        successful=len(results),
        failed=len(errors),
        results=results,
        errors=errors,
    )


def parallel_map(
    func: Callable[[T], R],
    items: List[T],
    max_workers: Optional[int] = None,
) -> List[R]:
    """Map function over items in parallel.

    Args:
        func: Function to apply.
        items: Items to process.
        max_workers: Max parallel workers.

    Returns:
        List of results.
    """
    if max_workers is None:
        max_workers = min(32, multiprocessing.cpu_count() * 2)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        return list(executor.map(func, items))


def chunk_process(
    items: List[T],
    processor: Callable[[List[T]], List[R]],
    chunk_size: int = 100,
) -> Generator[List[T], None, None]:
    """Split items into chunks.

    Args:
        items: Items to chunk.
        processor: Unused parameter (for API compatibility).
        chunk_size: Size of each chunk.

    Yields:
        Each chunk of items.
    """
    for i in range(0, len(items), chunk_size):
        chunk = items[i:i + chunk_size]
        yield chunk


@dataclass
class WorkerStats:
    """Statistics for a worker."""
    worker_id: int
    items_processed: int = 0
    errors: int = 0


class WorkerPool:
    """Pool of workers for batch processing.

    Provides worker management and load balancing.
    """

    def __init__(self, num_workers: Optional[int] = None) -> None:
        """Initialize worker pool.

        Args:
            num_workers: Number of workers (defaults to CPU count).
        """
        self.num_workers = num_workers or multiprocessing.cpu_count()
        self._stats: List[WorkerStats] = []

    def map(
        self,
        func: Callable[[T], R],
        items: List[T],
    ) -> List[R]:
        """Map function over items using pool.

        Args:
            func: Function to apply.
            items: Items to process.

        Returns:
            List of results.
        """
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            return list(executor.map(func, items))

    def submit_batch(
        self,
        func: Callable[[T], R],
        items: List[T],
        batch_size: int = 10,
    ) -> BatchResult:
        """Submit batch of items for processing.

        Args:
            func: Function to apply.
            items: Items to process.
            batch_size: Batch size.

        Returns:
            BatchResult.
        """
        return batch_process(items, func, batch_size, self.num_workers)

    @property
    def stats(self) -> List[WorkerStats]:
        """Get worker statistics."""
        return self._stats.copy()


def imap(
    func: Callable[[T], R],
    items: List[T],
    max_workers: int = 4,
) -> Generator[R, None, None]:
    """Lazily map function over items.

    Args:
        func: Function to apply.
        items: Items to process.
        max_workers: Max parallel workers.

    Yields:
        Results as they complete.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(func, item): i for i, item in enumerate(items)}

        results = [None] * len(items)

        for future in as_completed(futures):
            idx = futures[future]
            try:
                results[idx] = future.result()
            except Exception as e:
                results[idx] = e

        for result in results:
            yield result


def starmap(
    func: Callable[..., R],
    args_list: List[tuple],
    max_workers: int = 4,
) -> List[R]:
    """Map function over argument tuples.

    Args:
        func: Function to apply.
        args_list: List of argument tuples.
        max_workers: Max parallel workers.

    Returns:
        List of results.
    """
    def unpack_and_apply(args):
        return func(*args)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        return list(executor.map(unpack_and_apply, args_list))
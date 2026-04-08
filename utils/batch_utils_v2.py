"""
Batch processing utilities with chunking, windowing, and retry support.

Provides functions for processing large datasets in batches
with configurable parallelism, error handling, and progress tracking.

Example:
    >>> from utils.batch_utils_v2 import chunked, batch_process
    >>> for batch in chunked(large_list, size=100):
    ...     process(batch)
"""

from __future__ import annotations

import asyncio
import concurrent.futures
from collections.abc import Iterator
from functools import partial
from typing import Any, Callable, Iterable, Iterator as IterType, List, Optional, TypeVar, Union

T = TypeVar("T")
R = TypeVar("R")


def chunked(
    iterable: Iterable[T],
    size: int,
    drop_last: bool = False,
) -> IterType[List[T]]:
    """
    Split an iterable into chunks of specified size.

    Args:
        iterable: Input iterable to chunk.
        size: Maximum chunk size.
        drop_last: If True, drop the last chunk if smaller than size.

    Yields:
        Lists of items, each up to size elements.

    Example:
        >>> list(chunked([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]
    """
    iterator = iter(iterable)
    while True:
        chunk: List[T] = []
        try:
            for _ in range(size):
                chunk.append(next(iterator))
            yield chunk
        except StopIteration:
            if chunk and not drop_last:
                yield chunk
            return


def batch_process(
    items: List[T],
    func: Callable[[T], R],
    batch_size: int = 10,
    max_workers: Optional[int] = None,
    return_exceptions: bool = False,
) -> List[Union[R, Exception]]:
    """
    Process items in batches with optional parallelization.

    Args:
        items: List of items to process.
        func: Function to apply to each item.
        batch_size: Number of items per batch.
        max_workers: Maximum worker threads (None for sequential).
        return_exceptions: If True, return exceptions instead of raising.

    Returns:
        List of results in the same order as input items.
    """
    if max_workers is None or max_workers == 1:
        return _process_sequential(items, func, return_exceptions)

    return _process_parallel(
        items, func, batch_size, max_workers, return_exceptions
    )


def _process_sequential(
    items: List[T],
    func: Callable[[T], R],
    return_exceptions: bool,
) -> List[Union[R, Exception]]:
    """Process items sequentially."""
    results: List[Union[R, Exception]] = []
    for item in items:
        try:
            results.append(func(item))
        except Exception as e:
            if return_exceptions:
                results.append(e)
            else:
                raise
    return results


def _process_parallel(
    items: List[T],
    func: Callable[[T], R],
    batch_size: int,
    max_workers: int,
    return_exceptions: bool,
) -> List[Union[R, Exception]]:
    """Process items in parallel using thread pool."""
    results: List[Union[R, Exception]] = [None] * len(items)
    index_map: List[tuple] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for batch_idx, batch in enumerate(chunked(items, batch_size)):
            start_idx = batch_idx * batch_size
            for local_idx, item in enumerate(batch):
                future = executor.submit(func, item)
                index_map.append((start_idx + local_idx, future))

        for idx, future in index_map:
            try:
                results[idx] = future.result()
            except Exception as e:
                if return_exceptions:
                    results[idx] = e
                else:
                    raise

    return results


async def batch_process_async(
    items: List[T],
    func: Callable[[T], R],
    batch_size: int = 10,
    max_concurrency: int = 5,
    return_exceptions: bool = False,
) -> List[Union[R, Exception]]:
    """
    Process items asynchronously in batches with concurrency control.

    Args:
        items: List of items to process.
        func: Async function to apply to each item.
        batch_size: Number of items per batch.
        max_concurrency: Maximum concurrent tasks.
        return_exceptions: If True, return exceptions instead of raising.

    Returns:
        List of results in the same order as input items.
    """
    semaphore = asyncio.Semaphore(max_concurrency)
    results: List[Union[R, Exception]] = [None] * len(items)

    async def process_with_semaphore(idx: int, item: T) -> None:
        async with semaphore:
            try:
                results[idx] = await func(item)
            except Exception as e:
                if return_exceptions:
                    results[idx] = e
                else:
                    raise

    tasks = [
        process_with_semaphore(i, item)
        for i, item in enumerate(items)
    ]

    for batch in chunked(tasks, batch_size):
        await asyncio.gather(*batch, return_exceptions=return_exceptions)

    return results


def windowed(
    iterable: Iterable[T],
    size: int,
    step: Optional[int] = None,
) -> IterType[List[T]]:
    """
    Create sliding windows over an iterable.

    Args:
        iterable: Input iterable.
        size: Window size.
        step: Step size between windows (defaults to size).

    Yields:
        Lists representing each window position.

    Example:
        >>> list(windowed([1, 2, 3, 4, 5], size=3))
        [[1, 2, 3], [2, 3, 4], [3, 4, 5]]
    """
    if step is None:
        step = size

    iterator = iter(iterable)
    window: List[T] = []

    for item in iterator:
        window.append(item)
        if len(window) == size:
            yield window
            window = window[step:]

    while len(window) == size:
        yield window
        window = window[step:]


class BatchProcessor:
    """
    Configurable batch processor with lifecycle hooks.

    Provides before_batch, after_batch, and on_error hooks
    for complex batch processing workflows.

    Attributes:
        batch_size: Items per batch.
        max_workers: Worker thread count.
    """

    def __init__(
        self,
        batch_size: int = 10,
        max_workers: Optional[int] = None,
    ) -> None:
        """
        Initialize the batch processor.

        Args:
            batch_size: Number of items per batch.
            max_workers: Maximum worker threads.
        """
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.before_batch: Optional[Callable[[List[T], int], None]] = None
        self.after_batch: Optional[Callable[[List[T], List[R], int], None]] = None
        self.on_error: Optional[Callable[[T, Exception], None]] = None

    def process(
        self,
        items: List[T],
        func: Callable[[T], R],
        return_exceptions: bool = False,
    ) -> List[Union[R, Exception]]:
        """
        Process items with lifecycle hooks.

        Args:
            items: Items to process.
            func: Processing function.
            return_exceptions: Handle exceptions gracefully.

        Returns:
            List of results.
        """
        results: List[Union[R, Exception]] = []

        for batch_idx, batch in enumerate(chunked(items, self.batch_size)):
            if self.before_batch:
                self.before_batch(batch, batch_idx)

            batch_results = batch_process(
                batch,
                func,
                batch_size=len(batch),
                max_workers=self.max_workers,
                return_exceptions=return_exceptions,
            )

            processed_results: List[Union[R, Exception]] = []
            for item, result in zip(batch, batch_results):
                if isinstance(result, Exception) and self.on_error:
                    self.on_error(item, result)
                processed_results.append(result)

            if self.after_batch:
                self.after_batch(batch, processed_results, batch_idx)

            results.extend(processed_results)

        return results


def retry_batch(
    items: List[T],
    func: Callable[[T], R],
    batch_size: int = 10,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    backoff_factor: float = 2.0,
) -> List[Union[R, Exception]]:
    """
    Process items in batches with retry logic.

    Args:
        items: Items to process.
        func: Processing function.
        batch_size: Items per batch.
        max_retries: Maximum retry attempts.
        retry_delay: Initial delay between retries in seconds.
        backoff_factor: Multiplier for each retry delay.

    Returns:
        List of results (exceptions for failed items after all retries).
    """
    import time

    results: List[Union[R, Exception]] = [None] * len(items)
    failed_indices: List[int] = list(range(len(items)))
    current_delay = retry_delay

    for attempt in range(max_retries + 1):
        batch_results = batch_process(
            [items[i] for i in failed_indices],
            func,
            batch_size=batch_size,
            return_exceptions=True,
        )

        still_failing: List[int] = []
        for idx, (orig_idx, result) in enumerate(zip(failed_indices, batch_results)):
            if isinstance(result, Exception):
                if attempt < max_retries:
                    still_failing.append(orig_idx)
                else:
                    results[orig_idx] = result
            else:
                results[orig_idx] = result

        if not still_failing:
            break

        failed_indices = still_failing
        if attempt < max_retries:
            time.sleep(current_delay)
            current_delay *= backoff_factor

    return results


def flatten_batches(
    batches: Iterable[List[T]],
) -> IterType[T]:
    """
    Flatten a stream of batches back into individual items.

    Args:
        batches: Iterable of batches.

    Yields:
        Individual items.
    """
    for batch in batches:
        yield from batch

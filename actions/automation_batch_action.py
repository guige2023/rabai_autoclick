"""Batch processing utilities for handling bulk operations.

Supports chunking, parallel execution, and result aggregation.
"""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Generator, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class BatchResult(Generic[R]):
    """Result of a batch operation."""

    results: list[R]
    errors: list[dict[str, Any]]
    total: int
    successful: int
    failed: int
    duration_seconds: float


@dataclass
class BatchConfig:
    """Configuration for batch processing."""

    chunk_size: int = 100
    max_concurrent: int = 5
    timeout_seconds: float = 300.0
    continue_on_error: bool = True
    on_error: Callable[[Exception, Any], None] | None = None


def chunk_list(data: list[T], chunk_size: int) -> Generator[list[T], None, None]:
    """Split list into chunks.

    Args:
        data: List to chunk.
        chunk_size: Size of each chunk.

    Yields:
        Chunks of the list.
    """
    for i in range(0, len(data), chunk_size):
        yield data[i : i + chunk_size]


def chunk_dict(data: dict[K, V], chunk_size: int) -> Generator[dict[K, V], None, None]:
    """Split dictionary into chunks.

    Args:
        data: Dict to chunk.
        chunk_size: Size of each chunk.

    Yields:
        Chunks of the dict.
    """
    items = list(data.items())
    for i in range(0, len(items), chunk_size):
        chunk_items = items[i : i + chunk_size]
        yield dict(chunk_items)


class BatchProcessor(Generic[T, R]):
    """Process items in batches with configurable concurrency.

    Args:
        config: Batch processing configuration.
    """

    def __init__(self, config: BatchConfig | None = None) -> None:
        self.config = config or BatchConfig()
        self._results: list[R] = []
        self._errors: list[dict[str, Any]] = []

    def process(
        self,
        items: list[T],
        process_fn: Callable[[T], R],
    ) -> BatchResult[R]:
        """Process items in batches.

        Args:
            items: Items to process.
            process_fn: Function to apply to each item.

        Returns:
            BatchResult with all results and errors.
        """
        import time

        start_time = time.time()
        results: list[R] = []
        errors: list[dict[str, Any]] = []

        chunks = list(chunk_list(items, self.config.chunk_size))
        logger.info("Processing %d items in %d chunks", len(items), len(chunks))

        with ThreadPoolExecutor(max_workers=self.config.max_concurrent) as executor:
            futures = {}
            for chunk_idx, chunk in enumerate(chunks):
                for item_idx, item in enumerate(chunk):
                    future = executor.submit(self._process_item, item, process_fn)
                    futures[future] = {"chunk": chunk_idx, "item": item, "item_idx": item_idx}

            for future in as_completed(futures, timeout=self.config.timeout_seconds):
                info = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    error_info = {
                        "error": str(e),
                        "type": type(e).__name__,
                        "chunk": info["chunk"],
                        "item": info["item"],
                    }
                    errors.append(error_info)
                    logger.error("Batch processing error: %s", e)
                    if self.config.on_error:
                        self.config.on_error(e, info["item"])
                    if not self.config.continue_on_error:
                        raise

        duration = time.time() - start_time
        return BatchResult(
            results=results,
            errors=errors,
            total=len(items),
            successful=len(results),
            failed=len(errors),
            duration_seconds=duration,
        )

    def _process_item(self, item: T, process_fn: Callable[[T], R]) -> R:
        """Process single item."""
        return process_fn(item)

    async def process_async(
        self,
        items: list[T],
        process_fn: Callable[[T], R],
    ) -> BatchResult[R]:
        """Async version of process.

        Args:
            items: Items to process.
            process_fn: Async function to apply to each item.

        Returns:
            BatchResult with all results and errors.
        """
        import time

        start_time = time.time()
        results: list[R] = []
        errors: list[dict[str, Any]] = []

        semaphore = asyncio.Semaphore(self.config.max_concurrent)

        async def process_with_semaphore(item: T, idx: int) -> R | Exception:
            async with semaphore:
                return await process_fn(item)

        tasks = [process_with_semaphore(item, idx) for idx, item in enumerate(items)]

        try:
            completed = await asyncio.gather(*tasks, return_exceptions=True)

            for idx, result in enumerate(completed):
                if isinstance(result, Exception):
                    error_info = {"error": str(result), "type": type(result).__name__, "index": idx, "item": items[idx]}
                    errors.append(error_info)
                    logger.error("Async batch error at index %d: %s", idx, result)
                else:
                    results.append(result)

        except asyncio.TimeoutError:
            logger.error("Batch processing timed out after %ds", self.config.timeout_seconds)
            raise

        duration = time.time() - start_time
        return BatchResult(
            results=results,
            errors=errors,
            total=len(items),
            successful=len(results),
            failed=len(errors),
            duration_seconds=duration,
        )

    def process_stream(
        self,
        items: list[T],
        process_fn: Callable[[T], R],
    ) -> Generator[tuple[R | Exception, int, T], None, None]:
        """Process items as a generator, yielding results as they complete.

        Args:
            items: Items to process.
            process_fn: Function to apply to each item.

        Yields:
            Tuples of (result_or_exception, index, item).
        """
        with ThreadPoolExecutor(max_workers=self.config.max_concurrent) as executor:
            future_to_idx = {executor.submit(process_fn, item): (idx, item) for idx, item in enumerate(items)}

            for future in as_completed(future_to_idx):
                idx, item = future_to_idx[future]
                try:
                    yield (future.result(), idx, item)
                except Exception as e:
                    yield (e, idx, item)


class BatchAggregator(Generic[T, R]):
    """Aggregate results from batch operations.

    Args:
        aggregator_fn: Function to aggregate a list of results.
    """

    def __init__(self, aggregator_fn: Callable[[list[T]], R] | None = None) -> None:
        self.aggregator_fn = aggregator_fn

    def aggregate(self, batches: list[list[T]]) -> list[T]:
        """Aggregate multiple batches into single list.

        Args:
            batches: List of result batches.

        Returns:
            Combined list of all items.
        """
        result = []
        for batch in batches:
            result.extend(batch)
        return result

    def aggregate_dict(self, batches: list[dict[str, T]]) -> dict[str, T]:
        """Aggregate multiple dict batches.

        Args:
            batches: List of result dicts.

        Returns:
            Combined dict.
        """
        result = {}
        for batch in batches:
            result.update(batch)
        return result

    def aggregate_with_fn(self, results: list[R]) -> R:
        """Aggregate using custom aggregator function.

        Args:
            results: List of results.

        Returns:
            Aggregated result.
        """
        if self.aggregator_fn is None:
            raise ValueError("No aggregator function provided")
        return self.aggregator_fn(results)


def batch_process(
    items: list[T],
    fn: Callable[[T], R],
    chunk_size: int = 100,
    max_workers: int = 5,
) -> BatchResult[R]:
    """Convenience function for batch processing.

    Args:
        items: Items to process.
        fn: Processing function.
        chunk_size: Chunk size.
        max_workers: Max concurrent workers.

    Returns:
        BatchResult.
    """
    config = BatchConfig(chunk_size=chunk_size, max_concurrent=max_workers)
    processor = BatchProcessor[T, R](config)
    return processor.process(items, fn)


def batch_process_stream(
    items: list[T],
    fn: Callable[[T], R],
    chunk_size: int = 100,
    max_workers: int = 5,
) -> Generator[tuple[R | Exception, int, T], None, None]:
    """Stream-based batch processing.

    Args:
        items: Items to process.
        fn: Processing function.
        chunk_size: Chunk size.
        max_workers: Max concurrent workers.

    Yields:
        Results as they complete.
    """
    config = BatchConfig(chunk_size=chunk_size, max_concurrent=max_workers)
    processor = BatchProcessor[T, R](config)
    yield from processor.process_stream(items, fn)


@dataclass
class WindowResult(Generic[R]):
    """Result of a windowed operation."""

    windows: list[list[R]]
    total_items: int
    num_windows: int


def sliding_window(data: list[T], size: int, step: int = 1) -> Generator[list[T], None, None]:
    """Create sliding windows over data.

    Args:
        data: List to window.
        size: Window size.
        step: Step between windows.

    Yields:
        Window slices.
    """
    for i in range(0, len(data) - size + 1, step):
        yield data[i : i + size]


def tumbling_window(data: list[T], size: int) -> Generator[list[T], None, None]:
    """Create non-overlapping tumbling windows.

    Args:
        data: List to window.
        size: Window size.

    Yields:
        Window slices.
    """
    for i in range(0, len(data), size):
        yield data[i : i + size]


class WindowProcessor(Generic[T, R]):
    """Process data in windows with aggregation."""

    def __init__(self, window_size: int, step: int = 1, window_type: str = "sliding") -> None:
        self.window_size = window_size
        self.step = step
        self.window_type = window_type

    def process(
        self,
        data: list[T],
        window_fn: Callable[[list[T]], R],
    ) -> list[R]:
        """Process data in windows.

        Args:
            data: Data to process.
            window_fn: Function to apply to each window.

        Returns:
            List of window results.
        """
        windows = sliding_window(data, self.window_size, self.step) if self.window_type == "sliding" else tumbling_window(data, self.window_size)
        return [window_fn(window) for window in windows]

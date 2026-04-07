"""
Batch Processing Utilities

Provides utilities for efficient batch processing,
including batching, chunking, and parallel batch execution.
"""

from __future__ import annotations

import asyncio
import copy
import time
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")
TResult = TypeVar("TResult")


@dataclass
class BatchResult(Generic[TResult]):
    """Result of a batch operation."""
    success: bool
    results: list[TResult] = field(default_factory=list)
    errors: list[tuple[int, str]] = field(default_factory=list)  # (index, error)
    total_time_ms: float = 0.0
    processed_count: int = 0

    @property
    def failed_count(self) -> int:
        return len(self.errors)

    @property
    def success_count(self) -> int:
        return len(self.results)


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    batch_size: int = 100
    max_workers: int = 4
    max_retries: int = 0
    retry_delay_ms: float = 100.0
    timeout_seconds: float | None = None
    stop_on_error: bool = False


def chunk(iterable: Sequence[T], chunk_size: int) -> list[list[T]]:
    """
    Split an iterable into chunks of specified size.

    Args:
        iterable: The input sequence.
        chunk_size: Size of each chunk.

    Returns:
        List of chunks.
    """
    result = []
    for i in range(0, len(iterable), chunk_size):
        result.append(list(iterable[i:i + chunk_size]))
    return result


def batch_process(
    items: Sequence[T],
    func: Callable[[T], TResult],
    config: BatchConfig | None = None,
) -> BatchResult[TResult]:
    """
    Process items in batches.

    Args:
        items: Items to process.
        func: Function to apply to each item.
        config: Batch processing configuration.

    Returns:
        BatchResult with all results and errors.
    """
    config = config or BatchConfig()
    start_time = time.time()
    results: list[TResult] = []
    errors: list[tuple[int, str]] = []

    batches = chunk(items, config.batch_size)

    for batch_idx, batch in enumerate(batches):
        for item_idx, item in enumerate(batch):
            global_idx = batch_idx * config.batch_size + item_idx

            try:
                result = func(item)
                results.append(result)
            except Exception as e:
                error = (global_idx, str(e))
                errors.append(error)

                if config.stop_on_error:
                    return BatchResult(
                        success=False,
                        results=results,
                        errors=errors,
                        total_time_ms=(time.time() - start_time) * 1000,
                        processed_count=len(results) + len(errors),
                    )

    return BatchResult(
        success=len(errors) == 0,
        results=results,
        errors=errors,
        total_time_ms=(time.time() - start_time) * 1000,
        processed_count=len(items),
    )


def batch_process_parallel(
    items: Sequence[T],
    func: Callable[[T], TResult],
    config: BatchConfig | None = None,
) -> BatchResult[TResult]:
    """
    Process items in parallel batches.

    Args:
        items: Items to process.
        func: Function to apply to each item.
        config: Batch processing configuration.

    Returns:
        BatchResult with all results and errors.
    """
    config = config or BatchConfig()
    start_time = time.time()
    results: list[TResult | None] = [None] * len(items)
    errors: list[tuple[int, str]] = []

    def process_item(idx: int, item: T) -> tuple[int, TResult | None, str | None]:
        try:
            return (idx, func(item), None)
        except Exception as e:
            return (idx, None, str(e))

    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        futures = [
            executor.submit(process_item, idx, item)
            for idx, item in enumerate(items)
        ]

        for future in futures:
            idx, result, error = future.result()
            if error:
                errors.append((idx, error))
            else:
                results[idx] = result  # type: ignore

    final_results = [r for r in results if r is not None]  # type: ignore

    return BatchResult(
        success=len(errors) == 0,
        results=final_results,  # type: ignore
        errors=errors,
        total_time_ms=(time.time() - start_time) * 1000,
        processed_count=len(items),
    )


async def batch_process_async(
    items: Sequence[T],
    func: Callable[[T], TResult],
    config: BatchConfig | None = None,
) -> BatchResult[TResult]:
    """
    Async batch processing with concurrency control.

    Args:
        items: Items to process.
        func: Async function to apply to each item.
        config: Batch processing configuration.

    Returns:
        BatchResult with all results and errors.
    """
    config = config or BatchConfig()
    start_time = time.time()
    results: list[TResult] = []
    errors: list[tuple[int, str]] = []

    semaphore = asyncio.Semaphore(config.max_workers)

    async def process_with_semaphore(idx: int, item: T) -> tuple[int, TResult | None, str | None]:
        async with semaphore:
            try:
                result = await func(item)
                return (idx, result, None)
            except Exception as e:
                return (idx, None, str(e))

    tasks = [
        process_with_semaphore(idx, item)
        for idx, item in enumerate(items)
    ]

    completed = await asyncio.gather(*tasks, return_exceptions=True)

    for item in completed:
        if isinstance(item, Exception):
            errors.append((-1, str(item)))
        else:
            idx, result, error = item
            if error:
                errors.append((idx, error))
            else:
                results.append(result)  # type: ignore

    return BatchResult(
        success=len(errors) == 0,
        results=results,
        errors=errors,
        total_time_ms=(time.time() - start_time) * 1000,
        processed_count=len(items),
    )


class BatchProcessor(Generic[T]):
    """
    Configurable batch processor.
    """

    def __init__(self, func: Callable[[T], TResult], config: BatchConfig | None = None):
        self._func = func
        self._config = config or BatchConfig()

    def process(self, items: Sequence[T]) -> BatchResult[TResult]:
        """Process items synchronously."""
        return batch_process(items, self._func, self._config)

    def process_parallel(self, items: Sequence[T]) -> BatchResult[TResult]:
        """Process items in parallel."""
        return batch_process_parallel(items, self._func, self._config)

    async def process_async(self, items: Sequence[T]) -> BatchResult[TResult]:
        """Process items asynchronously."""
        return await batch_process_async(items, self._func, self._config)


@dataclass
class StreamingBatchConfig:
    """Configuration for streaming batch processing."""
    batch_size: int = 100
    max_buffer_size: int = 1000
    flush_interval_seconds: float = 1.0


class StreamingBatchProcessor(Generic[T, TResult]):
    """
    Processor that accumulates items and processes them in batches.
    """

    def __init__(
        self,
        func: Callable[[T], TResult],
        config: StreamingBatchConfig | None = None,
    ):
        self._func = func
        self._config = config or StreamingBatchConfig()
        self._buffer: list[T] = []
        self._results: list[TResult] = []

    def add(self, item: T) -> list[TResult] | None:
        """
        Add an item to the buffer.
        Returns results if batch was processed.
        """
        self._buffer.append(item)

        if len(self._buffer) >= self._config.batch_size:
            return self._flush()

        return None

    def _flush(self) -> list[TResult]:
        """Flush the buffer and process."""
        if not self._buffer:
            return []

        results = batch_process(self._buffer, self._func).results
        self._results.extend(results)
        self._buffer.clear()
        return results

    def finalize(self) -> list[TResult]:
        """Process any remaining items in the buffer."""
        results = self._flush()
        self._results.extend(results)
        return results

    @property
    def total_processed(self) -> int:
        """Total number of items processed."""
        return len(self._results)

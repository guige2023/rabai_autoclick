"""
Async Batch Processing Utilities for UI Automation.

This module provides utilities for batch processing async tasks,
including rate limiting, chunking, and result aggregation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Callable, Awaitable, List, Optional, TypeVar, Any, Generic
from enum import Enum
import time


T = TypeVar("T")
R = TypeVar("R")


class BatchStrategy(Enum):
    """Batch processing strategies."""
    FIXED_SIZE = "fixed_size"
    TIME_WINDOW = "time_window"
    ADAPTIVE = "adaptive"


@dataclass
class BatchResult(Generic[T]):
    """Result of a batch operation."""
    items: List[T]
    start_time: float
    end_time: float
    duration: float
    errors: List[Exception] = field(default_factory=list)

    @property
    def success_count(self) -> int:
        return len(self.items)

    @property
    def error_count(self) -> int:
        return len(self.errors)


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    batch_size: int = 10
    max_concurrent: int = 5
    timeout: float = 30.0
    strategy: BatchStrategy = BatchStrategy.FIXED_SIZE
    time_window_seconds: float = 1.0
    retry_attempts: int = 0
    retry_delay: float = 0.5


class AsyncBatchProcessor(Generic[T, R]):
    """
    Async batch processor with configurable strategies.
    """

    def __init__(self, config: Optional[BatchConfig] = None):
        """
        Initialize batch processor.

        Args:
            config: Batch processing configuration
        """
        self.config = config or BatchConfig()
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._results: List[R] = []
        self._errors: List[Exception] = []

    async def process(
        self,
        items: List[T],
        processor: Callable[[T], Awaitable[R]],
    ) -> BatchResult[R]:
        """
        Process items in batches.

        Args:
            items: Items to process
            processor: Async function to process each item

        Returns:
            BatchResult with processed items
        """
        self._results.clear()
        self._errors.clear()
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent)

        start_time = time.time()
        batches = self._create_batches(items)

        async def process_batch(batch: List[T]) -> List[R]:
            """Process a single batch."""
            tasks = [self._process_with_semaphore(item, processor) for item in batch]
            return await asyncio.gather(*tasks, return_exceptions=True)

        all_results = []
        for batch in batches:
            batch_results = await process_batch(batch)
            for result in batch_results:
                if isinstance(result, Exception):
                    self._errors.append(result)
                else:
                    all_results.append(result)

        end_time = time.time()
        return BatchResult(
            items=all_results,
            start_time=start_time,
            end_time=end_time,
            duration=end_time - start_time,
            errors=self._errors.copy()
        )

    async def _process_with_semaphore(
        self,
        item: T,
        processor: Callable[[T], Awaitable[R]]
    ) -> R:
        """Process item with semaphore control."""
        async with self._semaphore:
            return await asyncio.wait_for(processor(item), timeout=self.config.timeout)

    def _create_batches(self, items: List[T]) -> List[List[T]]:
        """Create batches based on strategy."""
        if self.config.strategy == BatchStrategy.FIXED_SIZE:
            return self._fixed_size_batches(items)
        elif self.config.strategy == BatchStrategy.TIME_WINDOW:
            return self._time_window_batches(items)
        else:
            return self._fixed_size_batches(items)

    def _fixed_size_batches(self, items: List[T]) -> List[List[T]]:
        """Create fixed-size batches."""
        batches = []
        for i in range(0, len(items), self.config.batch_size):
            batches.append(items[i:i + self.config.batch_size])
        return batches

    def _time_window_batches(self, items: List[T]) -> List[List[T]]:
        """Create time-window batches (all items in one batch for simplicity)."""
        return [items]


async def batch_map_async(
    items: List[T],
    func: Callable[[T], Awaitable[R]],
    max_concurrent: int = 5,
    timeout: float = 30.0
) -> List[R]:
    """
    Map async function over items with concurrency limit.

    Args:
        items: Items to process
        func: Async function to apply
        max_concurrent: Maximum concurrent tasks
        timeout: Timeout per item

    Returns:
        List of results in same order as items
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    async def bounded_func(item: T) -> R:
        async with semaphore:
            return await asyncio.wait_for(func(item), timeout=timeout)

    return await asyncio.gather(*[bounded_func(item) for item in items])


async def batch_process_with_retry(
    items: List[T],
    processor: Callable[[T], Awaitable[R]],
    attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0
) -> List[R]:
    """
    Process items with retry logic.

    Args:
        items: Items to process
        processor: Async processor function
        attempts: Number of retry attempts
        delay: Initial delay between retries
        backoff: Backoff multiplier

    Returns:
        List of results
    """
    results = []
    for item in items:
        last_error = None
        current_delay = delay
        for attempt in range(attempts):
            try:
                result = await processor(item)
                results.append(result)
                break
            except Exception as e:
                last_error = e
                if attempt < attempts - 1:
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
        else:
            if last_error:
                raise last_error
    return results


@dataclass
class ChunkResult:
    """Result of chunk processing."""
    chunk_index: int
    items_processed: int
    items_failed: int
    duration: float
    errors: List[str] = field(default_factory=list)


async def process_in_chunks(
    items: List[T],
    chunk_size: int,
    processor: Callable[[List[T]], Awaitable[List[R]]]
) -> List[ChunkResult]:
    """
    Process items in chunks.

    Args:
        items: Items to process
        chunk_size: Size of each chunk
        processor: Async function that processes a chunk

    Returns:
        List of ChunkResult for each chunk
    """
    results = []
    for i in range(0, len(items), chunk_size):
        chunk = items[i:i + chunk_size]
        chunk_index = i // chunk_size
        start = time.time()
        try:
            await processor(chunk)
            duration = time.time() - start
            results.append(ChunkResult(
                chunk_index=chunk_index,
                items_processed=len(chunk),
                items_failed=0,
                duration=duration
            ))
        except Exception as e:
            duration = time.time() - start
            results.append(ChunkResult(
                chunk_index=chunk_index,
                items_processed=0,
                items_failed=len(chunk),
                duration=duration,
                errors=[str(e)]
            ))
    return results

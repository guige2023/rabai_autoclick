"""Batch Processor Action Module.

Efficient batch processing with configurable parallelism and retry.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar
import time

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    batch_size: int = 100
    max_concurrency: int = 10
    retry_attempts: int = 3
    retry_delay: float = 1.0
    timeout_seconds: float = 300.0


@dataclass
class BatchResult(Generic[R]):
    """Result of batch processing."""
    successful: list[R] = field(default_factory=list)
    failed: list[tuple[T, Exception]] = field(default_factory=list)
    total_processed: int = 0
    total_time: float = 0.0


@dataclass
class BatchProgress:
    """Progress tracking for batch operations."""
    total_items: int
    processed: int = 0
    successful: int = 0
    failed: int = 0
    start_time: float = field(default_factory=time.monotonic)


class BatchProcessor(Generic[T, R]):
    """Batch processor with parallelism and error handling."""

    def __init__(self, config: BatchConfig | None = None) -> None:
        self.config = config or BatchConfig()
        self.progress_callback: Callable[[BatchProgress], None] | None = None

    def set_progress_callback(self, callback: Callable[[BatchProgress], None]) -> None:
        """Set callback for progress updates."""
        self.progress_callback = callback

    async def process(
        self,
        items: list[T],
        processor: Callable[[T], R | asyncio.coroutine],
    ) -> BatchResult[R]:
        """Process items in batches with configurable parallelism."""
        result = BatchResult[R]()
        start = time.monotonic()
        progress = BatchProgress(total_items=len(items))
        semaphore = asyncio.Semaphore(self.config.max_concurrency)
        retry_count = {}

        async def process_item(item: T, index: int) -> tuple[int, R | Exception, int]:
            async with semaphore:
                attempts = 0
                last_error: Exception | None = None
                while attempts < self.config.retry_attempts:
                    try:
                        if asyncio.iscoroutinefunction(processor):
                            output = await processor(item)
                        else:
                            output = processor(item)
                        return (index, output, 0)
                    except Exception as e:
                        last_error = e
                        attempts += 1
                        if attempts < self.config.retry_attempts:
                            await asyncio.sleep(self.config.retry_delay * attempts)
                return (index, last_error or Exception("Unknown error"), attempts)

        tasks = [process_item(item, i) for i, item in enumerate(items)]
        for coro in asyncio.as_completed(tasks):
            idx, output, attempts = await coro
            progress.processed += 1
            if isinstance(output, Exception):
                progress.failed += 1
                result.failed.append((items[idx], output))
            else:
                progress.successful += 1
                result.successful.append(output)
            if self.progress_callback:
                self.progress_callback(progress)

        result.total_processed = len(items)
        result.total_time = time.monotonic() - start
        return result

    async def process_batched(
        self,
        items: list[T],
        processor: Callable[[list[T]], list[R] | asyncio.coroutine],
    ) -> BatchResult[R]:
        """Process items in fixed-size batches."""
        result = BatchResult[R]()
        start = time.monotonic()
        batch_size = self.config.batch_size
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            try:
                if asyncio.iscoroutinefunction(processor):
                    outputs = await processor(batch)
                else:
                    outputs = processor(batch)
                result.successful.extend(outputs)
            except Exception as e:
                for item in batch:
                    result.failed.append((item, e))
        result.total_processed = len(items)
        result.total_time = time.monotonic() - start
        return result

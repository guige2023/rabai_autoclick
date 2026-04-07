"""
Batch processing utilities for bulk operations.

Provides chunking, parallel processing, progress tracking,
error aggregation, and result merging.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Coroutine, Optional

logger = logging.getLogger(__name__)


class ProcessingMode(Enum):
    SEQUENTIAL = auto()
    PARALLEL = auto()
    CHUNKED = auto()


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    chunk_size: int = 100
    max_concurrency: int = 10
    timeout_per_item: float = 30.0
    stop_on_error: bool = False
    retry_count: int = 0
    retry_delay: float = 1.0


@dataclass
class BatchResult:
    """Result of a batch operation."""
    total: int
    successful: int
    failed: int
    results: list[Any]
    errors: list[tuple[int, str]] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def success_rate(self) -> float:
        if self.total == 0:
            return 0.0
        return self.successful / self.total * 100


@dataclass
class ProcessingProgress:
    """Progress tracking for batch operations."""
    processed: int = 0
    successful: int = 0
    failed: int = 0
    started_at: float = field(default_factory=time.time)
    estimated_remaining: float = 0.0

    @property
    def percentage(self) -> float:
        return 0.0

    @property
    def items_per_second(self) -> float:
        elapsed = time.time() - self.started_at
        return self.processed / elapsed if elapsed > 0 else 0.0


class BatchProcessor:
    """Generic batch processor with multiple processing modes."""

    def __init__(self, config: Optional[BatchConfig] = None) -> None:
        self.config = config or BatchConfig()
        self._progress_callback: Optional[Callable[[ProcessingProgress], None]] = None

    def on_progress(self, callback: Callable[[ProcessingProgress], None]) -> "BatchProcessor":
        """Register a progress callback."""
        self._progress_callback = callback
        return self

    def process(
        self,
        items: list[Any],
        processor: Callable[[Any], Any],
        mode: ProcessingMode = ProcessingMode.CHUNKED,
    ) -> BatchResult:
        """Process items in batch mode."""
        start = time.perf_counter()
        results = []
        errors = []
        successful = 0
        failed = 0

        if mode == ProcessingMode.SEQUENTIAL:
            for i, item in enumerate(items):
                try:
                    result = processor(item)
                    results.append(result)
                    successful += 1
                except Exception as e:
                    errors.append((i, str(e)))
                    failed += 1
                    if self.config.stop_on_error:
                        break

        elif mode == ProcessingMode.PARALLEL:
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_concurrency) as executor:
                futures = {executor.submit(processor, item): i for i, item in enumerate(items)}
                for future in concurrent.futures.as_completed(futures):
                    i = futures[future]
                    try:
                        result = future.result(timeout=self.config.timeout_per_item)
                        results.append(result)
                        successful += 1
                    except Exception as e:
                        errors.append((i, str(e)))
                        failed += 1

        elif mode == ProcessingMode.CHUNKED:
            chunks = self._chunk_list(items, self.config.chunk_size)
            for chunk_idx, chunk in enumerate(chunks):
                chunk_start = chunk_idx * self.config.chunk_size
                for i, item in enumerate(chunk):
                    global_idx = chunk_start + i
                    try:
                        result = processor(item)
                        results.append(result)
                        successful += 1
                    except Exception as e:
                        errors.append((global_idx, str(e)))
                        failed += 1

        return BatchResult(
            total=len(items),
            successful=successful,
            failed=failed,
            results=results,
            errors=errors,
            duration_seconds=time.perf_counter() - start,
        )

    async def process_async(
        self,
        items: list[Any],
        processor: Callable[..., Coroutine[Any, Any, Any]],
        mode: ProcessingMode = ProcessingMode.CHUNKED,
    ) -> BatchResult:
        """Process items asynchronously."""
        start = time.perf_counter()
        results = []
        errors = []
        successful = 0
        failed = 0

        if mode == ProcessingMode.PARALLEL:
            semaphore = asyncio.Semaphore(self.config.max_concurrency)

            async def process_with_semaphore(item: Any, idx: int) -> tuple[int, Any, Optional[str]]:
                async with semaphore:
                    try:
                        result = await asyncio.wait_for(processor(item), timeout=self.config.timeout_per_item)
                        return idx, result, None
                    except Exception as e:
                        return idx, None, str(e)

            tasks = [process_with_semaphore(item, i) for i, item in enumerate(items)]
            task_results = await asyncio.gather(*tasks)

            for idx, result, error in task_results:
                if error:
                    errors.append((idx, error))
                    failed += 1
                else:
                    results.append(result)
                    successful += 1

        elif mode == ProcessingMode.CHUNKED:
            chunks = self._chunk_list(items, self.config.chunk_size)
            for chunk_idx, chunk in enumerate(chunks):
                chunk_start = chunk_idx * self.config.chunk_size
                for i, item in enumerate(chunk):
                    global_idx = chunk_start + i
                    try:
                        result = await asyncio.wait_for(processor(item), timeout=self.config.timeout_per_item)
                        results.append(result)
                        successful += 1
                    except Exception as e:
                        errors.append((global_idx, str(e)))
                        failed += 1

        return BatchResult(
            total=len(items),
            successful=successful,
            failed=failed,
            results=results,
            errors=errors,
            duration_seconds=time.perf_counter() - start,
        )

    def _chunk_list(self, items: list[Any], chunk_size: int) -> list[list[Any]]:
        """Split a list into chunks."""
        return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


class BulkUpsertProcessor:
    """Batch upsert processor for database operations."""

    def __init__(self, batch_size: int = 100) -> None:
        self.batch_size = batch_size

    def group_by_operation(self, items: list[dict[str, Any]], key_field: str = "id") -> tuple[list[dict], list[dict]]:
        """Group items into inserts and updates based on presence of key field."""
        inserts = []
        updates = []
        for item in items:
            if key_field in item and item[key_field]:
                updates.append(item)
            else:
                inserts.append(item)
        return inserts, updates

    def create_batches(self, items: list[Any]) -> list[list[Any]]:
        """Create batches from items."""
        return [items[i:i + self.batch_size] for i in range(0, len(items), self.batch_size)]

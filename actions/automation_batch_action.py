"""
Automation Batch Action Module.

Provides batch processing capabilities with chunking,
parallel execution, progress tracking, and error recovery.
"""

import asyncio
import threading
import time
from typing import Optional, Callable, Any, List, Dict, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
from collections import deque


class BatchStrategy(Enum):
    """Batch processing strategies."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CHUNKED_PARALLEL = "chunked_parallel"
    WORK_QUEUE = "work_queue"


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    strategy: BatchStrategy = BatchStrategy.SEQUENTIAL
    chunk_size: int = 100
    max_parallel: int = 4
    max_retries: int = 2
    retry_delay: float = 1.0
    timeout: Optional[float] = None
    fail_fast: bool = False
    progress_interval: int = 10  # report progress every N items


@dataclass
class BatchResult:
    """Result of batch processing."""
    total: int
    successful: int
    failed: int
    skipped: int
    results: List[Any]
    errors: List[Dict[str, Any]]
    duration_ms: float
    items_per_second: float


@dataclass
class BatchProgress:
    """Progress tracking for batch operations."""
    total: int
    processed: int
    successful: int
    failed: int
    skipped: int
    current_chunk: int
    start_time: float
    estimated_remaining_seconds: float = 0.0


class AutomationBatchAction:
    """
    Batch processing action with multiple execution strategies.

    Supports sequential, parallel, and chunked parallel processing
    with comprehensive progress tracking and error handling.
    """

    def __init__(self, config: Optional[BatchConfig] = None):
        self.config = config or BatchConfig()
        self._progress_callbacks: List[Callable[[BatchProgress], None]] = []
        self._lock = threading.Lock()

    def on_progress(
        self,
        callback: Callable[[BatchProgress], None],
    ) -> "AutomationBatchAction":
        """Register progress callback."""
        self._progress_callbacks.append(callback)
        return self

    def _report_progress(
        self,
        progress: BatchProgress,
    ) -> None:
        """Report progress to callbacks."""
        for callback in self._progress_callbacks:
            try:
                callback(progress)
            except Exception:
                pass

    def _calculate_progress(
        self,
        total: int,
        processed: int,
        successful: int,
        failed: int,
        skipped: int,
        start_time: float,
        current_chunk: int,
    ) -> BatchProgress:
        """Calculate current progress."""
        elapsed = time.time() - start_time
        if processed > 0:
            rate = processed / elapsed
            remaining = total - processed
            estimated_remaining = remaining / rate if rate > 0 else 0
        else:
            estimated_remaining = 0

        return BatchProgress(
            total=total,
            processed=processed,
            successful=successful,
            failed=failed,
            skipped=skipped,
            current_chunk=current_chunk,
            start_time=start_time,
            estimated_remaining_seconds=estimated_remaining,
        )

    def _chunks(self, items: List[Any], chunk_size: int) -> List[List[Any]]:
        """Split items into chunks."""
        return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]

    async def _process_item_async(
        self,
        item: Any,
        processor: Callable[[Any], Any],
        retries: int = 0,
    ) -> tuple[bool, Any, Optional[Exception]]:
        """Process a single item asynchronously."""
        last_error = None

        for attempt in range(retries + 1):
            try:
                result = processor(item)
                if asyncio.iscoroutine(result):
                    result = await result
                return True, result, None
            except Exception as e:
                last_error = e
                if attempt < retries:
                    await asyncio.sleep(self.config.retry_delay)

        return False, None, last_error

    async def process_async(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
    ) -> BatchResult:
        """
        Process items in batch asynchronously.

        Args:
            items: List of items to process
            processor: Function to process each item

        Returns:
            BatchResult with processing outcomes
        """
        start_time = time.time()
        total = len(items)
        results: List[Any] = []
        errors: List[Dict[str, Any]] = []
        successful = 0
        failed = 0
        skipped = 0
        processed = 0

        strategy = self.config.strategy

        if strategy == BatchStrategy.SEQUENTIAL:
            for i, item in enumerate(items):
                success, result, error = await self._process_item_async(
                    item, processor, self.config.max_retries
                )
                processed += 1

                if success:
                    results.append(result)
                    successful += 1
                elif error is None:
                    skipped += 1
                else:
                    errors.append({
                        "index": i,
                        "item": item,
                        "error": str(error),
                    })
                    failed += 1
                    if self.config.fail_fast:
                        break

                # Progress reporting
                if processed % self.config.progress_interval == 0:
                    progress = self._calculate_progress(
                        total, processed, successful, failed, skipped,
                        start_time, 0
                    )
                    self._report_progress(progress)

        elif strategy == BatchStrategy.PARALLEL:
            tasks = []
            for i, item in enumerate(items):
                task = asyncio.create_task(
                    self._process_item_async(
                        item, processor, self.config.max_retries
                    )
                )
                tasks.append((i, item, task))

            for i, item, task in tasks:
                success, result, error = await task
                processed += 1

                if success:
                    results.append(result)
                    successful += 1
                else:
                    errors.append({
                        "index": i,
                        "item": item,
                        "error": str(error) if error else "skipped",
                    })
                    failed += 1

                if processed % self.config.progress_interval == 0:
                    progress = self._calculate_progress(
                        total, processed, successful, failed, skipped,
                        start_time, 0
                    )
                    self._report_progress(progress)

        elif strategy == BatchStrategy.CHUNKED_PARALLEL:
            chunks = self._chunks(items, self.config.chunk_size)
            max_parallel = min(self.config.max_parallel, len(chunks))

            for chunk_idx, chunk in enumerate(chunks):
                # Process chunk with limited parallelism
                semaphore = asyncio.Semaphore(max_parallel)

                async def process_with_semaphore(item, idx):
                    async with semaphore:
                        return await self._process_item_async(
                            item, processor, self.config.max_retries
                        )

                tasks = [
                    asyncio.create_task(process_with_semaphore(item, i))
                    for i, item in enumerate(chunk)
                ]

                chunk_results = await asyncio.gather(*tasks)

                for local_idx, (success, result, error) in enumerate(chunk_results):
                    processed += 1
                    global_idx = chunk_idx * self.config.chunk_size + local_idx

                    if success:
                        results.append(result)
                        successful += 1
                    else:
                        errors.append({
                            "index": global_idx,
                            "item": items[global_idx],
                            "error": str(error) if error else "skipped",
                        })
                        failed += 1

                # Progress reporting
                progress = self._calculate_progress(
                    total, processed, successful, failed, skipped,
                    start_time, chunk_idx + 1
                )
                self._report_progress(progress)

                if self.config.fail_fast and failed > 0:
                    break

        # Final statistics
        duration_ms = (time.time() - start_time) * 1000
        items_per_second = processed / (duration_ms / 1000) if duration_ms > 0 else 0

        return BatchResult(
            total=total,
            successful=successful,
            failed=failed,
            skipped=skipped,
            results=results,
            errors=errors,
            duration_ms=duration_ms,
            items_per_second=items_per_second,
        )

    def process(
        self,
        items: List[Any],
        processor: Callable[[Any], Any],
    ) -> BatchResult:
        """Process items in batch (sync version)."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                future = asyncio.run_coroutine_threadsafe(
                    self.process_async(items, processor), loop
                )
                return future.result(timeout=self.config.timeout)
            return asyncio.run(self.process_async(items, processor))
        except Exception as e:
            return BatchResult(
                total=len(items),
                successful=0,
                failed=len(items),
                skipped=0,
                results=[],
                errors=[{"error": str(e)}],
                duration_ms=0,
                items_per_second=0,
            )

    async def map_async(
        self,
        items: List[Any],
        mapper: Callable[[Any], Any],
    ) -> List[Any]:
        """Map function over items, returning all results."""
        result = await self.process_async(items, mapper)
        return result.results

    def map(
        self,
        items: List[Any],
        mapper: Callable[[Any], Any],
    ) -> List[Any]:
        """Map function over items (sync version)."""
        batch_result = self.process(items, mapper)
        return batch_result.results

    async def filter_async(
        self,
        items: List[Any],
        predicate: Callable[[Any], bool],
    ) -> List[Any]:
        """Filter items using async predicate."""
        async def filter_wrapper(item):
            result = predicate(item)
            if asyncio.iscoroutine(result):
                result = await result
            return result, item

        tasks = [filter_wrapper(item) for item in items]
        results = await asyncio.gather(*tasks)
        return [item for passed, item in results if passed]

    def filter(
        self,
        items: List[Any],
        predicate: Callable[[Any], bool],
    ) -> List[Any]:
        """Filter items (sync version)."""
        return [item for item in items if predicate(item)]


class BatchProcessor:
    """Helper for creating batch processors with common configurations."""

    @staticmethod
    def small_jobs(config: Optional[BatchConfig] = None) -> BatchConfig:
        """Configuration for small, quick jobs."""
        return config or BatchConfig(
            strategy=BatchStrategy.PARALLEL,
            chunk_size=10,
            max_parallel=8,
            progress_interval=5,
        )

    @staticmethod
    def large_jobs(config: Optional[BatchConfig] = None) -> BatchConfig:
        """Configuration for large, long-running jobs."""
        return config or BatchConfig(
            strategy=BatchStrategy.CHUNKED_PARALLEL,
            chunk_size=100,
            max_parallel=4,
            progress_interval=50,
            fail_fast=False,
        )

    @staticmethod
    def critical_jobs(config: Optional[BatchConfig] = None) -> BatchConfig:
        """Configuration for critical jobs requiring all successes."""
        return config or BatchConfig(
            strategy=BatchStrategy.SEQUENTIAL,
            max_retries=3,
            fail_fast=True,
            progress_interval=1,
        )

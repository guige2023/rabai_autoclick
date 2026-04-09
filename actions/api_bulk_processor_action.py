"""
Bulk API processor with chunking, batching, and parallel execution.

This module handles high-volume API operations by splitting large datasets
into manageable chunks and processing them with configurable concurrency.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from enum import Enum, auto
from concurrent.futures import ThreadPoolExecutor
import json

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class ProcessingMode(Enum):
    """Bulk processing mode."""
    SEQUENTIAL = auto()
    PARALLEL = auto()
    BATCHED = auto()


@dataclass
class BulkOperationResult:
    """Result of a bulk operation."""
    total_items: int
    successful_items: int
    failed_items: int
    results: List[Any]
    errors: List[Dict[str, Any]]
    duration: float
    items_perSecond: float = field(init=False)

    def __post_init(self):
        self.items_per_second = (
            self.total_items / self.duration if self.duration > 0 else 0
        )

    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_items == 0:
            return 0.0
        return (self.successful_items / self.total_items) * 100


@dataclass
class BulkConfig:
    """Configuration for bulk processing."""
    chunk_size: int = 100
    max_concurrency: int = 4
    retry_count: int = 3
    retry_delay: float = 1.0
    timeout: float = 300.0
    progress_callback: Optional[Callable[[int, int], None]] = None


class BulkProcessor:
    """
    Process large datasets through API endpoints with chunking and concurrency.

    Features:
    - Configurable chunk sizes
    - Multiple processing modes (sequential, parallel, batched)
    - Automatic retry with backoff
    - Progress tracking
    - Error collection and reporting
    - Thread-safe execution

    Example:
        >>> def api_call(item):
        ...     return requests.post("https://api.example.com", json=item)
        >>> processor = BulkProcessor(api_call)
        >>> result = processor.process(items, chunk_size=50, max_concurrency=4)
        >>> print(f"Processed {result.successful_items}/{result.total_items}")
    """

    def __init__(
        self,
        operation: Callable[[Any], Any],
        config: Optional[BulkConfig] = None,
    ):
        """
        Initialize bulk processor.

        Args:
            operation: Function to apply to each item
            config: Bulk processing configuration
        """
        self.operation = operation
        self.config = config or BulkConfig()
        self._executor: Optional[ThreadPoolExecutor] = None
        logger.info(
            f"BulkProcessor initialized (chunk_size={self.config.chunk_size}, "
            f"concurrency={self.config.max_concurrency})"
        )

    def process(
        self,
        items: List[T],
        mode: ProcessingMode = ProcessingMode.PARALLEL,
    ) -> BulkOperationResult:
        """
        Process items in bulk.

        Args:
            items: List of items to process
            mode: Processing mode to use
            config: Optional config override

        Returns:
            BulkOperationResult with processing statistics
        """
        if not items:
            return BulkOperationResult(
                total_items=0,
                successful_items=0,
                failed_items=0,
                results=[],
                errors=[],
                duration=0.0,
            )

        start_time = time.time()
        results: List[Any] = []
        errors: List[Dict[str, Any]] = []

        self._executor = ThreadPoolExecutor(
            max_workers=self.config.max_concurrency
        )

        try:
            if mode == ProcessingMode.SEQUENTIAL:
                results, errors = self._process_sequential(items)
            elif mode == ProcessingMode.PARALLEL:
                results, errors = self._process_parallel(items)
            elif mode == ProcessingMode.BATCHED:
                results, errors = self._process_batched(items)
        finally:
            self._executor.shutdown(wait=True)
            self._executor = None

        duration = time.time() - start_time
        successful = len(results)

        logger.info(
            f"Bulk processing completed: {successful}/{len(items)} successful "
            f"in {duration:.2f}s ({successful/duration:.1f} items/s)"
        )

        return BulkOperationResult(
            total_items=len(items),
            successful_items=successful,
            failed_items=len(errors),
            results=results,
            errors=errors,
            duration=duration,
        )

    def _process_sequential(
        self,
        items: List[T],
    ) -> tuple[List[Any], List[Dict[str, Any]]]:
        """Process items one at a time."""
        results = []
        errors = []

        for i, item in enumerate(items):
            success, result, error = self._execute_with_retry(item)
            if success:
                results.append(result)
            else:
                errors.append({"index": i, "item": item, "error": str(error)})

            if self.config.progress_callback:
                self.config.progress_callback(i + 1, len(items))

        return results, errors

    def _process_parallel(
        self,
        items: List[T],
    ) -> tuple[List[Any], List[Dict[str, Any]]]:
        """Process items in parallel with thread pool."""
        results: List[Any] = [None] * len(items)
        errors: List[Dict[str, Any]] = []

        futures = {}
        for i, item in enumerate(items):
            future = self._executor.submit(self._execute_with_retry, item)
            futures[future] = i

        completed = 0
        for future in asyncio.as_completed(
            [asyncio.wrap_future(f) for f in futures.keys()]
        ):
            i = futures[future.obj] if hasattr(future.obj, 'obj') else futures.get(id(future.obj), completed]
            try:
                success, result, error = future.result()
                if success:
                    results[i] = result
                else:
                    errors.append({"index": i, "item": items[i], "error": str(error)})
            except Exception as e:
                errors.append({"index": i, "item": items[i], "error": str(e)})

            completed += 1
            if self.config.progress_callback:
                self.config.progress_callback(completed, len(items))

        return [r for r in results if r is not None], errors

    def _process_batched(
        self,
        items: List[T],
    ) -> tuple[List[Any], List[Dict[str, Any]]]:
        """Process items in chunks."""
        results = []
        errors = []
        chunks = self._create_chunks(items, self.config.chunk_size)

        for chunk_idx, chunk in enumerate(chunks):
            logger.debug(f"Processing chunk {chunk_idx + 1}/{len(chunks)}")
            chunk_results, chunk_errors = self._process_chunk(chunk, chunk_idx)
            results.extend(chunk_results)
            errors.extend(chunk_errors)

            if self.config.progress_callback:
                processed = min(
                    (chunk_idx + 1) * self.config.chunk_size, len(items)
                )
                self.config.progress_callback(processed, len(items))

        return results, errors

    def _process_chunk(
        self,
        chunk: List[T],
        chunk_idx: int,
    ) -> tuple[List[Any], List[Dict[str, Any]]]:
        """Process a single chunk."""
        results = []
        errors = []

        for i, item in enumerate(chunk):
            global_idx = chunk_idx * self.config.chunk_size + i
            success, result, error = self._execute_with_retry(item)
            if success:
                results.append(result)
            else:
                errors.append({"index": global_idx, "item": item, "error": str(error)})

        return results, errors

    def _execute_with_retry(
        self,
        item: T,
    ) -> tuple[bool, Optional[Any], Optional[Exception]]:
        """Execute operation with retry logic."""
        last_error = None

        for attempt in range(self.config.retry_count + 1):
            try:
                result = self.operation(item)
                return True, result, None
            except Exception as e:
                last_error = e
                if attempt < self.config.retry_count:
                    sleep_time = self.config.retry_delay * (2 ** attempt)
                    logger.debug(f"Retry {attempt + 1}/{self.config.retry_count} after {sleep_time}s")
                    time.sleep(sleep_time)

        return False, None, last_error

    def _create_chunks(
        self,
        items: List[T],
        chunk_size: int,
    ) -> List[List[T]]:
        """Split items into chunks."""
        return [
            items[i:i + chunk_size]
            for i in range(0, len(items), chunk_size)
        ]

    async def process_async(
        self,
        items: List[T],
        mode: ProcessingMode = ProcessingMode.PARALLEL,
    ) -> BulkOperationResult:
        """
        Async version of process.

        Args:
            items: List of items to process
            mode: Processing mode to use

        Returns:
            BulkOperationResult with processing statistics
        """
        if not items:
            return BulkOperationResult(
                total_items=0,
                successful_items=0,
                failed_items=0,
                results=[],
                errors=[],
                duration=0.0,
            )

        start_time = time.time()
        semaphore = asyncio.Semaphore(self.config.max_concurrency)

        async def process_item(item: T, index: int) -> tuple[int, Any, Optional[Exception], Any]:
            """Process a single item with semaphore."""
            async with semaphore:
                for attempt in range(self.config.retry_count + 1):
                    try:
                        loop = asyncio.get_event_loop()
                        result = await loop.run_in_executor(None, self.operation, item)
                        return index, result, None, item
                    except Exception as e:
                        last_error = e
                        if attempt < self.config.retry_count:
                            await asyncio.sleep(self.config.retry_delay * (2 ** attempt))
            return index, None, last_error, item

        tasks = [process_item(item, i) for i, item in enumerate(items)]
        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        results = []
        errors = []

        for i, result in enumerate(task_results):
            if isinstance(result, Exception):
                errors.append({"index": i, "item": items[i], "error": str(result)})
            else:
                idx, res, err, item = result
                if err is None:
                    results.append(res)
                else:
                    errors.append({"index": idx, "item": item, "error": str(err)})

        duration = time.time() - start_time
        successful = len(results)

        logger.info(
            f"Bulk async processing completed: {successful}/{len(items)} successful "
            f"in {duration:.2f}s"
        )

        return BulkOperationResult(
            total_items=len(items),
            successful_items=successful,
            failed_items=len(errors),
            results=results,
            errors=errors,
            duration=duration,
        )

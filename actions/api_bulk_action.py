"""Bulk API operations with chunking and batching.

This module provides bulk operation support:
- Chunk large datasets
- Batch API calls
- Parallel execution
- Progress tracking

Example:
    >>> from actions.api_bulk_action import BulkProcessor
    >>> processor = BulkProcessor(chunk_size=100)
    >>> results = processor.process_bulk(api_call, dataset)
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


@dataclass
class BulkResult:
    """Result of a bulk operation."""
    total: int
    succeeded: int
    failed: int
    results: list[Any] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)
    duration: float = 0.0


@dataclass
class ChunkResult:
    """Result of processing a single chunk."""
    chunk_index: int
    succeeded: int
    failed: int
    results: list[Any] = field(default_factory=list)
    errors: list[dict[str, Any]] = field(default_factory=list)


class BulkProcessor:
    """Process bulk API operations.

    Example:
        >>> processor = BulkProcessor(chunk_size=50, max_workers=4)
        >>> result = processor.process(api_func, items)
    """

    def __init__(
        self,
        chunk_size: int = 100,
        max_workers: int = 5,
        continue_on_error: bool = True,
    ) -> None:
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        self.continue_on_error = continue_on_error
        self._progress_callback: Optional[Callable[[int, int], None]] = None

    def set_progress_callback(
        self,
        callback: Callable[[int, int], None],
    ) -> None:
        """Set progress callback.

        Args:
            callback: Function(completed, total) called on progress.
        """
        self._progress_callback = callback

    def process_bulk(
        self,
        func: Callable[..., Any],
        items: list[Any],
        *args: Any,
        **kwargs: Any,
    ) -> BulkResult:
        """Process items in bulk.

        Args:
            func: Function to apply to each item.
            items: List of items to process.
            *args: Additional positional args for func.
            **kwargs: Additional keyword args for func.

        Returns:
            BulkResult with all results and errors.
        """
        start_time = time.time()
        chunks = self._split_into_chunks(items)
        total = len(items)
        all_results = []
        all_errors = []
        total_succeeded = 0
        total_failed = 0
        completed = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._process_chunk,
                    func,
                    chunk,
                    i,
                    *args,
                    **kwargs,
                ): i
                for i, chunk in enumerate(chunks)
            }
            for future in as_completed(futures):
                try:
                    chunk_result = future.result()
                    all_results.extend(chunk_result.results)
                    all_errors.extend(chunk_result.errors)
                    total_succeeded += chunk_result.succeeded
                    total_failed += chunk_result.failed
                    completed += len(chunk_result.results) + len(chunk_result.errors)
                    if self._progress_callback:
                        self._progress_callback(completed, total)
                except Exception as e:
                    logger.error(f"Chunk processing failed: {e}")
                    if not self.continue_on_error:
                        raise

        return BulkResult(
            total=total,
            succeeded=total_succeeded,
            failed=total_failed,
            results=all_results,
            errors=all_errors,
            duration=time.time() - start_time,
        )

    def _split_into_chunks(self, items: list[Any]) -> list[list[Any]]:
        """Split items into chunks."""
        return [
            items[i:i + self.chunk_size]
            for i in range(0, len(items), self.chunk_size)
        ]

    def _process_chunk(
        self,
        func: Callable[..., Any],
        chunk: list[Any],
        chunk_index: int,
        *args: Any,
        **kwargs: Any,
    ) -> ChunkResult:
        """Process a single chunk."""
        results = []
        errors = []
        succeeded = 0
        failed = 0
        for item in chunk:
            try:
                result = func(item, *args, **kwargs)
                results.append(result)
                succeeded += 1
            except Exception as e:
                errors.append({
                    "item": item,
                    "error": str(e),
                    "chunk_index": chunk_index,
                })
                failed += 1
        return ChunkResult(
            chunk_index=chunk_index,
            succeeded=succeeded,
            failed=failed,
            results=results,
            errors=errors,
        )


class BulkBatchProcessor:
    """Process items in batches where each batch is a single API call.

    Example:
        >>> processor = BulkBatchProcessor(batch_size=50)
        >>> result = processor.process_batch(batch_api_func, all_items)
    """

    def __init__(self, batch_size: int = 100) -> None:
        self.batch_size = batch_size

    def process_batch(
        self,
        func: Callable[..., Any],
        items: list[Any],
        *args: Any,
        **kwargs: Any,
    ) -> BulkResult:
        """Process items in batches.

        Args:
            func: Function that takes a list of items and returns results.
            items: All items to process.
            *args: Additional args for func.
            **kwargs: Additional kwargs for func.

        Returns:
            BulkResult with batch results.
        """
        start_time = time.time()
        batches = self._split_into_batches(items)
        all_results = []
        all_errors = []
        total_succeeded = 0
        total_failed = 0

        for i, batch in enumerate(batches):
            try:
                result = func(batch, *args, **kwargs)
                if isinstance(result, list):
                    all_results.extend(result)
                    total_succeeded += len(result)
                else:
                    all_results.append(result)
                    total_succeeded += 1
            except Exception as e:
                logger.error(f"Batch {i} failed: {e}")
                all_errors.append({
                    "batch_index": i,
                    "batch_size": len(batch),
                    "error": str(e),
                })
                total_failed += len(batch)

        return BulkResult(
            total=len(items),
            succeeded=total_succeeded,
            failed=total_failed,
            results=all_results,
            errors=all_errors,
            duration=time.time() - start_time,
        )

    def _split_into_batches(self, items: list[Any]) -> list[list[Any]]:
        """Split items into batches."""
        return [
            items[i:i + self.batch_size]
            for i in range(0, len(items), self.batch_size)
        ]


def bulk_update(
    items: list[dict[str, Any]],
    update_func: Callable[[dict[str, Any]], Any],
    chunk_size: int = 100,
    max_workers: int = 5,
) -> BulkResult:
    """Bulk update items.

    Args:
        items: Items to update.
        update_func: Function to apply to each item.
        chunk_size: Size of processing chunks.
        max_workers: Max parallel workers.

    Returns:
        BulkResult with update results.
    """
    processor = BulkProcessor(
        chunk_size=chunk_size,
        max_workers=max_workers,
    )
    return processor.process_bulk(update_func, items)

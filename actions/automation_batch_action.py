"""Automation Batch Processor.

This module provides batch processing:
- Batch creation and management
- Progress tracking
- Error collection
- Result aggregation

Example:
    >>> from actions.automation_batch_action import BatchProcessor
    >>> processor = BatchProcessor(batch_size=100)
    >>> results = processor.process_items(items, process_fn)
"""

from __future__ import annotations

import time
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class BatchResult:
    """Result of batch processing."""
    total_items: int
    successful: int
    failed: int
    results: list[Any]
    errors: list[dict]
    duration_ms: float
    batch_id: str


@dataclass
class BatchProgress:
    """Batch processing progress."""
    batch_id: str
    total_items: int
    processed: int
    successful: int
    failed: int
    current_batch: int
    total_batches: int
    started_at: float
    estimated_remaining_ms: float = 0.0


class BatchProcessor:
    """Processes items in batches with progress tracking."""

    def __init__(
        self,
        batch_size: int = 100,
        max_workers: int = 4,
        stop_on_error: bool = False,
    ) -> None:
        """Initialize the batch processor.

        Args:
            batch_size: Size of each batch.
            max_workers: Maximum parallel workers.
            stop_on_error: Stop processing on first error.
        """
        self._batch_size = batch_size
        self._max_workers = max_workers
        self._stop_on_error = stop_on_error
        self._lock = threading.Lock()
        self._stats = {"batches_processed": 0, "items_processed": 0, "items_failed": 0}

    def process_items(
        self,
        items: list[Any],
        process_fn: Callable[[Any], Any],
        batch_id: Optional[str] = None,
    ) -> BatchResult:
        """Process items in batches.

        Args:
            items: Items to process.
            process_fn: Function to apply to each item.
            batch_id: Optional batch identifier.

        Returns:
            BatchResult with all results and errors.
        """
        import uuid
        batch_id = batch_id or str(uuid.uuid4())[:8]

        start_time = time.time()
        all_results = []
        all_errors = []
        successful = 0
        failed = 0

        batches = self._create_batches(items)

        for batch_idx, batch in enumerate(batches):
            for item in batch:
                try:
                    result = process_fn(item)
                    all_results.append(result)
                    successful += 1
                except Exception as e:
                    logger.error("Batch %s item failed: %s", batch_id, e)
                    all_errors.append({
                        "item": str(item),
                        "error": str(e),
                        "batch": batch_idx,
                    })
                    failed += 1

                    if self._stop_on_error:
                        break

            if self._stop_on_error and failed > 0:
                break

        duration_ms = (time.time() - start_time) * 1000

        with self._lock:
            self._stats["batches_processed"] += 1
            self._stats["items_processed"] += successful
            self._stats["items_failed"] += failed

        return BatchResult(
            total_items=len(items),
            successful=successful,
            failed=failed,
            results=all_results,
            errors=all_errors,
            duration_ms=duration_ms,
            batch_id=batch_id,
        )

    def process_items_parallel(
        self,
        items: list[Any],
        process_fn: Callable[[Any], Any],
        batch_id: Optional[str] = None,
    ) -> BatchResult:
        """Process items in parallel batches.

        Args:
            items: Items to process.
            process_fn: Function to apply to each item.
            batch_id: Optional batch identifier.

        Returns:
            BatchResult with all results and errors.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed
        import uuid

        batch_id = batch_id or str(uuid.uuid4())[:8]
        start_time = time.time()

        all_results = []
        all_errors = []
        successful = 0
        failed = 0

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {executor.submit(process_fn, item): item for item in items}

            for future in as_completed(futures):
                item = futures[future]
                try:
                    result = future.result()
                    all_results.append(result)
                    successful += 1
                except Exception as e:
                    logger.error("Parallel item failed: %s", e)
                    all_errors.append({"item": str(item), "error": str(e)})
                    failed += 1

        duration_ms = (time.time() - start_time) * 1000

        with self._lock:
            self._stats["batches_processed"] += 1
            self._stats["items_processed"] += successful
            self._stats["items_failed"] += failed

        return BatchResult(
            total_items=len(items),
            successful=successful,
            failed=failed,
            results=all_results,
            errors=all_errors,
            duration_ms=duration_ms,
            batch_id=batch_id,
        )

    def _create_batches(
        self,
        items: list[Any],
    ) -> list[list[Any]]:
        """Split items into batches."""
        batches = []
        for i in range(0, len(items), self._batch_size):
            batches.append(items[i:i + self._batch_size])
        return batches

    def get_progress(
        self,
        batch_result: BatchResult,
    ) -> BatchProgress:
        """Get progress for a batch result.

        Args:
            batch_result: BatchResult to track.

        Returns:
            BatchProgress with current state.
        """
        total_batches = (batch_result.total_items + self._batch_size - 1) // self._batch_size
        processed = batch_result.successful + batch_result.failed
        current_batch = (processed + self._batch_size - 1) // self._batch_size

        elapsed_ms = batch_result.duration_ms
        items_per_ms = processed / elapsed_ms if elapsed_ms > 0 else 0
        remaining = batch_result.total_items - processed
        estimated_remaining_ms = remaining / items_per_ms if items_per_ms > 0 else 0

        return BatchProgress(
            batch_id=batch_result.batch_id,
            total_items=batch_result.total_items,
            processed=processed,
            successful=batch_result.successful,
            failed=batch_result.failed,
            current_batch=current_batch,
            total_batches=total_batches,
            started_at=time.time() - (elapsed_ms / 1000),
            estimated_remaining_ms=estimated_remaining_ms,
        )

    def process_with_retry(
        self,
        items: list[Any],
        process_fn: Callable[[Any], Any],
        max_retries: int = 3,
    ) -> BatchResult:
        """Process items with automatic retry on failure.

        Args:
            items: Items to process.
            process_fn: Function to apply.
            max_retries: Maximum retry attempts.

        Returns:
            BatchResult.
        """
        import uuid
        batch_id = str(uuid.uuid4())[:8]
        start_time = time.time()

        results = []
        errors = []
        pending = list(items)
        successful = 0
        failed = 0

        for attempt in range(max_retries + 1):
            if not pending:
                break

            still_pending = []

            for item in pending:
                try:
                    result = process_fn(item)
                    results.append(result)
                    successful += 1
                except Exception as e:
                    if attempt < max_retries:
                        still_pending.append(item)
                    else:
                        errors.append({"item": str(item), "error": str(e), "retries": max_retries})
                        failed += 1

            pending = still_pending

            if pending and attempt < max_retries:
                time.sleep(0.1 * (attempt + 1))

        duration_ms = (time.time() - start_time) * 1000

        return BatchResult(
            total_items=len(items),
            successful=successful,
            failed=failed,
            results=results,
            errors=errors,
            duration_ms=duration_ms,
            batch_id=batch_id,
        )

    def get_stats(self) -> dict[str, int]:
        """Get processor statistics."""
        with self._lock:
            return dict(self._stats)

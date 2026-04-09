"""Batch API operations with parallel execution.

This module provides batch processing capabilities for API operations:
- Parallel execution with configurable concurrency
- Batch request splitting
- Result aggregation and error handling

Example:
    >>> from actions.api_batch_action import BatchProcessor
    >>> processor = BatchProcessor(max_concurrency=5)
    >>> results = await processor.execute_batch(api_calls)
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from collections import deque

logger = logging.getLogger(__name__)


@dataclass
class BatchItem:
    """A single item in a batch operation."""
    id: str
    operation: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    retries: int = 0
    max_retries: int = 3


@dataclass
class BatchResult:
    """Result of a batch operation."""
    id: str
    success: bool
    data: Any = None
    error: Optional[str] = None
    duration: float = 0.0


@dataclass
class BatchResponse:
    """Response from a batch execution."""
    results: list[BatchResult]
    total_duration: float
    success_count: int
    failure_count: int


class BatchProcessor:
    """Process API operations in batches with concurrency control.

    Attributes:
        max_concurrency: Maximum concurrent operations.
        batch_size: Maximum items per batch.
    """

    def __init__(
        self,
        max_concurrency: int = 10,
        batch_size: int = 100,
    ) -> None:
        self.max_concurrency = max_concurrency
        self.batch_size = batch_size
        self._semaphore: Optional[asyncio.Semaphore] = None

    async def execute_batch(
        self,
        items: list[BatchItem],
    ) -> BatchResponse:
        """Execute a batch of operations.

        Args:
            items: List of BatchItems to execute.

        Returns:
            BatchResponse with all results.
        """
        if asyncio.get_event_loop().is_running():
            self._semaphore = asyncio.Semaphore(self.max_concurrency)
        else:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._semaphore = asyncio.Semaphore(self.max_concurrency)

        start_time = time.time()
        tasks = [self._execute_item(item) for item in items]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        batch_results: list[BatchResult] = []
        for item, result in zip(items, results):
            if isinstance(result, Exception):
                batch_results.append(BatchResult(
                    id=item.id,
                    success=False,
                    error=str(result),
                ))
            else:
                batch_results.append(result)

        total_duration = time.time() - start_time
        success_count = sum(1 for r in batch_results if r.success)
        failure_count = len(batch_results) - success_count

        return BatchResponse(
            results=batch_results,
            total_duration=total_duration,
            success_count=success_count,
            failure_count=failure_count,
        )

    async def _execute_item(self, item: BatchItem) -> BatchResult:
        """Execute a single batch item."""
        if not self._semaphore:
            self._semaphore = asyncio.Semaphore(self.max_concurrency)

        async with self._semaphore:
            start_time = time.time()
            try:
                if asyncio.iscoroutinefunction(item.operation):
                    result = await item.operation(*item.args, **item.kwargs)
                else:
                    result = item.operation(*item.args, **item.kwargs)
                duration = time.time() - start_time
                return BatchResult(
                    id=item.id,
                    success=True,
                    data=result,
                    duration=duration,
                )
            except Exception as e:
                duration = time.time() - start_time
                if item.retries < item.max_retries:
                    item.retries += 1
                    logger.info(f"Retrying item {item.id}, attempt {item.retries}")
                    return await self._execute_item(item)
                return BatchResult(
                    id=item.id,
                    success=False,
                    error=str(e),
                    duration=duration,
                )

    def split_batches(
        self,
        items: list[Any],
        batch_size: Optional[int] = None,
    ) -> list[list[Any]]:
        """Split items into batches.

        Args:
            items: Items to split.
            batch_size: Optional custom batch size.

        Returns:
            List of batches.
        """
        size = batch_size or self.batch_size
        return [items[i:i + size] for i in range(0, len(items), size)]


class BatchQueue:
    """Queue for managing batch operations with priorities.

    Supports priority-based ordering and batch formation.
    """

    def __init__(self, batch_size: int = 100) -> None:
        self.batch_size = batch_size
        self._queue: deque[BatchItem] = deque()

    def enqueue(
        self,
        id: str,
        operation: Callable[..., Any],
        *args: Any,
        priority: int = 0,
        **kwargs: Any,
    ) -> None:
        """Add an item to the queue.

        Args:
            id: Unique identifier for the item.
            operation: The operation to execute.
            *args: Positional arguments for the operation.
            priority: Priority (higher = earlier execution).
            **kwargs: Keyword arguments for the operation.
        """
        item = BatchItem(
            id=id,
            operation=operation,
            args=args,
            kwargs=kwargs,
            priority=priority,
        )
        self._queue.append(item)
        self._queue = deque(sorted(self._queue, key=lambda x: -x.priority))

    def dequeue(self, count: Optional[int] = None) -> list[BatchItem]:
        """Get items from the queue.

        Args:
            count: Number of items to get.

        Returns:
            List of BatchItems.
        """
        count = count or self.batch_size
        items = list(self._queue)[:count]
        for _ in range(min(count, len(self._queue))):
            self._queue.popleft()
        return items

    def size(self) -> int:
        """Get number of items in queue."""
        return len(self._queue)

    def clear(self) -> None:
        """Clear all items from queue."""
        self._queue.clear()


def batch_operation(
    id: str,
    operation: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> BatchItem:
    """Create a batch item from an operation.

    Args:
        id: Unique identifier.
        operation: The operation to execute.
        *args: Positional arguments.
        **kwargs: Keyword arguments.

    Returns:
        BatchItem ready for execution.
    """
    return BatchItem(id=id, operation=operation, args=args, kwargs=kwargs)

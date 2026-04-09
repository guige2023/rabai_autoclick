"""Retry queue for failed operations with exponential backoff.

This module provides a retry queue that manages failed operations,
schedules them for retry with exponential backoff, and tracks
retry statistics.

Example:
    >>> from actions.retry_queue_action import RetryQueue
    >>> queue = RetryQueue(max_retries=3, base_delay=1.0)
    >>> queue.enqueue(my_operation, *args, **kwargs)
    >>> queue.process()
"""

from __future__ import annotations

import time
import threading
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from collections import deque
from enum import Enum

logger = logging.getLogger(__name__)


class RetryStatus(Enum):
    """Status of a retry item."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    EXHAUSTED = "exhausted"


@dataclass
class RetryItem:
    """An item in the retry queue."""
    operation: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    attempt: int = 0
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    status: RetryStatus = RetryStatus.PENDING
    error: Optional[Exception] = None
    result: Optional[Any] = None
    created_at: float = field(default_factory=time.time)
    last_attempt_at: Optional[float] = None
    next_attempt_at: Optional[float] = None

    def calculate_delay(self) -> float:
        """Calculate the delay before the next retry attempt."""
        delay = self.base_delay * (2 ** self.attempt)
        return min(delay, self.max_delay)

    def should_retry(self) -> bool:
        """Check if the item should be retried."""
        return self.attempt < self.max_retries and self.status != RetryStatus.SUCCESS


class RetryQueue:
    """A queue that manages retry operations with exponential backoff.

    Attributes:
        max_retries: Maximum number of retry attempts.
        base_delay: Base delay in seconds between retries.
        max_delay: Maximum delay in seconds between retries.
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
    ) -> None:
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self._queue: deque[RetryItem] = deque()
        self._lock = threading.Lock()
        self._running = False
        self._stats = {
            "total_enqueued": 0,
            "total_succeeded": 0,
            "total_failed": 0,
            "total_retried": 0,
        }

    def enqueue(
        self,
        operation: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> RetryItem:
        """Add an operation to the retry queue.

        Args:
            operation: The callable to execute.
            *args: Positional arguments for the operation.
            **kwargs: Keyword arguments for the operation.

        Returns:
            The RetryItem that was added to the queue.
        """
        item = RetryItem(
            operation=operation,
            args=args,
            kwargs=kwargs,
            max_retries=self.max_retries,
            base_delay=self.base_delay,
            max_delay=self.max_delay,
        )
        with self._lock:
            self._queue.append(item)
            self._stats["total_enqueued"] += 1
        logger.debug(f"Enqueued operation: {operation.__name__}")
        return item

    def process(self, timeout: Optional[float] = None) -> list[RetryItem]:
        """Process all items in the retry queue.

        Args:
            timeout: Maximum time to spend processing (seconds).

        Returns:
            List of RetryItems that were processed.
        """
        self._running = True
        start_time = time.time()
        processed: list[RetryItem] = []

        while self._running:
            item = self._get_next_item()
            if item is None:
                break

            if timeout and (time.time() - start_time) >= timeout:
                logger.warning("Retry queue processing timed out")
                break

            self._process_item(item)
            processed.append(item)

        return processed

    def _get_next_item(self) -> Optional[RetryItem]:
        """Get the next item that is ready for processing."""
        with self._lock:
            current_time = time.time()
            for i, item in enumerate(self._queue):
                if item.next_attempt_at is None or item.next_attempt_at <= current_time:
                    return self._queue.pop(i)
            return None

    def _process_item(self, item: RetryItem) -> None:
        """Process a single retry item."""
        item.status = RetryStatus.RUNNING
        item.attempt += 1
        item.last_attempt_at = time.time()

        try:
            item.result = item.operation(*item.args, **item.kwargs)
            item.status = RetryStatus.SUCCESS
            self._stats["total_succeeded"] += 1
            logger.debug(f"Operation succeeded on attempt {item.attempt}")
        except Exception as e:
            item.error = e
            self._stats["total_retried"] += 1
            if item.should_retry():
                item.status = RetryStatus.PENDING
                item.next_attempt_at = time.time() + item.calculate_delay()
                logger.info(
                    f"Operation failed, scheduling retry {item.attempt}/{item.max_retries} "
                    f"in {item.calculate_delay():.1f}s: {e}"
                )
            else:
                item.status = RetryStatus.EXHAUSTED
                self._stats["total_failed"] += 1
                logger.error(
                    f"Operation exhausted all retries ({item.max_retries}): {e}"
                )

    def get_stats(self) -> dict[str, Any]:
        """Get retry queue statistics.

        Returns:
            Dictionary containing queue statistics.
        """
        with self._lock:
            return {
                **self._stats,
                "pending": len(self._queue),
                "running": sum(
                    1 for item in self._queue if item.status == RetryStatus.RUNNING
                ),
            }

    def clear(self) -> int:
        """Clear all pending items from the queue.

        Returns:
            Number of items cleared.
        """
        with self._lock:
            count = len(self._queue)
            self._queue.clear()
            return count

    def stop(self) -> None:
        """Stop the retry queue processing."""
        self._running = False

    def get_pending(self) -> list[RetryItem]:
        """Get all pending items."""
        with self._lock:
            return [item for item in self._queue if item.status == RetryStatus.PENDING]

    def get_failed(self) -> list[RetryItem]:
        """Get all failed items."""
        with self._lock:
            return [
                item
                for item in self._queue
                if item.status in (RetryStatus.FAILED, RetryStatus.EXHAUSTED)
            ]

    def requeue_failed(self) -> int:
        """Requeue all failed items for retry.

        Returns:
            Number of items requeued.
        """
        with self._lock:
            count = 0
            new_queue: deque[RetryItem] = deque()
            for item in self._queue:
                if item.status in (RetryStatus.FAILED, RetryStatus.EXHAUSTED):
                    item.attempt = 0
                    item.status = RetryStatus.PENDING
                    item.next_attempt_at = None
                    item.error = None
                    new_queue.append(item)
                    count += 1
                else:
                    new_queue.append(item)
            self._queue = new_queue
            return count

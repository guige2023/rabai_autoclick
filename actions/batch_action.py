"""
Batch Action Module.

Provides batch processing capabilities with configurable
batch sizes, windows, and concurrency controls.
"""

import time
import asyncio
import threading
from typing import Callable, Any, Optional, List, Dict, Generic, TypeVar
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import heapq


T = TypeVar("T")
R = TypeVar("R")


class BatchMode(Enum):
    """Batch processing modes."""
    FIXED_SIZE = "fixed_size"
    FIXED_TIME = "fixed_time"
    FIXED_SIZE_OR_TIME = "fixed_size_or_time"
    ADAPTIVE = "adaptive"


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    mode: BatchMode = BatchMode.FIXED_SIZE_OR_TIME
    size: int = 100
    window_seconds: float = 5.0
    max_concurrency: int = 4
    retry_failed: bool = True
    max_retries: int = 3


@dataclass
class Batch:
    """Represents a batch of items."""
    batch_id: str
    items: List[Any] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BatchResult:
    """Result of processing a batch."""
    batch_id: str
    success_count: int = 0
    failed_count: int = 0
    results: List[Any] = field(default_factory=list)
    errors: List[Exception] = field(default_factory=list)
    duration: float = 0.0


class BatchQueue:
    """Queue for managing batches."""

    def __init__(self):
        self._queue: deque = deque()
        self._lock = threading.RLock()

    def put(self, item: Any) -> None:
        with self._lock:
            self._queue.append(item)

    def get(self, count: int) -> List[Any]:
        with self._lock:
            items = []
            for _ in range(min(count, len(self._queue))):
                if self._queue:
                    items.append(self._queue.popleft())
            return items

    def peek(self, count: int) -> List[Any]:
        with self._lock:
            return list(self._queue)[:count]

    def size(self) -> int:
        with self._lock:
            return len(self._queue)

    def clear(self) -> List[Any]:
        with self._lock:
            items = list(self._queue)
            self._queue.clear()
            return items


class BatchAction(Generic[T, R]):
    """
    Action that processes items in batches.

    Example:
        def process_items(items):
            return [item * 2 for item in items]

        action = BatchAction("doubler", process_items)
        action.add(1)
        action.add(2)
        action.add(3)
        results = action.flush()
    """

    def __init__(
        self,
        name: str,
        processor: Callable[[List[T]], List[R]],
        config: Optional[BatchConfig] = None,
    ):
        self.name = name
        self.processor = processor
        self.config = config or BatchConfig()
        self._queue = BatchQueue()
        self._lock = threading.RLock()
        self._batch_counter = 0
        self._results: List[R] = []
        self._last_flush_time = time.time()
        self._semaphore = threading.Semaphore(self.config.max_concurrency)
        self._stats = {
            "batches_created": 0,
            "items_processed": 0,
            "items_failed": 0,
            "total_duration": 0.0,
        }

    @property
    def queue_size(self) -> int:
        """Current queue size."""
        return self._queue.size()

    def add(self, item: T) -> None:
        """Add an item to the batch queue."""
        self._queue.put(item)

        if self.config.mode == BatchMode.FIXED_SIZE:
            if self._queue.size() >= self.config.size:
                self.flush()
        elif self.config.mode == BatchMode.FIXED_SIZE_OR_TIME:
            if self._queue.size() >= self.config.size:
                self.flush()
            else:
                self._check_time_window()
        elif self.config.mode == BatchMode.ADAPTIVE:
            if self._queue.size() >= self._calculate_adaptive_size():
                self.flush()

    def _check_time_window(self) -> None:
        """Check if time window has elapsed."""
        elapsed = time.time() - self._last_flush_time
        if elapsed >= self.config.window_seconds and self._queue.size() > 0:
            self.flush()

    def _calculate_adaptive_size(self) -> int:
        """Calculate adaptive batch size based on queue."""
        base_size = self.config.size
        queue_size = self._queue.size()

        if queue_size < base_size * 0.5:
            return base_size * 2
        elif queue_size > base_size * 1.5:
            return int(base_size * 0.75)
        return base_size

    def _create_batch(self, items: List[T]) -> Batch:
        """Create a new batch."""
        self._batch_counter += 1
        return Batch(
            batch_id=f"{self.name}_{self._batch_counter}",
            items=items,
        )

    def _process_batch(self, batch: Batch) -> BatchResult:
        """Process a single batch."""
        start_time = time.time()
        result = BatchResult(batch_id=batch.batch_id)

        try:
            if asyncio.iscoroutinefunction(self.processor):
                results = asyncio.run(self.processor(batch.items))
            else:
                results = self.processor(batch.items)

            result.results = results
            result.success_count = len(results)
            self._stats["items_processed"] += result.success_count

        except Exception as e:
            result.failed_count = len(batch.items)
            result.errors.append(e)
            self._stats["items_failed"] += result.failed_count

            if self.config.retry_failed:
                self._retry_batch(batch)

        result.duration = time.time() - start_time
        self._stats["total_duration"] += result.duration
        self._results.extend(result.results)

        return result

    def _retry_batch(self, batch: Batch) -> None:
        """Retry failed batch processing."""
        for attempt in range(self.config.max_retries):
            try:
                results = self.processor(batch.items)
                self._stats["items_processed"] += len(results)
                self._stats["items_failed"] -= len(batch.items)
                self._results.extend(results)
                break
            except Exception:
                if attempt == self.config.max_retries - 1:
                    raise

    def flush(self) -> List[R]:
        """Flush all queued items as a batch."""
        with self._lock:
            items = self._queue.clear()
            if not items:
                return []

            batch = self._create_batch(items)
            self._stats["batches_created"] += 1
            self._last_flush_time = time.time()

            if self.config.max_concurrency > 1:
                return self._process_batch_concurrent(batch).results
            return self._process_batch(batch).results

    def _process_batch_concurrent(self, batch: Batch) -> BatchResult:
        """Process batch with concurrency control."""
        chunks = [
            batch.items[i:i + self.config.size]
            for i in range(0, len(batch.items), self.config.size)
        ]

        result = BatchResult(batch_id=batch.batch_id)

        def process_chunk(chunk):
            return self._process_batch(self._create_batch(chunk))

        threads = []
        for chunk in chunks:
            self._semaphore.acquire()

            def thread_target(chunk):
                try:
                    chunk_result = self.processor(chunk)
                    with self._lock:
                        result.results.extend(chunk_result)
                        result.success_count += len(chunk_result)
                finally:
                    self._semaphore.release()

            t = threading.Thread(target=thread_target, args=(chunk,))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        return result

    async def flush_async(self) -> List[R]:
        """Flush all queued items asynchronously."""
        await asyncio.sleep(0)
        return self.flush()

    def get_results(self) -> List[R]:
        """Get all processed results."""
        with self._lock:
            return list(self._results)

    def clear_results(self) -> None:
        """Clear all results."""
        with self._lock:
            self._results.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get batch processing statistics."""
        return {
            "name": self.name,
            "queue_size": self._queue.size(),
            "batches_created": self._stats["batches_created"],
            "items_processed": self._stats["items_processed"],
            "items_failed": self._stats["items_failed"],
            "total_duration": self._stats["total_duration"],
            "avg_batch_duration": (
                self._stats["total_duration"] / max(1, self._stats["batches_created"])
            ),
        }

    def reset_stats(self) -> None:
        """Reset statistics."""
        with self._lock:
            self._stats = {
                "batches_created": 0,
                "items_processed": 0,
                "items_failed": 0,
                "total_duration": 0.0,
            }
            self._results.clear()

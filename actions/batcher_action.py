"""batcher action module for rabai_autoclick.

Provides batch processing utilities: collect-and-flush batching,
size-based and time-based triggers, batch transformations,
and parallel batch execution.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Generic, Iterable, Iterator, List, Optional, Sequence, TypeVar, Union

__all__ = [
    "Batch",
    "BatchProcessor",
    "Batcher",
    "TimeBatcher",
    "SizeBatcher",
    "CountBatcher",
    "FlushingBatcher",
    "ParallelBatcher",
    "batch_transform",
    "BatchStrategy",
    "BatchResult",
]


T = TypeVar("T")
U = TypeVar("U")


class BatchStrategy(Enum):
    """How batches are formed."""
    SIZE = auto()
    TIME = auto()
    COUNT = auto()
    SIZE_OR_TIME = auto()
    COUNT_OR_TIME = auto()


@dataclass
class BatchResult(Generic[T]):
    """Result of batch processing."""
    items: List[T]
    count: int
    duration_ms: float
    success: bool
    error: Optional[Exception] = None

    @property
    def is_success(self) -> bool:
        return self.success


@dataclass
class Batch(Generic[T]):
    """A collection of items processed together."""
    items: List[T] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)

    def add(self, item: T) -> None:
        """Add item to batch."""
        self.items.append(item)

    def size(self) -> int:
        """Number of items in batch."""
        return len(self.items)

    def is_empty(self) -> bool:
        return len(self.items) == 0

    def age(self) -> float:
        """Seconds since batch was created."""
        return time.time() - self.created_at


class BatchProcessor(Generic[T, U]):
    """Base class for batch processors."""

    def __init__(
        self,
        process_fn: Callable[[List[T]], List[U]],
        batch_size: int = 100,
        max_wait_seconds: float = 5.0,
    ) -> None:
        self.process_fn = process_fn
        self.batch_size = batch_size
        self.max_wait_seconds = max_wait_seconds

    def process(self, batch: Batch[T]) -> BatchResult[U]:
        """Process a batch of items."""
        start = time.perf_counter()
        try:
            results = self.process_fn(batch.items)
            duration = (time.perf_counter() - start) * 1000
            return BatchResult(
                items=results,
                count=len(results),
                duration_ms=duration,
                success=True,
            )
        except Exception as e:
            duration = (time.perf_counter() - start) * 1000
            return BatchResult(
                items=[],
                count=0,
                duration_ms=duration,
                success=False,
                error=e,
            )

    def process_batch(self, items: Sequence[T]) -> List[U]:
        """Process items in batches."""
        results = []
        for i in range(0, len(items), self.batch_size):
            batch = Batch(items=list(items[i:i + self.batch_size]))
            result = self.process(batch)
            if result.is_success:
                results.extend(result.items)
        return results


class Batcher(Generic[T]):
    """Accumulates items and flushes when conditions are met."""

    def __init__(
        self,
        flush_fn: Callable[[List[T]], Any],
        max_size: int = 100,
        max_wait: float = 5.0,
        strategy: BatchStrategy = BatchStrategy.SIZE_OR_TIME,
    ) -> None:
        self.flush_fn = flush_fn
        self.max_size = max_size
        self.max_wait = max_wait
        self.strategy = strategy
        self._buffer: List[T] = []
        self._lock = threading.RLock()
        self._last_flush = time.time()
        self._flush_thread: Optional[threading.Thread] = None
        self._running = False

    def add(self, item: T) -> None:
        """Add an item to the batcher."""
        with self._lock:
            self._buffer.append(item)
            if self._should_flush():
                self._do_flush()

    def add_many(self, items: Iterable[T]) -> None:
        """Add multiple items."""
        for item in items:
            self.add(item)

    def _should_flush(self) -> bool:
        """Check if batch should be flushed."""
        if self.strategy == BatchStrategy.SIZE:
            return len(self._buffer) >= self.max_size
        elif self.strategy == BatchStrategy.TIME:
            return self._buffer and (time.time() - self._last_flush) >= self.max_wait
        elif self.strategy == BatchStrategy.COUNT:
            return len(self._buffer) >= self.max_size
        elif self.strategy in (BatchStrategy.SIZE_OR_TIME, BatchStrategy.COUNT_OR_TIME):
            return len(self._buffer) >= self.max_size or (
                self._buffer and (time.time() - self._last_flush) >= self.max_wait
            )
        return False

    def _do_flush(self) -> None:
        """Flush the buffer."""
        if not self._buffer:
            return
        items = list(self._buffer)
        self._buffer.clear()
        self._last_flush = time.time()
        try:
            self.flush_fn(items)
        except Exception:
            pass

    def flush(self) -> None:
        """Manually flush the buffer."""
        with self._lock:
            self._do_flush()

    def start(self) -> None:
        """Start background flush thread."""
        if self._running:
            return
        self._running = True
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()

    def stop(self) -> None:
        """Stop background flush thread and flush remaining."""
        self._running = False
        if self._flush_thread:
            self._flush_thread.join(timeout=2.0)
        self.flush()

    def _flush_loop(self) -> None:
        """Background flush loop for time-based flushing."""
        while self._running:
            time.sleep(0.1)
            with self._lock:
                if self.strategy in (BatchStrategy.TIME, BatchStrategy.SIZE_OR_TIME, BatchStrategy.COUNT_OR_TIME):
                    if self._buffer and (time.time() - self._last_flush) >= self.max_wait:
                        self._do_flush()

    def __enter__(self) -> "Batcher[T]":
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()


class TimeBatcher(Generic[T]):
    """Batcher that flushes based on time interval."""

    def __init__(
        self,
        flush_fn: Callable[[List[T]], Any],
        interval_seconds: float = 1.0,
    ) -> None:
        self.flush_fn = flush_fn
        self.interval_seconds = interval_seconds
        self._buffer: List[T] = []
        self._lock = threading.Lock()
        self._last_flush = time.time()
        self._running = False

    def add(self, item: T) -> None:
        """Add item to batch."""
        with self._lock:
            self._buffer.append(item)

    def _should_flush(self) -> bool:
        """Check if time to flush."""
        return time.time() - self._last_flush >= self.interval_seconds

    def _do_flush(self) -> List[T]:
        """Flush and return items."""
        if not self._buffer:
            return []
        items = list(self._buffer)
        self._buffer.clear()
        self._last_flush = time.time()
        try:
            self.flush_fn(items)
        except Exception:
            pass
        return items

    def flush_if_due(self) -> Optional[List[T]]:
        """Flush if interval has elapsed."""
        with self._lock:
            if self._should_flush():
                return self._do_flush()
        return None

    def start(self) -> None:
        """Start background flushing."""
        self._running = True
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop and flush remaining."""
        self._running = False
        if hasattr(self, "_thread"):
            self._thread.join(timeout=2.0)
        self.flush()

    def flush(self) -> List[T]:
        """Force flush."""
        with self._lock:
            return self._do_flush()

    def _loop(self) -> None:
        """Background loop."""
        while self._running:
            time.sleep(0.1)
            self.flush_if_due()


class SizeBatcher(Generic[T]):
    """Batcher that flushes when buffer reaches max size."""

    def __init__(
        self,
        flush_fn: Callable[[List[T]], Any],
        max_size: int = 100,
    ) -> None:
        self.flush_fn = flush_fn
        self.max_size = max_size
        self._buffer: List[T] = []
        self._lock = threading.Lock()

    def add(self, item: T) -> bool:
        """Add item, returns True if flush occurred."""
        with self._lock:
            self._buffer.append(item)
            if len(self._buffer) >= self.max_size:
                self._do_flush()
                return True
        return False

    def _do_flush(self) -> List[T]:
        """Flush buffer."""
        if not self._buffer:
            return []
        items = list(self._buffer)
        self._buffer.clear()
        try:
            self.flush_fn(items)
        except Exception:
            pass
        return items

    def flush(self) -> List[T]:
        """Manually flush."""
        with self._lock:
            return self._do_flush()


class CountBatcher(Generic[T]):
    """Batcher that collects exactly N items before flushing."""

    def __init__(
        self,
        flush_fn: Callable[[List[T]], Any],
        count: int = 100,
    ) -> None:
        self.flush_fn = flush_fn
        self.count = count
        self._buffer: List[T] = []
        self._lock = threading.Lock()

    def add(self, item: T) -> bool:
        """Add item, flushes when count is reached."""
        with self._lock:
            self._buffer.append(item)
            if len(self._buffer) >= self.count:
                self._do_flush()
                return True
        return False

    def _do_flush(self) -> List[T]:
        """Flush exactly count items."""
        if len(self._buffer) < self.count:
            return []
        items = self._buffer[:self.count]
        self._buffer = self._buffer[self.count:]
        try:
            self.flush_fn(items)
        except Exception:
            pass
        return items

    def flush(self) -> List[T]:
        """Flush remaining items (may be fewer than count)."""
        with self._lock:
            if not self._buffer:
                return []
            items = list(self._buffer)
            self._buffer.clear()
            try:
                self.flush_fn(items)
            except Exception:
                pass
            return items


class FlushingBatcher(Generic[T]):
    """Batcher with both size and time triggers."""

    def __init__(
        self,
        flush_fn: Callable[[List[T]], Any],
        max_size: int = 100,
        max_wait: float = 5.0,
    ) -> None:
        self.flush_fn = flush_fn
        self.max_size = max_size
        self.max_wait = max_wait
        self._buffer: List[T] = []
        self._lock = threading.Lock()
        self._last_flush = time.time()
        self._running = False

    def add(self, item: T) -> None:
        """Add item, auto-flush if size reached."""
        with self._lock:
            self._buffer.append(item)
            if len(self._buffer) >= self.max_size:
                self._do_flush()

    def _do_flush(self) -> List[T]:
        """Flush buffer if not empty."""
        if not self._buffer:
            return []
        items = list(self._buffer)
        self._buffer.clear()
        self._last_flush = time.time()
        try:
            self.flush_fn(items)
        except Exception:
            pass
        return items

    def _check_time_flush(self) -> Optional[List[T]]:
        """Check if time-based flush needed."""
        if self._buffer and (time.time() - self._last_flush) >= self.max_wait:
            return self._do_flush()
        return None

    def flush(self) -> List[T]:
        """Manual flush."""
        with self._lock:
            return self._do_flush()

    def flush_if_due(self) -> Optional[List[T]]:
        """Time-based flush check."""
        with self._lock:
            return self._check_time_flush()


class ParallelBatcher(Generic[T, U]):
    """Batcher that processes batches in parallel threads."""

    def __init__(
        self,
        process_fn: Callable[[List[T]], List[U]],
        max_workers: int = 4,
        batch_size: int = 100,
    ) -> None:
        self.process_fn = process_fn
        self.max_workers = max_workers
        self.batch_size = batch_size
        self._batches: deque = deque()
        self._results: deque = deque()
        self._lock = threading.Lock()
        self._workers: List[threading.Thread] = []
        self._running = False

    def submit(self, items: Sequence[T]) -> None:
        """Submit items for batch processing."""
        for i in range(0, len(items), self.batch_size):
            batch = list(items[i:i + self.batch_size])
            with self._lock:
                self._batches.append(batch)

    def _worker_loop(self) -> None:
        """Worker thread main loop."""
        while self._running:
            batch = None
            with self._lock:
                if self._batches:
                    batch = self._batches.popleft()
            if batch is not None:
                try:
                    results = self.process_fn(batch)
                    with self._lock:
                        self._results.append(results)
                except Exception:
                    pass
            else:
                time.sleep(0.01)

    def start(self) -> None:
        """Start worker threads."""
        if self._running:
            return
        self._running = True
        for _ in range(self.max_workers):
            t = threading.Thread(target=self._worker_loop, daemon=True)
            t.start()
            self._workers.append(t)

    def stop(self) -> None:
        """Stop workers."""
        self._running = False
        for t in self._workers:
            t.join(timeout=1.0)
        self._workers.clear()

    def drain_results(self) -> List[List[U]]:
        """Get all completed results."""
        results = []
        while True:
            with self._lock:
                if not self._results:
                    break
                results.append(self._results.popleft())
        return results


def batch_transform(
    items: Sequence[T],
    batch_size: int,
    transform_fn: Callable[[List[T]], List[U]],
    max_workers: int = 1,
) -> List[U]:
    """Transform items in batches.

    Args:
        items: Input items.
        batch_size: Items per batch.
        transform_fn: Function to transform each batch.
        max_workers: Parallel workers (1 = sequential).

    Returns:
        Transformed items in order.
    """
    if max_workers <= 1:
        processor = BatchProcessor(transform_fn, batch_size=batch_size)
        return processor.process_batch(items)
    else:
        batcher = ParallelBatcher(transform_fn, max_workers=max_workers, batch_size=batch_size)
        batcher.start()
        try:
            batcher.submit(items)
            time.sleep(0.1)
            all_results = []
            while True:
                results = batcher.drain_results()
                if not results:
                    break
                for r in results:
                    all_results.extend(r)
                time.sleep(0.01)
            return all_results
        finally:
            batcher.stop()

"""Batch processing utilities.

Provides batching, chunking, and bulk operation
support for efficient data processing.
"""

import time
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Generic, List, Optional, TypeVar, Iterator


T = TypeVar("T")
R = TypeVar("R")


@dataclass
class BatchResult(Generic[R]):
    """Result of batch processing."""
    results: List[R]
    failed: List[tuple[int, Exception]]
    total_processed: int
    elapsed_seconds: float


class BatchProcessor(Generic[T, R]):
    """Process items in batches.

    Example:
        def process_item(item):
            return heavy_computation(item)

        processor = BatchProcessor(batch_size=100)
        results = processor.process(items, process_item)
    """

    def __init__(
        self,
        batch_size: int = 50,
        max_workers: int = 4,
        on_error: Optional[Callable[[int, Exception], None]] = None,
    ) -> None:
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.on_error = on_error

    def process(
        self,
        items: List[T],
        processor: Callable[[T], R],
        stop_on_error: bool = False,
    ) -> BatchResult[R]:
        """Process items in batches.

        Args:
            items: Items to process.
            processor: Function to apply to each item.
            stop_on_error: Stop processing on first error.

        Returns:
            BatchResult with all results and errors.
        """
        results: List[R] = []
        failed: List[tuple[int, Exception]] = []
        total = 0
        start = time.time()

        for i, item in enumerate(items):
            try:
                result = processor(item)
                results.append(result)
                total += 1
            except Exception as e:
                failed.append((i, e))
                if self.on_error:
                    self.on_error(i, e)
                if stop_on_error:
                    break

        return BatchResult(
            results=results,
            failed=failed,
            total_processed=total,
            elapsed_seconds=time.time() - start,
        )

    def process_batches(
        self,
        items: List[T],
        batch_processor: Callable[[List[T]], List[R]],
    ) -> BatchResult[R]:
        """Process items as batches.

        Args:
            items: Items to process.
            batch_processor: Function that processes a batch.

        Returns:
            BatchResult with combined results.
        """
        results: List[R] = []
        failed: List[tuple[int, Exception]] = []
        total = 0
        start = time.time()

        for i in range(0, len(items), self.batch_size):
            batch = items[i:i + self.batch_size]
            batch_idx = i // self.batch_size
            try:
                batch_results = batch_processor(batch)
                results.extend(batch_results)
                total += len(batch)
            except Exception as e:
                failed.append((batch_idx, e))

        return BatchResult(
            results=results,
            failed=failed,
            total_processed=total,
            elapsed_seconds=time.time() - start,
        )


class ChunkBuffer(Generic[T]):
    """Buffer items and yield in chunks.

    Example:
        buffer = ChunkBuffer(max_size=1000, flush_interval=5.0)
        for item in generate_items():
            buffer.add(item)
            for chunk in buffer.flush():
                send_to_server(chunk)
    """

    def __init__(
        self,
        max_size: int = 1000,
        flush_interval: float = 0.0,
    ) -> None:
        self.max_size = max_size
        self.flush_interval = flush_interval
        self._buffer: Deque[T] = deque()
        self._last_flush = time.time()

    def add(self, item: T) -> None:
        """Add item to buffer."""
        self._buffer.append(item)
        if len(self._buffer) >= self.max_size:
            self.flush()

    def add_many(self, items: List[T]) -> None:
        """Add multiple items to buffer."""
        self._buffer.extend(items)
        while len(self._buffer) >= self.max_size:
            self.flush()

    def flush(self) -> Iterator[List[T]]:
        """Yield chunks if buffer needs flushing.

        Yields:
            Chunks of buffered items.
        """
        should_flush = (
            len(self._buffer) >= self.max_size or
            (self.flush_interval > 0 and
             time.time() - self._last_flush >= self.flush_interval)
        )

        if should_flush and self._buffer:
            chunk = list(self._buffer)
            self._buffer.clear()
            self._last_flush = time.time()
            yield chunk

    def get_all(self) -> List[T]:
        """Get all buffered items and clear buffer."""
        items = list(self._buffer)
        self._buffer.clear()
        self._last_flush = time.time()
        return items

    def size(self) -> int:
        """Current buffer size."""
        return len(self._buffer)


class SlidingWindowBatch(Generic[T]):
    """Batch items based on count or time window.

    Example:
        batcher = SlidingWindowBatch(batch_size=10, window_seconds=1.0)
        for item in stream:
            batcher.add(item)
            for batch in batcher.get_ready():
                process(batch)
    """

    def __init__(
        self,
        batch_size: int = 10,
        window_seconds: float = 1.0,
    ) -> None:
        self.batch_size = batch_size
        self.window_seconds = window_seconds
        self._buffer: Deque[tuple[float, T]] = deque()

    def add(self, item: T) -> None:
        """Add item to window."""
        self._buffer.append((time.time(), item))

    def get_ready(self) -> Iterator[List[T]]:
        """Yield batches that are ready.

        Yields:
            Batches that meet size or time criteria.
        """
        cutoff = time.time() - self.window_seconds

        while len(self._buffer) >= self.batch_size:
            batch = [item for _, item in list(self._buffer)[:self.batch_size]]
            for _ in range(min(self.batch_size, len(self._buffer))):
                self._buffer.popleft()
            yield batch

        while self._buffer and self._buffer[0][0] < cutoff:
            self._buffer.popleft()

    def size(self) -> int:
        """Current window size."""
        return len(self._buffer)

    def clear(self) -> None:
        """Clear all buffered items."""
        self._buffer.clear()


class Deduplicator(Generic[T]):
    """Deduplicate items based on key function.

    Example:
        dedup = Deduplicator(key=lambda x: x["id"])
        unique_items = dedup.process(all_items)
    """

    def __init__(
        self,
        key: Optional[Callable[[T], Any]] = None,
        max_size: int = 10000,
    ) -> None:
        self.key = key or (lambda x: x)
        self.max_size = max_size
        self._seen: set = set()
        self._order: List = deque(maxlen=max_size)

    def process(self, items: List[T]) -> List[T]:
        """Remove duplicates from items.

        Args:
            items: Items to deduplicate.

        Returns:
            Unique items preserving order.
        """
        unique: List[T] = []
        for item in items:
            k = self.key(item)
            if k not in self._seen:
                self._seen.add(k)
                self._order.append(k)
                unique.append(item)

            if len(self._seen) >= self.max_size:
                oldest = self._order.popleft()
                self._seen.discard(oldest)

        return unique

    def is_unique(self, item: T) -> bool:
        """Check if item is unique (not seen before).

        Args:
            item: Item to check.

        Returns:
            True if item has not been seen.
        """
        k = self.key(item)
        if k in self._seen:
            return False
        self._seen.add(k)
        self._order.append(k)
        return True

    def reset(self) -> None:
        """Reset seen items."""
        self._seen.clear()
        self._order.clear()


def chunk_list(items: List[T], chunk_size: int) -> Iterator[List[T]]:
    """Split list into chunks.

    Example:
        for chunk in chunk_list(items, 100):
            process(chunk)
    """
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def batch_items(
    items: List[T],
    batch_size: int,
    processor: Callable[[List[T]], List[R]],
) -> List[List[R]]:
    """Process items in batches.

    Example:
        def process_batch(batch):
            return [transform(item) for item in batch]
        results = batch_items(items, 50, process_batch)
    """
    results: List[List[R]] = []
    for chunk in chunk_list(items, batch_size):
        results.append(processor(chunk))
    return results

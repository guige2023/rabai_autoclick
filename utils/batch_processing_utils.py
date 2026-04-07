"""Batch processing utilities with chunking, parallelization, and error handling."""

from __future__ import annotations

import concurrent.futures
import threading
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Generator, Generic, TypeVar

__all__ = [
    "BatchConfig",
    "BatchResult",
    "batch_process",
    "chunked",
    "ParallelBatch",
    "StreamProcessor",
]

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class BatchConfig:
    """Configuration for batch processing."""
    chunk_size: int = 100
    max_workers: int = 4
    max_retries: int = 2
    on_error: str = "continue"


@dataclass
class BatchResult(Generic[T]):
    """Result of a batch operation."""
    total: int
    succeeded: list[T] = field(default_factory=list)
    failed: list[tuple[T, Exception]] = field(default_factory=list)
    skipped: int = 0


def chunked(iterable: list[T] | tuple[T, ...] | Generator[T, None, None], size: int) -> Generator[list[T], None, None]:
    """Split an iterable into chunks of specified size."""
    chunk: list[T] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


class ParallelBatch(Generic[T, R]):
    """Parallel batch processor with configurable concurrency."""

    def __init__(
        self,
        items: list[T],
        processor: Callable[[T], R],
        config: BatchConfig | None = None,
    ) -> None:
        self.items = items
        self.processor = processor
        self.config = config or BatchConfig()
        self._results: BatchResult[R] = BatchResult(total=len(items))
        self._lock = threading.Lock()

    def process_all(self) -> BatchResult[R]:
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {executor.submit(self._process_item, item): item for item in self.items}
            for future in concurrent.futures.as_completed(futures):
                item = futures[future]
                try:
                    result = future.result()
                    with self._lock:
                        self._results.succeeded.append(result)  # type: ignore
                except Exception as e:
                    with self._lock:
                        self._results.failed.append((item, e))  # type: ignore
        return self._results

    def _process_item(self, item: T) -> R:
        return self.processor(item)


def batch_process(
    items: list[T],
    fn: Callable[[list[T]], list[R]],
    chunk_size: int = 100,
) -> list[R]:
    """Process items in batches, applying fn to each chunk."""
    results: list[R] = []
    for chunk in chunked(items, chunk_size):
        results.extend(fn(chunk))
    return results


class StreamProcessor(Generic[T]):
    """Process a stream of items with buffering and flush callbacks."""

    def __init__(
        self,
        flush_fn: Callable[[list[T]], None],
        buffer_size: int = 100,
        flush_interval_seconds: float = 5.0,
    ) -> None:
        self.flush_fn = flush_fn
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval_seconds
        self._buffer: deque[T] = deque(maxlen=buffer_size)
        self._last_flush = time.time()
        self._lock = threading.Lock()

    def push(self, item: T) -> None:
        with self._lock:
            self._buffer.append(item)
            if len(self._buffer) >= self.buffer_size or self._should_flush():
                self._flush()

    def _should_flush(self) -> bool:
        return time.time() - self._last_flush >= self.flush_interval

    def _flush(self) -> None:
        if not self._buffer:
            return
        items = list(self._buffer)
        self._buffer.clear()
        self._last_flush = time.time()
        try:
            self.flush_fn(items)
        except Exception:
            pass

    def close(self) -> None:
        with self._lock:
            self._flush()


import time as _time

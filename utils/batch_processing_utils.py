"""Batch processing utilities: chunking, parallel processing, and result aggregation."""

from __future__ import annotations

import multiprocessing
import threading
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Generator, Iterator

__all__ = [
    "BatchConfig",
    "BatchProcessor",
    "chunk",
    "process_parallel",
    "process_map",
]


@dataclass
class BatchConfig:
    """Configuration for batch processing."""

    chunk_size: int = 100
    max_workers: int = 4
    use_processes: bool = False
    timeout_seconds: float = 300.0


def chunk(items: list[Any], size: int) -> Generator[list[Any], None, None]:
    """Split items into chunks of specified size."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


@dataclass
class BatchProcessor:
    """Process items in configurable batches."""

    config: BatchConfig = field(default_factory=BatchConfig)

    def __post_init__(self) -> None:
        self._pool: Callable[[], Any]
        if self.config.use_processes:
            self._pool = lambda: ProcessPoolExecutor(max_workers=self.config.max_workers)
        else:
            self._pool = lambda: ThreadPoolExecutor(max_workers=self.config.max_workers)

    def process(
        self,
        items: list[Any],
        process_fn: Callable[[Any], Any],
        reduce_fn: Callable[[list[Any]], Any] | None = None,
    ) -> list[Any]:
        """Process items in parallel batches."""
        chunks_list = list(chunk(items, self.config.chunk_size))
        results: list[Any] = []

        with self._pool() as executor:
            futures = [executor.submit(self._process_chunk, chunk, process_fn) for chunk in chunks_list]
            for future in futures:
                try:
                    result = future.result(timeout=self.config.timeout_seconds)
                    if reduce_fn:
                        results.append(result)
                    else:
                        results.extend(result)
                except Exception:
                    pass

        return results

    def _process_chunk(
        self,
        chunk: list[Any],
        process_fn: Callable[[Any], Any],
    ) -> list[Any]:
        """Process a single chunk of items."""
        return [process_fn(item) for item in chunk]


def process_parallel(
    func: Callable[[Any], Any],
    items: list[Any],
    max_workers: int = 4,
    use_processes: bool = False,
) -> list[Any]:
    """Process items in parallel using a thread or process pool."""
    executor_class = ProcessPoolExecutor if use_processes else ThreadPoolExecutor
    with executor_class(max_workers=max_workers) as executor:
        return list(executor.map(func, items))


def process_map(
    func: Callable[[Any], Any],
    items: Iterator[Any],
    chunk_size: int = 1,
    max_workers: int = 4,
) -> Generator[Any, None, None]:
    """Process items as a generator with configurable chunk size."""
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(func, items, chunksize=chunk_size):
            yield result


class StreamBatchProcessor:
    """Process streaming data in batches."""

    def __init__(self, batch_size: int = 100, flush_interval: float = 5.0) -> None:
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._buffer: list[Any] = []
        self._last_flush = time.time()
        self._lock = threading.Lock()

    def add(self, item: Any) -> list[Any] | None:
        """Add an item to the buffer, flush if full."""
        with self._lock:
            self._buffer.append(item)
            if len(self._buffer) >= self.batch_size:
                return self._flush()
            return None

    def should_flush(self) -> bool:
        """Check if buffer should be flushed by time."""
        return (time.time() - self._last_flush) >= self.flush_interval

    def flush(self) -> list[Any]:
        """Force flush the buffer."""
        with self._lock:
            return self._flush()

    def _flush(self) -> list[Any]:
        """Internal flush."""
        batch = list(self._buffer)
        self._buffer.clear()
        self._last_flush = time.time()
        return batch

"""
Action Batch Module.

Provides utilities for batching multiple actions together for efficient
execution, including batching strategies, priority handling, and batch
result aggregation.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Generic, TypeVar


logger = logging.getLogger(__name__)

T = TypeVar('T')
R = TypeVar('R')


class BatchStrategy(Enum):
    """Strategy for batch execution."""
    FIXED_SIZE = auto()
    TIMED = auto()
    ADAPTIVE = auto()
    PRIORITY = auto()


@dataclass
class BatchItem(Generic[T]):
    """An item to be processed in a batch."""
    id: str
    data: T
    priority: int = 0
    created_at: float = field(default_factory=time.time)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __lt__(self, other: BatchItem) -> bool:
        """Compare items by priority for heap ordering."""
        return self.priority < other.priority


@dataclass
class BatchResult(Generic[R]):
    """Result from processing a batch."""
    batch_id: str
    total_items: int
    successful: int
    failed: int
    results: list[R]
    errors: list[Exception]
    duration_ms: float
    metadata: dict[str, Any] = field(default_factory=dict)


class BatchProcessor(Generic[T, R]):
    """
    Processes items in batches using configurable strategies.

    Example:
        >>> processor = BatchProcessor(
        ...     batch_size=10,
        ...     processor_func=my_func
        ... )
        >>> processor.add_item(BatchItem("1", data))
        >>> results = await processor.flush()
    """

    def __init__(
        self,
        processor_func: Callable[[list[T]], list[R]],
        batch_size: int = 10,
        strategy: BatchStrategy = BatchStrategy.FIXED_SIZE,
        timeout_ms: float = 1000.0
    ) -> None:
        """
        Initialize the batch processor.

        Args:
            processor_func: Function that processes a batch of items.
            batch_size: Maximum items per batch.
            strategy: Batching strategy to use.
            timeout_ms: Timeout in milliseconds for timed batches.
        """
        self.processor_func = processor_func
        self.batch_size = batch_size
        self.strategy = strategy
        self.timeout_ms = timeout_ms

        self._items: list[BatchItem[T]] = []
        self._batch_counter: int = 0
        self._lock = asyncio.Lock()

    def add_item(self, item: BatchItem[T]) -> None:
        """
        Add an item to the batch queue.

        Args:
            item: The item to add.
        """
        self._items.append(item)

        if self.strategy == BatchStrategy.PRIORITY:
            self._items.sort(reverse=True)

    def add_items(self, items: list[BatchItem[T]]) -> None:
        """
        Add multiple items to the batch queue.

        Args:
            items: Items to add.
        """
        for item in items:
            self.add_item(item)

    def should_flush(self) -> bool:
        """
        Determine if the batch should be flushed.

        Returns:
            True if batch should be processed.
        """
        if not self._items:
            return False

        if self.strategy == BatchStrategy.FIXED_SIZE:
            return len(self._items) >= self.batch_size

        if self.strategy == BatchStrategy.TIMED:
            if self._items:
                oldest = min(item.created_at for item in self._items)
                elapsed = (time.time() - oldest) * 1000
                return elapsed >= self.timeout_ms

        if self.strategy == BatchStrategy.ADAPTIVE:
            return len(self._items) >= max(1, self.batch_size // 2)

        if self.strategy == BatchStrategy.PRIORITY:
            return len(self._items) >= max(1, self.batch_size // 3)

        return len(self._items) >= self.batch_size

    async def flush(self) -> BatchResult[R]:
        """
        Flush the current batch and process items.

        Returns:
            BatchResult with processing outcomes.
        """
        async with self._lock:
            if not self._items:
                return BatchResult(
                    batch_id="",
                    total_items=0,
                    successful=0,
                    failed=0,
                    results=[],
                    errors=[],
                    duration_ms=0.0
                )

            self._batch_counter += 1
            batch_id = f"batch_{self._batch_counter}"

            items_to_process = self._items[:]
            self._items.clear()

            logger.info(
                f"Processing {len(items_to_process)} items in {batch_id}"
            )

            start = time.perf_counter()

            try:
                data_list = [item.data for item in items_to_process]
                results = await asyncio.get_event_loop().run_in_executor(
                    None,
                    self.processor_func,
                    data_list
                )

                duration = (time.perf_counter() - start) * 1000

                successful = len(results) if results else 0
                failed = len(items_to_process) - successful

                return BatchResult(
                    batch_id=batch_id,
                    total_items=len(items_to_process),
                    successful=successful,
                    failed=failed,
                    results=results if results else [],
                    errors=[],
                    duration_ms=duration,
                    metadata={"strategy": self.strategy.name}
                )

            except Exception as e:
                duration = (time.perf_counter() - start) * 1000
                logger.error(f"Batch {batch_id} failed: {e}")

                return BatchResult(
                    batch_id=batch_id,
                    total_items=len(items_to_process),
                    successful=0,
                    failed=len(items_to_process),
                    results=[],
                    errors=[e],
                    duration_ms=duration
                )

    def pending_count(self) -> int:
        """
        Get the number of pending items.

        Returns:
            Count of items waiting to be processed.
        """
        return len(self._items)


class BatchManager(Generic[T, R]):
    """
    Manages multiple batch processors with different configurations.
    """

    def __init__(self) -> None:
        """Initialize the batch manager."""
        self._processors: dict[str, BatchProcessor[T, R]] = {}
        self._flush_tasks: dict[str, asyncio.Task[None]] = {}

    def create_processor(
        self,
        name: str,
        processor_func: Callable[[list[T]], list[R]],
        batch_size: int = 10,
        strategy: BatchStrategy = BatchStrategy.FIXED_SIZE,
        timeout_ms: float = 1000.0
    ) -> BatchProcessor[T, R]:
        """
        Create a named batch processor.

        Args:
            name: Unique name for the processor.
            processor_func: Processing function.
            batch_size: Batch size.
            strategy: Batching strategy.
            timeout_ms: Timeout for timed batches.

        Returns:
            The created BatchProcessor.
        """
        processor = BatchProcessor(
            processor_func=processor_func,
            batch_size=batch_size,
            strategy=strategy,
            timeout_ms=timeout_ms
        )

        self._processors[name] = processor  # type: ignore
        return processor  # type: ignore

    def get_processor(self, name: str) -> BatchProcessor[T, R] | None:
        """
        Get a processor by name.

        Args:
            name: Processor name.

        Returns:
            The processor or None if not found.
        """
        return self._processors.get(name)  # type: ignore

    async def flush_all(self) -> list[BatchResult[R]]:
        """
        Flush all processors.

        Returns:
            List of results from all processors.
        """
        results: list[BatchResult[R]] = []

        for name, processor in self._processors.items():
            if processor.should_flush():
                result = await processor.flush()
                results.append(result)
                logger.info(f"Flushed processor '{name}'")

        return results

    async def start_auto_flush(
        self,
        name: str,
        interval_ms: float = 1000.0
    ) -> None:
        """
        Start automatic periodic flushing for a processor.

        Args:
            name: Processor name.
            interval_ms: Flush interval in milliseconds.
        """
        if name not in self._processors:
            raise ValueError(f"Processor '{name}' not found")

        async def flush_loop() -> None:
            while True:
                await asyncio.sleep(interval_ms / 1000.0)
                processor = self._processors[name]
                if processor.should_flush():
                    await processor.flush()

        self._flush_tasks[name] = asyncio.create_task(flush_loop())
        logger.info(f"Started auto-flush for '{name}'")

    async def stop_auto_flush(self, name: str) -> None:
        """
        Stop automatic flushing for a processor.

        Args:
            name: Processor name.
        """
        task = self._flush_tasks.get(name)
        if task:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            del self._flush_tasks[name]
            logger.info(f"Stopped auto-flush for '{name}'")


class BatchBuilder(Generic[T, R]):
    """
    Builder for creating configured batch processors.
    """

    def __init__(self) -> None:
        """Initialize the batch builder."""
        self._func: Callable[[list[T]], list[R]] | None = None
        self._size: int = 10
        self._strategy: BatchStrategy = BatchStrategy.FIXED_SIZE
        self._timeout: float = 1000.0

    def with_processor_func(
        self,
        func: Callable[[list[T]], list[R]]
    ) -> BatchBuilder[T, R]:
        """Set the processor function."""
        self._func = func
        return self

    def with_batch_size(self, size: int) -> BatchBuilder[T, R]:
        """Set the batch size."""
        self._size = size
        return self

    def with_strategy(self, strategy: BatchStrategy) -> BatchBuilder[T, R]:
        """Set the batching strategy."""
        self._strategy = strategy
        return self

    def with_timeout(self, timeout_ms: float) -> BatchBuilder[T, R]:
        """Set the timeout for timed batches."""
        self._timeout = timeout_ms
        return self

    def build(self) -> BatchProcessor[T, R]:
        """
        Build the batch processor.

        Returns:
            Configured BatchProcessor.

        Raises:
            ValueError: If processor function not set.
        """
        if not self._func:
            raise ValueError("Processor function not set")

        return BatchProcessor(
            processor_func=self._func,
            batch_size=self._size,
            strategy=self._strategy,
            timeout_ms=self._timeout
        )

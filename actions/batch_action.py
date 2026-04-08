"""Batch action module for RabAI AutoClick.

Provides batch processing utilities:
- BatchProcessor: Process items in batches
- BatchExecutor: Execute batch operations
- BatchCollector: Collect results
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


T = TypeVar("T")
R = TypeVar("R")


@dataclass
class BatchResult:
    """Result of batch processing."""
    total: int
    successful: int
    failed: int
    duration: float
    results: List[Any]


class BatchProcessor(Generic[T, R]):
    """Batch processor."""

    def __init__(
        self,
        batch_size: int = 100,
        max_workers: int = 4,
        stop_on_error: bool = False,
    ):
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.stop_on_error = stop_on_error

    def process(
        self,
        items: List[T],
        processor: Callable[[T], R],
    ) -> BatchResult:
        """Process items in batches."""
        start_time = time.time()
        total = len(items)
        successful = 0
        failed = 0
        results = []

        batches = [items[i:i+self.batch_size] for i in range(0, total, self.batch_size)]

        for batch in batches:
            for item in batch:
                try:
                    result = processor(item)
                    results.append({"success": True, "result": result})
                    successful += 1
                except Exception as e:
                    results.append({"success": False, "error": str(e)})
                    failed += 1
                    if self.stop_on_error:
                        break

        duration = time.time() - start_time

        return BatchResult(
            total=total,
            successful=successful,
            failed=failed,
            duration=duration,
            results=results,
        )

    def process_parallel(
        self,
        items: List[T],
        processor: Callable[[T], R],
    ) -> BatchResult:
        """Process items in parallel."""
        start_time = time.time()
        total = len(items)
        successful = 0
        failed = 0
        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_item = {executor.submit(processor, item): item for item in items}

            for future in as_completed(future_to_item):
                item = future_to_item[future]
                try:
                    result = future.result()
                    results.append({"success": True, "result": result, "item": str(item)[:50]})
                    successful += 1
                except Exception as e:
                    results.append({"success": False, "error": str(e), "item": str(item)[:50]})
                    failed += 1

        duration = time.time() - start_time

        return BatchResult(
            total=total,
            successful=successful,
            failed=failed,
            duration=duration,
            results=results,
        )

    def chunk(self, items: List[T]) -> List[List[T]]:
        """Split items into chunks."""
        return [items[i:i+self.batch_size] for i in range(0, len(items), self.batch_size)]


class BatchExecutor:
    """Batch executor."""

    def __init__(self, batch_size: int = 50):
        self.batch_size = batch_size

    def execute(
        self,
        items: List[T],
        executor: Callable[[List[T]], List[R]],
    ) -> List[R]:
        """Execute batches."""
        chunks = [items[i:i+self.batch_size] for i in range(0, len(items), self.batch_size)]
        results = []

        for chunk in chunks:
            chunk_results = executor(chunk)
            results.extend(chunk_results)

        return results

    def execute_parallel(
        self,
        items: List[T],
        executor: Callable[[List[T]], List[R]],
        max_workers: int = 4,
    ) -> List[R]:
        """Execute batches in parallel."""
        chunks = [items[i:i+self.batch_size] for i in range(0, len(items), self.batch_size)]
        results = []

        with ThreadPoolExecutor(max_workers=max_workers) as exec:
            futures = [exec.submit(executor, chunk) for chunk in chunks]

            for future in as_completed(futures):
                results.extend(future.result())

        return results


class BatchCollector(Generic[T]):
    """Collect batch results."""

    def __init__(self):
        self._items: List[T] = []
        self._lock = False

    def add(self, item: T) -> None:
        """Add an item."""
        self._items.append(item)

    def add_batch(self, items: List[T]) -> None:
        """Add multiple items."""
        self._items.extend(items)

    def get_all(self) -> List[T]:
        """Get all items."""
        return self._items.copy()

    def clear(self) -> None:
        """Clear all items."""
        self._items.clear()

    def size(self) -> int:
        """Get number of items."""
        return len(self._items)

    def filter(self, predicate: Callable[[T], bool]) -> List[T]:
        """Filter items."""
        return [item for item in self._items if predicate(item)]

    def map(self, mapper: Callable[[T], R]) -> List[R]:
        """Map items."""
        return [mapper(item) for item in self._items]


class BatchAction(BaseAction):
    """Batch processing action."""
    action_type = "batch"
    display_name = "批量处理"
    description = "批量数据处理"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "process")

            if operation == "process":
                return self._process(params)
            elif operation == "process_parallel":
                return self._process_parallel(params)
            elif operation == "chunk":
                return self._chunk(params)
            elif operation == "collect":
                return self._collect(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Batch error: {str(e)}")

    def _process(self, params: Dict[str, Any]) -> ActionResult:
        """Process items in batches."""
        items = params.get("items", [])
        batch_size = params.get("batch_size", 100)
        processor_fn = params.get("processor")

        if not items:
            return ActionResult(success=False, message="items is required")

        def default_processor(item):
            return item

        processor = processor_fn if callable(processor_fn) else default_processor

        bp = BatchProcessor(batch_size=batch_size)
        result = bp.process(items, processor)

        return ActionResult(
            success=result.failed == 0,
            message=f"Processed: {result.successful}/{result.total} in {result.duration:.2f}s",
            data={
                "total": result.total,
                "successful": result.successful,
                "failed": result.failed,
                "duration": result.duration,
            },
        )

    def _process_parallel(self, params: Dict[str, Any]) -> ActionResult:
        """Process items in parallel."""
        items = params.get("items", [])
        max_workers = params.get("max_workers", 4)
        processor_fn = params.get("processor")

        if not items:
            return ActionResult(success=False, message="items is required")

        def default_processor(item):
            return item

        processor = processor_fn if callable(processor_fn) else default_processor

        bp = BatchProcessor(max_workers=max_workers)
        result = bp.process_parallel(items, processor)

        return ActionResult(
            success=result.failed == 0,
            message=f"Processed: {result.successful}/{result.total} in {result.duration:.2f}s",
            data={
                "total": result.total,
                "successful": result.successful,
                "failed": result.failed,
                "duration": result.duration,
            },
        )

    def _chunk(self, params: Dict[str, Any]) -> ActionResult:
        """Split items into chunks."""
        items = params.get("items", [])
        batch_size = params.get("batch_size", 10)

        bp = BatchProcessor(batch_size=batch_size)
        chunks = bp.chunk(items)

        return ActionResult(
            success=True,
            message=f"Split into {len(chunks)} chunks",
            data={"chunks": chunks, "count": len(chunks)},
        )

    def _collect(self, params: Dict[str, Any]) -> ActionResult:
        """Collect items."""
        items = params.get("items", [])
        operation = params.get("collect_operation", "get_all")

        collector = BatchCollector()
        collector.add_batch(items)

        if operation == "get_all":
            result = collector.get_all()
        elif operation == "filter":
            predicate = params.get("predicate", lambda x: True)
            result = collector.filter(predicate)
        elif operation == "map":
            mapper = params.get("mapper", lambda x: x)
            result = collector.map(mapper)
        elif operation == "size":
            return ActionResult(success=True, message=f"Size: {collector.size()}", data={"size": collector.size()})
        elif operation == "clear":
            collector.clear()
            result = []
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")

        return ActionResult(success=True, message=f"Collected {len(result)} items", data={"items": result, "count": len(result)})

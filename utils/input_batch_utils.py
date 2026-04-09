"""
Input batch processing utilities for automation.

This module provides utilities for batching multiple input
operations together for efficient execution.
"""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Any, Dict, Tuple
from enum import Enum, auto


class BatchStrategy(Enum):
    """Strategy for processing input batches."""
    SEQUENTIAL = auto()
    PARALLEL = auto()
    PRIORITY = auto()
    DEBOUNCE = auto()


@dataclass
class InputBatchItem:
    """
    Single item in an input batch.

    Attributes:
        item_id: Unique identifier for the item.
        action: The action to execute.
        priority: Priority level (higher = earlier execution).
        created_at: When the item was created.
        dependencies: IDs of items that must complete first.
    """
    item_id: str
    action: Callable[[], Any]
    priority: int = 0
    created_at: float = field(default_factory=time.time)
    dependencies: List[str] = field(default_factory=list)


@dataclass
class BatchResult:
    """
    Result of batch processing.

    Attributes:
        batch_id: Identifier for the batch.
        results: Map of item_id to result/error.
        total_duration: Total processing time.
        success_count: Number of successful items.
        failure_count: Number of failed items.
    """
    batch_id: str
    results: Dict[str, Any] = field(default_factory=dict)
    total_duration: float = 0.0
    success_count: int = 0
    failure_count: int = 0


class InputBatch:
    """
    Collection of input operations to be executed together.

    Supports various processing strategies and dependencies.
    """

    def __init__(self, batch_id: str = "") -> None:
        self._batch_id = batch_id or f"batch_{time.time()}"
        self._items: List[InputBatchItem] = []
        self._lock = threading.Lock()

    @property
    def batch_id(self) -> str:
        """Get batch identifier."""
        return self._batch_id

    def add(
        self,
        action: Callable[[], Any],
        item_id: Optional[str] = None,
        priority: int = 0,
        dependencies: Optional[List[str]] = None,
    ) -> str:
        """
        Add an action to the batch.

        Returns the item ID for later reference.
        """
        item_id = item_id or f"item_{len(self._items)}"

        with self._lock:
            self._items.append(InputBatchItem(
                item_id=item_id,
                action=action,
                priority=priority,
                dependencies=dependencies or [],
            ))

        return item_id

    def add_click(self, x: float, y: float, item_id: Optional[str] = None) -> str:
        """Add a click action to the batch."""
        def click_action() -> None:
            pass  # Actual click implementation would go here
        return self.add(click_action, item_id)

    def add_type(self, text: str, item_id: Optional[str] = None) -> str:
        """Add a text input action to the batch."""
        def type_action() -> None:
            pass  # Actual type implementation would go here
        return self.add(type_action, item_id)

    def add_wait(self, duration: float, item_id: Optional[str] = None) -> str:
        """Add a wait action to the batch."""
        def wait_action() -> None:
            time.sleep(duration)
        return self.add(wait_action, item_id)

    def size(self) -> int:
        """Get number of items in batch."""
        return len(self._items)

    def clear(self) -> None:
        """Remove all items from batch."""
        with self._lock:
            self._items.clear()

    def get_items(self, sorted_by: BatchStrategy = BatchStrategy.SEQUENTIAL) -> List[InputBatchItem]:
        """Get batch items, optionally sorted by strategy."""
        with self._lock:
            items = self._items.copy()

        if sorted_by == BatchStrategy.PRIORITY:
            items.sort(key=lambda x: -x.priority)
        elif sorted_by == BatchStrategy.SEQUENTIAL:
            items.sort(key=lambda x: x.created_at)

        return items


class BatchProcessor:
    """
    Processes batches of input operations.

    Supports sequential, parallel, and priority-based execution.
    """

    def __init__(self, strategy: BatchStrategy = BatchStrategy.SEQUENTIAL) -> None:
        self._strategy = strategy
        self._max_workers: int = 4
        self._batch_handlers: Dict[str, Callable[[InputBatch], BatchResult]] = {}

    def set_strategy(self, strategy: BatchStrategy) -> BatchProcessor:
        """Set the batch processing strategy."""
        self._strategy = strategy
        return self

    def set_max_workers(self, workers: int) -> BatchProcessor:
        """Set maximum parallel workers."""
        self._max_workers = max(1, workers)
        return self

    def process(self, batch: InputBatch) -> BatchResult:
        """
        Process a batch according to the current strategy.

        Returns BatchResult with all outcomes.
        """
        start_time = time.time()
        results: Dict[str, Any] = {}

        if self._strategy == BatchStrategy.SEQUENTIAL:
            results = self._process_sequential(batch)
        elif self._strategy == BatchStrategy.PARALLEL:
            results = self._process_parallel(batch)
        elif self._strategy == BatchStrategy.PRIORITY:
            results = self._process_priority(batch)
        elif self._strategy == BatchStrategy.DEBOUNCE:
            results = self._process_debounce(batch)

        duration = time.time() - start_time

        success_count = sum(1 for r in results.values() if not isinstance(r, Exception))
        failure_count = len(results) - success_count

        return BatchResult(
            batch_id=batch.batch_id,
            results=results,
            total_duration=duration,
            success_count=success_count,
            failure_count=failure_count,
        )

    def _process_sequential(self, batch: InputBatch) -> Dict[str, Any]:
        """Process items one at a time."""
        results: Dict[str, Any] = {}

        for item in batch.get_items(BatchStrategy.SEQUENTIAL):
            try:
                results[item.item_id] = item.action()
            except Exception as e:
                results[item.item_id] = e

        return results

    def _process_parallel(self, batch: InputBatch) -> Dict[str, Any]:
        """Process items in parallel threads."""
        results: Dict[str, Any] = {}
        items = batch.get_items()
        lock = threading.Lock()

        def process_item(item: InputBatchItem) -> None:
            try:
                result = item.action()
                with lock:
                    results[item.item_id] = result
            except Exception as e:
                with lock:
                    results[item.item_id] = e

        threads: List[threading.Thread] = []
        for item in items:
            t = threading.Thread(target=process_item, args=(item,))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        return results

    def _process_priority(self, batch: InputBatch) -> Dict[str, Any]:
        """Process items by priority order."""
        return self._process_sequential(batch)

    def _process_debounce(self, batch: InputBatch) -> Dict[str, Any]:
        """Process items with debouncing."""
        return self._process_sequential(batch)


class BatchBuilder:
    """
    Fluent builder for creating input batches.

    Provides a chainable API for constructing batches.
    """

    def __init__(self, batch_id: Optional[str] = None) -> None:
        self._batch = InputBatch(batch_id)

    def click(self, x: float, y: float, priority: int = 0) -> BatchBuilder:
        """Add a click action."""
        self._batch.add_click(x, y, priority=priority)
        return self

    def type(self, text: str, priority: int = 0) -> BatchBuilder:
        """Add a type action."""
        self._batch.add_type(text, priority=priority)
        return self

    def wait(self, duration: float, priority: int = 0) -> BatchBuilder:
        """Add a wait action."""
        self._batch.add_wait(duration, priority=priority)
        return self

    def custom(
        self,
        action: Callable[[], Any],
        item_id: Optional[str] = None,
        priority: int = 0,
    ) -> BatchBuilder:
        """Add a custom action."""
        self._batch.add(action, item_id, priority)
        return self

    def build(self) -> InputBatch:
        """Build and return the batch."""
        return self._batch


class InputSequenceBuilder:
    """
    Builder for creating sequences of input operations.

    Supports chaining of multiple input types with timing.
    """

    def __init__(self) -> None:
        self._actions: List[Tuple[str, Dict[str, Any]]] = []

    def move_to(self, x: float, y: float) -> InputSequenceBuilder:
        """Add mouse move action."""
        self._actions.append(("move_to", {"x": x, "y": y}))
        return self

    def click(self, x: float, y: float, button: str = "left") -> InputSequenceBuilder:
        """Add click action."""
        self._actions.append(("click", {"x": x, "y": y, "button": button}))
        return self

    def double_click(self, x: float, y: float) -> InputSequenceBuilder:
        """Add double click action."""
        self._actions.append(("double_click", {"x": x, "y": y}))
        return self

    def right_click(self, x: float, y: float) -> InputSequenceBuilder:
        """Add right click action."""
        self._actions.append(("right_click", {"x": x, "y": y}))
        return self

    def scroll(self, dx: float, dy: float) -> InputSequenceBuilder:
        """Add scroll action."""
        self._actions.append(("scroll", {"dx": dx, "dy": dy}))
        return self

    def type_text(self, text: str) -> InputSequenceBuilder:
        """Add text input action."""
        self._actions.append(("type", {"text": text}))
        return self

    def press_key(self, key: str) -> InputSequenceBuilder:
        """Add key press action."""
        self._actions.append(("key", {"key": key}))
        return self

    def wait(self, duration: float) -> InputSequenceBuilder:
        """Add wait action."""
        self._actions.append(("wait", {"duration": duration}))
        return self

    def build(self) -> List[Tuple[str, Dict[str, Any]]]:
        """Build and return the action sequence."""
        return self._actions.copy()

    def clear(self) -> InputSequenceBuilder:
        """Clear all actions."""
        self._actions.clear()
        return self

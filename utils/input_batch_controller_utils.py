"""
Input batch controller utilities.

Batch multiple input actions for efficient execution.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional, Any
from enum import Enum, auto


class BatchStrategy(Enum):
    """Strategy for batching input actions."""
    SEQUENTIAL = auto()
    PARALLEL = auto()
    PRIORITY = auto()
    DEBOUNCE = auto()


@dataclass
class BatchItem:
    """An item in an input batch."""
    item_id: str
    action_type: str
    priority: int = 0
    timestamp: float = field(default_factory=time.time)
    data: dict = field(default_factory=dict)
    callback: Optional[Callable] = None


class InputBatchController:
    """Controller for batching input actions."""
    
    def __init__(self, strategy: BatchStrategy = BatchStrategy.SEQUENTIAL):
        self.strategy = strategy
        self._queue: list[BatchItem] = []
        self._batch_size = 10
        self._batch_timeout_ms = 100
        self._last_batch_time = time.time()
        self._execution_callbacks: list[Callable[[list[BatchItem]], None]] = []
    
    def add_item(self, item: BatchItem) -> None:
        """Add an item to the batch queue."""
        self._queue.append(item)
        
        if len(self._queue) >= self._batch_size:
            self.flush()
    
    def add(
        self,
        action_type: str,
        data: dict,
        priority: int = 0,
        callback: Optional[Callable] = None
    ) -> BatchItem:
        """Add an action to the batch."""
        item = BatchItem(
            item_id=f"{action_type}_{len(self._queue)}",
            action_type=action_type,
            priority=priority,
            data=data,
            callback=callback
        )
        self.add_item(item)
        return item
    
    def should_flush(self) -> bool:
        """Check if batch should be flushed."""
        if len(self._queue) >= self._batch_size:
            return True
        
        elapsed = (time.time() - self._last_batch_time) * 1000
        if elapsed >= self._batch_timeout_ms and self._queue:
            return True
        
        return False
    
    def flush(self) -> list[BatchItem]:
        """Flush the batch and execute actions."""
        if not self._queue:
            return []
        
        items = self._get_sorted_items()
        self._execute_batch(items)
        
        self._queue.clear()
        self._last_batch_time = time.time()
        
        return items
    
    def _get_sorted_items(self) -> list[BatchItem]:
        """Get items sorted by strategy."""
        if self.strategy == BatchStrategy.PRIORITY:
            return sorted(self._queue, key=lambda i: -i.priority)
        elif self.strategy == BatchStrategy.DEBOUNCE:
            return self._debounce_items()
        return list(self._queue)
    
    def _debounce_items(self) -> list[BatchItem]:
        """Debounce items by action type."""
        seen: dict[str, BatchItem] = {}
        
        for item in self._queue:
            key = item.action_type
            if key not in seen:
                seen[key] = item
        
        return list(seen.values())
    
    def _execute_batch(self, items: list[BatchItem]) -> None:
        """Execute a batch of items."""
        for callback in self._execution_callbacks:
            callback(items)
        
        for item in items:
            if item.callback:
                item.callback(item)
    
    def on_batch_execute(self, callback: Callable[[list[BatchItem]], None]) -> None:
        """Register callback for batch execution."""
        self._execution_callbacks.append(callback)
    
    def set_batch_size(self, size: int) -> None:
        """Set maximum batch size."""
        self._batch_size = size
    
    def set_batch_timeout(self, timeout_ms: float) -> None:
        """Set batch timeout in milliseconds."""
        self._batch_timeout_ms = timeout_ms
    
    def get_queue_size(self) -> int:
        """Get current queue size."""
        return len(self._queue)
    
    def clear(self) -> None:
        """Clear the queue without executing."""
        self._queue.clear()

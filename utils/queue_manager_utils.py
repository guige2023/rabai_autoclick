"""
Queue Manager Utilities

Provides utilities for managing task queues
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any, Callable
from dataclasses import dataclass
import asyncio


@dataclass
class QueueItem:
    """Item in the queue."""
    id: str
    data: Any
    priority: int = 0


class QueueManager:
    """
    Manages task queues with priorities.
    
    Supports enqueue, dequeue, and bulk
    operations.
    """

    def __init__(self) -> None:
        self._queue: list[QueueItem] = []
        self._processing = False
        self._handlers: dict[str, Callable[[Any], None]] = {}

    def enqueue(
        self,
        item_id: str,
        data: Any,
        priority: int = 0,
    ) -> None:
        """Add item to queue."""
        item = QueueItem(id=item_id, data=data, priority=priority)
        self._queue.append(item)
        self._queue.sort(key=lambda x: -x.priority)

    def dequeue(self) -> QueueItem | None:
        """Remove and return next item."""
        if self._queue:
            return self._queue.pop(0)
        return None

    def peek(self) -> QueueItem | None:
        """View next item without removing."""
        if self._queue:
            return self._queue[0]
        return None

    def size(self) -> int:
        """Get queue size."""
        return len(self._queue)

    def is_empty(self) -> bool:
        """Check if queue is empty."""
        return len(self._queue) == 0

    def clear(self) -> None:
        """Clear all items."""
        self._queue.clear()

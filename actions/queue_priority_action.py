"""
Priority queue action for managing items with priority levels.

This module provides actions for priority queue operations including
enqueue, dequeue, peek, and priority management with multiple backend options.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import heapq
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


class QueueBackend(Enum):
    """Backend storage types for priority queues."""
    HEAP = "heap"
    SORTED_LIST = "sorted_list"
    DOUBLE_ENDED = "double_ended"


class ItemStatus(Enum):
    """Status of a queue item."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class QueueItem:
    """A single item in the priority queue."""
    id: str
    value: Any
    priority: int
    created_at: datetime
    status: ItemStatus = ItemStatus.PENDING
    attempts: int = 0
    max_attempts: int = 3
    metadata: Dict[str, Any] = field(default_factory=dict)
    scheduled_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error: Optional[str] = None

    @property
    def is_retriable(self) -> bool:
        """Check if the item can be retried."""
        return self.attempts < self.max_attempts and self.status == ItemStatus.FAILED

    def to_dict(self) -> Dict[str, Any]:
        """Convert item to dictionary."""
        return {
            "id": self.id,
            "value": self.value,
            "priority": self.priority,
            "created_at": self.created_at.isoformat(),
            "status": self.status.value,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "metadata": self.metadata,
            "scheduled_at": self.scheduled_at.isoformat() if self.scheduled_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error": self.error,
        }


@dataclass
class PriorityQueueConfig:
    """Configuration for priority queue."""
    backend: QueueBackend = QueueBackend.HEAP
    max_size: Optional[int] = None
    default_priority: int = 0
    min_priority: int = 0
    max_priority: int = 100
    default_max_attempts: int = 3
    visibility_timeout: int = 30
    dead_letter_queue: Optional[str] = None
    allow_duplicates: bool = True


class PriorityQueue:
    """
    Thread-safe priority queue with configurable backends.

    Supports heap and sorted list backends, priority ranges,
    item retry, and dead letter handling.
    """

    def __init__(
        self,
        name: str,
        config: Optional[PriorityQueueConfig] = None,
    ):
        """
        Initialize the priority queue.

        Args:
            name: Name of the queue.
            config: Optional queue configuration.
        """
        self.name = name
        self.config = config or PriorityQueueConfig()

        self._heap: List[Tuple[int, float, QueueItem]] = []
        self._items: Dict[str, QueueItem] = {}
        self._lock = threading.RLock()
        self._counter = 0

        self._processing: Dict[str, QueueItem] = {}
        self._dead_letter: List[QueueItem] = []

    def enqueue(
        self,
        value: Any,
        priority: Optional[int] = None,
        max_attempts: Optional[int] = None,
        metadata: Optional[Dict[str, Any]] = None,
        scheduled_at: Optional[datetime] = None,
    ) -> QueueItem:
        """
        Add an item to the priority queue.

        Args:
            value: The item value to enqueue.
            priority: Priority level (0 = highest, higher = lower priority).
            max_attempts: Maximum retry attempts.
            metadata: Optional metadata to attach.
            scheduled_at: Optional scheduled time to make item available.

        Returns:
            The enqueued QueueItem.

        Raises:
            ValueError: If queue is full or priority is invalid.
        """
        if priority is None:
            priority = self.config.default_priority

        if not (self.config.min_priority <= priority <= self.config.max_priority):
            raise ValueError(
                f"Priority must be between {self.config.min_priority} "
                f"and {self.config.max_priority}"
            )

        with self._lock:
            if self.config.max_size and len(self._items) >= self.config.max_size:
                raise ValueError(f"Queue is full (max size: {self.config.max_size})")

            item_id = str(uuid.uuid4())
            self._counter += 1

            item = QueueItem(
                id=item_id,
                value=value,
                priority=priority,
                created_at=datetime.now(),
                max_attempts=max_attempts or self.config.default_max_attempts,
                metadata=metadata or {},
                scheduled_at=scheduled_at,
            )

            self._items[item_id] = item

            if self.config.backend == QueueBackend.HEAP:
                heapq.heappush(
                    self._heap,
                    (priority, self._counter, item_id)
                )
            elif self.config.backend == QueueBackend.SORTED_LIST:
                self._sorted_insert(item)

            return item

    def _sorted_insert(self, item: QueueItem) -> None:
        """Insert item into sorted list (binary search)."""
        lo, hi = 0, len(self._heap)
        priority = item.priority

        while lo < hi:
            mid = (lo + hi) // 2
            if self._heap[mid].priority <= priority:
                lo = mid + 1
            else:
                hi = mid

        self._heap.insert(lo, item)

    def dequeue(
        self,
        timeout: float = 0,
    ) -> Optional[QueueItem]:
        """
        Remove and return the highest priority item.

        Args:
            timeout: How long to wait for an item (0 = non-blocking).

        Returns:
            The highest priority item, or None if queue is empty.

        Raises:
            TimeoutError: If timeout expires with no item.
        """
        deadline = time.time() + timeout if timeout > 0 else None

        while True:
            with self._lock:
                item = self._peek_internal()
                if item:
                    return self._do_dequeue(item.id)
                if timeout == 0:
                    return None
                if deadline and time.time() >= deadline:
                    return None

            time.sleep(0.01)

    def _peek_internal(self) -> Optional[QueueItem]:
        """Internal peek without locking."""
        while self._heap:
            if self.config.backend == QueueBackend.HEAP:
                priority, counter, item_id = self._heap[0]
            else:
                item = self._heap[0]
                item_id = item.id

            item = self._items.get(item_id)
            if item and item.status == ItemStatus.PENDING:
                if item.scheduled_at and item.scheduled_at > datetime.now():
                    return None
                return item

            if self.config.backend == QueueBackend.HEAP:
                heapq.heappop(self._heap)
            else:
                self._heap.pop(0)

        return None

    def peek(self) -> Optional[QueueItem]:
        """
        Get the highest priority item without removing it.

        Returns:
            The highest priority item, or None if queue is empty.
        """
        with self._lock:
            return self._peek_internal()

    def _do_dequeue(self, item_id: str) -> Optional[QueueItem]:
        """Internal dequeue implementation."""
        item = self._items.get(item_id)
        if not item:
            return None

        item.status = ItemStatus.PROCESSING
        self._processing[item_id] = item

        if self.config.backend == QueueBackend.HEAP:
            heapq.heappop(self._heap)
        else:
            self._heap = [i for i in self._heap if i.id != item_id]

        return item

    def acknowledge(self, item_id: str) -> bool:
        """
        Acknowledge successful processing of an item.

        Args:
            item_id: ID of the item to acknowledge.

        Returns:
            True if acknowledged, False if not found.
        """
        with self._lock:
            item = self._processing.pop(item_id, None)
            if item:
                item.status = ItemStatus.COMPLETED
                item.completed_at = datetime.now()
                self._items[item_id] = item
                return True
            return False

    def fail(
        self,
        item_id: str,
        error: Optional[str] = None,
        requeue: bool = True,
    ) -> bool:
        """
        Mark an item as failed.

        Args:
            item_id: ID of the item to fail.
            error: Optional error message.
            requeue: Whether to requeue the item for retry.

        Returns:
            True if failed, False if not found.
        """
        with self._lock:
            item = self._processing.pop(item_id, None)
            if not item:
                return False

            item.attempts += 1
            item.error = error

            if item.is_retriable and requeue:
                item.status = ItemStatus.PENDING
                self._items[item_id] = item
                if self.config.backend == QueueBackend.HEAP:
                    self._counter += 1
                    heapq.heappush(
                        self._heap,
                        (item.priority, self._counter, item_id)
                    )
                else:
                    self._sorted_insert(item)
            else:
                item.status = ItemStatus.FAILED
                self._items[item_id] = item
                if self.config.dead_letter_queue:
                    self._dead_letter.append(item)

            return True

    def cancel(self, item_id: str) -> bool:
        """
        Cancel a pending item.

        Args:
            item_id: ID of the item to cancel.

        Returns:
            True if cancelled, False if not found.
        """
        with self._lock:
            item = self._items.get(item_id)
            if item and item.status == ItemStatus.PENDING:
                item.status = ItemStatus.CANCELLED
                self._heap = [
                    (p, c, i) for p, c, i in self._heap
                    if i != item_id
                ]
                return True
            return False

    def get(self, item_id: str) -> Optional[QueueItem]:
        """Get an item by ID."""
        with self._lock:
            return self._items.get(item_id)

    def size(self) -> int:
        """Get the number of pending items."""
        with self._lock:
            return sum(
                1 for item in self._items.values()
                if item.status == ItemStatus.PENDING
            )

    def is_empty(self) -> bool:
        """Check if the queue is empty."""
        return self.size() == 0

    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics."""
        with self._lock:
            pending = sum(1 for i in self._items.values() if i.status == ItemStatus.PENDING)
            processing = sum(1 for i in self._items.values() if i.status == ItemStatus.PROCESSING)
            completed = sum(1 for i in self._items.values() if i.status == ItemStatus.COMPLETED)
            failed = sum(1 for i in self._items.values() if i.status == ItemStatus.FAILED)
            cancelled = sum(1 for i in self._items.values() if i.status == ItemStatus.CANCELLED)

            return {
                "name": self.name,
                "pending": pending,
                "processing": processing,
                "completed": completed,
                "failed": failed,
                "cancelled": cancelled,
                "dead_letter": len(self._dead_letter),
                "total": len(self._items),
            }


class PriorityQueueRegistry:
    """Thread-safe registry of priority queues."""

    def __init__(self):
        """Initialize the queue registry."""
        self._queues: Dict[str, PriorityQueue] = {}
        self._lock = threading.RLock()

    def get_or_create(
        self,
        name: str,
        config: Optional[PriorityQueueConfig] = None,
    ) -> PriorityQueue:
        """Get an existing queue or create a new one."""
        with self._lock:
            if name not in self._queues:
                self._queues[name] = PriorityQueue(name, config)
            return self._queues[name]

    def get(self, name: str) -> Optional[PriorityQueue]:
        """Get a queue by name."""
        with self._lock:
            return self._queues.get(name)


_default_registry = PriorityQueueRegistry()


def priority_enqueue_action(
    queue_name: str,
    value: Any,
    priority: int = 0,
    metadata: Optional[Dict[str, Any]] = None,
    max_attempts: int = 3,
) -> Dict[str, Any]:
    """
    Action function to enqueue an item with priority.

    Args:
        queue_name: Name of the queue.
        value: Item value to enqueue.
        priority: Priority level (0 = highest).
        metadata: Optional metadata.
        max_attempts: Maximum retry attempts.

    Returns:
        Dictionary with enqueue result.
    """
    config = PriorityQueueConfig(default_priority=priority)
    registry = PriorityQueueRegistry()
    queue = registry.get_or_create(queue_name, config)

    item = queue.enqueue(value, priority, max_attempts, metadata)
    return item.to_dict()


def priority_dequeue_action(
    queue_name: str,
    timeout: float = 0,
) -> Optional[Dict[str, Any]]:
    """
    Action function to dequeue the highest priority item.

    Args:
        queue_name: Name of the queue.
        timeout: Wait timeout in seconds.

    Returns:
        Dictionary with dequeued item or None.
    """
    registry = PriorityQueueRegistry()
    queue = registry.get_or_create(queue_name)

    item = queue.dequeue(timeout)
    return item.to_dict() if item else None


def priority_acknowledge_action(
    queue_name: str,
    item_id: str,
) -> bool:
    """Acknowledge successful processing of an item."""
    registry = PriorityQueueRegistry()
    queue = registry.get(queue_name)
    if queue:
        return queue.acknowledge(item_id)
    return False


def priority_fail_action(
    queue_name: str,
    item_id: str,
    error: Optional[str] = None,
    requeue: bool = True,
) -> bool:
    """Mark an item as failed."""
    registry = PriorityQueueRegistry()
    queue = registry.get(queue_name)
    if queue:
        return queue.fail(item_id, error, requeue)
    return False

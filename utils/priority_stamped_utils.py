"""
Priority-stamped utilities for element processing.

Provides timestamp-based priority handling for automation workflows
where ordering and timing are critical.

Author: AutoClick Team
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class PriorityLevel(Enum):
    """Priority levels for stamped operations."""

    CRITICAL = auto()
    HIGH = auto()
    NORMAL = auto()
    LOW = auto()
    BACKGROUND = auto()


@dataclass
class StampedItem(Generic[T]):
    """
    An item with priority and timestamp metadata.

    Attributes:
        data: The actual item data
        priority: Priority level for processing order
        timestamp: Unix timestamp when item was created
        deadline: Optional deadline for processing
        metadata: Additional metadata dict
    """

    data: T
    priority: PriorityLevel = PriorityLevel.NORMAL
    timestamp: float = field(default_factory=time.time)
    deadline: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def is_expired(self) -> bool:
        """Check if the item has passed its deadline."""
        if self.deadline is None:
            return False
        return time.time() > self.deadline

    def age(self) -> float:
        """Return seconds since creation."""
        return time.time() - self.timestamp

    def sort_key(self) -> tuple[int, float]:
        """Return sort key for priority queue ordering."""
        priority_order = {
            PriorityLevel.CRITICAL: 0,
            PriorityLevel.HIGH: 1,
            PriorityLevel.NORMAL: 2,
            PriorityLevel.LOW: 3,
            PriorityLevel.BACKGROUND: 4,
        }
        return (priority_order.get(self.priority, 2), self.timestamp)


class PriorityStamper:
    """
    Manages priority-stamped items for ordered processing.

    Example:
        stamper = PriorityStamper()
        stamper.stamp("click button", priority=PriorityLevel.HIGH)
        stamper.stamp("load page", priority=PriorityLevel.NORMAL)

        while item := stamper.get_next():
            process(item.data)
    """

    def __init__(self, max_size: int = 1000) -> None:
        """
        Initialize the priority stamper.

        Args:
            max_size: Maximum number of items to hold
        """
        self._items: list[StampedItem[Any]] = []
        self._max_size = max_size
        self._processed_count: int = 0

    def stamp(
        self,
        data: T,
        priority: PriorityLevel = PriorityLevel.NORMAL,
        deadline: float | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> StampedItem[T]:
        """
        Create and store a stamped item.

        Args:
            data: Item to stamp
            priority: Processing priority
            deadline: Optional deadline timestamp
            metadata: Optional additional metadata

        Returns:
            The created stamped item
        """
        item = StampedItem(
            data=data,
            priority=priority,
            deadline=deadline,
            metadata=metadata or {},
        )
        self._items.append(item)
        self._items.sort(key=lambda x: x.sort_key())

        if len(self._items) > self._max_size:
            self._items = self._items[-self._max_size :]

        return item

    def get_next(self) -> StampedItem[T] | None:
        """
        Get the next highest priority item.

        Returns:
            Next item or None if queue is empty
        """
        if not self._items:
            return None

        expired = [i for i in self._items if i.is_expired()]
        if expired:
            for item in expired:
                self._items.remove(item)

        if not self._items:
            return None

        item = self._items.pop(0)
        self._processed_count += 1
        return item

    def peek(self) -> StampedItem[T] | None:
        """View next item without removing it."""
        if not self._items:
            return None
        return self._items[0]

    def size(self) -> int:
        """Return current queue size."""
        return len(self._items)

    def clear(self) -> None:
        """Clear all pending items."""
        self._items.clear()

    def stats(self) -> dict[str, Any]:
        """Return processing statistics."""
        return {
            "pending": len(self._items),
            "processed": self._processed_count,
            "by_priority": {
                p.name: sum(1 for i in self._items if i.priority == p)
                for p in PriorityLevel
            },
        }


def stamp_with_priority(
    items: list[T],
    priority_fn: Callable[[T], PriorityLevel],
    deadline_fn: Callable[[T], float | None] | None = None,
) -> list[StampedItem[T]]:
    """
    Stamp a list of items with priorities.

    Args:
        items: Items to stamp
        priority_fn: Function to determine priority for each item
        deadline_fn: Optional function for deadline per item

    Returns:
        List of stamped items sorted by priority
    """
    stamped = []
    for item in items:
        stamped.append(
            StampedItem(
                data=item,
                priority=priority_fn(item),
                deadline=deadline_fn(item) if deadline_fn else None,
            )
        )
    stamped.sort(key=lambda x: x.sort_key())
    return stamped

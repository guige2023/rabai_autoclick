"""Data Min-Heap Action Module.

Provides min-heap priority queue implementation for
efficient minimum element retrieval and management.

Author: RabAi Team
"""

from __future__ import annotations

import sys
import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class HeapNode:
    """Node in the heap."""
    value: Any
    priority: float
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __lt__(self, other: "HeapNode") -> bool:
        return self.priority < other.priority

    def __le__(self, other: "HeapNode") -> bool:
        return self.priority <= other.priority

    def __gt__(self, other: "HeapNode") -> bool:
        return self.priority > other.priority

    def __ge__(self, other: "HeapNode") -> bool:
        return self.priority >= other.priority


class MinHeap:
    """Min-heap implementation with O(log n) insert and extract."""

    def __init__(self, max_size: Optional[int] = None):
        self._heap: List[HeapNode] = []
        self._max_size = max_size
        self._size = 0

    def __len__(self) -> int:
        return self._size

    def is_empty(self) -> bool:
        return self._size == 0

    def is_full(self) -> bool:
        if self._max_size is None:
            return False
        return self._size >= self._max_size

    def _parent(self, i: int) -> int:
        return (i - 1) // 2

    def _left_child(self, i: int) -> int:
        return 2 * i + 1

    def _right_child(self, i: int) -> int:
        return 2 * i + 2

    def _swap(self, i: int, j: int) -> None:
        self._heap[i], self._heap[j] = self._heap[j], self._heap[i]

    def _sift_up(self, i: int) -> None:
        """Move node up to maintain heap property."""
        while i > 0:
            parent = self._parent(i)
            if self._heap[i] < self._heap[parent]:
                self._swap(i, parent)
                i = parent
            else:
                break

    def _sift_down(self, i: int) -> None:
        """Move node down to maintain heap property."""
        while True:
            smallest = i
            left = self._left_child(i)
            right = self._right_child(i)

            if left < self._size and self._heap[left] < self._heap[smallest]:
                smallest = left

            if right < self._size and self._heap[right] < self._heap[smallest]:
                smallest = right

            if smallest != i:
                self._swap(i, smallest)
                i = smallest
            else:
                break

    def insert(
        self,
        value: Any,
        priority: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Insert element with priority."""
        if self.is_full():
            if priority > self._heap[0].priority:
                return False
            self.extract_min()

        node = HeapNode(value, priority, metadata or {})
        self._heap.append(node)
        self._size += 1
        self._sift_up(self._size - 1)

        return True

    def peek(self) -> Optional[HeapNode]:
        """Get minimum element without removing."""
        if self.is_empty():
            return None
        return self._heap[0]

    def extract_min(self) -> Optional[HeapNode]:
        """Remove and return minimum element."""
        if self.is_empty():
            return None

        min_node = self._heap[0]

        if self._size == 1:
            self._heap.pop()
        else:
            self._heap[0] = self._heap.pop()

        self._size -= 1
        self._sift_down(0)

        return min_node

    def peek_priority(self) -> Optional[float]:
        """Get minimum priority without removing."""
        node = self.peek()
        return node.priority if node else None

    def remove(self, value: Any) -> bool:
        """Remove first occurrence of value."""
        for i, node in enumerate(self._heap):
            if node.value == value:
                self._remove_at(i)
                return True
        return False

    def _remove_at(self, i: int) -> None:
        """Remove node at index."""
        if i == self._size - 1:
            self._heap.pop()
            self._size -= 1
            return

        self._heap[i] = self._heap[self._size - 1]
        self._heap.pop()
        self._size -= 1

        parent = self._parent(i)
        if i > 0 and self._heap[i] < self._heap[parent]:
            self._sift_up(i)
        else:
            self._sift_down(i)

    def update_priority(self, value: Any, new_priority: float) -> bool:
        """Update priority of value."""
        for i, node in enumerate(self._heap):
            if node.value == value:
                old_priority = node.priority
                self._heap[i].priority = new_priority

                if new_priority < old_priority:
                    self._sift_up(i)
                else:
                    self._sift_down(i)

                return True
        return False

    def get_all(self) -> List[HeapNode]:
        """Get all nodes sorted by priority (not in-place sort)."""
        sorted_nodes = sorted(self._heap[:self._size])
        return sorted_nodes

    def merge(self, other: "MinHeap") -> None:
        """Merge another heap into this one."""
        for node in other._heap[:other._size]:
            self.insert(node.value, node.priority, node.metadata)

    def clear(self) -> None:
        """Clear all elements."""
        self._heap.clear()
        self._size = 0

    def get_statistics(self) -> Dict[str, Any]:
        """Get heap statistics."""
        priorities = [n.priority for n in self._heap[:self._size]]

        return {
            "size": self._size,
            "max_size": self._max_size,
            "is_empty": self.is_empty(),
            "is_full": self.is_full(),
            "min_priority": min(priorities) if priorities else None,
            "max_priority": max(priorities) if priorities else None
        }


class DataMinHeapAction(BaseAction):
    """Action for min-heap operations."""

    def __init__(self):
        super().__init__("data_minheap")
        self._heaps: Dict[str, MinHeap] = {}

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute heap action."""
        try:
            operation = params.get("operation", "insert")

            if operation == "create":
                return self._create(params)
            elif operation == "insert":
                return self._insert(params)
            elif operation == "extract":
                return self._extract(params)
            elif operation == "peek":
                return self._peek(params)
            elif operation == "update":
                return self._update(params)
            elif operation == "remove":
                return self._remove(params)
            elif operation == "stats":
                return self._get_stats(params)
            elif operation == "clear":
                return self._clear(params)
            elif operation == "merge":
                return self._merge(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _get_heap(self, name: str) -> MinHeap:
        """Get or create heap by name."""
        if name not in self._heaps:
            max_size = self._heaps.get(name, MinHeap())._max_size
            self._heaps[name] = MinHeap(max_size)
        return self._heaps[name]

    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create new heap."""
        name = params.get("name", "default")
        max_size = params.get("max_size")

        heap = MinHeap(max_size) if max_size else MinHeap()
        self._heaps[name] = heap

        return ActionResult(
            success=True,
            message=f"Heap created: {name}"
        )

    def _insert(self, params: Dict[str, Any]) -> ActionResult:
        """Insert element into heap."""
        name = params.get("name", "default")
        value = params.get("value")
        priority = params.get("priority", 0.0)
        metadata = params.get("metadata")

        if name not in self._heaps:
            self._heaps[name] = MinHeap()

        success = self._heaps[name].insert(value, priority, metadata)

        return ActionResult(
            success=success,
            data={
                "inserted": success,
                "size": len(self._heaps[name])
            }
        )

    def _extract(self, params: Dict[str, Any]) -> ActionResult:
        """Extract minimum element."""
        name = params.get("name", "default")

        if name not in self._heaps:
            return ActionResult(success=False, message=f"Heap not found: {name}")

        node = self._heaps[name].extract_min()

        if not node:
            return ActionResult(success=True, data={"value": None})

        return ActionResult(
            success=True,
            data={
                "value": node.value,
                "priority": node.priority,
                "metadata": node.metadata
            }
        )

    def _peek(self, params: Dict[str, Any]) -> ActionResult:
        """Peek at minimum element."""
        name = params.get("name", "default")

        if name not in self._heaps:
            return ActionResult(success=False, message=f"Heap not found: {name}")

        node = self._heaps[name].peek()

        if not node:
            return ActionResult(success=True, data={"value": None})

        return ActionResult(
            success=True,
            data={
                "value": node.value,
                "priority": node.priority,
                "metadata": node.metadata
            }
        )

    def _update(self, params: Dict[str, Any]) -> ActionResult:
        """Update priority of element."""
        name = params.get("name", "default")
        value = params.get("value")
        new_priority = params.get("new_priority")

        if name not in self._heaps:
            return ActionResult(success=False, message=f"Heap not found: {name}")

        success = self._heaps[name].update_priority(value, new_priority)

        return ActionResult(
            success=success,
            message="Priority updated" if success else "Value not found"
        )

    def _remove(self, params: Dict[str, Any]) -> ActionResult:
        """Remove element from heap."""
        name = params.get("name", "default")
        value = params.get("value")

        if name not in self._heaps:
            return ActionResult(success=False, message=f"Heap not found: {name}")

        success = self._heaps[name].remove(value)

        return ActionResult(
            success=success,
            message="Removed" if success else "Value not found"
        )

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get heap statistics."""
        name = params.get("name", "default")

        if name not in self._heaps:
            return ActionResult(success=False, message=f"Heap not found: {name}")

        stats = self._heaps[name].get_statistics()
        return ActionResult(success=True, data=stats)

    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        """Clear heap."""
        name = params.get("name", "default")

        if name not in self._heaps:
            return ActionResult(success=False, message=f"Heap not found: {name}")

        self._heaps[name].clear()

        return ActionResult(success=True, message=f"Heap cleared: {name}")

    def _merge(self, params: Dict[str, Any]) -> ActionResult:
        """Merge two heaps."""
        target = params.get("target", "default")
        source = params.get("source")

        if target not in self._heaps:
            self._heaps[target] = MinHeap()

        if source and source in self._heaps:
            self._heaps[target].merge(self._heaps[source])

        return ActionResult(
            success=True,
            message=f"Merged {source} into {target}"
        )

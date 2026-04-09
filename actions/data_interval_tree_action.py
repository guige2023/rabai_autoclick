"""
Data Interval Tree Action Module

Interval tree implementation for efficient range query operations.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field


@dataclass
class Interval:
    """Interval with start and end values."""
    start: float
    end: float
    data: Any = None
    id: Optional[str] = None

    def overlaps(self, other: "Interval") -> bool:
        return self.start <= other.end and self.end >= other.start

    def overlaps_point(self, point: float) -> bool:
        return self.start <= point <= self.end

    def contains(self, other: "Interval") -> bool:
        return self.start <= other.start and self.end >= other.end


@dataclass
class IntervalNode:
    """Node in interval tree."""
    interval: Interval
    max_end: float
    left: Optional["IntervalNode"] = None
    right: Optional["IntervalNode"] = None


class IntervalTree:
    """
    Interval tree for efficient interval overlap queries.

    Supports O(log n) average case for insertion, deletion,
    and overlap queries.
    """

    def __init__(self):
        self.root: Optional[IntervalNode] = None
        self.size = 0

    def insert(self, interval: Interval) -> None:
        """Insert an interval into the tree."""
        node = IntervalNode(interval, interval.end)
        if self.root is None:
            self.root = node
            self.size += 1
            return

        current = self.root
        while True:
            current.max_end = max(current.max_end, interval.end)
            if interval.start < current.interval.start:
                if current.left is None:
                    current.left = node
                    self.size += 1
                    return
                current = current.left
            else:
                if current.right is None:
                    current.right = node
                    self.size += 1
                    return
                current = current.right

    def query_point(self, point: float) -> List[Interval]:
        """Find all intervals containing the point."""
        results: List[Interval] = []

        def search(node: Optional[IntervalNode]) -> None:
            if node is None:
                return

            if node.interval.overlaps_point(point):
                results.append(node.interval)

            if node.left and node.left.max_end >= point:
                search(node.left)

            search(node.right)

        search(self.root)
        return results

    def query_interval(self, interval: Interval) -> List[Interval]:
        """Find all intervals overlapping the given interval."""
        results: List[Interval] = []

        def search(node: Optional[IntervalNode]) -> None:
            if node is None:
                return

            if node.interval.overlaps(interval):
                results.append(node.interval)

            if node.left and node.left.max_end >= interval.start:
                search(node.left)

            if node.right and node.interval.start <= interval.end:
                search(node.right)

        search(self.root)
        return results

    def query_range(self, start: float, end: float) -> List[Interval]:
        """Find all intervals within [start, end] range."""
        return self.query_interval(Interval(start, end))

    def remove(self, interval: Interval) -> bool:
        """Remove an interval from the tree."""
        def find_min(node: IntervalNode) -> IntervalNode:
            current = node
            while current.left:
                current = current.left
            return current

        def remove_node(node: Optional[IntervalNode], interval: Interval) -> Optional[IntervalNode]:
            if node is None:
                return None

            if interval.start < node.interval.start:
                node.left = remove_node(node.left, interval)
            elif interval.start > node.interval.start:
                node.right = remove_node(node.right, interval)
            else:
                if node.right is None:
                    return node.left
                if node.left is None:
                    return node.right
                min_node = find_min(node.right)
                node.interval = min_node.interval
                node.max_end = interval.end
                node.right = remove_node(node.right, min_node.interval)

            node.max_end = max(
                node.interval.end,
                node.left.max_end if node.left else float('-inf'),
                node.right.max_end if node.right else float('-inf')
            )
            return node

        self.root = remove_node(self.root, interval)
        self.size -= 1
        return True

    def get_all(self) -> List[Interval]:
        """Get all intervals in sorted order."""
        results: List[Interval] = []

        def inorder(node: Optional[IntervalNode]) -> None:
            if node is None:
                return
            inorder(node.left)
            results.append(node.interval)
            inorder(node.right)

        inorder(self.root)
        return results

    def get_stats(self) -> Dict[str, Any]:
        """Get tree statistics."""
        def get_depth(node: Optional[IntervalNode]) -> int:
            if node is None:
                return 0
            return 1 + max(get_depth(node.left), get_depth(node.right))

        return {
            "size": self.size,
            "depth": get_depth(self.root)
        }


class DataIntervalTreeAction:
    """
    Interval tree for efficient range and overlap queries.

    Example:
        tree = DataIntervalTreeAction()
        tree.insert(0.0, 10.0, {"id": "interval_1"})
        tree.insert(5.0, 15.0, {"id": "interval_2"})
        overlaps = tree.query_point(7.0)
        range_results = tree.query_range(3.0, 12.0)
    """

    def __init__(self):
        self.tree = IntervalTree()

    def insert(self, start: float, end: float, data: Any = None) -> None:
        interval = Interval(start, end, data)
        self.tree.insert(interval)

    def query_point(self, point: float) -> List[Dict[str, Any]]:
        results = self.tree.query_point(point)
        return [{"start": i.start, "end": i.end, "data": i.data} for i in results]

    def query_range(self, start: float, end: float) -> List[Dict[str, Any]]:
        results = self.tree.query_range(start, end)
        return [{"start": i.start, "end": i.end, "data": i.data} for i in results]

    def get_all(self) -> List[Dict[str, Any]]:
        results = self.tree.get_all()
        return [{"start": i.start, "end": i.end, "data": i.data} for i in results]

    def get_stats(self) -> Dict[str, Any]:
        return self.tree.get_stats()

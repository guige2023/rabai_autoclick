"""Interval tree utilities.

Provides interval data structure for efficient range queries,
useful for scheduling, resource allocation, and collision detection.
"""

from typing import Any, List, Optional, Tuple


Interval = Tuple[int, int, Any]  # (start, end, data)


class IntervalNode:
    """Node in the interval tree."""

    __slots__ = ("interval", "max", "left", "right")

    def __init__(self, interval: Interval) -> None:
        self.interval = interval
        self.max: int = interval[1]
        self.left: Optional[IntervalNode] = None
        self.right: Optional[IntervalNode] = None


class IntervalTree:
    """Interval tree for efficient overlap queries.

    Example:
        tree = IntervalTree()
        tree.insert((1, 5, "event1"))
        tree.insert((3, 8, "event2"))
        overlaps = tree.find_overlap((4, 10))
    """

    def __init__(self) -> None:
        self._root: Optional[IntervalNode] = None

    def insert(self, interval: Interval) -> None:
        """Insert an interval.

        Args:
            interval: (start, end, data) tuple.
        """
        self._root = self._insert(self._root, interval)
        self._update_max(self._root)

    def _insert(self, node: Optional[IntervalNode], interval: Interval) -> IntervalNode:
        if node is None:
            return IntervalNode(interval)

        start, _, _ = interval
        node_start, _, _ = node.interval

        if start < node_start:
            node.left = self._insert(node.left, interval)
        else:
            node.right = self._insert(node.right, interval)

        return node

    def _update_max(self, node: Optional[IntervalNode]) -> None:
        if node is None:
            return
        left_max = node.left.max if node.left else node.interval[1]
        right_max = node.right.max if node.right else node.interval[1]
        node.max = max(node.interval[1], left_max, right_max)

    def find_overlap(
        self,
        query: Tuple[int, int],
    ) -> List[Interval]:
        """Find all intervals overlapping with query range.

        Args:
            query: (start, end) query range.

        Returns:
            List of overlapping intervals.
        """
        return self._find_overlap(self._root, query)

    def _find_overlap(
        self,
        node: Optional[IntervalNode],
        query: Tuple[int, int],
    ) -> List[Interval]:
        if node is None:
            return []

        q_start, q_end = query
        n_start, n_end, n_data = node.interval
        overlaps: List[Interval] = []

        if n_start <= q_end and n_end >= q_start:
            overlaps.append(node.interval)

        if node.left and node.left.max >= q_start:
            overlaps.extend(self._find_overlap(node.left, query))

        overlaps.extend(self._find_overlap(node.right, query))

        return overlaps

    def find_containing_point(self, point: int) -> List[Interval]:
        """Find all intervals containing a point.

        Args:
            point: Point to query.

        Returns:
            List of intervals containing the point.
        """
        return self._find_containing(self._root, point)

    def _find_containing(
        self,
        node: Optional[IntervalNode],
        point: int,
    ) -> List[Interval]:
        if node is None:
            return []

        n_start, n_end, _ = node.interval
        overlaps: List[Interval] = []

        if n_start <= point <= n_end:
            overlaps.append(node.interval)

        if node.left and node.left.max >= n_start:
            overlaps.extend(self._find_containing(node.left, point))

        overlaps.extend(self._find_containing(node.right, point))

        return overlaps

    def remove(self, interval: Interval) -> bool:
        """Remove an interval from the tree.

        Args:
            interval: Interval to remove.

        Returns:
            True if removed.
        """
        original = self._root
        self._root = self._remove(self._root, interval)
        if self._root != original:
            self._update_max(self._root)
            return True
        return False

    def _remove(
        self,
        node: Optional[IntervalNode],
        interval: Interval,
    ) -> Optional[IntervalNode]:
        if node is None:
            return None

        if interval == node.interval:
            if node.left is None:
                return node.right
            if node.right is None:
                return node.left
            min_node = self._find_min(node.right)
            node.interval = min_node.interval
            node.right = self._remove(node.right, min_node.interval)
        elif interval[0] < node.interval[0]:
            node.left = self._remove(node.left, interval)
        else:
            node.right = self._remove(node.right, interval)

        return node

    def _find_min(self, node: IntervalNode) -> IntervalNode:
        while node.left is not None:
            node = node.left
        return node

    def all_intervals(self) -> List[Interval]:
        """Get all intervals in sorted order.

        Returns:
            List of all intervals.
        """
        result: List[Interval] = []
        self._inorder(self._root, result)
        return result

    def _inorder(self, node: Optional[IntervalNode], result: List[Interval]) -> None:
        if node is None:
            return
        self._inorder(node.left, result)
        result.append(node.interval)
        self._inorder(node.right, result)

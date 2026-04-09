"""
Data KD-Tree Action Module

K-dimensional tree implementation for efficient spatial queries.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import math
import heapq


@dataclass
class KDPoint:
    """Point in k-dimensional space."""
    coords: Tuple[float, ...]
    data: Any = None
    id: Optional[str] = None


@dataclass
class KDBounds:
    """K-dimensional bounding box."""
    min_coords: Tuple[float, ...]
    max_coords: Tuple[float, ...]

    def contains_point(self, point: KDPoint) -> bool:
        for i, c in enumerate(point.coords):
            if c < self.min_coords[i] or c > self.max_coords[i]:
                return False
        return True

    def intersects(self, other: "KDBounds") -> bool:
        for i in range(len(self.min_coords)):
            if self.max_coords[i] < other.min_coords[i] or self.min_coords[i] > other.max_coords[i]:
                return False
        return True


class KDNode:
    """Node in KD-tree."""

    def __init__(self, point: KDPoint, depth: int = 0):
        self.point = point
        self.depth = depth
        self.left: Optional[KDNode] = None
        self.right: Optional[KDNode] = None

    @property
    def axis(self) -> int:
        return self.depth % len(self.point.coords)


class KDTree:
    """
    K-dimensional tree for efficient nearest neighbor and range queries.

    Supports 2D to any number of dimensions with O(log n) average case
    for search operations.
    """

    def __init__(self, dim: int = 2):
        self.dim = dim
        self.root: Optional[KDNode] = None
        self.size = 0

    def insert(self, coords: Tuple[float, ...], data: Any = None, id: Optional[str] = None) -> bool:
        """Insert a point into the tree."""
        if len(coords) != self.dim:
            raise ValueError(f"Expected {self.dim} coordinates, got {len(coords)}")

        point = KDPoint(coords, data, id)

        if self.root is None:
            self.root = KDNode(point)
            self.size += 1
            return True

        node = self.root
        while True:
            axis = node.axis
            if coords[axis] < node.point.coords[axis]:
                if node.left is None:
                    node.left = KDNode(point, node.depth + 1)
                    self.size += 1
                    return True
                node = node.left
            else:
                if node.right is None:
                    node.right = KDNode(point, node.depth + 1)
                    self.size += 1
                    return True
                node = node.right

    def search_nearest(self, coords: Tuple[float, ...]) -> Optional[Tuple[KDPoint, float]]:
        """Find nearest neighbor to given coordinates."""
        if self.root is None:
            return None

        target = KDPoint(coords)
        best: Tuple[KDNode, float] = (self.root, float('inf'))

        def search(node: Optional[KDNode]) -> None:
            nonlocal best
            if node is None:
                return

            d = self._distance(target, node.point)
            if d < best[1]:
                best = (node, d)

            axis = node.axis
            diff = coords[axis] - node.point.coords[axis]

            first = node.left if diff < 0 else node.right
            second = node.right if diff < 0 else node.left

            search(first)

            if abs(diff) < best[1]:
                search(second)

        search(self.root)
        return (best[0].point, best[1])

    def search_k_nearest(
        self,
        coords: Tuple[float, ...],
        k: int = 5
    ) -> List[Tuple[KDPoint, float]]:
        """Find k nearest neighbors."""
        if self.root is None:
            return []

        target = KDPoint(coords)
        heap: List[Tuple[float, KDNode]] = []

        def search(node: Optional[KDNode]) -> None:
            if node is None:
                return

            d = self._distance(target, node.point)
            if len(heap) < k:
                heapq.heappush(heap, (-d, node))
            elif d < -heap[0][0]:
                heapq.heapreplace(heap, (-d, node))

            axis = node.axis
            diff = coords[axis] - node.point.coords[axis]

            first = node.left if diff < 0 else node.right
            second = node.right if diff < 0 else node.left

            search(first)
            if abs(diff) < -heap[0][0] if heap else float('inf'):
                search(second)

        search(self.root)
        return [(-d, n.point) for d, n in sorted(heap, key=lambda x: -x[0])]

    def search_range(
        self,
        min_coords: Tuple[float, ...],
        max_coords: Tuple[float, ...]
    ) -> List[KDPoint]:
        """Find all points within axis-aligned bounding box."""
        if self.root is None:
            return []

        bounds = KDBounds(min_coords, max_coords)
        results: List[KDPoint] = []

        def search(node: Optional[KDNode]) -> None:
            if node is None:
                return

            if bounds.contains_point(node.point):
                results.append(node.point)

            axis = node.axis
            node_min = node.point.coords[axis]

            if min_coords[axis] <= node_min:
                search(node.left)
            if max_coords[axis] >= node_min:
                search(node.right)

        search(self.root)
        return results

    def _distance(self, p1: KDPoint, p2: KDPoint) -> float:
        """Calculate squared Euclidean distance."""
        return sum((a - b) ** 2 for a, b in zip(p1.coords, p2.coords)) ** 0.5

    def _distance_sq(self, p1: KDPoint, p2: KDPoint) -> float:
        """Calculate squared Euclidean distance (faster, no sqrt)."""
        return sum((a - b) ** 2 for a, b in zip(p1.coords, p2.coords))

    def get_stats(self) -> Dict[str, Any]:
        """Get tree statistics."""
        def get_depth(node: Optional[KDNode]) -> int:
            if node is None:
                return 0
            return 1 + max(get_depth(node.left), get_depth(node.right))

        return {
            "size": self.size,
            "dim": self.dim,
            "depth": get_depth(self.root),
            "balanced": self._is_balanced()

    def _is_balanced(self) -> bool:
        """Check if tree is approximately balanced."""
        def check(node: Optional[KDNode]) -> Tuple[bool, int]:
            if node is None:
                return True, 0
            left_bal, left_depth = check(node.left)
            right_bal, right_depth = check(node.right)
            balanced = left_bal and right_bal and abs(left_depth - right_depth) <= 2
            return balanced, 1 + max(left_depth, right_depth)

        bal, _ = check(self.root)
        return bal


class DataKDTreeAction:
    """
    K-dimensional tree for efficient spatial queries.

    Example:
        tree = DataKDTreeAction(dim=3)
        tree.insert((1.0, 2.0, 3.0), {"name": "point_a"})
        nearest = tree.search_nearest((1.1, 2.1, 3.1))
        results = tree.search_range((0, 0, 0), (2, 3, 4))
    """

    def __init__(self, dim: int = 2):
        self.tree = KDTree(dim)

    def insert(self, coords: Tuple[float, ...], data: Any = None) -> bool:
        return self.tree.insert(coords, data)

    def search_nearest(self, coords: Tuple[float, ...]) -> Optional[Dict[str, Any]]:
        result = self.tree.search_nearest(coords)
        if result:
            point, dist = result
            return {"coords": point.coords, "data": point.data, "distance": dist}
        return None

    def search_k_nearest(self, coords: Tuple[float, ...], k: int = 5) -> List[Dict[str, Any]]:
        results = self.tree.search_k_nearest(coords, k)
        return [{"coords": p.coords, "data": p.data, "distance": d} for p, d in results]

    def search_range(
        self,
        min_coords: Tuple[float, ...],
        max_coords: Tuple[float, ...]
    ) -> List[Dict[str, Any]]:
        results = self.tree.search_range(min_coords, max_coords)
        return [{"coords": p.coords, "data": p.data} for p in results]

    def get_stats(self) -> Dict[str, Any]:
        return self.tree.get_stats()

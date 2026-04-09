"""
Data Spatial Index Action Module

Provides spatial indexing structures for efficient 2D/3D queries.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
import math
import heapq


@dataclass
class Point:
    """2D or 3D point."""
    x: float
    y: float
    z: Optional[float] = None
    id: Optional[Any] = None


@dataclass
class Rectangle:
    """Axis-aligned bounding rectangle."""
    min_x: float
    min_y: float
    max_x: float
    max_y: float
    min_z: Optional[float] = None
    max_z: Optional[float] = None


@dataclass
class SpatialEntry:
    """Entry in spatial index."""
    point: Point
    data: Any = None


class QuadTreeNode:
    """Node in a QuadTree."""

    MAX_ENTRIES = 4
    MAX_DEPTH = 8

    def __init__(
        self,
        bounds: Rectangle,
        depth: int = 0
    ):
        self.bounds = bounds
        self.depth = depth
        self.entries: List[SpatialEntry] = []
        self.children: Optional[List["QuadTreeNode"]] = None

    def _subdivide(self) -> None:
        """Subdivide node into 4 quadrants."""
        mid_x = (self.bounds.min_x + self.bounds.max_x) / 2
        mid_y = (self.bounds.min_y + self.bounds.max_y) / 2

        self.children = [
            QuadTreeNode(Rectangle(self.bounds.min_x, self.bounds.min_y, mid_x, mid_y), self.depth + 1),
            QuadTreeNode(Rectangle(mid_x, self.bounds.min_y, self.bounds.max_x, mid_y), self.depth + 1),
            QuadTreeNode(Rectangle(self.bounds.min_x, mid_y, mid_x, self.bounds.max_y), self.depth + 1),
            QuadTreeNode(Rectangle(mid_x, mid_y, self.bounds.max_x, self.bounds.max_y), self.depth + 1),
        ]

        # Redistribute entries
        for entry in self.entries:
            self._insert_into_children(entry)
        self.entries.clear()

    def _insert_into_children(self, entry: SpatialEntry) -> bool:
        """Try to insert entry into children."""
        if self.children is None:
            return False
        for child in self.children:
            if self._contains_point(child.bounds, entry.point):
                child.insert(entry)
                return True
        return False

    def _contains_point(self, bounds: Rectangle, point: Point) -> bool:
        """Check if bounds contains point."""
        in_x = bounds.min_x <= point.x <= bounds.max_x
        in_y = bounds.min_y <= point.y <= bounds.max_y
        if in_x and in_y:
            if point.z is not None and bounds.min_z is not None:
                return bounds.min_z <= point.z <= bounds.max_z
            return True
        return False

    def _intersects(self, bounds: Rectangle, query: Rectangle) -> bool:
        """Check if bounds intersects query rectangle."""
        return not (
            query.max_x < bounds.min_x or query.min_x > bounds.max_x or
            query.max_y < bounds.min_y or query.min_y > bounds.max_y or
            (bounds.min_z is not None and query.max_z is not None and
             (query.max_z < bounds.min_z or query.min_z > bounds.max_z))
        )

    def insert(self, entry: SpatialEntry) -> bool:
        """Insert entry into quadtree."""
        if not self._contains_point(self.bounds, entry.point):
            return False

        if self.children is None:
            if len(self.entries) < self.MAX_ENTRIES or self.depth >= self.MAX_DEPTH:
                self.entries.append(entry)
                return True
            self._subdivide()

        self._insert_into_children(entry)
        return True

    def query_range(self, query: Rectangle) -> List[SpatialEntry]:
        """Query all entries within range."""
        results = []

        if not self._intersects(self.bounds, query):
            return results

        # Check entries in this node
        for entry in self.entries:
            if (query.min_x <= entry.point.x <= query.max_x and
                query.min_y <= entry.point.y <= query.max_y):
                if entry.point.z is None or (query.min_z or -math.inf) <= entry.point.z <= (query.max_z or math.inf):
                    results.append(entry)

        # Check children
        if self.children:
            for child in self.children:
                results.extend(child.query_range(query))

        return results

    def query_nearest(
        self,
        point: Point,
        k: int = 1
    ) -> List[Tuple[SpatialEntry, float]]:
        """Find k nearest neighbors."""
        heap: List[Tuple[float, SpatialEntry]] = []

        def search(node: QuadTreeNode) -> None:
            # Compute distance to node bounds
            dx = max(0, max(node.bounds.min_x - point.x, point.x - node.bounds.max_x))
            dy = max(0, max(node.bounds.min_y - point.y, point.y - node.bounds.max_y))
            dist = math.sqrt(dx * dx + dy * dy)

            # Check if we should explore this node
            if len(heap) >= k:
                worst_dist = -heap[0][0]
                if dist > worst_dist:
                    return

            # Check entries
            for entry in node.entries:
                d = self._distance(point, entry.point)
                if len(heap) < k:
                    heapq.heappush(heap, (-d, entry))
                elif d < -heap[0][0]:
                    heapq.heapreplace(heap, (-d, entry))

            # Check children
            if node.children:
                # Sort children by distance
                child_dists = []
                for child in node.children:
                    dx = max(0, max(child.bounds.min_x - point.x, point.x - child.bounds.max_x))
                    dy = max(0, max(child.bounds.min_y - point.y, point.y - child.bounds.max_y))
                    dist = math.sqrt(dx * dx + dy * dy)
                    child_dists.append((dist, child))
                child_dists.sort()

                for dist, child in child_dists:
                    if len(heap) >= k and dist > -heap[0][0]:
                        break
                    search(child)

        search(self)

        return [(-d, e) for d, e in heap]

    def _distance(self, p1: Point, p2: Point) -> float:
        """Calculate Euclidean distance."""
        dx = p1.x - p2.x
        dy = p1.y - p2.y
        dz = (p1.z or 0) - (p2.z or 0)
        return math.sqrt(dx * dx + dy * dy + dz * dz)


class QuadTree:
    """QuadTree spatial index for 2D points."""

    def __init__(self, bounds: Rectangle):
        self.root = QuadTreeNode(bounds)

    def insert(self, x: float, y: float, data: Any = None, id: Any = None) -> bool:
        """Insert a point."""
        point = Point(x, y, id=id)
        return self.root.insert(SpatialEntry(point, data))

    def query_range(
        self,
        min_x: float,
        min_y: float,
        max_x: float,
        max_y: float
    ) -> List[SpatialEntry]:
        """Query points in rectangle."""
        query = Rectangle(min_x, min_y, max_x, max_y)
        return self.root.query_range(query)

    def query_circle(
        self,
        cx: float,
        cy: float,
        radius: float
    ) -> List[SpatialEntry]:
        """Query points within circle."""
        # First query bounding rectangle
        results = self.query_range(
            cx - radius, cy - radius, cx + radius, cy + radius
        )
        # Then filter by exact distance
        return [
            e for e in results
            if self._distance(cx, cy, e.point.x, e.point.y) <= radius
        ]

    def query_nearest(
        self,
        x: float,
        y: float,
        k: int = 1
    ) -> List[Tuple[SpatialEntry, float]]:
        """Find k nearest points."""
        return self.root.query_nearest(Point(x, y), k)

    def _distance(self, x1: float, y1: float, x2: float, y2: float) -> float:
        """Calculate distance."""
        dx = x1 - x2
        dy = y1 - y2
        return math.sqrt(dx * dx + dy * dy)


class KDTreeNode:
    """Node in a KD-Tree."""

    def __init__(self, point: Point, data: Any, depth: int = 0):
        self.point = point
        self.data = data
        self.left: Optional[KDTreeNode] = None
        self.right: Optional[KDTreeNode] = None
        self.depth = depth


class KDTree:
    """
    K-dimensional tree for efficient nearest neighbor queries.

    Supports 2D and 3D points with O(log n) average case search.
    """

    def __init__(self, dim: int = 2):
        self.dim = dim
        self.root: Optional[KDTreeNode] = None
        self.size = 0

    def insert(self, x: float, y: float, data: Any = None, z: Optional[float] = None) -> None:
        """Insert a point."""
        point = Point(x, y, z)
        if self.root is None:
            self.root = KDTreeNode(point, data)
            self.size += 1
            return

        node = self.root
        depth = 0
        while True:
            axis = depth % self.dim
            if (axis == 0 and x < node.point.x) or (axis == 1 and y < node.point.y):
                if node.left is None:
                    node.left = KDTreeNode(point, data, depth + 1)
                    break
                node = node.left
            else:
                if node.right is None:
                    node.right = KDTreeNode(point, data, depth + 1)
                    break
                node = node.right
            depth += 1
        self.size += 1

    def query_nearest(
        self,
        x: float,
        y: float,
        z: Optional[float] = None
    ) -> Optional[Tuple[Point, Any, float]]:
        """Find nearest neighbor."""
        target = Point(x, y, z)
        best: Tuple[KDTreeNode, float] = (None, float('inf'))

        def search(node: Optional[KDTreeNode], depth: int) -> None:
            nonlocal best
            if node is None:
                return

            # Calculate distance to this node
            d = self._distance(target, node.point)
            if d < best[1]:
                best = (node, d)

            # Determine which side to search first
            axis = depth % self.dim
            diff = (x - node.point.x) if axis == 0 else (y - node.point.y)

            first = node.left if diff < 0 else node.right
            second = node.right if diff < 0 else node.left

            search(first, depth + 1)

            # Check if we need to search the other side
            if abs(diff) < best[1]:
                search(second, depth + 1)

        search(self.root, 0)
        if best[0] is None:
            return None
        return (best[0].point, best[0].data, best[1])

    def query_range(
        self,
        min_x: float,
        min_y: float,
        max_x: float,
        max_y: float,
        node: Optional[KDTreeNode] = None,
        depth: int = 0
    ) -> List[Tuple[Point, Any]]:
        """Query points within rectangle."""
        results = []

        if node is None:
            node = self.root

        if node is None:
            return results

        # Check if point is in range
        if (min_x <= node.point.x <= max_x and
            min_y <= node.point.y <= max_y):
            results.append((node.point, node.data))

        # Recurse
        axis = depth % self.dim
        if axis == 0:
            if min_x <= node.point.x and node.left:
                results.extend(self.query_range(min_x, min_y, max_x, max_y, node.left, depth + 1))
            if max_x >= node.point.x and node.right:
                results.extend(self.query_range(min_x, min_y, max_x, max_y, node.right, depth + 1))
        else:
            if min_y <= node.point.y and node.left:
                results.extend(self.query_range(min_x, min_y, max_x, max_y, node.left, depth + 1))
            if max_y >= node.point.y and node.right:
                results.extend(self.query_range(min_x, min_y, max_x, max_y, node.right, depth + 1))

        return results

    def _distance(self, p1: Point, p2: Point) -> float:
        """Calculate Euclidean distance."""
        dx = p1.x - p2.x
        dy = p1.y - p2.y
        dz = (p1.z or 0) - (p2.z or 0)
        return math.sqrt(dx * dx + dy * dy + dz * dz)


class DataSpatialIndexAction:
    """
    Provides spatial indexing for efficient 2D/3D queries.

    Supports QuadTree (good for bounded 2D data) and KD-Tree (good for
    nearest neighbor queries in any dimension).

    Example:
        # Using QuadTree
        qt = DataSpatialIndexAction(index_type="quadtree")
        qt.insert(10.0, 20.0, {"name": "point_a"})
        results = qt.query_range(0, 0, 15, 25)

        # Using KD-Tree
        kd = DataSpatialIndexAction(index_type="kdtree", dim=2)
        kd.insert(10.0, 20.0, {"name": "point_a"})
        nearest = kd.query_nearest(15.0, 25.0)
    """

    def __init__(self, index_type: str = "quadtree", dim: int = 2):
        """
        Initialize spatial index.

        Args:
            index_type: Type of index ("quadtree" or "kdtree")
            dim: Dimension for KD-tree (2 or 3)
        """
        self.index_type = index_type
        self.dim = dim
        self.quadtree: Optional[QuadTree] = None
        self.kdtree: Optional[KDTree] = None

        if index_type == "quadtree":
            # Default bounds of -10000 to 10000 in each axis
            bounds = Rectangle(-10000, -10000, 10000, 10000)
            self.quadtree = QuadTree(bounds)
        elif index_type == "kdtree":
            self.kdtree = KDTree(dim)
        else:
            raise ValueError(f"Unknown index type: {index_type}")

    def insert(
        self,
        x: float,
        y: float,
        data: Any = None,
        z: Optional[float] = None
    ) -> None:
        """Insert a point into the index."""
        if self.quadtree:
            self.quadtree.insert(x, y, data)
        elif self.kdtree:
            self.kdtree.insert(x, y, data, z)

    def query_range(
        self,
        min_x: float,
        min_y: float,
        max_x: float,
        max_y: float
    ) -> List[Tuple[Any, float, float]]:
        """Query points in a rectangular region."""
        if self.quadtree:
            entries = self.quadtree.query_range(min_x, min_y, max_x, max_y)
            return [(e.data, e.point.x, e.point.y) for e in entries]
        elif self.kdtree:
            results = self.kdtree.query_range(min_x, min_y, max_x, max_y)
            return [(data, pt.x, pt.y) for pt, data in results]
        return []

    def query_circle(
        self,
        cx: float,
        cy: float,
        radius: float
    ) -> List[Tuple[Any, float, float]]:
        """Query points within a circle."""
        if self.quadtree:
            entries = self.quadtree.query_circle(cx, cy, radius)
            return [(e.data, e.point.x, e.point.y) for e in entries]
        return []

    def query_nearest(
        self,
        x: float,
        y: float,
        k: int = 1,
        z: Optional[float] = None
    ) -> List[Tuple[Any, float, float, float]]:
        """
        Find k nearest neighbors.

        Returns:
            List of (data, x, y, distance) tuples
        """
        if self.quadtree:
            results = self.quadtree.query_nearest(x, y, k)
            return [(e.data, e.point.x, e.point.y, d) for e, d in results]
        elif self.kdtree:
            result = self.kdtree.query_nearest(x, y, z)
            if result:
                pt, data, d = result
                return [(data, pt.x, pt.y, d)]
        return []

    def get_stats(self) -> Dict[str, Any]:
        """Get index statistics."""
        stats = {"type": self.index_type}
        if self.quadtree:
            stats["size"] = self.quadtree.size
        elif self.kdtree:
            stats["size"] = self.kdtree.size
        return stats

"""Quadtree data structure utilities for RabAI AutoClick.

Provides:
- Quadtree for spatial indexing
- Point and region queries
- Collision detection helpers
"""

from typing import List, Optional, Tuple, Set, Any
from dataclasses import dataclass, field


@dataclass
class QuadTreeNode:
    """Node in a quadtree."""
    bounds: Tuple[float, float, float, float]  # x, y, w, h
    points: List[Tuple[float, float, Any]] = field(default_factory=list)
    children: Optional[List["QuadTreeNode"]] = None


class QuadTree:
    """Quadtree for 2D spatial partitioning."""

    MAX_POINTS = 4
    MAX_DEPTH = 8

    def __init__(
        self,
        bounds: Tuple[float, float, float, float],
        depth: int = 0,
    ):
        """Initialize quadtree.

        Args:
            bounds: (x, y, width, height) of region.
            depth: Current depth level.
        """
        self.bounds = bounds
        self.depth = depth
        self.points: List[Tuple[float, float, Any]] = []
        self.children: Optional[List[QuadTreeNode]] = None

    def _subdivide(self) -> None:
        """Subdivide node into 4 children."""
        x, y, w, h = self.bounds
        hw, hh = w / 2, h / 2
        self.children = [
            QuadTree((x, y, hw, hh), self.depth + 1),          # SW
            QuadTree((x + hw, y, hw, hh), self.depth + 1),    # SE
            QuadTree((x, y + hh, hw, hh), self.depth + 1),     # NW
            QuadTree((x + hw, y + hh, hw, hh), self.depth + 1), # NE
        ]
        # Redistribute points
        for pt in self.points:
            self._insert_into_children(pt)
        self.points.clear()

    def _get_quadrant(self, px: float, py: float) -> int:
        """Get quadrant index for point."""
        x, y, w, h = self.bounds
        mx, my = x + w / 2, y + h / 2
        if px < mx:
            return 0 if py < my else 2
        else:
            return 1 if py < my else 3

    def _insert_into_children(self, pt: Tuple[float, float, Any]) -> bool:
        """Insert point into appropriate child."""
        if self.children is None:
            return False
        quad = self._get_quadrant(pt[0], pt[1])
        return self.children[quad].insert(pt)

    def _contains_point(self, px: float, py: float) -> bool:
        """Check if point is within bounds."""
        x, y, w, h = self.bounds
        return x <= px < x + w and y <= py < y + h

    def _intersects(
        self,
        rx: float, ry: float, rw: float, rh: float,
    ) -> bool:
        """Check if query rect intersects this node."""
        x, y, w, h = self.bounds
        return not (rx + rw < x or rx > x + w or ry + rh < y or ry > y + h)

    def insert(self, point: Tuple[float, float, Any]) -> bool:
        """Insert point into quadtree.

        Args:
            point: (x, y, data) tuple.

        Returns:
            True if inserted successfully.
        """
        if not self._contains_point(point[0], point[1]):
            return False

        if self.children is None:
            if len(self.points) < self.MAX_POINTS or self.depth >= self.MAX_DEPTH:
                self.points.append(point)
                return True
            self._subdivide()

        return self._insert_into_children(point)

    def query_region(
        self,
        rx: float, ry: float, rw: float, rh: float,
    ) -> List[Tuple[float, float, Any]]:
        """Find all points within a rectangular region.

        Args:
            rx, ry, rw, rh: Query rectangle.

        Returns:
            List of points in region.
        """
        results: List[Tuple[float, float, Any]] = []
        self._query_region(rx, ry, rw, rh, results)
        return results

    def _query_region(
        self,
        rx: float, ry: float, rw: float, rh: float,
        results: List[Tuple[float, float, Any]],
    ) -> None:
        """Internal region query."""
        if not self._intersects(rx, ry, rw, rh):
            return

        for pt in self.points:
            if rx <= pt[0] < rx + rw and ry <= pt[1] < ry + rh:
                results.append(pt)

        if self.children:
            for child in self.children:
                child._query_region(rx, ry, rw, rh, results)

    def query_radius(
        self,
        cx: float, cy: float, radius: float,
    ) -> List[Tuple[float, float, Any]]:
        """Find all points within a circle.

        Args:
            cx, cy: Circle center.
            radius: Circle radius.

        Returns:
            List of points within radius.
        """
        r2 = radius * radius
        # Query bounding box first
        candidates = self.query_region(cx - radius, cy - radius, radius * 2, radius * 2)
        return [p for p in candidates
                if (p[0] - cx) ** 2 + (p[1] - cy) ** 2 <= r2]

    def nearest_neighbor(
        self,
        x: float, y: float,
        max_radius: float = float("inf"),
    ) -> Optional[Tuple[float, float, Any]]:
        """Find nearest point to (x, y).

        Args:
            x, y: Query point.
            max_radius: Maximum search radius.

        Returns:
            Nearest point or None.
        """
        best: Optional[Tuple[float, float, Any]] = None
        best_dist = max_radius * max_radius

        self._nearest(x, y, best_dist, best, [])
        return best

    def _nearest(
        self,
        x: float, y: float,
        best_dist: float,
        best: List[Optional[Tuple[float, float, Any]]],
        path: List[QuadTreeNode],
    ) -> None:
        """Internal nearest neighbor search."""
        if best and len(best) > 0 and best[0] is not None:
            current_best = best[0]
            if current_best is not None:
                bd = (current_best[0] - x) ** 2 + (current_best[1] - y) ** 2
                if bd < best_dist:
                    best_dist = bd

        for pt in self.points:
            d = (pt[0] - x) ** 2 + (pt[1] - y) ** 2
            if d < best_dist:
                best_dist = d
                best.clear()
                best.append(pt)

        if self.children:
            # Sort children by distance to query point
            def child_min_dist(child: QuadTreeNode) -> float:
                cx, cy, cw, ch = child.bounds
                dx = max(cx - x, 0, x - (cx + cw))
                dy = max(cy - y, 0, y - (cy + ch))
                return dx * dx + dy * dy

            sorted_children = sorted(self.children, key=child_min_dist)
            for child in sorted_children:
                if child_min_dist(child) < best_dist:
                    child._nearest(x, y, best_dist, best, path)

    def clear(self) -> None:
        """Remove all points."""
        self.points.clear()
        self.children = None

    def all_points(self) -> List[Tuple[float, float, Any]]:
        """Return all points in tree."""
        result = self.points[:]
        if self.children:
            for child in self.children:
                result.extend(child.all_points())
        return result

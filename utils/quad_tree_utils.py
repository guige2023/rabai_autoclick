"""
QuadTree spatial partitioning utility for efficient element hit testing.

Provides 2D spatial partitioning using a quadtree structure for O(log n)
element lookups based on coordinate position.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Iterator, Optional


@dataclass
class Bounds:
    """2D bounding box."""
    x: float
    y: float
    width: float
    height: float

    @property
    def x2(self) -> float:
        return self.x + self.width

    @property
    def y2(self) -> float:
        return self.y + self.height

    @property
    def center_x(self) -> float:
        return self.x + self.width / 2

    @property
    def center_y(self) -> float:
        return self.y + self.height / 2

    def intersects(self, other: Bounds) -> bool:
        """Check if this bounds intersects with another."""
        return not (
            self.x2 < other.x or other.x2 < self.x or
            self.y2 < other.y or other.y2 < self.y
        )

    def contains_point(self, px: float, py: float) -> bool:
        """Check if a point is inside this bounds."""
        return self.x <= px <= self.x2 and self.y <= py <= self.y2


@dataclass
class QuadTreeItem:
    """Item stored in the quadtree with its bounds."""
    bounds: Bounds
    data: Any = None


_MAX_ITEMS = 8
_MAX_DEPTH = 8


class QuadTreeNode:
    """Single node in a quadtree."""

    def __init__(self, bounds: Bounds, depth: int = 0):
        self.bounds = bounds
        self.depth = depth
        self.items: list[QuadTreeItem] = []
        self._children: Optional[tuple[QuadTreeNode, QuadTreeNode, QuadTreeNode, QuadTreeNode]] = None

    @property
    def is_leaf(self) -> bool:
        return self._children is None

    def _subdivide(self) -> None:
        """Split this node into 4 quadrants."""
        bx, by = self.bounds.x, self.bounds.y
        bw, bh = self.bounds.width, self.bounds.height
        hw, hh = bw / 2, bh / 2

        self._children = (
            QuadTreeNode(Bounds(bx, by, hw, hh), self.depth + 1),       # NW
            QuadTreeNode(Bounds(bx + hw, by, hw, hh), self.depth + 1), # NE
            QuadTreeNode(Bounds(bx, by + hh, hw, hh), self.depth + 1), # SW
            QuadTreeNode(Bounds(bx + hw, by + hh, hw, hh), self.depth + 1), # SE
        )

    def _quadrant_for(self, item_bounds: Bounds) -> int | None:
        """Return which quadrant(s) this bounds belongs to. None if spans multiple."""
        cx, cy = self.bounds.center_x, self.bounds.center_y
        item_cx, item_cy = item_bounds.center_x, item_bounds.center_y

        if item_bounds.x >= cx:
            if item_bounds.y >= cy:
                return 3  # SE
            elif item_bounds.y2 <= cy:
                return 1  # NE
        elif item_bounds.x2 <= cx:
            if item_bounds.y >= cy:
                return 2  # SW
            elif item_bounds.y2 <= cy:
                return 0  # NW

        return None  # Spans multiple quadrants

    def insert(self, item: QuadTreeItem) -> bool:
        """Insert an item into this node. Returns True if successful."""
        if not self.bounds.intersects(item.bounds):
            return False

        if self.is_leaf and len(self.items) < _MAX_ITEMS:
            self.items.append(item)
            return True

        if self.is_leaf:
            self._subdivide()
            # Redistribute existing items
            existing = self.items[:]
            self.items.clear()
            for ex in existing:
                self._insert_into_children(ex)

        if not self.is_leaf:
            self._insert_into_children(item)
        else:
            self.items.append(item)

        return True

    def _insert_into_children(self, item: QuadTreeItem) -> None:
        """Try to insert into children; if fails, keep in this node."""
        quad = self._quadrant_for(item.bounds)
        if quad is not None and self._children:
            self._children[quad].insert(item)
        else:
            self.items.append(item)

    def query_point(self, px: float, py: float) -> Iterator[QuadTreeItem]:
        """Yield all items that contain the given point."""
        if not self.bounds.contains_point(px, py):
            return

        yield from self.items

        if self._children:
            for child in self._children:
                yield from child.query_point(px, py)

    def query_bounds(self, query: Bounds) -> Iterator[QuadTreeItem]:
        """Yield all items that intersect with the query bounds."""
        if not self.bounds.intersects(query):
            return

        for item in self.items:
            if item.bounds.intersects(query):
                yield item

        if self._children:
            for child in self._children:
                yield from child.query_bounds(query)


class QuadTree:
    """Quadtree for spatial partitioning of UI elements."""

    def __init__(self, width: float, height: float):
        self.bounds = Bounds(0, 0, width, height)
        self._root = QuadTreeNode(self.bounds)
        self._count = 0

    def insert(self, x: float, y: float, width: float, height: float, data: Any = None) -> bool:
        """Insert an element by its bounds."""
        item = QuadTreeItem(Bounds(x, y, width, height), data)
        if self._root.insert(item):
            self._count += 1
            return True
        return False

    def insert_bounds(self, bounds: Bounds, data: Any = None) -> bool:
        """Insert an element by its bounds object."""
        item = QuadTreeItem(bounds, data)
        if self._root.insert(item):
            self._count += 1
            return True
        return False

    def query_point(self, px: float, py: float) -> list[QuadTreeItem]:
        """Find all items at a given point."""
        return list(self._root.query_point(px, py))

    def query_nearest(self, px: float, py: float, max_distance: float = float('inf')) -> Optional[QuadTreeItem]:
        """Find the nearest item to a point within max_distance."""
        candidates = list(self._root.query_point(px, py))
        if not candidates:
            # Expand search
            search_bounds = Bounds(
                px - max_distance, py - max_distance,
                max_distance * 2, max_distance * 2
            )
            candidates = list(self._root.query_bounds(search_bounds))

        best = None
        best_dist = max_distance

        for item in candidates:
            cx = item.bounds.center_x
            cy = item.bounds.center_y
            dist = math.hypot(cx - px, cy - py)
            if dist < best_dist:
                best_dist = dist
                best = item

        return best

    @property
    def count(self) -> int:
        return self._count


__all__ = ["QuadTree", "QuadTreeNode", "QuadTreeItem", "Bounds"]

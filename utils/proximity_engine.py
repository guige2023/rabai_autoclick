"""
Proximity Engine Utility

Proximity-based element detection and spatial relationship analysis.
Finds elements near a given point or other elements.

Example:
    >>> engine = ProximityEngine(accessibility_tree)
    >>> nearby = engine.find_nearby(target_point=(100, 200), radius=50)
    >>> print([e.name for e in nearby])
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Callable
import math


@dataclass
class Point:
    """A 2D point."""
    x: float
    y: float

    def distance_to(self, other: Point) -> float:
        """Euclidean distance to another point."""
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)

    def manhattan_distance_to(self, other: Point) -> float:
        """Manhattan distance to another point."""
        return abs(self.x - other.x) + abs(self.y - other.y)

    def angle_to(self, other: Point) -> float:
        """Angle in degrees from self to other."""
        return math.degrees(math.atan2(other.y - self.y, other.x - self.x))


@dataclass
class Rect:
    """An axis-aligned rectangle."""
    x: float
    y: float
    width: float
    height: float

    @property
    def center(self) -> Point:
        return Point(self.x + self.width / 2, self.y + self.height / 2)

    @property
    def left(self) -> float:
        return self.x

    @property
    def right(self) -> float:
        return self.x + self.width

    @property
    def top(self) -> float:
        return self.y

    @property
    def bottom(self) -> float:
        return self.y + self.height

    def contains_point(self, point: Point) -> bool:
        """Check if point is inside rectangle."""
        return self.left <= point.x <= self.right and self.top <= point.y <= self.bottom

    def distance_to_point(self, point: Point) -> float:
        """Minimum distance from point to rectangle edge."""
        dx = max(self.left - point.x, 0, point.x - self.right)
        dy = max(self.top - point.y, 0, point.y - self.bottom)
        return math.sqrt(dx ** 2 + dy ** 2)

    def intersection(self, other: Rect) -> Optional[Rect]:
        """Compute intersection with another rectangle."""
        x1 = max(self.left, other.left)
        y1 = max(self.top, other.top)
        x2 = min(self.right, other.right)
        y2 = min(self.bottom, other.bottom)
        if x1 < x2 and y1 < y2:
            return Rect(x1, y1, x2 - x1, y2 - y1)
        return None

    def union(self, other: Rect) -> Rect:
        """Compute bounding box of union with another rectangle."""
        x1 = min(self.left, other.left)
        y1 = min(self.top, other.top)
        x2 = max(self.right, other.right)
        y2 = max(self.bottom, other.bottom)
        return Rect(x1, y1, x2 - x1, y2 - y1)


@dataclass
class SpatialElement:
    """An element with spatial properties."""
    id: str
    name: Optional[str] = None
    role: str = "unknown"
    rect: Optional[Rect] = None
    data: any = None

    @property
    def center(self) -> Optional[Point]:
        return self.rect.center if self.rect else None


class ProximityEngine:
    """
    Engine for proximity-based spatial queries on UI elements.

    Args:
        elements: List of SpatialElements to query.
    """

    def __init__(self, elements: list[SpatialElement] | None = None) -> None:
        self._elements: list[SpatialElement] = elements or []

    def set_elements(self, elements: list[SpatialElement]) -> None:
        """Replace the element list."""
        self._elements = elements

    def add_element(self, element: SpatialElement) -> None:
        """Add an element to the engine."""
        self._elements.append(element)

    def clear(self) -> None:
        """Remove all elements."""
        self._elements.clear()

    def find_nearby(
        self,
        target: Point | tuple[float, float],
        radius: float,
        filter_fn: Optional[Callable[[SpatialElement], bool]] = None,
    ) -> list[tuple[SpatialElement, float]]:
        """
        Find elements within radius of target point.

        Args:
            target: Center point for search.
            radius: Search radius in pixels.
            filter_fn: Optional filter function.

        Returns:
            List of (element, distance) tuples, sorted by distance.
        """
        if isinstance(target, tuple):
            target = Point(target[0], target[1])

        results: list[tuple[SpatialElement, float]] = []
        for element in self._elements:
            if filter_fn and not filter_fn(element):
                continue
            if element.center is None:
                continue
            dist = target.distance_to(element.center)
            if dist <= radius:
                results.append((element, dist))

        results.sort(key=lambda x: x[1])
        return results

    def find_nearest(
        self,
        target: Point | tuple[float, float],
        filter_fn: Optional[Callable[[SpatialElement], bool]] = None,
    ) -> Optional[tuple[SpatialElement, float]]:
        """Find the nearest element to a point."""
        nearby = self.find_nearby(target, radius=float("inf"), filter_fn=filter_fn)
        return nearby[0] if nearby else None

    def find_in_direction(
        self,
        from_point: Point | tuple[float, float],
        angle_degrees: float,
        max_distance: float = 500,
        filter_fn: Optional[Callable[[SpatialElement], bool]] = None,
    ) -> list[tuple[SpatialElement, float]]:
        """
        Find elements in a specific direction from a point.

        Args:
            from_point: Starting point.
            angle_degrees: Direction angle (0 = right, 90 = down).
            max_distance: Maximum search distance.
            filter_fn: Optional filter.

        Returns:
            List of (element, distance) in the given direction.
        """
        if isinstance(from_point, tuple):
            from_point = Point(from_point[0], from_point[1])

        results: list[tuple[SpatialElement, float]] = []
        angle_rad = math.radians(angle_degrees)

        for element in self._elements:
            if filter_fn and not filter_fn(element):
                continue
            if element.center is None:
                continue

            dist = from_point.distance_to(element.center)
            if dist > max_distance:
                continue

            element_angle = from_point.angle_to(element.center)
            angle_diff = abs(element_angle - angle_degrees) % 360
            if angle_diff > 180:
                angle_diff = 360 - angle_diff

            # Within 30 degrees of direction
            if angle_diff <= 30:
                results.append((element, dist))

        results.sort(key=lambda x: x[1])
        return results

    def find_in_region(
        self,
        rect: Rect,
        filter_fn: Optional[Callable[[SpatialElement], bool]] = None,
    ) -> list[SpatialElement]:
        """Find all elements whose center is within a rectangle."""
        results: list[SpatialElement] = []
        for element in self._elements:
            if filter_fn and not filter_fn(element):
                continue
            if element.center and rect.contains_point(element.center):
                results.append(element)
        return results

    def find_neighbors(
        self,
        element: SpatialElement,
        radius: float = 50,
    ) -> list[tuple[SpatialElement, float]]:
        """Find elements near another element."""
        if element.center is None:
            return []
        return self.find_nearby(element.center, radius=radius)

    def build_spatial_index(self) -> dict[str, list[SpatialElement]]:
        """
        Build a spatial index keyed by rough grid region.

        Returns:
            Dict mapping region keys to lists of elements.
        """
        grid_size = 100
        index: dict[str, list[SpatialElement]] = {}

        for element in self._elements:
            if element.center is None:
                continue
            gx = int(element.center.x // grid_size)
            gy = int(element.center.y // grid_size)
            key = f"{gx},{gy}"
            if key not in index:
                index[key] = []
            index[key].append(element)

        return index

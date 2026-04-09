"""Distance metric and spatial measurement utilities.

Provides various distance metrics, proximity calculations,
and spatial relationship measurements for automation.
"""

from __future__ import annotations

from typing import Tuple, Sequence, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum, auto
import math


class DistanceMetric(Enum):
    """Supported distance metric types."""
    EUCLIDEAN = auto()
    MANHATTAN = auto()
    CHEBYSHEV = auto()
    HAMMING = auto()
    MINKOWSKI = auto()
    COSINE = auto()
    GREAT_CIRCLE = auto()


def euclidean_distance(
    x1: float, y1: float, x2: float, y2: float
) -> float:
    """Calculate Euclidean (L2) distance between two points."""
    dx = x2 - x1
    dy = y2 - y1
    return math.sqrt(dx * dx + dy * dy)


def manhattan_distance(
    x1: float, y1: float, x2: float, y2: float
) -> float:
    """Calculate Manhattan (L1) distance between two points."""
    return abs(x2 - x1) + abs(y2 - y1)


def chebyshev_distance(
    x1: float, y1: float, x2: float, y2: float
) -> float:
    """Calculate Chebyshev (L-infinity) distance between two points."""
    return max(abs(x2 - x1), abs(y2 - y1))


def minkowski_distance(
    x1: float, y1: float, x2: float, y2: float, p: float = 3.0
) -> float:
    """Calculate Minkowski distance (L-p norm)."""
    if p <= 0:
        raise ValueError("Order p must be positive")
    dx = abs(x2 - x1)
    dy = abs(y2 - y1)
    return math.pow(math.pow(dx, p) + math.pow(dy, p), 1.0 / p)


def cosine_distance(
    x1: float, y1: float, x2: float, y2: float
) -> float:
    """Calculate cosine distance between two vectors."""
    dot = x1 * x2 + y1 * y2
    norm1 = math.sqrt(x1 * x1 + y1 * y1)
    norm2 = math.sqrt(x2 * x2 + y2 * y2)
    if norm1 == 0 or norm2 == 0:
        return 1.0
    return 1.0 - (dot / (norm1 * norm2))


def distance_to_segment(
    px: float, py: float,
    x1: float, y1: float, x2: float, y2: float
) -> float:
    """Calculate minimum distance from point to line segment."""
    dx = x2 - x1
    dy = y2 - y1
    length_sq = dx * dx + dy * dy
    if length_sq == 0:
        return euclidean_distance(px, py, x1, y1)
    t = max(0.0, min(1.0, ((px - x1) * dx + (py - y1) * dy) / length_sq))
    proj_x = x1 + t * dx
    proj_y = y1 + t * dy
    return euclidean_distance(px, py, proj_x, proj_y)


def distance_to_line(
    px: float, py: float,
    x1: float, y1: float, x2: float, y2: float
) -> float:
    """Calculate minimum distance from point to infinite line."""
    dx = x2 - x1
    dy = y2 - y1
    length = math.sqrt(dx * dx + dy * dy)
    if length == 0:
        return euclidean_distance(px, py, x1, y1)
    return abs(dy * px - dx * py + x2 * y1 - y2 * x1) / length


@dataclass
class Point:
    """2D point with optional metadata."""
    x: float
    y: float
    data: Optional[dict] = None

    def distance_to(self, other: Point) -> float:
        """Calculate Euclidean distance to another point."""
        return euclidean_distance(self.x, self.y, other.x, other.y)

    def manhattan_distance_to(self, other: Point) -> float:
        """Calculate Manhattan distance to another point."""
        return manhattan_distance(self.x, self.y, other.x, other.y)

    def chebyshev_distance_to(self, other: Point) -> float:
        """Calculate Chebyshev distance to another point."""
        return chebyshev_distance(self.x, self.y, other.x, other.y)

    def angle_to(self, other: Point) -> float:
        """Calculate angle from this point to another in radians."""
        return math.atan2(other.y - self.y, other.x - self.x)

    def midpoint_to(self, other: Point) -> Point:
        """Get midpoint between this and another point."""
        return Point(
            x=(self.x + other.x) / 2.0,
            y=(self.y + other.y) / 2.0,
        )

    def __repr__(self) -> str:
        return f"Point(x={self.x:.2f}, y={self.y:.2f})"


@dataclass
class Rect:
    """Axis-aligned rectangle."""
    x: float
    y: float
    width: float
    height: float

    @property
    def center(self) -> Point:
        """Get center point of rectangle."""
        return Point(x=self.x + self.width / 2, y=self.y + self.height / 2)

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

    def contains_point(self, px: float, py: float) -> bool:
        """Check if point is inside rectangle."""
        return (self.x <= px <= self.right and self.y <= py <= self.bottom)

    def distance_to_point(self, px: float, py: float) -> float:
        """Calculate minimum distance from rectangle to point."""
        dx = max(self.x - px, 0, px - self.right)
        dy = max(self.y - py, 0, py - self.bottom)
        if dx == 0 and dy == 0:
            return 0.0
        return math.sqrt(dx * dx + dy * dy)

    def intersects(self, other: Rect) -> bool:
        """Check if two rectangles intersect."""
        return not (
            self.right < other.x or self.x > other.right
            or self.bottom < other.y or self.y > other.bottom
        )

    def union(self, other: Rect) -> Rect:
        """Get bounding box of union of two rectangles."""
        x = min(self.x, other.x)
        y = min(self.y, other.y)
        right = max(self.right, other.right)
        bottom = max(self.bottom, other.bottom)
        return Rect(x=x, y=y, width=right - x, height=bottom - y)


@dataclass
class Path:
    """Path represented as sequence of points."""
    points: List[Point]

    @classmethod
    def from_coords(
        cls, coords: Sequence[Tuple[float, float]]
    ) -> Path:
        """Create path from sequence of (x, y) tuples."""
        return cls(points=[Point(x, y) for x, y in coords])

    @property
    def length(self) -> float:
        """Calculate total path length."""
        if len(self.points) < 2:
            return 0.0
        total = 0.0
        for i in range(1, len(self.points)):
            total += self.points[i - 1].distance_to(self.points[i])
        return total

    def point_at_distance(self, dist: float) -> Point:
        """Get point at specified distance along path."""
        if len(self.points) < 2:
            if not self.points:
                raise ValueError("Path has no points")
            return self.points[0]
        if dist <= 0:
            return self.points[0]
        accumulated = 0.0
        for i in range(1, len(self.points)):
            segment_len = self.points[i - 1].distance_to(self.points[i])
            if accumulated + segment_len >= dist:
                t = (dist - accumulated) / segment_len
                return Point(
                    x=self.points[i - 1].x + t * (
                        self.points[i].x - self.points[i - 1].x),
                    y=self.points[i - 1].y + t * (
                        self.points[i].y - self.points[i - 1].y),
                )
            accumulated += segment_len
        return self.points[-1]

    def simplify(self, epsilon: float = 1.0) -> Path:
        """Simplify path using Ramer-Douglas-Peucker algorithm."""
        if len(self.points) < 3:
            return self
        first = self.points[0]
        last = self.points[-1]
        max_dist = 0.0
        max_idx = 0
        for i in range(1, len(self.points) - 1):
            d = distance_to_line(
                self.points[i].x, self.points[i].y,
                first.x, first.y, last.x, last.y
            )
            if d > max_dist:
                max_dist = d
                max_idx = i
        if max_dist > epsilon:
            left = Path(self.points[:max_idx + 1]).simplify(epsilon)
            right = Path(self.points[max_idx:]).simplify(epsilon)
            return Path(left.points[:-1] + right.points)
        return Path([first, last])


def nearest_point(
    px: float, py: float,
    points: Sequence[Point]
) -> Tuple[Point, float, int]:
    """Find nearest point to given coordinates.

    Returns:
        Tuple of (nearest_point, distance, index)
    """
    if not points:
        raise ValueError("Points sequence is empty")
    best_idx = 0
    best_dist = euclidean_distance(px, py, points[0].x, points[0].y)
    for i in range(1, len(points)):
        d = euclidean_distance(px, py, points[i].x, points[i].y)
        if d < best_dist:
            best_dist = d
            best_idx = i
    return (points[best_idx], best_dist, best_idx)


def k_nearest_points(
    px: float, py: float,
    points: Sequence[Point],
    k: int
) -> List[Tuple[Point, float, int]]:
    """Find k nearest points to given coordinates."""
    if not points:
        raise ValueError("Points sequence is empty")
    k = min(k, len(points))
    with_distances = [
        (p, euclidean_distance(px, py, p.x, p.y), i)
        for i, p in enumerate(points)
    ]
    with_distances.sort(key=lambda x: x[1])
    return with_distances[:k]

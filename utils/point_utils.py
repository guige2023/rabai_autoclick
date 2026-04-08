"""
Point, vector, and geometric utilities for automation.

Provides utilities for point manipulation, vector math,
geometric calculations, and coordinate transformations.
"""

from __future__ import annotations

import math
from typing import Tuple, List, Optional, Callable, Iterator


@dataclass
class Point:
    """Represents a 2D point."""
    x: float
    y: float
    
    def __add__(self, other: "Point") -> "Point":
        return Point(self.x + other.x, self.y + other.y)
    
    def __sub__(self, other: "Point") -> "Point":
        return Point(self.x - other.x, self.y - other.y)
    
    def __mul__(self, scalar: float) -> "Point":
        return Point(self.x * scalar, self.y * scalar)
    
    def __rmul__(self, scalar: float) -> "Point":
        return Point(self.x * scalar, self.y * scalar)
    
    def __truediv__(self, scalar: float) -> "Point":
        return Point(self.x / scalar, self.y / scalar)
    
    def __neg__(self) -> "Point":
        return Point(-self.x, -self.y)
    
    def __iter__(self) -> Iterator[float]:
        yield self.x
        yield self.y
    
    def __getitem__(self, index: int) -> float:
        if index == 0:
            return self.x
        elif index == 1:
            return self.y
        raise IndexError("Point index out of range")
    
    def __len__(self) -> int:
        return 2
    
    def __repr__(self) -> str:
        return f"Point({self.x}, {self.y})"
    
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Point):
            return False
        return abs(self.x - other.x) < 1e-9 and abs(self.y - other.y) < 1e-9
    
    def distance_to(self, other: "Point") -> float:
        """Euclidean distance to another point."""
        return math.sqrt((self.x - other.x)**2 + (self.y - other.y)**2)
    
    def manhattan_distance_to(self, other: "Point") -> float:
        """Manhattan distance to another point."""
        return abs(self.x - other.x) + abs(self.y - other.y)
    
    def dot(self, other: "Point") -> float:
        """Dot product with another point (as vector)."""
        return self.x * other.x + self.y * other.y
    
    def cross(self, other: "Point") -> float:
        """Cross product (z-component) with another point."""
        return self.x * other.y - self.y * other.x
    
    def magnitude(self) -> float:
        """Magnitude (length) of vector from origin."""
        return math.sqrt(self.x**2 + self.y**2)
    
    def magnitude_squared(self) -> float:
        """Squared magnitude (avoids sqrt)."""
        return self.x**2 + self.y**2
    
    def normalize(self) -> "Point":
        """Return unit vector in same direction."""
        mag = self.magnitude()
        if mag < 1e-9:
            return Point(0, 0)
        return Point(self.x / mag, self.y / mag)
    
    def perpendicular(self) -> "Point":
        """Return perpendicular vector (rotated 90° counterclockwise)."""
        return Point(-self.y, self.x)
    
    def angle_to(self, other: "Point") -> float:
        """Angle in radians to another point."""
        dx = other.x - self.x
        dy = other.y - self.y
        return math.atan2(dy, dx)
    
    def lerp(self, other: "Point", t: float) -> "Point":
        """Linear interpolation to another point."""
        return Point(
            self.x + (other.x - self.x) * t,
            self.y + (other.y - self.y) * t
        )
    
    def rotate(self, angle: float, center: Optional["Point"] = None) -> "Point":
        """Rotate point by angle (radians) around center."""
        if center is None:
            center = Point(0, 0)
        
        cos_a = math.cos(angle)
        sin_a = math.sin(angle)
        
        dx = self.x - center.x
        dy = self.y - center.y
        
        return Point(
            center.x + dx * cos_a - dy * sin_a,
            center.y + dx * sin_a + dy * cos_a
        )
    
    def scale(self, sx: float, sy: Optional[float] = None, center: Optional["Point"] = None) -> "Point":
        """Scale point by factors."""
        if sy is None:
            sy = sx
        if center is None:
            center = Point(0, 0)
        
        return Point(
            center.x + (self.x - center.x) * sx,
            center.y + (self.y - center.y) * sy
        )
    
    def to_tuple(self) -> Tuple[int, int]:
        """Convert to integer tuple."""
        return (int(self.x), int(self.y))
    
    def to_float_tuple(self) -> Tuple[float, float]:
        """Convert to float tuple."""
        return (self.x, self.y)


@dataclass 
class Rect:
    """Represents a rectangle."""
    x: float
    y: float
    width: float
    height: float
    
    def __post_init__(self):
        if self.width < 0:
            self.x += self.width
            self.width = -self.width
        if self.height < 0:
            self.y += self.height
            self.height = -self.height
    
    @property
    def left(self) -> float:
        return self.x
    
    @property
    def top(self) -> float:
        return self.y
    
    @property
    def right(self) -> float:
        return self.x + self.width
    
    @property
    def bottom(self) -> float:
        return self.y + self.height
    
    @property
    def center(self) -> Point:
        return Point(self.x + self.width / 2, self.y + self.height / 2)
    
    @property
    def center_x(self) -> float:
        return self.x + self.width / 2
    
    @property
    def center_y(self) -> float:
        return self.y + self.height / 2
    
    def contains_point(self, point: Point) -> bool:
        """Check if point is inside rectangle."""
        return (self.x <= point.x <= self.right and 
                self.y <= point.y <= self.bottom)
    
    def contains_point_tuple(self, x: float, y: float) -> bool:
        """Check if coordinates are inside rectangle."""
        return self.x <= x <= self.right and self.y <= y <= self.bottom
    
    def intersects(self, other: "Rect") -> bool:
        """Check if rectangles intersect."""
        return not (self.right < other.x or 
                   other.right < self.x or
                   self.bottom < other.y or
                   other.bottom < self.y)
    
    def intersection(self, other: "Rect") -> Optional["Rect"]:
        """Get intersection rectangle."""
        if not self.intersects(other):
            return None
        
        x = max(self.x, other.x)
        y = max(self.y, other.y)
        width = min(self.right, other.right) - x
        height = min(self.bottom, other.bottom) - y
        
        return Rect(x, y, width, height)
    
    def union(self, other: "Rect") -> "Rect":
        """Get bounding rectangle of both."""
        x = min(self.x, other.x)
        y = min(self.y, other.y)
        right = max(self.right, other.right)
        bottom = max(self.bottom, other.bottom)
        return Rect(x, y, right - x, bottom - y)
    
    def expand(self, amount: float) -> "Rect":
        """Expand rectangle by amount in all directions."""
        return Rect(
            self.x - amount,
            self.y - amount,
            self.width + amount * 2,
            self.height + amount * 2
        )
    
    def shrink(self, amount: float) -> "Rect":
        """Shrink rectangle by amount in all directions."""
        return self.expand(-amount)
    
    def to_corners(self) -> List[Point]:
        """Get four corners as points."""
        return [
            Point(self.x, self.y),
            Point(self.right, self.y),
            Point(self.right, self.bottom),
            Point(self.x, self.bottom)
        ]
    
    def to_tuple(self) -> Tuple[int, int, int, int]:
        """Convert to (x, y, width, height) tuple."""
        return (int(self.x), int(self.y), int(self.width), int(self.height))


def distance(p1: Point, p2: Point) -> float:
    """Euclidean distance between two points."""
    return p1.distance_to(p2)


def manhattan_distance(p1: Point, p2: Point) -> float:
    """Manhattan distance between two points."""
    return p1.manhattan_distance_to(p2)


def midpoint(p1: Point, p2: Point) -> Point:
    """Midpoint between two points."""
    return Point((p1.x + p2.x) / 2, (p1.y + p2.y) / 2)


def angle_between(p1: Point, p2: Point, p3: Point) -> float:
    """Angle at p2 between vectors p1-p2 and p3-p2 in radians."""
    v1 = p1 - p2
    v2 = p3 - p2
    
    cos_angle = v1.dot(v2) / (v1.magnitude() * v2.magnitude())
    cos_angle = max(-1, min(1, cos_angle))
    
    return math.acos(cos_angle)


def perpendicular_distance(point: Point, line_start: Point, line_end: Point) -> float:
    """Perpendicular distance from point to line segment."""
    dx = line_end.x - line_start.x
    dy = line_end.y - line_start.y
    
    if dx == 0 and dy == 0:
        return point.distance_to(line_start)
    
    t = max(0, min(1, ((point.x - line_start.x) * dx + (point.y - line_start.y) * dy) / (dx*dx + dy*dy)))
    
    projection = Point(
        line_start.x + t * dx,
        line_start.y + t * dy
    )
    
    return point.distance_to(projection)


def closest_point_on_segment(point: Point, seg_start: Point, seg_end: Point) -> Point:
    """Closest point on line segment to given point."""
    dx = seg_end.x - seg_start.x
    dy = seg_end.y - seg_start.y
    
    if dx == 0 and dy == 0:
        return seg_start
    
    t = max(0, min(1, ((point.x - seg_start.x) * dx + (point.y - seg_start.y) * dy) / (dx*dx + dy*dy)))
    
    return Point(
        seg_start.x + t * dx,
        seg_start.y + t * dy
    )


def points_on_circle(center: Point, radius: float, count: int) -> List[Point]:
    """Generate evenly spaced points on a circle."""
    points = []
    for i in range(count):
        angle = (2 * math.pi * i) / count
        points.append(Point(
            center.x + radius * math.cos(angle),
            center.y + radius * math.sin(angle)
        ))
    return points


def points_on_ellipse(center: Point, rx: float, ry: float, count: int) -> List[Point]:
    """Generate evenly spaced points on an ellipse."""
    points = []
    for i in range(count):
        angle = (2 * math.pi * i) / count
        points.append(Point(
            center.x + rx * math.cos(angle),
            center.y + ry * math.sin(angle)
        ))
    return points


def bounding_box(points: List[Point]) -> Rect:
    """Get bounding rectangle for a list of points."""
    if not points:
        return Rect(0, 0, 0, 0)
    
    min_x = min(p.x for p in points)
    min_y = min(p.y for p in points)
    max_x = max(p.x for p in points)
    max_y = max(p.y for p in points)
    
    return Rect(min_x, min_y, max_x - min_x, max_y - min_y)


def convex_hull(points: List[Point]) -> List[Point]:
    """Compute convex hull using Graham scan."""
    if len(points) < 3:
        return list(points)
    
    def cross(O: Point, A: Point, B: Point) -> float:
        return (A.x - O.x) * (B.y - O.y) - (A.y - O.y) * (B.x - O.x)
    
    # Sort by x, then by y
    sorted_points = sorted(points, key=lambda p: (p.x, p.y))
    
    # Build lower hull
    lower = []
    for p in sorted_points:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)
    
    # Build upper hull
    upper = []
    for p in reversed(sorted_points):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)
    
    # Remove last point of each half (it's repeated)
    return lower[:-1] + upper[:-1]


def point_in_polygon(point: Point, polygon: List[Point]) -> bool:
    """Check if point is inside polygon using ray casting."""
    n = len(polygon)
    inside = False
    
    j = n - 1
    for i in range(n):
        if ((polygon[i].y > point.y) != (polygon[j].y > point.y) and
            point.x < (polygon[j].x - polygon[i].x) * (point.y - polygon[i].y) / (polygon[j].y - polygon[i].y) + polygon[i].x):
            inside = not inside
        j = i
    
    return inside


def transform_point(
    point: Point,
    scale: Tuple[float, float] = (1.0, 1.0),
    rotation: float = 0.0,
    translation: Tuple[float, float] = (0.0, 0.0)
) -> Point:
    """Apply affine transformation to point."""
    # Scale
    p = Point(point.x * scale[0], point.y * scale[1])
    
    # Rotate
    if rotation != 0:
        cos_r = math.cos(rotation)
        sin_r = math.sin(rotation)
        p = Point(p.x * cos_r - p.y * sin_r, p.x * sin_r + p.y * cos_r)
    
    # Translate
    p = Point(p.x + translation[0], p.y + translation[1])
    
    return p


def normalize_coordinates(
    point: Point,
    source_bounds: Rect,
    dest_bounds: Rect
) -> Point:
    """Normalize point from source bounds to destination bounds."""
    if source_bounds.width == 0 or source_bounds.height == 0:
        return Point(dest_bounds.x, dest_bounds.y)
    
    normalized = Point(
        (point.x - source_bounds.x) / source_bounds.width,
        (point.y - source_bounds.y) / source_bounds.height
    )
    
    return Point(
        dest_bounds.x + normalized.x * dest_bounds.width,
        dest_bounds.y + normalized.y * dest_bounds.height
    )

"""
Geometry utilities for automation actions.

Provides 2D/3D geometric primitives, transformations,
distance calculations, and intersection tests.
"""

from __future__ import annotations

import math
from typing import NamedTuple


class Point2D(NamedTuple):
    """2D point."""
    x: float
    y: float

    def distance_to(self, other: "Point2D") -> float:
        """Euclidean distance to another point."""
        return math.hypot(self.x - other.x, self.y - other.y)

    def manhattan_distance_to(self, other: "Point2D") -> float:
        """Manhattan distance to another point."""
        return abs(self.x - other.x) + abs(self.y - other.y)

    def midpoint_to(self, other: "Point2D") -> "Point2D":
        """Midpoint between this point and another."""
        return Point2D((self.x + other.x) / 2, (self.y + other.y) / 2)

    def translate(self, dx: float, dy: float) -> "Point2D":
        """Translate point by (dx, dy)."""
        return Point2D(self.x + dx, self.y + dy)

    def rotate(self, angle: float, center: "Point2D" | None = None) -> "Point2D":
        """Rotate point by angle (radians) around center."""
        if center is None:
            center = Point2D(0, 0)
        cos_a = math.cos(angle)
        sin_a = math.sin(angle)
        dx = self.x - center.x
        dy = self.y - center.y
        return Point2D(
            center.x + dx * cos_a - dy * sin_a,
            center.y + dx * sin_a + dy * cos_a,
        )

    def scale(self, sx: float, sy: float, center: "Point2D" | None = None) -> "Point2D":
        """Scale point by (sx, sy) from center."""
        if center is None:
            center = Point2D(0, 0)
        return Point2D(center.x + (self.x - center.x) * sx, center.y + (self.y - center.y) * sy)


class Point3D(NamedTuple):
    """3D point."""
    x: float
    y: float
    z: float

    def distance_to(self, other: "Point3D") -> float:
        """Euclidean distance to another point."""
        return math.sqrt(
            (self.x - other.x) ** 2 + (self.y - other.y) ** 2 + (self.z - other.z) ** 2
        )

    def midpoint_to(self, other: "Point3D") -> "Point3D":
        """Midpoint between this point and another."""
        return Point3D(
            (self.x + other.x) / 2, (self.y + other.y) / 2, (self.z + other.z) / 2
        )


class Segment2D(NamedTuple):
    """2D line segment."""
    p1: Point2D
    p2: Point2D

    def length(self) -> float:
        """Length of segment."""
        return self.p1.distance_to(self.p2)

    def midpoint(self) -> Point2D:
        """Midpoint of segment."""
        return self.p1.midpoint_to(self.p2)

    def point_at(self, t: float) -> Point2D:
        """Point at parameter t (0 = p1, 1 = p2)."""
        return Point2D(
            self.p1.x + (self.p2.x - self.p1.x) * t,
            self.p1.y + (self.p2.y - self.p1.y) * t,
        )

    def closest_point_to(self, p: Point2D) -> Point2D:
        """Find closest point on segment to p."""
        dx = self.p2.x - self.p1.x
        dy = self.p2.y - self.p1.y
        length_sq = dx * dx + dy * dy
        if length_sq == 0:
            return self.p1
        t = max(0, min(1, ((p.x - self.p1.x) * dx + (p.y - self.p1.y) * dy) / length_sq))
        return self.point_at(t)

    def distance_to_point(self, p: Point2D) -> float:
        """Distance from segment to point."""
        return self.closest_point_to(p).distance_to(p)


class Circle:
    """2D circle."""

    def __init__(self, center: Point2D, radius: float) -> None:
        self.center = center
        self.radius = radius

    def area(self) -> float:
        """Circle area."""
        return math.pi * self.radius * self.radius

    def circumference(self) -> float:
        """Circle circumference."""
        return 2 * math.pi * self.radius

    def contains_point(self, p: Point2D) -> bool:
        """Check if circle contains point."""
        return self.center.distance_to(p) <= self.radius

    def intersects_segment(self, seg: Segment2D) -> bool:
        """Check if circle intersects segment."""
        return seg.distance_to_point(self.center) <= self.radius


class Rectangle:
    """Axis-aligned 2D rectangle."""

    def __init__(self, min_pt: Point2D, max_pt: Point2D) -> None:
        self.min_pt = min_pt
        self.max_pt = max_pt

    @classmethod
    def from_center(cls, center: Point2D, half_width: float, half_height: float) -> "Rectangle":
        """Create rectangle from center and half-dimensions."""
        return cls(
            Point2D(center.x - half_width, center.y - half_height),
            Point2D(center.x + half_width, center.y + half_height),
        )

    @property
    def width(self) -> float:
        return self.max_pt.x - self.min_pt.x

    @property
    def height(self) -> float:
        return self.max_pt.y - self.min_pt.y

    @property
    def center(self) -> Point2D:
        return self.min_pt.midpoint_to(self.max_pt)

    def area(self) -> float:
        """Rectangle area."""
        return self.width * self.height

    def perimeter(self) -> float:
        """Rectangle perimeter."""
        return 2 * (self.width + self.height)

    def contains_point(self, p: Point2D) -> bool:
        """Check if rectangle contains point."""
        return (
            self.min_pt.x <= p.x <= self.max_pt.x and self.min_pt.y <= p.y <= self.max_pt.y
        )

    def intersects_rect(self, other: "Rectangle") -> bool:
        """Check if rectangles intersect."""
        return not (
            self.max_pt.x < other.min_pt.x
            or self.min_pt.x > other.max_pt.x
            or self.max_pt.y < other.min_pt.y
            or self.min_pt.y > other.max_pt.y
        )

    def intersection(self, other: "Rectangle") -> "Rectangle | None":
        """Get intersection rectangle or None."""
        if not self.intersects_rect(other):
            return None
        return Rectangle(
            Point2D(max(self.min_pt.x, other.min_pt.x), max(self.min_pt.y, other.min_pt.y)),
            Point2D(min(self.max_pt.x, other.max_pt.x), min(self.max_pt.y, other.max_pt.y)),
        )

    def union(self, other: "Rectangle") -> "Rectangle":
        """Get minimum bounding rectangle containing both."""
        return Rectangle(
            Point2D(min(self.min_pt.x, other.min_pt.x), min(self.min_pt.y, other.min_pt.y)),
            Point2D(max(self.max_pt.x, other.max_pt.x), max(self.max_pt.y, other.max_pt.y)),
        )


class Triangle:
    """2D triangle."""

    def __init__(self, p1: Point2D, p2: Point2D, p3: Point2D) -> None:
        self.p1 = p1
        self.p2 = p2
        self.p3 = p3

    def area(self) -> float:
        """Triangle area using cross product."""
        return abs(
            (self.p2.x - self.p1.x) * (self.p3.y - self.p1.y)
            - (self.p3.x - self.p1.x) * (self.p2.y - self.p1.y)
        ) / 2

    def perimeter(self) -> float:
        """Triangle perimeter."""
        return self.p1.distance_to(self.p2) + self.p2.distance_to(self.p3) + self.p3.distance_to(self.p1)

    def contains_point(self, p: Point2D) -> bool:
        """Check if triangle contains point using barycentric coordinates."""
        def sign(p_a: Point2D, p_b: Point2D, p_c: Point2D) -> float:
            return (p_a.x - p_c.x) * (p_b.y - p_c.y) - (p_b.x - p_c.x) * (p_a.y - p_c.y)
        d1 = sign(p, self.p1, self.p2)
        d2 = sign(p, self.p2, self.p3)
        d3 = sign(p, self.p3, self.p1)
        has_neg = (d1 < 0) or (d2 < 0) or (d3 < 0)
        has_pos = (d1 > 0) or (d2 > 0) or (d3 > 0)
        return not (has_neg and has_pos)

    def centroid(self) -> Point2D:
        """Triangle centroid."""
        return Point2D(
            (self.p1.x + self.p2.x + self.p3.x) / 3,
            (self.p1.y + self.p2.y + self.p3.y) / 3,
        )


class Line2D:
    """2D line in ax + by + c = 0 form."""

    def __init__(self, a: float, b: float, c: float) -> None:
        self.a = a
        self.b = b
        self.c = c

    @classmethod
    def from_points(cls, p1: Point2D, p2: Point2D) -> "Line2D":
        """Create line through two points."""
        a = p2.y - p1.y
        b = p1.x - p2.x
        c = a * p1.x + b * p1.y
        return cls(a, b, c)

    @classmethod
    def from_segment(cls, seg: Segment2D) -> "Line2D":
        """Create line from segment."""
        return cls.from_points(seg.p1, seg.p2)

    def eval(self, p: Point2D) -> float:
        """Evaluate line equation at point."""
        return self.a * p.x + self.b * p.y - self.c

    def distance_to_point(self, p: Point2D) -> float:
        """Distance from line to point."""
        denom = math.sqrt(self.a * self.a + self.b * self.b)
        if denom == 0:
            return float("inf")
        return abs(self.eval(p)) / denom

    def parallel_to(self, other: "Line2D") -> bool:
        """Check if lines are parallel."""
        return self.a * other.b == self.b * other.a

    def intersection_with(self, other: "Line2D") -> Point2D | None:
        """Find intersection with another line."""
        det = self.a * other.b - self.b * other.a
        if det == 0:
            return None
        return Point2D(
            (other.c * self.b - self.c * other.b) / det,
            (self.c * other.a - other.c * self.a) / det,
        )


class Polygon:
    """2D polygon defined by vertices."""

    def __init__(self, vertices: list[Point2D]) -> None:
        self.vertices = vertices

    def area(self) -> float:
        """Polygon area using Shoelace formula."""
        n = len(self.vertices)
        if n < 3:
            return 0.0
        area = 0.0
        for i in range(n):
            j = (i + 1) % n
            area += self.vertices[i].x * self.vertices[j].y
            area -= self.vertices[j].x * self.vertices[i].y
        return abs(area) / 2

    def perimeter(self) -> float:
        """Polygon perimeter."""
        n = len(self.vertices)
        return sum(
            self.vertices[i].distance_to(self.vertices[(i + 1) % n]) for i in range(n)
        )

    def centroid(self) -> Point2D:
        """Polygon centroid."""
        n = len(self.vertices)
        if n == 0:
            return Point2D(0, 0)
        cx = sum(v.x for v in self.vertices) / n
        cy = sum(v.y for v in self.vertices) / n
        return Point2D(cx, cy)

    def bounding_box(self) -> Rectangle:
        """Axis-aligned bounding box."""
        xs = [v.x for v in self.vertices]
        ys = [v.y for v in self.vertices]
        return Rectangle(Point2D(min(xs), min(ys)), Point2D(max(xs), max(ys)))


class ConvexHull:
    """Convex hull using Graham scan."""

    @staticmethod
    def compute(points: list[Point2D]) -> list[Point2D]:
        """Compute convex hull of points."""
        if len(points) < 3:
            return points[:]
        sorted_pts = sorted(set(points), key=lambda p: (p.x, p.y))
        if len(sorted_pts) <= 1:
            return sorted_pts[:]
        cross = lambda o, a, b: (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x)
        lower = []
        for p in sorted_pts:
            while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
                lower.pop()
            lower.append(p)
        upper = []
        for p in reversed(sorted_pts):
            while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
                upper.pop()
            upper.append(p)
        return lower[:-1] + upper[:-1]


def point_in_convex_polygon(p: Point2D, polygon: list[Point2D]) -> bool:
    """Check if point is inside convex polygon."""
    n = len(polygon)
    if n < 3:
        return False
    sign = None
    for i in range(n):
        j = (i + 1) % n
        val = (polygon[j].x - polygon[i].x) * (p.y - polygon[i].y) - (polygon[j].y - polygon[i].y) * (p.x - polygon[i].x)
        if val != 0:
            cur_sign = val > 0
            if sign is None:
                sign = cur_sign
            elif sign != cur_sign:
                return False
    return True


def line_segment_intersection(seg1: Segment2D, seg2: Segment2D) -> Point2D | None:
    """Find intersection point of two line segments."""
    x1, y1 = seg1.p1.x, seg1.p1.y
    x2, y2 = seg1.p2.x, seg1.p2.y
    x3, y3 = seg2.p1.x, seg2.p1.y
    x4, y4 = seg2.p2.x, seg2.p2.y
    denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)
    if denom == 0:
        return None
    t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denom
    u = -((x1 - x2) * (y1 - y3) - (y1 - y2) * (x1 - x3)) / denom
    if 0 <= t <= 1 and 0 <= u <= 1:
        return seg1.point_at(t)
    return None

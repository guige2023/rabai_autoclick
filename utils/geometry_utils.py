"""
Geometry utilities for points, lines, circles, and polygons.

Provides common 2D/3D geometry computations including
distances, intersections, areas, and transformations.
"""

from __future__ import annotations

import math
from typing import NamedTuple


Point2D = tuple[float, float]
Point3D = tuple[float, float, float]


class Line2D(NamedTuple):
    """2D line defined by two points."""
    p1: Point2D
    p2: Point2D


class Circle(NamedTuple):
    """Circle defined by center and radius."""
    center: Point2D
    radius: float


def distance_2d(p1: Point2D, p2: Point2D) -> float:
    """Euclidean distance between two 2D points."""
    return math.hypot(p1[0] - p2[0], p1[1] - p2[1])


def distance_3d(p1: Point3D, p2: Point3D) -> float:
    """Euclidean distance between two 3D points."""
    return math.sqrt((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2 + (p1[2] - p2[2])**2)


def midpoint(p1: Point2D, p2: Point2D) -> Point2D:
    """Midpoint between two points."""
    return ((p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2)


def slope(p1: Point2D, p2: Point2D) -> float | None:
    """Slope of line through two points. None for vertical lines."""
    dx = p2[0] - p1[0]
    return (p2[1] - p1[1]) / dx if dx != 0 else None


def line_intercept(p1: Point2D, p2: Point2D) -> tuple[float, float] | None:
    """Get line in ax + by = c form. Returns (a, b, c) or None for vertical."""
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]
    if dx == 0:
        return None
    return (dy, -dx, dy * p1[0] - dx * p1[1])


def line_intersection(l1: Line2D, l2: Line2D) -> Point2D | None:
    """Find intersection point of two lines. Returns None if parallel."""
    a1, b1 = l1.p2[1] - l1.p1[1], l1.p1[0] - l1.p2[0]
    c1 = a1 * l1.p1[0] + b1 * l1.p1[1]
    a2, b2 = l2.p2[1] - l2.p1[1], l2.p1[0] - l2.p2[0]
    c2 = a2 * l2.p1[0] + b2 * l2.p1[1]
    det = a1 * b2 - a2 * b1
    if abs(det) < 1e-12:
        return None
    return ((b2 * c1 - b1 * c2) / det, (a1 * c2 - a2 * c1) / det)


def point_in_circle(p: Point2D, circle: Circle) -> bool:
    """Check if point is inside circle."""
    return distance_2d(p, circle.center) <= circle.radius


def circle_intersection(c1: Circle, c2: Circle) -> list[Point2D]:
    """Find intersection points of two circles."""
    d = distance_2d(c1.center, c2.center)
    if d > c1.radius + c2.radius or d < abs(c1.radius - c2.radius):
        return []
    a = (c1.radius**2 - c2.radius**2 + d**2) / (2 * d)
    h = math.sqrt(c1.radius**2 - a**2)
    cx = c1.center[0] + a * (c2.center[0] - c1.center[0]) / d
    cy = c1.center[1] + a * (c2.center[1] - c1.center[1]) / d
    rx = -h * (c2.center[1] - c1.center[1]) / d
    ry = h * (c2.center[0] - c1.center[0]) / d
    return [(cx + rx, cy + ry), (cx - rx, cy - ry)]


def polygon_area(vertices: list[Point2D]) -> float:
    """Compute area of polygon using Shoelace formula."""
    n = len(vertices)
    if n < 3:
        return 0.0
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += vertices[i][0] * vertices[j][1]
        area -= vertices[j][0] * vertices[i][1]
    return abs(area) / 2


def point_in_polygon(p: Point2D, vertices: list[Point2D]) -> bool:
    """Ray casting algorithm to check if point is inside polygon."""
    n = len(vertices)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = vertices[i]
        xj, yj = vertices[j]
        if ((yi > p[1]) != (yj > p[1])) and (p[0] < (xj - xi) * (p[1] - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def convex_hull(points: list[Point2D]) -> list[Point2D]:
    """Compute convex hull using Graham scan."""
    if len(points) < 3:
        return points[:]
    points_sorted = sorted(set(points))
    cross = lambda o, a, b: (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])
    lower, upper = [], []
    for p in points_sorted:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)
    for p in reversed(points_sorted):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)
    return lower[:-1] + upper[:-1]


def rotate_point_2d(p: Point2D, angle: float, center: Point2D = (0, 0)) -> Point2D:
    """Rotate point around center by angle (radians)."""
    cos_a, sin_a = math.cos(angle), math.sin(angle)
    dx, dy = p[0] - center[0], p[1] - center[1]
    return (
        center[0] + dx * cos_a - dy * sin_a,
        center[1] + dx * sin_a + dy * cos_a,
    )


def closest_point_on_line(p: Point2D, line: Line2D) -> Point2D:
    """Find closest point on line segment to given point."""
    dx, dy = line.p2[0] - line.p1[0], line.p2[1] - line.p1[1]
    t = max(0, min(1, ((p[0] - line.p1[0]) * dx + (p[1] - line.p1[1]) * dy) / (dx*dx + dy*dy)))
    return (line.p1[0] + t * dx, line.p1[1] + t * dy)

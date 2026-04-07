"""
Computational geometry utilities.

Provides vector operations, distance functions, line/segment operations,
convex hull, polygon area, and point-in-polygon tests.
"""

from __future__ import annotations

import math


Point = tuple[float, float]
Point3D = tuple[float, float, float]
Segment = tuple[Point, Point]


def distance_2d(p1: Point, p2: Point) -> float:
    """Euclidean distance between two 2D points."""
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]
    return math.sqrt(dx * dx + dy * dy)


def distance_3d(p1: Point3D, p2: Point3D) -> float:
    """Euclidean distance between two 3D points."""
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]
    dz = p2[2] - p1[2]
    return math.sqrt(dx * dx + dy * dy + dz * dz)


def manhattan_distance(p1: Point, p2: Point) -> float:
    """Manhattan distance between two 2D points."""
    return abs(p2[0] - p1[0]) + abs(p2[1] - p1[1])


def chebyshev_distance(p1: Point, p2: Point) -> float:
    """Chebyshev (max) distance between two 2D points."""
    return max(abs(p2[0] - p1[0]), abs(p2[1] - p1[1]))


def cross_product_2d(o: Point, a: Point, b: Point) -> float:
    """2D cross product (OA × OB). Returns positive if OAB makes a left turn."""
    return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])


def dot_product_2d(a: Point, b: Point) -> float:
    """2D dot product."""
    return a[0] * b[0] + a[1] * b[1]


def point_in_polygon(pt: Point, polygon: list[Point]) -> bool:
    """
    Ray casting algorithm to test if point is inside polygon.

    Args:
        pt: (x, y) point
        polygon: List of vertices in order (clockwise or counterclockwise)

    Returns:
        True if point is inside or on boundary.
    """
    x, y = pt
    n = len(polygon)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        if (yi > y) != (yj > y) and x < (xj - xi) * (y - yi) / (yj - yi) + xi:
            inside = not inside
        j = i
    return inside


def point_on_segment(pt: Point, seg: Segment) -> bool:
    """Check if point lies exactly on segment."""
    a, b = seg
    cross = cross_product_2d(a, b, pt)
    if abs(cross) > 1e-10:
        return False
    dot = dot_product_2d((pt[0] - a[0], pt[1] - a[1]), (pt[0] - b[0], pt[1] - b[1]))
    return dot <= 0


def line_intersection(
    p1: Point, p2: Point, p3: Point, p4: Point
) -> Point | None:
    """
    Find intersection point of two line segments (if they intersect).

    Uses parametric form: p = p1 + t*(p2-p1) = p3 + u*(p4-p3)
    Returns None if lines are parallel or coincident.
    """
    x1, y1 = p1
    x2, y2 = p2
    x3, y3 = p3
    x4, y4 = p4

    denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)
    if abs(denom) < 1e-12:
        return None  # parallel

    t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denom
    # u = ((x1 - x3) * (y1 - y2) - (y1 - y3) * (x1 - x2)) / denom

    xi = x1 + t * (x2 - x1)
    yi = y1 + t * (y2 - y1)
    return (xi, yi)


def convex_hull(points: list[Point]) -> list[Point]:
    """
    Graham scan algorithm for convex hull.

    Args:
        points: List of 2D points

    Returns:
        Vertices of the convex hull in counterclockwise order.
    """
    if len(points) < 3:
        return list(points)

    # Find the lowest point
    pivot = min(points, key=lambda p: (p[1], p[0]))
    sorted_pts = sorted(points, key=lambda p: (
        math.atan2(p[1] - pivot[1], p[0] - pivot[0]),
        distance_2d(p, pivot)
    ))

    hull: list[Point] = []
    for pt in sorted_pts:
        while len(hull) >= 2 and cross_product_2d(hull[-2], hull[-1], pt) <= 0:
            hull.pop()
        hull.append(pt)

    return hull


def polygon_area(polygon: list[Point]) -> float:
    """
    Shoelace formula for polygon area.

    Returns:
        Signed area (positive for CCW, negative for CW).
    """
    n = len(polygon)
    if n < 3:
        return 0.0
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += polygon[i][0] * polygon[j][1]
        area -= polygon[j][0] * polygon[i][1]
    return area / 2.0


def polygon_centroid(polygon: list[Point]) -> Point:
    """Compute centroid of a polygon."""
    n = len(polygon)
    if n == 0:
        return (0.0, 0.0)
    if n == 1:
        return polygon[0]
    if n == 2:
        return ((polygon[0][0] + polygon[1][0]) / 2, (polygon[0][1] + polygon[1][1]) / 2)

    area = polygon_area(polygon)
    if abs(area) < 1e-12:
        cx = sum(p[0] for p in polygon) / n
        cy = sum(p[1] for p in polygon) / n
        return (cx, cy)

    cx = 0.0
    cy = 0.0
    for i in range(n):
        j = (i + 1) % n
        factor = polygon[i][0] * polygon[j][1] - polygon[j][0] * polygon[i][1]
        cx += (polygon[i][0] + polygon[j][0]) * factor
        cy += (polygon[i][1] + polygon[j][1]) * factor
    cx /= 6 * area
    cy /= 6 * area
    return (cx, cy)


def closest_point_on_segment(pt: Point, seg: Segment) -> Point:
    """Find the closest point on a line segment to a given point."""
    a, b = seg
    ax, ay = a
    bx, by = b
    px, py = pt

    abx, aby = bx - ax, by - ay
    apx, apy = px - ax, py - ay
    len_sq = abx * abx + aby * aby
    if len_sq < 1e-12:
        return a

    t = max(0.0, min(1.0, (apx * abx + apy * aby) / len_sq))
    return (ax + t * abx, ay + t * aby)


def angle_between_vectors(v1: Point, v2: Point) -> float:
    """Angle in radians between two vectors (0 to pi)."""
    dot = v1[0] * v2[0] + v1[1] * v2[1]
    mag1 = math.sqrt(v1[0] ** 2 + v1[1] ** 2)
    mag2 = math.sqrt(v2[0] ** 2 + v2[1] ** 2)
    if mag1 < 1e-12 or mag2 < 1e-12:
        return 0.0
    cos_angle = max(-1.0, min(1.0, dot / (mag1 * mag2)))
    return math.acos(cos_angle)


def rotate_point_2d(pt: Point, center: Point, angle_rad: float) -> Point:
    """Rotate a 2D point around a center by angle (radians)."""
    cos_a = math.cos(angle_rad)
    sin_a = math.sin(angle_rad)
    dx = pt[0] - center[0]
    dy = pt[1] - center[1]
    return (
        center[0] + dx * cos_a - dy * sin_a,
        center[1] + dx * sin_a + dy * cos_a,
    )


def bounding_box(points: list[Point]) -> tuple[float, float, float, float]:
    """Return (min_x, min_y, max_x, max_y) bounding box."""
    if not points:
        return 0.0, 0.0, 0.0, 0.0
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    return min(xs), min(ys), max(xs), max(ys)

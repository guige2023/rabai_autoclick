"""
Coordinate system and geometry utilities for UI automation.

Provides functions for working with screen coordinates,
geometric calculations, and coordinate transformations.
"""

from __future__ import annotations

import math
from typing import Tuple, List, Optional, Sequence


Point = Tuple[float, float]
Rect = Tuple[float, float, float, float]  # x, y, width, height
Line = Tuple[Point, Point]
Polygon = List[Point]


def distance(p1: Point, p2: Point) -> float:
    """Calculate Euclidean distance between two points.
    
    Args:
        p1: First point (x, y)
        p2: Second point (x, y)
    
    Returns:
        Distance in pixels
    """
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]
    return math.sqrt(dx * dx + dy * dy)


def midpoint(p1: Point, p2: Point) -> Point:
    """Calculate midpoint between two points.
    
    Args:
        p1: First point
        p2: Second point
    
    Returns:
        Midpoint (x, y)
    """
    return ((p1[0] + p2[0]) / 2, (p1[1] + p2[1]) / 2)


def manhattan_distance(p1: Point, p2: Point) -> float:
    """Calculate Manhattan distance between two points.
    
    Args:
        p1: First point
        p2: Second point
    
    Returns:
        Manhattan distance
    """
    return abs(p2[0] - p1[0]) + abs(p2[1] - p1[1])


def chebyshev_distance(p1: Point, p2: Point) -> float:
    """Calculate Chebyshev distance (max of dx, dy).
    
    Args:
        p1: First point
        p2: Second point
    
    Returns:
        Chebyshev distance
    """
    return max(abs(p2[0] - p1[0]), abs(p2[1] - p1[1]))


def angle_between_points(p1: Point, p2: Point) -> float:
    """Calculate angle in degrees from p1 to p2.
    
    Args:
        p1: Origin point
        p2: Target point
    
    Returns:
        Angle in degrees (0 = right, 90 = up)
    """
    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]
    return math.degrees(math.atan2(-dy, dx))


def rotate_point(point: Point, center: Point, angle_deg: float) -> Point:
    """Rotate a point around a center point.
    
    Args:
        point: Point to rotate
        center: Center of rotation
        angle_deg: Rotation angle in degrees (clockwise positive)
    
    Returns:
        Rotated point
    """
    angle_rad = math.radians(angle_deg)
    cos_a = math.cos(angle_rad)
    sin_a = math.sin(angle_rad)
    
    dx = point[0] - center[0]
    dy = point[1] - center[1]
    
    new_x = center[0] + dx * cos_a - dy * sin_a
    new_y = center[1] + dx * sin_a + dy * cos_a
    
    return (new_x, new_y)


def point_in_rect(point: Point, rect: Rect) -> bool:
    """Check if point is inside rectangle.
    
    Args:
        point: Point (x, y)
        rect: Rectangle (x, y, width, height)
    
    Returns:
        True if point is inside rect
    """
    x, y = point
    rx, ry, rw, rh = rect
    return (rx <= x <= rx + rw) and (ry <= y <= ry + rh)


def point_in_circle(point: Point, center: Point, radius: float) -> bool:
    """Check if point is inside circle.
    
    Args:
        point: Point to check
        center: Circle center
        radius: Circle radius
    
    Returns:
        True if point is inside circle
    """
    return distance(point, center) <= radius


def rect_contains_rect(outer: Rect, inner: Rect) -> bool:
    """Check if outer rectangle fully contains inner rectangle.
    
    Args:
        outer: Outer rectangle
        inner: Inner rectangle
    
    Returns:
        True if outer contains inner
    """
    ox, oy, ow, oh = outer
    ix, iy, iw, ih = inner
    return (ox <= ix and oy <= iy and 
            ox + ow >= ix + iw and oy + oh >= iy + ih)


def rects_overlap(r1: Rect, r2: Rect) -> bool:
    """Check if two rectangles overlap.
    
    Args:
        r1: First rectangle
        r2: Second rectangle
    
    Returns:
        True if rectangles overlap
    """
    x1, y1, w1, h1 = r1
    x2, y2, w2, h2 = r2
    
    return not (x1 + w1 <= x2 or x2 + w2 <= x1 or
               y1 + h1 <= y2 or y2 + h2 <= y1)


def rect_intersection(r1: Rect, r2: Rect) -> Optional[Rect]:
    """Calculate intersection of two rectangles.
    
    Args:
        r1: First rectangle
        r2: Second rectangle
    
    Returns:
        Intersection rectangle or None if no overlap
    """
    x1, y1, w1, h1 = r1
    x2, y2, w2, h2 = r2
    
    x = max(x1, x2)
    y = max(y1, y2)
    w = max(0, min(x1 + w1, x2 + w2) - x)
    h = max(0, min(y1 + h1, y2 + h2) - y)
    
    if w == 0 or h == 0:
        return None
    return (x, y, w, h)


def rect_union(r1: Rect, r2: Rect) -> Rect:
    """Calculate bounding union of two rectangles.
    
    Args:
        r1: First rectangle
        r2: Second rectangle
    
    Returns:
        Bounding rectangle of both
    """
    x1, y1, w1, h1 = r1
    x2, y2, w2, h2 = r2
    
    x = min(x1, x2)
    y = min(y1, y2)
    right = max(x1 + w1, x2 + w2)
    bottom = max(y1 + h1, y2 + h2)
    
    return (x, y, right - x, bottom - y)


def rect_center(rect: Rect) -> Point:
    """Get center point of rectangle.
    
    Args:
        rect: Rectangle
    
    Returns:
        Center point (x, y)
    """
    x, y, w, h = rect
    return (x + w / 2, y + h / 2)


def rect_area(rect: Rect) -> float:
    """Calculate area of rectangle.
    
    Args:
        rect: Rectangle
    
    Returns:
        Area in square pixels
    """
    return rect[2] * rect[3]


def rect_perimeter(rect: Rect) -> float:
    """Calculate perimeter of rectangle.
    
    Args:
        rect: Rectangle
    
    Returns:
        Perimeter in pixels
    """
    w, h = rect[2], rect[3]
    return 2 * (w + h)


def normalize_rect(x1: float, y1: float, x2: float, y2: float) -> Rect:
    """Create normalized rect from two corner points.
    
    Args:
        x1: First x coordinate
        y1: First y coordinate
        x2: Second x coordinate
        y2: Second y coordinate
    
    Returns:
        Normalized rect (x, y, width, height)
    """
    x = min(x1, x2)
    y = min(y1, y2)
    w = abs(x2 - x1)
    h = abs(y2 - y1)
    return (x, y, w, h)


def expand_rect(rect: Rect, dx: float, dy: float) -> Rect:
    """Expand rectangle by given amounts.
    
    Args:
        rect: Original rectangle
        dx: Horizontal expansion (total, both sides)
        dy: Vertical expansion (total, both sides)
    
    Returns:
        Expanded rectangle
    """
    x, y, w, h = rect
    return (x - dx/2, y - dy/2, w + dx, h + dy)


def shrink_rect(rect: Rect, dx: float, dy: float) -> Rect:
    """Shrink rectangle by given amounts.
    
    Args:
        rect: Original rectangle
        dx: Horizontal shrink (total)
        dy: Vertical shrink (total)
    
    Returns:
        Shrunk rectangle (may have zero or negative dimensions)
    """
    return expand_rect(rect, -dx, -dy)


def closest_point_on_rect(point: Point, rect: Rect) -> Point:
    """Find closest point on rectangle to given point.
    
    Args:
        point: Query point
        rect: Rectangle
    
    Returns:
        Closest point on rectangle edge
    """
    x, y = point
    rx, ry, rw, rh = rect
    
    cx = max(rx, min(x, rx + rw))
    cy = max(ry, min(y, ry + rh))
    
    return (cx, cy)


def polygon_area(polygon: Polygon) -> float:
    """Calculate area of polygon using shoelace formula.
    
    Args:
        polygon: List of points forming polygon
    
    Returns:
        Area in square pixels
    """
    n = len(polygon)
    if n < 3:
        return 0.0
    
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += polygon[i][0] * polygon[j][1]
        area -= polygon[j][0] * polygon[i][1]
    
    return abs(area) / 2.0


def point_in_polygon(point: Point, polygon: Polygon) -> bool:
    """Check if point is inside polygon using ray casting.
    
    Args:
        point: Point to check
        polygon: Polygon vertices
    
    Returns:
        True if point is inside polygon
    """
    x, y = point
    n = len(polygon)
    inside = False
    
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    
    return inside

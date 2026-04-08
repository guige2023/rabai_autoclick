"""Point-in-shape detection utilities."""

from typing import List, Tuple, Optional, Union
import math


Point = Tuple[float, float]


def point_in_circle(
    point: Point,
    center: Point,
    radius: float
) -> bool:
    """Check if point is inside circle.
    
    Args:
        point: (x, y) point to check.
        center: (cx, cy) circle center.
        radius: Circle radius.
    
    Returns:
        True if point is inside or on circle.
    """
    dx = point[0] - center[0]
    dy = point[1] - center[1]
    return dx * dx + dy * dy <= radius * radius


def point_in_ellipse(
    point: Point,
    center: Point,
    radius_x: float,
    radius_y: float
) -> bool:
    """Check if point is inside ellipse.
    
    Args:
        point: (x, y) point to check.
        center: (cx, cy) ellipse center.
        radius_x: Semi-major axis in x.
        radius_y: Semi-major axis in y.
    
    Returns:
        True if point is inside ellipse.
    """
    dx = (point[0] - center[0]) / radius_x
    dy = (point[1] - center[1]) / radius_y
    return dx * dx + dy * dy <= 1.0


def point_in_polygon(
    point: Point,
    polygon: List[Point]
) -> bool:
    """Check if point is inside polygon using ray casting.
    
    Args:
        point: (x, y) point to check.
        polygon: List of (x, y) vertices.
    
    Returns:
        True if point is inside polygon.
    """
    x, y = point
    n = len(polygon)
    if n < 3:
        return False
    inside = False
    p1x, p1y = polygon[0]
    for i in range(1, n + 1):
        p2x, p2y = polygon[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y
    return inside


def point_near_line(
    point: Point,
    line_start: Point,
    line_end: Point,
    threshold: float = 5.0
) -> bool:
    """Check if point is near a line segment.
    
    Args:
        point: (x, y) point to check.
        line_start: (x1, y1) line start.
        line_end: (x2, y2) line end.
        threshold: Max distance to line.
    
    Returns:
        True if point is within threshold of line.
    """
    x, y = point
    x1, y1 = line_start
    x2, y2 = line_end
    dx = x2 - x1
    dy = y2 - y1
    if dx == 0 and dy == 0:
        return math.sqrt((x - x1) ** 2 + (y - y1) ** 2) <= threshold
    t = max(0, min(1, ((x - x1) * dx + (y - y1) * dy) / (dx * dx + dy * dy)))
    proj_x = x1 + t * dx
    proj_y = y1 + t * dy
    dist = math.sqrt((x - proj_x) ** 2 + (y - proj_y) ** 2)
    return dist <= threshold


def point_in_rectangle(
    point: Point,
    rect: Tuple[float, float, float, float]
) -> bool:
    """Check if point is inside rectangle.
    
    Args:
        point: (x, y) point.
        rect: (x, y, width, height) rectangle.
    
    Returns:
        True if point is inside rectangle.
    """
    x, y = point
    rx, ry, rw, rh = rect
    return rx <= x <= rx + rw and ry <= y <= ry + rh


def distance_to_circle(
    point: Point,
    center: Point,
    radius: float
) -> float:
    """Get signed distance from point to circle edge.
    
    Args:
        point: (x, y) point.
        center: (cx, cy) circle center.
        radius: Circle radius.
    
    Returns:
        Negative if inside, positive if outside.
    """
    dx = point[0] - center[0]
    dy = point[1] - center[1]
    return math.sqrt(dx * dx + dy * dy) - radius


def closest_point_on_line(
    point: Point,
    line_start: Point,
    line_end: Point
) -> Point:
    """Find closest point on line segment to given point.
    
    Args:
        point: (x, y) reference point.
        line_start: (x1, y1) line start.
        line_end: (x2, y2) line end.
    
    Returns:
        (x, y) closest point on line.
    """
    dx = line_end[0] - line_start[0]
    dy = line_end[1] - line_start[1]
    if dx == 0 and dy == 0:
        return line_start
    t = max(0.0, min(1.0, ((point[0] - line_start[0]) * dx + (point[1] - line_start[1]) * dy) / (dx * dx + dy * dy)))
    return (line_start[0] + t * dx, line_start[1] + t * dy)

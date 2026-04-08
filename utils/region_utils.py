"""Region utilities for rectangular region operations."""

from typing import Tuple, List, Optional, Union
import math


Rect = Tuple[int, int, int, int]
Point = Tuple[int, int]


def make_rect(
    x: int, y: int,
    width: int, height: int
) -> Rect:
    """Create a normalized rectangle.
    
    Args:
        x, y: Top-left corner.
        width, height: Dimensions.
    
    Returns:
        (x, y, width, height) tuple.
    """
    return (x, y, width, height)


def rect_from_points(
    p1: Point,
    p2: Point
) -> Rect:
    """Create rectangle from two opposite corners.
    
    Args:
        p1, p2: Two opposite corners.
    
    Returns:
        (x, y, width, height) rectangle.
    """
    x1, y1 = p1
    x2, y2 = p2
    x = min(x1, x2)
    y = min(y1, y2)
    w = abs(x2 - x1)
    h = abs(y2 - y1)
    return (x, y, w, h)


def rect_center(rect: Rect) -> Point:
    """Get center point of rectangle.
    
    Args:
        rect: (x, y, w, h) rectangle.
    
    Returns:
        (cx, cy) center point.
    """
    x, y, w, h = rect
    return (x + w // 2, y + h // 2)


def rect_area(rect: Rect) -> int:
    """Get area of rectangle.
    
    Args:
        rect: Rectangle.
    
    Returns:
        Area in pixels.
    """
    _, _, w, h = rect
    return w * h


def rect_intersection(r1: Rect, r2: Rect) -> Optional[Rect]:
    """Get intersection of two rectangles.
    
    Args:
        r1, r2: Input rectangles.
    
    Returns:
        Intersection rectangle or None if no overlap.
    """
    x1, y1, w1, h1 = r1
    x2, y2, w2, h2 = r2
    x1_max = x1 + w1
    y1_max = y1 + h1
    x2_max = x2 + w2
    y2_max = y2 + h2
    x = max(x1, x2)
    y = max(y1, y2)
    x_max = min(x1_max, x2_max)
    y_max = min(y1_max, y2_max)
    if x >= x_max or y >= y_max:
        return None
    return (x, y, x_max - x, y_max - y)


def rect_union(r1: Rect, r2: Rect) -> Rect:
    """Get bounding union of two rectangles.
    
    Args:
        r1, r2: Input rectangles.
    
    Returns:
        Bounding rectangle containing both.
    """
    x1, y1, w1, h1 = r1
    x2, y2, w2, h2 = r2
    x1_max = x1 + w1
    y1_max = y1 + h1
    x2_max = x2 + w2
    y2_max = y2 + h2
    x = min(x1, x2)
    y = min(y1, y2)
    x_max = max(x1_max, x2_max)
    y_max = max(y1_max, y2_max)
    return (x, y, x_max - x, y_max - y)


def rect_contains_point(rect: Rect, point: Point) -> bool:
    """Check if rectangle contains a point.
    
    Args:
        rect: Rectangle.
        point: (x, y) point.
    
    Returns:
        True if point is inside rectangle.
    """
    x, y, w, h = rect
    px, py = point
    return x <= px < x + w and y <= py < y + h


def rect_contains_rect(r1: Rect, r2: Rect) -> bool:
    """Check if r1 fully contains r2.
    
    Args:
        r1: Container rectangle.
        r2: Contained rectangle.
    
    Returns:
        True if r1 contains r2.
    """
    inter = rect_intersection(r1, r2)
    return inter is not None and inter == r2


def rect_overlaps(r1: Rect, r2: Rect) -> bool:
    """Check if two rectangles overlap.
    
    Args:
        r1, r2: Input rectangles.
    
    Returns:
        True if rectangles overlap.
    """
    return rect_intersection(r1, r2) is not None


def rect_distance(p1: Point, p2: Point) -> float:
    """Calculate Euclidean distance between two points.
    
    Args:
        p1, p2: Points.
    
    Returns:
        Distance in pixels.
    """
    return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)


def rect_to_list(rect: Rect) -> List[int]:
    """Convert rectangle to list format."""
    return list(rect)


def expand_rect(rect: Rect, margin: int) -> Rect:
    """Expand rectangle by margin in all directions.
    
    Args:
        rect: Input rectangle.
        margin: Pixels to expand.
    
    Returns:
        Expanded rectangle.
    """
    x, y, w, h = rect
    return (x - margin, y - margin, w + 2 * margin, h + 2 * margin)


def shrink_rect(rect: Rect, margin: int) -> Rect:
    """Shrink rectangle by margin in all directions.
    
    Args:
        rect: Input rectangle.
        margin: Pixels to shrink.
    
    Returns:
        Shrunk rectangle.
    """
    x, y, w, h = rect
    return (x + margin, y + margin, max(0, w - 2 * margin), max(0, h - 2 * margin))

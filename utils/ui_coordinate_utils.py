"""
UI coordinate transformation utilities.

This module provides utilities for transforming coordinates
between different UI spaces: screen, window, element, and accessibility.
"""

from __future__ import annotations

from typing import Tuple, Optional, Dict, Any, List
from dataclasses import dataclass


@dataclass
class CoordinateSpace:
    """
    Represents a coordinate space for UI coordinates.

    Attributes:
        origin: Origin point (x, y).
        scale: Scale factor for this space.
        rotation: Rotation in degrees (0, 90, 180, 270).
        flip_x: Whether the x-axis is flipped.
        flip_y: Whether the y-axis is flipped.
    """
    origin: Tuple[int, int] = (0, 0)
    scale: float = 1.0
    rotation: int = 0
    flip_x: bool = False
    flip_y: bool = False

    def transform_point(self, x: int, y: int) -> Tuple[int, int]:
        """Transform a point through this coordinate space."""
        # Apply scale
        sx = int(x * self.scale)
        sy = int(y * self.scale)
        # Apply flip
        if self.flip_x:
            sx = -sx
        if self.flip_y:
            sy = -sy
        # Apply origin offset
        return (sx + self.origin[0], sy + self.origin[1])


@dataclass
class BoundingBox:
    """
    A bounding box in UI coordinates.

    Attributes:
        x: X coordinate of top-left corner.
        y: Y coordinate of top-left corner.
        width: Width of the box.
        height: Height of the box.
    """
    x: int
    y: int
    width: int
    height: int

    @property
    def x1(self) -> int:
        """Left edge."""
        return self.x

    @property
    def y1(self) -> int:
        """Top edge."""
        return self.y

    @property
    def x2(self) -> int:
        """Right edge."""
        return self.x + self.width

    @property
    def y2(self) -> int:
        """Bottom edge."""
        return self.y + self.height

    @property
    def center(self) -> Tuple[int, int]:
        """Center point."""
        return (self.x + self.width // 2, self.y + self.height // 2)

    @property
    def center_x(self) -> int:
        """Center x coordinate."""
        return self.x + self.width // 2

    @property
    def center_y(self) -> int:
        """Center y coordinate."""
        return self.y + self.height // 2

    def contains_point(self, x: int, y: int) -> bool:
        """Check if a point is inside the bounding box."""
        return self.x <= x < self.x + self.width and self.y <= y < self.y + self.height

    def overlaps(self, other: BoundingBox) -> bool:
        """Check if this box overlaps another."""
        return not (
            self.x2 <= other.x
            or other.x2 <= self.x
            or self.y2 <= other.y
            or other.y2 <= self.y
        )

    def union(self, other: BoundingBox) -> BoundingBox:
        """Get the smallest box containing both boxes."""
        x = min(self.x, other.x)
        y = min(self.y, other.y)
        x2 = max(self.x2, other.x2)
        y2 = max(self.y2, other.y2)
        return BoundingBox(x=x, y=y, width=x2 - x, height=y2 - y)

    def intersection(self, other: BoundingBox) -> Optional[BoundingBox]:
        """Get the intersection of two boxes, if any."""
        x = max(self.x, other.x)
        y = max(self.y, other.y)
        x2 = min(self.x2, other.x2)
        y2 = min(self.y2, other.y2)
        if x < x2 and y < y2:
            return BoundingBox(x=x, y=y, width=x2 - x, height=y2 - y)
        return None

    def expand(self, dx: int, dy: int) -> BoundingBox:
        """Expand the box by dx/dy on each side."""
        return BoundingBox(
            x=self.x - dx,
            y=self.y - dy,
            width=self.width + 2 * dx,
            height=self.height + 2 * dy
        )

    def contract(self, dx: int, dy: int) -> BoundingBox:
        """Contract the box by dx/dy on each side."""
        return self.expand(-dx, -dy)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            'x': self.x,
            'y': self.y,
            'width': self.width,
            'height': self.height,
        }


def screen_to_window_coords(
    screen_x: int, screen_y: int,
    window_x: int, window_y: int
) -> Tuple[int, int]:
    """
    Convert screen coordinates to window-relative coordinates.

    Args:
        screen_x: X coordinate in screen space.
        screen_y: Y coordinate in screen space.
        window_x: Window origin x in screen space.
        window_y: Window origin y in screen space.

    Returns:
        Tuple of (x, y) in window coordinates.
    """
    return (screen_x - window_x, screen_y - window_y)


def window_to_screen_coords(
    window_x: int, window_y: int,
    screen_origin_x: int, screen_origin_y: int
) -> Tuple[int, int]:
    """
    Convert window coordinates to screen coordinates.

    Args:
        window_x: X coordinate in window space.
        window_y: Y coordinate in window space.
        screen_origin_x: Window origin x in screen space.
        screen_origin_y: Window origin y in screen space.

    Returns:
        Tuple of (x, y) in screen coordinates.
    """
    return (window_x + screen_origin_x, window_y + screen_origin_y)


def element_to_screen_coords(
    element_x: int, element_y: int,
    element_bounds: BoundingBox,
    window_x: int, window_y: int
) -> Tuple[int, int]:
    """
    Convert element-relative coordinates to screen coordinates.

    Args:
        element_x: X coordinate relative to element origin.
        element_y: Y coordinate relative to element origin.
        element_bounds: Bounds of the element in window coordinates.
        window_x: Window origin x in screen space.
        window_y: Window origin y in screen space.

    Returns:
        Tuple of (x, y) in screen coordinates.
    """
    elem_screen_x = element_bounds.x + element_x
    elem_screen_y = element_bounds.y + element_y
    return window_to_screen_coords(elem_screen_x, elem_screen_y, window_x, window_y)


def normalize_point_in_bounds(
    x: int, y: int, bounds: BoundingBox
) -> Tuple[float, float]:
    """
    Normalize a point to 0.0-1.0 range within bounds.

    Args:
        x: X coordinate.
        y: Y coordinate.
        bounds: Bounding box.

    Returns:
        Tuple of (normalized_x, normalized_y) in 0.0-1.0 range.
    """
    nx = (x - bounds.x) / max(bounds.width, 1)
    ny = (y - bounds.y) / max(bounds.height, 1)
    return (max(0.0, min(1.0, nx)), max(0.0, min(1.0, ny)))


def denormalize_point_to_bounds(
    nx: float, ny: float, bounds: BoundingBox
) -> Tuple[int, int]:
    """
    Convert normalized (0.0-1.0) coordinates to pixel coordinates.

    Args:
        nx: Normalized x (0.0-1.0).
        ny: Normalized y (0.0-1.0).
        bounds: Target bounding box.

    Returns:
        Tuple of (x, y) in pixel coordinates.
    """
    x = int(bounds.x + nx * bounds.width)
    y = int(bounds.y + ny * bounds.height)
    return (x, y)


def snap_to_pixel_grid(x: float, y: float) -> Tuple[int, int]:
    """
    Snap floating point coordinates to nearest pixel.

    Args:
        x: X coordinate (can be float).
        y: Y coordinate (can be float).

    Returns:
        Tuple of snapped (x, y) as integers.
    """
    return (round(x), round(y))


def rotate_point_around_origin(
    x: int, y: int, origin_x: int, origin_y: int, degrees: int
) -> Tuple[int, int]:
    """
    Rotate a point around an origin.

    Args:
        x: Point x coordinate.
        y: Point y coordinate.
        origin_x: Origin x coordinate.
        origin_y: Origin y coordinate.
        degrees: Rotation in degrees (positive = counter-clockwise).

    Returns:
        Rotated point as (x, y).
    """
    import math
    # Translate to origin
    tx = x - origin_x
    ty = y - origin_y
    # Rotate
    rad = math.radians(degrees)
    cos_a = math.cos(rad)
    sin_a = math.sin(rad)
    rx = int(tx * cos_a - ty * sin_a)
    ry = int(tx * sin_a + ty * cos_a)
    # Translate back
    return (rx + origin_x, ry + origin_y)


def distance_between_points(
    x1: int, y1: int, x2: int, y2: int
) -> float:
    """Calculate Euclidean distance between two points."""
    import math
    dx = x2 - x1
    dy = y2 - y1
    return math.sqrt(dx * dx + dy * dy)


def manhattan_distance(
    x1: int, y1: int, x2: int, y2: int
) -> int:
    """Calculate Manhattan distance between two points."""
    return abs(x2 - x1) + abs(y2 - y1)


def midpoint(
    x1: int, y1: int, x2: int, y2: int
) -> Tuple[int, int]:
    """Calculate the midpoint between two points."""
    return ((x1 + x2) // 2, (y1 + y2) // 2)


def points_on_line(
    x1: int, y1: int, x2: int, y2: int, num_points: int
) -> List[Tuple[int, int]]:
    """
    Generate evenly spaced points along a line.

    Args:
        x1: Start x.
        y1: Start y.
        x2: End x.
        y2: End y.
        num_points: Number of points to generate.

    Returns:
        List of (x, y) tuples along the line.
    """
    if num_points < 2:
        return [(x1, y1)]
    points = []
    for i in range(num_points):
        t = i / (num_points - 1)
        x = int(x1 + t * (x2 - x1))
        y = int(y1 + t * (y2 - y1))
        points.append((x, y))
    return points


def closest_point_on_line_segment(
    px: int, py: int,
    x1: int, y1: int, x2: int, y2: int
) -> Tuple[int, int]:
    """
    Find the closest point on a line segment to a given point.

    Args:
        px: Point x.
        py: Point y.
        x1: Line segment start x.
        y1: Line segment start y.
        x2: Line segment end x.
        y2: Line segment end y.

    Returns:
        Closest point on the segment as (x, y).
    """
    import math
    dx = x2 - x1
    dy = y2 - y1
    if dx == 0 and dy == 0:
        return (x1, y1)
    t = max(0.0, min(1.0, ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy)))
    return (int(x1 + t * dx), int(y1 + t * dy))

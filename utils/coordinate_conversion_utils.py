"""Coordinate conversion utilities for multi-display coordinate handling.

This module provides utilities for converting coordinates between different
display spaces, normalizing coordinates, and handling coordinate transforms
in multi-monitor setups for UI automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, List


@dataclass
class Coordinate2D:
    """A 2D coordinate point."""
    x: int
    y: int
    
    def to_tuple(self) -> Tuple[int, int]:
        return (self.x, self.y)
    
    def offset(self, dx: int, dy: int) -> "Coordinate2D":
        """Return new coordinate offset by dx, dy."""
        return Coordinate2D(self.x + dx, self.y + dy)
    
    def distance_to(self, other: "Coordinate2D") -> float:
        """Calculate Euclidean distance to another coordinate."""
        dx = self.x - other.x
        dy = self.y - other.y
        return (dx * dx + dy * dy) ** 0.5


@dataclass
class CoordinateTransform:
    """Transformation between coordinate spaces."""
    source_display_id: int
    target_display_id: int
    offset_x: int
    offset_y: int
    scale_x: float = 1.0
    scale_y: float = 1.0


def screen_to_display_coords(
    x: int,
    y: int,
    display_offset_x: int,
    display_offset_y: int,
) -> Tuple[int, int]:
    """Convert screen coordinates to display-local coordinates.
    
    Args:
        x: Screen X coordinate.
        y: Screen Y coordinate.
        display_offset_x: Display X offset from screen origin.
        display_offset_y: Display Y offset from screen origin.
    
    Returns:
        Tuple of (display_x, display_y).
    """
    return (x - display_offset_x, y - display_offset_y)


def display_to_screen_coords(
    x: int,
    y: int,
    display_offset_x: int,
    display_offset_y: int,
) -> Tuple[int, int]:
    """Convert display-local coordinates to screen coordinates.
    
    Args:
        x: Display X coordinate.
        y: Display Y coordinate.
        display_offset_x: Display X offset from screen origin.
        display_offset_y: Display Y offset from screen origin.
    
    Returns:
        Tuple of (screen_x, screen_y).
    """
    return (x + display_offset_x, y + display_offset_y)


def normalize_coordinates(
    x: int,
    y: int,
    width: int,
    height: int,
) -> Tuple[float, float]:
    """Normalize coordinates to 0.0-1.0 range.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
        width: Reference width.
        height: Reference height.
    
    Returns:
        Tuple of (normalized_x, normalized_y).
    """
    norm_x = x / width if width > 0 else 0.0
    norm_y = y / height if height > 0 else 0.0
    return (norm_x, norm_y)


def denormalize_coordinates(
    norm_x: float,
    norm_y: float,
    width: int,
    height: int,
) -> Tuple[int, int]:
    """Convert normalized coordinates back to pixel coordinates.
    
    Args:
        norm_x: Normalized X (0.0-1.0).
        norm_y: Normalized Y (0.0-1.0).
        width: Target width.
        height: Target height.
    
    Returns:
        Tuple of (x, y) pixel coordinates.
    """
    x = int(norm_x * width)
    y = int(norm_y * height)
    return (x, y)


def transform_coordinates(
    x: int,
    y: int,
    transform: CoordinateTransform,
) -> Tuple[int, int]:
    """Apply coordinate transformation.
    
    Args:
        x: Source X coordinate.
        y: Source Y coordinate.
        transform: Coordinate transformation to apply.
    
    Returns:
        Tuple of transformed (x, y).
    """
    scaled_x = int(x * transform.scale_x)
    scaled_y = int(y * transform.scale_y)
    
    return (
        scaled_x + transform.offset_x,
        scaled_y + transform.offset_y,
    )


def clamp_to_bounds(
    x: int,
    y: int,
    min_x: int,
    min_y: int,
    max_x: int,
    max_y: int,
) -> Tuple[int, int]:
    """Clamp coordinates to be within bounds.
    
    Args:
        x: X coordinate.
        y: Y coordinate.
        min_x: Minimum X.
        min_y: Minimum Y.
        max_x: Maximum X.
        max_y: Maximum Y.
    
    Returns:
        Tuple of clamped (x, y).
    """
    clamped_x = max(min_x, min(x, max_x))
    clamped_y = max(min_y, min(y, max_y))
    return (clamped_x, clamped_y)


def get_center_point(
    x1: int,
    y1: int,
    x2: int,
    y2: int,
) -> Tuple[int, int]:
    """Get center point between two coordinates.
    
    Args:
        x1: First point X.
        y1: First point Y.
        x2: Second point X.
        y2: Second point Y.
    
    Returns:
        Tuple of center (x, y).
    """
    return ((x1 + x2) // 2, (y1 + y2) // 2)


def interpolate_coordinates(
    x1: int,
    y1: int,
    x2: int,
    y2: int,
    t: float,
) -> Tuple[int, int]:
    """Linearly interpolate between two coordinates.
    
    Args:
        x1: Start X.
        y1: Start Y.
        x2: End X.
        y2: End Y.
        t: Interpolation factor (0.0 = start, 1.0 = end).
    
    Returns:
        Tuple of interpolated (x, y).
    """
    x = int(x1 + (x2 - x1) * t)
    y = int(y1 + (y2 - y1) * t)
    return (x, y)


def is_within_distance(
    x1: int,
    y1: int,
    x2: int,
    y2: int,
    max_distance: float,
) -> bool:
    """Check if two points are within specified distance.
    
    Args:
        x1: First point X.
        y1: First point Y.
        x2: Second point X.
        y2: Second point Y.
        max_distance: Maximum allowed distance.
    
    Returns:
        True if within distance.
    """
    dx = x2 - x1
    dy = y2 - y1
    return (dx * dx + dy * dy) <= (max_distance * max_distance)


def get_bounding_box_center(
    x: int,
    y: int,
    width: int,
    height: int,
) -> Tuple[int, int]:
    """Get center point of a bounding box.
    
    Args:
        x: Left edge.
        y: Top edge.
        width: Width.
        height: Height.
    
    Returns:
        Tuple of center (x, y).
    """
    return (x + width // 2, y + height // 2)

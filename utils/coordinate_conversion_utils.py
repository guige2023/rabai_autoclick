"""Coordinate system conversion utilities.

This module provides utilities for converting between different
coordinate systems used in GUI automation.
"""

from __future__ import annotations

from typing import Tuple, Optional
from dataclasses import dataclass


@dataclass
class Point:
    """A 2D point with optional screen identifier."""
    x: float
    y: float
    screen: Optional[str] = None


@dataclass
class CoordinateSystem:
    """Describes a coordinate system."""
    origin: str  # "top_left", "bottom_left", "center"
    unit: str  # "pixel", "percent", "point"
    scale: float = 1.0


# Screen coordinates (origin: top-left, unit: pixel)
SCREEN_SYSTEM = CoordinateSystem("top_left", "pixel", 1.0)
# Fractional coordinates (origin: top-left, unit: percent)
FRACTIONAL_SYSTEM = CoordinateSystem("top_left", "percent", 100.0)


def screen_to_fractional(
    x: int,
    y: int,
    screen_width: int,
    screen_height: int,
) -> Tuple[float, float]:
    """Convert screen coordinates to fractional (0-1) coordinates.

    Args:
        x: X coordinate in screen pixels.
        y: Y coordinate in screen pixels.
        screen_width: Total screen width.
        screen_height: Total screen height.

    Returns:
        Tuple of (fractional_x, fractional_y) in range [0, 1].
    """
    fx = max(0.0, min(1.0, x / screen_width))
    fy = max(0.0, min(1.0, y / screen_height))
    return (fx, fy)


def fractional_to_screen(
    fx: float,
    fy: float,
    screen_width: int,
    screen_height: int,
) -> Tuple[int, int]:
    """Convert fractional coordinates to screen pixels.

    Args:
        fx: Fractional X in range [0, 1].
        fy: Fractional Y in range [0, 1].
        screen_width: Total screen width.
        screen_height: Total screen height.

    Returns:
        Tuple of (screen_x, screen_y) in pixels.
    """
    sx = int(max(0, min(screen_width - 1, fx * screen_width)))
    sy = int(max(0, min(screen_height - 1, fy * screen_height)))
    return (sx, sy)


def flip_y(y: int, height: int) -> int:
    """Flip Y coordinate (convert between top-origin and bottom-origin).

    Args:
        y: Y coordinate in original system.
        height: Total height of the coordinate space.

    Returns:
        Flipped Y coordinate.
    """
    return height - 1 - y


def rotate_point(
    x: int,
    y: int,
    cx: int,
    cy: int,
    angle_deg: float,
) -> Tuple[int, int]:
    """Rotate a point around a center.

    Args:
        x: Point X.
        y: Point Y.
        cx: Center X.
        cy: Center Y.
        angle_deg: Rotation angle in degrees.

    Returns:
        Rotated point (rx, ry).
    """
    import math
    rad = math.radians(angle_deg)
    cos_a = math.cos(rad)
    sin_a = math.sin(rad)
    dx = x - cx
    dy = y - cy
    rx = int(dx * cos_a - dy * sin_a + cx)
    ry = int(dx * sin_a + dy * cos_a + cy)
    return (rx, ry)


def scale_point(
    x: int,
    y: int,
    scale_x: float,
    scale_y: float,
) -> Tuple[int, int]:
    """Scale a point by factors.

    Args:
        x: Point X.
        y: Point Y.
        scale_x: Scale factor for X.
        scale_y: Scale factor for Y.

    Returns:
        Scaled point (sx, sy).
    """
    return (int(x * scale_x), int(y * scale_y))


def translate_point(
    x: int,
    y: int,
    dx: int,
    dy: int,
) -> Tuple[int, int]:
    """Translate a point by an offset.

    Args:
        x: Point X.
        y: Point Y.
        dx: Delta X.
        dy: Delta Y.

    Returns:
        Translated point (tx, ty).
    """
    return (x + dx, y + dy)


def convert_between_screens(
    x: int,
    y: int,
    src_width: int,
    src_height: int,
    dst_width: int,
    dst_height: int,
) -> Tuple[int, int]:
    """Convert coordinates between screens of different resolutions.

    Args:
        x: Source X coordinate.
        y: Source Y coordinate.
        src_width: Source screen width.
        src_height: Source screen height.
        dst_width: Destination screen width.
        dst_height: Destination screen height.

    Returns:
        Converted coordinates (dx, dy).
    """
    fx, fy = screen_to_fractional(x, y, src_width, src_height)
    return fractional_to_screen(fx, fy, dst_width, dst_height)


def clamp_to_region(
    x: int,
    y: int,
    region_x: int,
    region_y: int,
    region_width: int,
    region_height: int,
) -> Tuple[int, int]:
    """Clamp coordinates to be within a region.

    Args:
        x: Input X.
        y: Input Y.
        region_x: Region left edge.
        region_y: Region top edge.
        region_width: Region width.
        region_height: Region height.

    Returns:
        Clamped (x, y).
    """
    cx = max(region_x, min(x, region_x + region_width - 1))
    cy = max(region_y, min(y, region_y + region_height - 1))
    return (cx, cy)


__all__ = [
    "Point",
    "CoordinateSystem",
    "SCREEN_SYSTEM",
    "FRACTIONAL_SYSTEM",
    "screen_to_fractional",
    "fractional_to_screen",
    "flip_y",
    "rotate_point",
    "scale_point",
    "translate_point",
    "convert_between_screens",
    "clamp_to_region",
]

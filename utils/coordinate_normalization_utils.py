"""Coordinate normalization utilities for cross-resolution UI automation.

This module provides utilities for normalizing coordinates between different
screen resolutions, DPI settings, and display configurations, ensuring
UI automation works consistently across varying environments.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, List
from enum import Enum, auto


class CoordinateSystem(Enum):
    """Type of coordinate system."""
    SCREEN_ABSOLUTE = auto()    # Absolute screen coordinates
    NORMALIZED = auto()         # 0.0 to 1.0 normalized
    PERCENTAGE = auto()          # 0 to 100 percent
    RELATIVE = auto()            # Relative to a reference point


@dataclass
class Resolution:
    """Screen resolution configuration."""
    width: int
    height: int
    dpi: int = 96
    scale_factor: float = 1.0
    
    @property
    def aspect_ratio(self) -> float:
        """Get aspect ratio (width/height)."""
        return self.width / self.height if self.height > 0 else 0
    
    @property
    def megapixels(self) -> float:
        """Get resolution in megapixels."""
        return (self.width * self.height) / 1_000_000


@dataclass
class NormalizedPoint:
    """A point in normalized coordinates."""
    x: float  # 0.0 to 1.0
    y: float  # 0.0 to 1.0
    
    def to_absolute(self, resolution: Resolution) -> Tuple[int, int]:
        """Convert to absolute pixel coordinates."""
        return (
            int(self.x * resolution.width),
            int(self.y * resolution.height),
        )
    
    def to_percentage(self) -> Tuple[float, float]:
        """Convert to percentage coordinates."""
        return (self.x * 100, self.y * 100)
    
    def scale(self, factor: float) -> NormalizedPoint:
        """Scale around center point (0.5, 0.5)."""
        return NormalizedPoint(
            0.5 + (self.x - 0.5) * factor,
            0.5 + (self.y - 0.5) * factor,
        )


@dataclass
class NormalizedRegion:
    """A region in normalized coordinates."""
    x: float  # 0.0 to 1.0
    y: float  # 0.0 to 1.0
    width: float   # 0.0 to 1.0
    height: float  # 0.0 to 1.0
    
    @property
    def center(self) -> NormalizedPoint:
        """Get center point."""
        return NormalizedPoint(
            self.x + self.width / 2,
            self.y + self.height / 2,
        )
    
    @property
    def right(self) -> float:
        """Get right edge."""
        return self.x + self.width
    
    @property
    def bottom(self) -> float:
        """Get bottom edge."""
        return self.y + self.height
    
    def contains(self, point: NormalizedPoint) -> bool:
        """Check if point is within region."""
        return (self.x <= point.x <= self.right and 
                self.y <= point.y <= self.bottom)
    
    def to_absolute(
        self,
        resolution: Resolution,
    ) -> Tuple[int, int, int, int]:
        """Convert to absolute pixel region."""
        x, y = int(self.x * resolution.width), int(self.y * resolution.height)
        w, h = int(self.width * resolution.width), int(self.height * resolution.height)
        return (x, y, w, h)
    
    def scale(self, factor: float) -> NormalizedRegion:
        """Scale region around its center."""
        cx = self.x + self.width / 2
        cy = self.y + self.height / 2
        new_width = self.width * factor
        new_height = self.height * factor
        return NormalizedRegion(
            cx - new_width / 2,
            cy - new_height / 2,
            new_width,
            new_height,
        )


def normalize_point(
    x: int,
    y: int,
    resolution: Resolution,
) -> NormalizedPoint:
    """Normalize a point to 0.0-1.0 range.
    
    Args:
        x: Absolute x coordinate.
        y: Absolute y coordinate.
        resolution: Reference resolution.
    
    Returns:
        NormalizedPoint instance.
    """
    return NormalizedPoint(
        x / resolution.width,
        y / resolution.height,
    )


def denormalize_point(
    point: NormalizedPoint,
    resolution: Resolution,
) -> Tuple[int, int]:
    """Convert normalized point to absolute coordinates.
    
    Args:
        point: NormalizedPoint to convert.
        resolution: Target resolution.
    
    Returns:
        Tuple of (x, y) absolute coordinates.
    """
    return point.to_absolute(resolution)


def normalize_region(
    x: int,
    y: int,
    width: int,
    height: int,
    resolution: Resolution,
) -> NormalizedRegion:
    """Normalize a region to 0.0-1.0 range.
    
    Args:
        x: Absolute x coordinate.
        y: Absolute y coordinate.
        width: Absolute width.
        height: Absolute height.
        resolution: Reference resolution.
    
    Returns:
        NormalizedRegion instance.
    """
    return NormalizedRegion(
        x / resolution.width,
        y / resolution.height,
        width / resolution.width,
        height / resolution.height,
    )


def denormalize_region(
    region: NormalizedRegion,
    resolution: Resolution,
) -> Tuple[int, int, int, int]:
    """Convert normalized region to absolute coordinates.
    
    Args:
        region: NormalizedRegion to convert.
        resolution: Target resolution.
    
    Returns:
        Tuple of (x, y, width, height) absolute values.
    """
    return region.to_absolute(resolution)


def scale_coordinates(
    x: int,
    y: int,
    from_resolution: Resolution,
    to_resolution: Resolution,
) -> Tuple[int, int]:
    """Scale coordinates from one resolution to another.
    
    Args:
        x: Source x coordinate.
        y: Source y coordinate.
        from_resolution: Source resolution.
        to_resolution: Target resolution.
    
    Returns:
        Tuple of scaled (x, y) coordinates.
    """
    scale_x = to_resolution.width / from_resolution.width
    scale_y = to_resolution.height / from_resolution.height
    
    return (
        int(x * scale_x),
        int(y * scale_y),
    )


def scale_region(
    x: int,
    y: int,
    width: int,
    height: int,
    from_resolution: Resolution,
    to_resolution: Resolution,
) -> Tuple[int, int, int, int]:
    """Scale a region from one resolution to another.
    
    Args:
        x: Source x coordinate.
        y: Source y coordinate.
        width: Source width.
        height: Source height.
        from_resolution: Source resolution.
        to_resolution: Target resolution.
    
    Returns:
        Tuple of scaled (x, y, width, height).
    """
    sx, sy = scale_coordinates(x, y, from_resolution, to_resolution)
    sw, sh = scale_coordinates(width, height, from_resolution, to_resolution)
    return (sx, sy, sw, sh)


def apply_dpi_scaling(
    x: int,
    y: int,
    from_dpi: int,
    to_dpi: int,
) -> Tuple[int, int]:
    """Apply DPI scaling adjustment to coordinates.
    
    Args:
        x: Source x coordinate.
        y: Source y coordinate.
        from_dpi: Source DPI.
        to_dpi: Target DPI.
    
    Returns:
        Tuple of scaled (x, y) coordinates.
    """
    if from_dpi == 0 or from_dpi == to_dpi:
        return (x, y)
    
    scale = to_dpi / from_dpi
    return (
        int(x * scale),
        int(y * scale),
    )


def clamp_coordinates(
    x: int,
    y: int,
    resolution: Resolution,
) -> Tuple[int, int]:
    """Clamp coordinates to be within screen bounds.
    
    Args:
        x: X coordinate to clamp.
        y: Y coordinate to clamp.
        resolution: Screen resolution.
    
    Returns:
        Tuple of clamped (x, y) coordinates.
    """
    return (
        max(0, min(x, resolution.width - 1)),
        max(0, min(y, resolution.height - 1)),
    )


def calculate_distance(
    x1: float,
    y1: float,
    x2: float,
    y2: float,
) -> float:
    """Calculate Euclidean distance between two points.
    
    Args:
        x1: First point x.
        y1: First point y.
        x2: Second point x.
        y2: Second point y.
    
    Returns:
        Euclidean distance.
    """
    dx = x2 - x1
    dy = y2 - y1
    return (dx * dx + dy * dy) ** 0.5


def interpolate_point(
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    t: float,
) -> Tuple[float, float]:
    """Linearly interpolate between two points.
    
    Args:
        p1: First point (x, y).
        p2: Second point (x, y).
        t: Interpolation factor (0.0 to 1.0).
    
    Returns:
        Interpolated point (x, y).
    """
    return (
        p1[0] + (p2[0] - p1[0]) * t,
        p1[1] + (p2[1] - p1[1]) * t,
    )

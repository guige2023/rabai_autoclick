"""
Coordinate validation utilities for UI automation.

Validates screen coordinates and regions to ensure
actions target valid, visible areas.

Author: AutoClick Team
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import NamedTuple


class Point(NamedTuple):
    """A 2D point."""

    x: float
    y: float


class Size(NamedTuple):
    """A 2D size."""

    width: float
    height: float


class Rect(NamedTuple):
    """A rectangle defined by origin and size."""

    x: float
    y: float
    width: float
    height: float


@dataclass
class ValidationResult:
    """Result of coordinate validation."""

    valid: bool
    message: str
    normalized_point: Point | None = None


class CoordinateValidator:
    """
    Validates coordinates and regions for UI automation.

    Ensures coordinates are within screen bounds, properly aligned,
    and meet precision requirements.

    Example:
        validator = CoordinateValidator(screen_bounds=Rect(0, 0, 1920, 1080))
        result = validator.validate_point(Point(100, 200))
        if result.valid:
            execute_click(result.normalized_point)
    """

    def __init__(
        self,
        screen_bounds: Rect,
        safe_margin: float = 0.0,
        snap_to_pixel: bool = True,
    ) -> None:
        """
        Initialize validator with screen configuration.

        Args:
            screen_bounds: Total screen dimensions
            safe_margin: Margin from screen edges to consider valid
            snap_to_pixel: Round coordinates to nearest pixel
        """
        self._bounds = screen_bounds
        self._safe_margin = safe_margin
        self._snap_to_pixel = snap_to_pixel

    def validate_point(self, point: Point) -> ValidationResult:
        """
        Validate a single point.

        Args:
            point: Point to validate

        Returns:
            ValidationResult with normalized coordinates
        """
        x, y = point.x, point.y

        min_x = self._bounds.x + self._safe_margin
        min_y = self._bounds.y + self._safe_margin
        max_x = self._bounds.x + self._bounds.width - self._safe_margin
        max_y = self._bounds.y + self._bounds.height - self._safe_margin

        if not (min_x <= x <= max_x):
            return ValidationResult(
                valid=False,
                message=f"X coordinate {x} out of bounds [{min_x}, {max_x}]",
            )

        if not (min_y <= y <= max_y):
            return ValidationResult(
                valid=False,
                message=f"Y coordinate {y} out of bounds [{min_y}, {max_y}]",
            )

        if self._snap_to_pixel:
            x = round(x)
            y = round(y)

        return ValidationResult(
            valid=True,
            message="Point is valid",
            normalized_point=Point(x, y),
        )

    def validate_region(self, region: Rect) -> ValidationResult:
        """
        Validate a rectangular region.

        Args:
            region: Region to validate

        Returns:
            ValidationResult with validation status
        """
        if region.width <= 0 or region.height <= 0:
            return ValidationResult(
                valid=False,
                message=f"Region has invalid dimensions: {region.width}x{region.height}",
            )

        top_left = self.validate_point(Point(region.x, region.y))
        if not top_left.valid:
            return top_left

        bottom_right = self.validate_point(
            Point(region.x + region.width, region.y + region.height)
        )
        if not bottom_right.valid:
            return bottom_right

        return ValidationResult(
            valid=True,
            message="Region is valid",
        )

    def clamp_point(self, point: Point) -> Point:
        """
        Clamp point to valid screen bounds.

        Args:
            point: Point to clamp

        Returns:
            Clamped point within bounds
        """
        x = max(self._bounds.x, min(point.x, self._bounds.x + self._bounds.width))
        y = max(self._bounds.y, min(point.y, self._bounds.y + self._bounds.height))
        return Point(round(x), round(y)) if self._snap_to_pixel else Point(x, y)

    def distance(self, p1: Point, p2: Point) -> float:
        """Calculate Euclidean distance between two points."""
        return math.sqrt((p2.x - p1.x) ** 2 + (p2.y - p1.y) ** 2)

    def midpoint(self, p1: Point, p2: Point) -> Point:
        """Calculate midpoint between two points."""
        return Point((p1.x + p2.x) / 2, (p1.y + p2.y) / 2)

    def normalize_coordinates(
        self, point: Point, from_region: Rect
    ) -> Point:
        """
        Normalize point from one coordinate space to another.

        Args:
            point: Point in source coordinates
            from_region: Source coordinate space

        Returns:
            Point normalized to screen coordinates
        """
        scale_x = self._bounds.width / from_region.width
        scale_y = self._bounds.height / from_region.height

        x = self._bounds.x + point.x * scale_x
        y = self._bounds.y + point.y * scale_y

        return Point(round(x), round(y)) if self._snap_to_pixel else Point(x, y)


def is_point_in_rect(point: Point, rect: Rect) -> bool:
    """Check if point is inside rectangle."""
    return (
        rect.x <= point.x <= rect.x + rect.width
        and rect.y <= point.y <= rect.y + rect.height
    )


def rects_overlap(a: Rect, b: Rect) -> bool:
    """Check if two rectangles overlap."""
    return not (
        a.x + a.width < b.x
        or b.x + b.width < a.x
        or a.y + a.height < b.y
        or b.y + b.height < a.y
    )


def normalize_rect(x: float, y: float, width: float, height: float) -> Rect:
    """Normalize rect coordinates (handle negative dimensions)."""
    if width < 0:
        x += width
        width = abs(width)
    if height < 0:
        y += height
        height = abs(height)
    return Rect(x, y, width, height)

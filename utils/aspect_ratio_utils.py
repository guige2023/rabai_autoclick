"""Aspect ratio calculation and transformation utilities.

This module provides utilities for computing, normalizing, and transforming
aspect ratios for UI elements and screen regions.
"""

from __future__ import annotations

import math
from typing import NamedTuple
from fractions import Fraction


class AspectRatio(NamedTuple):
    """Aspect ratio represented as width:height.

    Attributes:
        width: Width component.
        height: Height component.
    """
    width: float
    height: float

    @property
    def ratio(self) -> float:
        """Decimal ratio (width / height)."""
        if self.height == 0:
            return float('inf')
        return self.width / self.height

    @property
    def reciprocal(self) -> float:
        """Reciprocal ratio (height / width)."""
        if self.width == 0:
            return float('inf')
        return self.height / self.width

    @property
    def simplified(self) -> AspectRatio:
        """Simplified integer ratio using GCD."""
        if self.width == 0 or self.height == 0:
            return AspectRatio(0, 0)

        try:
            width_val = float(self.width)
            height_val = float(self.height)

            if width_val == int(width_val) and height_val == int(height_val):
                gcd_val = math.gcd(int(width_val), int(height_val))
                return AspectRatio(
                    int(width_val) // gcd_val,
                    int(height_val) // gcd_val
                )
        except (ValueError, TypeError):
            pass

        return AspectRatio(self.width, self.height)

    def __str__(self) -> str:
        """String representation as W:H."""
        return f"{self.width}:{self.height}"

    def matches(self, other: AspectRatio, tolerance: float = 0.01) -> bool:
        """Check if this aspect ratio matches another within tolerance.

        Args:
            other: Another aspect ratio to compare.
            tolerance: Maximum allowed difference.

        Returns:
            True if ratios match within tolerance.
        """
        return abs(self.ratio - other.ratio) <= tolerance

    def constrain(self, max_width: float, max_height: float) -> tuple[float, float]:
        """Constrain aspect ratio to fit within bounds.

        Scales down if necessary to fit within max_width x max_height.

        Args:
            max_width: Maximum allowed width.
            max_height: Maximum allowed height.

        Returns:
            Tuple of (constrained_width, constrained_height).
        """
        if self.height == 0:
            return (max_width, max_height)

        ideal_width = self.ratio * max_height

        if ideal_width <= max_width:
            return (ideal_width, max_height)
        else:
            ideal_height = max_width / self.ratio
            return (max_width, ideal_height)


# Common aspect ratios
ASPECT_RATIOS = {
    "16:9": AspectRatio(16, 9),
    "16:10": AspectRatio(16, 10),
    "4:3": AspectRatio(4, 3),
    "3:2": AspectRatio(3, 2),
    "21:9": AspectRatio(21, 9),
    "1:1": AspectRatio(1, 1),
    "9:16": AspectRatio(9, 16),
    "2:3": AspectRatio(2, 3),
    "iPhone": AspectRatio(9, 19.5),
    "iPad": AspectRatio(4, 3),
    "MacBook": AspectRatio(16, 10),
    "Ultrawide": AspectRatio(21, 9),
}


def from_dimensions(width: float, height: float) -> AspectRatio:
    """Create aspect ratio from width and height.

    Args:
        width: Width dimension.
        height: Height dimension.

    Returns:
        AspectRatio instance.
    """
    return AspectRatio(width, height)


def from_ratio(ratio: float) -> AspectRatio:
    """Create aspect ratio from decimal ratio.

    Args:
        ratio: Decimal ratio (width / height).

    Returns:
        AspectRatio instance with ratio preserved.
    """
    return AspectRatio(ratio, 1.0)


def from_fraction(width: int, height: int) -> AspectRatio:
    """Create aspect ratio from fraction components.

    Args:
        width: Width numerator.
        height: Height numerator.

    Returns:
        AspectRatio instance.
    """
    frac = Fraction(width, height)
    return AspectRatio(frac.numerator, frac.denominator)


def calculate_ratio(width: float, height: float) -> float:
    """Calculate decimal aspect ratio from dimensions.

    Args:
        width: Width in pixels.
        height: Height in pixels.

    Returns:
        Decimal ratio (width / height).
    """
    if height == 0:
        return float('inf')
    return width / height


def fit_within_bounds(
    source_width: float,
    source_height: float,
    max_width: float,
    max_height: float,
    preserve_ratio: bool = True
) -> tuple[float, float]:
    """Calculate dimensions that fit within bounds.

    Args:
        source_width: Original width.
        source_height: Original height.
        max_width: Maximum width allowed.
        max_height: Maximum height allowed.
        preserve_ratio: Whether to preserve aspect ratio.

    Returns:
        Tuple of (fitted_width, fitted_height).
    """
    if not preserve_ratio:
        return (max_width, max_height)

    if source_height == 0:
        return (max_width, max_height)

    ratio = source_width / source_height

    ideal_width = ratio * max_height
    ideal_height = max_width / ratio

    if ideal_width <= max_width:
        return (ideal_width, max_height)
    else:
        return (max_width, ideal_height)


def scale_dimensions(
    width: float,
    height: float,
    scale: float
) -> tuple[float, float]:
    """Scale dimensions by a factor.

    Args:
        width: Original width.
        height: Original height.
        scale: Scaling factor.

    Returns:
        Tuple of (scaled_width, scaled_height).
    """
    return (width * scale, height * scale)


def scale_to_fit(
    source_width: float,
    source_height: float,
    target_width: float,
    target_height: float
) -> float:
    """Calculate scale factor to fit source within target.

    Args:
        source_width: Source width.
        source_height: Source height.
        target_width: Target width to fit within.
        target_height: Target height to fit within.

    Returns:
        Scale factor to apply to source.
    """
    if source_height == 0 or target_height == 0:
        return 1.0

    scale_w = target_width / source_width
    scale_h = target_height / source_height

    return min(scale_w, scale_h)


def scale_to_fill(
    source_width: float,
    source_height: float,
    target_width: float,
    target_height: float
) -> float:
    """Calculate scale factor to fill target (crop if needed).

    Args:
        source_width: Source width.
        source_height: Source height.
        target_width: Target width to fill.
        target_height: Target height to fill.

    Returns:
        Scale factor to apply to source.
    """
    if source_height == 0 or target_height == 0:
        return 1.0

    scale_w = target_width / source_width
    scale_h = target_height / source_height

    return max(scale_w, scale_h)


def find_closest_standard(width: float, height: float) -> tuple[str, AspectRatio]:
    """Find the closest standard aspect ratio.

    Args:
        width: Width dimension.
        height: Height dimension.

    Returns:
        Tuple of (name, AspectRatio) of closest standard.
    """
    if height == 0:
        return ("Custom", AspectRatio(width, height))

    source_ratio = width / height
    closest_name = "Custom"
    closest_diff = float('inf')
    closest_ratio = AspectRatio(width, height)

    for name, ar in ASPECT_RATIOS.items():
        diff = abs(ar.ratio - source_ratio)
        if diff < closest_diff:
            closest_diff = diff
            closest_name = name
            closest_ratio = ar

    return (closest_name, closest_ratio)


def calculate_crop_for_center(
    source_width: float,
    source_height: float,
    target_ratio: AspectRatio
) -> tuple[float, float, float, float]:
    """Calculate crop rectangle to center-crop to target ratio.

    Args:
        source_width: Source width.
        source_height: Source height.
        target_ratio: Target aspect ratio.

    Returns:
        Tuple of (crop_x, crop_y, crop_width, crop_height).
    """
    if target_ratio.height == 0:
        return (0, 0, source_width, source_height)

    source_ratio = source_width / source_height
    target_r = target_ratio.ratio

    if source_ratio > target_r:
        new_width = source_height * target_r
        crop_x = (source_width - new_width) / 2
        return (crop_x, 0, new_width, source_height)
    else:
        new_height = source_width / target_r
        crop_y = (source_height - new_height) / 2
        return (0, crop_y, source_width, new_height)


def parse_ratio_string(ratio_str: str) -> AspectRatio | None:
    """Parse aspect ratio from string like "16:9" or "1.778".

    Args:
        ratio_str: String to parse.

    Returns:
        AspectRatio or None if invalid format.
    """
    ratio_str = ratio_str.strip()

    if ":" in ratio_str:
        parts = ratio_str.split(":")
        if len(parts) == 2:
            try:
                return AspectRatio(float(parts[0]), float(parts[1]))
            except ValueError:
                return None
    else:
        try:
            ratio = float(ratio_str)
            return from_ratio(ratio)
        except ValueError:
            return None

    return None

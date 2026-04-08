"""
Transform origin utilities for CSS-style transform origin calculations.

Computes transform origins and applies transformations
for UI element scaling and rotation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class TransformOrigin:
    """Transform origin point."""
    x: float
    y: float
    origin_type: str = "percentage"  # "percentage", "pixel", "keyword"

    @staticmethod
    def from_keyword(keyword: str, element_width: float, element_height: float) -> TransformOrigin:
        """Create from CSS-style keyword (top-left, center, bottom-right, etc.)."""
        keywords = {
            "top-left": (0, 0),
            "top": (50, 0),
            "top-right": (100, 0),
            "left": (0, 50),
            "center": (50, 50),
            "right": (100, 50),
            "bottom-left": (0, 100),
            "bottom": (50, 100),
            "bottom-right": (100, 100),
        }
        pct = keywords.get(keyword.lower(), (50, 50))
        return TransformOrigin(
            x=pct[0] / 100 * element_width,
            y=pct[1] / 100 * element_height,
            origin_type="pixel",
        )


@dataclass
class TransformState:
    """Applied transform state."""
    translate_x: float = 0.0
    translate_y: float = 0.0
    scale_x: float = 1.0
    scale_y: float = 1.0
    rotation: float = 0.0  # radians
    origin_x: float = 0.0
    origin_y: float = 0.0


class TransformOriginCalculator:
    """Calculates transform origins and applies transformations."""

    def compute_transformed_bounds(
        self,
        x: float, y: float,
        width: float, height: float,
        transform: TransformState,
    ) -> tuple[float, float, float, float]:
        """Compute the axis-aligned bounding box after transformation.

        Returns:
            (new_x, new_y, new_width, new_height)
        """
        # Get four corners
        corners = [
            (x, y),
            (x + width, y),
            (x + width, y + height),
            (x, y + height),
        ]

        # Transform each corner
        transformed = []
        for cx, cy in corners:
            tx, ty = self._apply_transform(
                cx, cy, x, y, width, height, transform
            )
            transformed.append((tx, ty))

        # Compute bounding box of transformed corners
        xs = [p[0] for p in transformed]
        ys = [p[1] for p in transformed]

        new_x = min(xs)
        new_y = min(ys)
        new_width = max(xs) - new_x
        new_height = max(ys) - new_y

        return (new_x, new_y, new_width, new_height)

    def _apply_transform(
        self,
        px: float, py: float,
        elem_x: float, elem_y: float,
        elem_width: float, elem_height: float,
        transform: TransformState,
    ) -> tuple[float, float]:
        """Apply transform to a single point."""
        # Origin relative to element
        ox = elem_x + transform.origin_x
        oy = elem_y + transform.origin_y

        # Translate to origin
        px -= ox
        py -= oy

        # Scale
        px *= transform.scale_x
        py *= transform.scale_y

        # Rotate
        if transform.rotation != 0:
            cos_r = math.cos(transform.rotation)
            sin_r = math.sin(transform.rotation)
            rx = px * cos_r - py * sin_r
            ry = px * sin_r + py * cos_r
            px, py = rx, ry

        # Translate back and apply translation
        px += ox + transform.translate_x
        py += oy + transform.translate_y

        return (px, py)

    def compute_rotation_bounds_change(
        self,
        width: float,
        height: float,
        rotation: float,
    ) -> tuple[float, float]:
        """Compute how much rotation increases the bounding box.

        Returns:
            (width_increase, height_increase)
        """
        # Diagonal of the rectangle
        diagonal = math.hypot(width, height)
        # Angle of diagonal
        diag_angle = math.atan2(height, width)
        # Total angle span
        half_span = diag_angle + rotation / 2

        # Bounding box dimensions
        new_width = diagonal * max(abs(math.cos(diag_angle + rotation / 2)),
                                   abs(math.cos(diag_angle - rotation / 2)))
        new_height = diagonal * max(abs(math.sin(diag_angle + rotation / 2)),
                                    abs(math.sin(diag_angle - rotation / 2)))

        return (new_width - width, new_height - height)

    def scale_around_center(
        self,
        scale: float,
        x: float, y: float,
        width: float, height: float,
    ) -> tuple[float, float, float, float]:
        """Scale an element around its center point.

        Returns:
            (new_x, new_y, new_width, new_height)
        """
        cx = x + width / 2
        cy = y + height / 2

        new_width = width * scale
        new_height = height * scale

        new_x = cx - new_width / 2
        new_y = cy - new_height / 2

        return (new_x, new_y, new_width, new_height)


__all__ = ["TransformOrigin", "TransformState", "TransformOriginCalculator"]

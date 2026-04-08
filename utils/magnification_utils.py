"""
Magnification utilities for screen zoom and magnification.

Provides screen magnification calculations for accessibility
zoom, magnifier tools, and pinch-to-zoom support.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class MagnificationConfig:
    """Configuration for magnification."""
    scale: float = 1.0
    focal_point_x: float = 0.0
    focal_point_y: float = 0.0
    smooth_scrolling: bool = True
    magnification_type: str = "linear"  # "linear", "discrete", "pinch"


@dataclass
class MagnifiedRegion:
    """Region after magnification."""
    source_x: float
    source_y: float
    source_width: float
    source_height: float
    output_x: float
    output_y: float
    output_width: float
    output_height: float
    scale: float


class MagnificationEngine:
    """Computes magnification transformations."""

    def __init__(self, config: Optional[MagnificationConfig] = None):
        self.config = config or MagnificationConfig()

    def set_scale(self, scale: float) -> None:
        """Set magnification scale factor."""
        self.config.scale = max(0.1, min(10.0, scale))

    def set_focal_point(self, x: float, y: float) -> None:
        """Set the focal point (center of magnification)."""
        self.config.focal_point_x = x
        self.config.focal_point_y = y

    def screen_to_content(
        self,
        screen_x: float,
        screen_y: float,
        viewport_x: float,
        viewport_y: float,
        viewport_width: float,
        viewport_height: float,
    ) -> tuple[float, float]:
        """Convert screen coordinates to magnified content coordinates.

        Returns:
            (content_x, content_y)
        """
        scale = self.config.scale
        fx = self.config.focal_point_x
        fy = self.config.focal_point_y

        # Screen position relative to focal point
        rel_x = screen_x - fx
        rel_y = screen_y - fy

        # Scale in reverse (divide by scale)
        content_rel_x = rel_x / scale
        content_rel_y = rel_y / scale

        # Content coordinates
        content_x = content_rel_x + viewport_x + viewport_width / 2
        content_y = content_rel_y + viewport_y + viewport_height / 2

        return (content_x, content_y)

    def content_to_screen(
        self,
        content_x: float,
        content_y: float,
        viewport_x: float,
        viewport_y: float,
        viewport_width: float,
        viewport_height: float,
    ) -> tuple[float, float]:
        """Convert content coordinates to screen coordinates.

        Returns:
            (screen_x, screen_y)
        """
        scale = self.config.scale
        fx = self.config.focal_point_x
        fy = self.config.focal_point_y

        # Content position relative to viewport center
        rel_x = content_x - (viewport_x + viewport_width / 2)
        rel_y = content_y - (viewport_y + viewport_height / 2)

        # Scale
        screen_rel_x = rel_x * scale
        screen_rel_y = rel_y * scale

        # Screen coordinates relative to focal point
        screen_x = screen_rel_x + fx
        screen_y = screen_rel_y + fy

        return (screen_x, screen_y)

    def get_magnified_region(
        self,
        viewport_x: float,
        viewport_y: float,
        viewport_width: float,
        viewport_height: float,
        output_width: float,
        output_height: float,
    ) -> MagnifiedRegion:
        """Compute which content region maps to the output.

        Returns:
            MagnifiedRegion describing the mapping
        """
        scale = self.config.scale

        # Source region in content coordinates
        source_width = output_width / scale
        source_height = output_height / scale

        fx = self.config.focal_point_x
        fy = self.config.focal_point_y

        # Source top-left
        source_x = fx - source_width / 2
        source_y = fy - source_height / 2

        return MagnifiedRegion(
            source_x=source_x,
            source_y=source_y,
            source_width=source_width,
            source_height=source_height,
            output_x=0,
            output_y=0,
            output_width=output_width,
            output_height=output_height,
            scale=scale,
        )

    def zoom_in(self, delta: float = 0.25) -> float:
        """Zoom in by delta and return new scale."""
        self.config.scale = min(10.0, self.config.scale + delta)
        return self.config.scale

    def zoom_out(self, delta: float = 0.25) -> float:
        """Zoom out by delta and return new scale."""
        self.config.scale = max(0.1, self.config.scale - delta)
        return self.config.scale

    def zoom_to_fit(
        self,
        content_width: float,
        content_height: float,
        viewport_width: float,
        viewport_height: float,
    ) -> float:
        """Compute scale to fit content in viewport."""
        if content_width <= 0 or content_height <= 0:
            return 1.0
        scale_x = viewport_width / content_width
        scale_y = viewport_height / content_height
        scale = min(scale_x, scale_y)
        self.config.scale = max(0.1, min(10.0, scale))
        return self.config.scale


__all__ = ["MagnificationEngine", "MagnificationConfig", "MagnifiedRegion"]

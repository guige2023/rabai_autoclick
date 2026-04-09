"""Display Rotation Utilities.

Handles display rotation detection and coordinate transformation.

Example:
    >>> from display_rotation_utils import DisplayRotationHandler
    >>> handler = DisplayRotationHandler()
    >>> handler.set_rotation(90)
    >>> mapped = handler.map_point(100, 200)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto


class RotationAngle(Enum):
    """Display rotation angles."""
    ANGLE_0 = 0
    ANGLE_90 = 90
    ANGLE_180 = 180
    ANGLE_270 = 270


@dataclass
class DisplayGeometry:
    """Display geometry with rotation."""
    width: int
    height: int
    rotation: RotationAngle = RotationAngle.ANGLE_0

    @property
    def rotated(self) -> tuple[int, int]:
        """Get dimensions after rotation."""
        if self.rotation in (RotationAngle.ANGLE_90, RotationAngle.ANGLE_270):
            return (self.height, self.width)
        return (self.width, self.height)


class DisplayRotationHandler:
    """Handles display rotation coordinate mapping."""

    def __init__(self, display: DisplayGeometry | None = None):
        """Initialize handler.

        Args:
            display: Display geometry.
        """
        self.display = display or DisplayGeometry(1920, 1080)

    def set_rotation(self, degrees: int) -> None:
        """Set display rotation.

        Args:
            degrees: Rotation in degrees (0, 90, 180, 270).
        """
        self.display.rotation = {
            0: RotationAngle.ANGLE_0,
            90: RotationAngle.ANGLE_90,
            180: RotationAngle.ANGLE_180,
            270: RotationAngle.ANGLE_270,
        }.get(degrees % 360, RotationAngle.ANGLE_0)

    def map_point(self, x: int, y: int) -> tuple[int, int]:
        """Map point based on current rotation.

        Args:
            x: Input X coordinate.
            y: Input Y coordinate.

        Returns:
            Tuple of (mapped_x, mapped_y).
        """
        w, h = self.display.width, self.display.height

        if self.display.rotation == RotationAngle.ANGLE_0:
            return (x, y)
        elif self.display.rotation == RotationAngle.ANGLE_90:
            return (h - y, x)
        elif self.display.rotation == RotationAngle.ANGLE_180:
            return (w - x, h - y)
        elif self.display.rotation == RotationAngle.ANGLE_270:
            return (y, w - x)
        return (x, y)

    def unmap_point(self, x: int, y: int) -> tuple[int, int]:
        """Reverse map point (screen to logical).

        Args:
            x: Screen X coordinate.
            y: Screen Y coordinate.

        Returns:
            Tuple of (logical_x, logical_y).
        """
        w, h = self.display.width, self.display.height

        if self.display.rotation == RotationAngle.ANGLE_0:
            return (x, y)
        elif self.display.rotation == RotationAngle.ANGLE_90:
            return (y, w - x)
        elif self.display.rotation == RotationAngle.ANGLE_180:
            return (w - x, h - y)
        elif self.display.rotation == RotationAngle.ANGLE_270:
            return (h - y, x)
        return (x, y)

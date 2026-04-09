"""Display Calibration Utilities.

Manages display calibration data for accurate coordinate mapping.

Example:
    >>> from display_calibration_utils import DisplayCalibrator
    >>> calib = DisplayCalibrator(display_id=0)
    >>> calib.calibrate([(100, 100), (500, 500)])
    >>> mapped = calib.map_point(250, 250)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class CalibrationPoint:
    """A calibration reference point."""
    screen_x: float
    screen_y: float
    display_x: float
    display_y: float


@dataclass
class CalibrationMatrix:
    """2D transformation matrix for calibration."""
    scale_x: float = 1.0
    scale_y: float = 1.0
    offset_x: float = 0.0
    offset_y: float = 0.0
    rotation: float = 0.0

    def map(self, x: float, y: float) -> Tuple[float, float]:
        """Map screen coordinates to display coordinates.

        Args:
            x: Screen X coordinate.
            y: Screen Y coordinate.

        Returns:
            Tuple of (display_x, display_y).
        """
        xr = x * math.cos(self.rotation) - y * math.sin(self.rotation)
        yr = x * math.sin(self.rotation) + y * math.cos(self.rotation)
        return (
            xr * self.scale_x + self.offset_x,
            yr * self.scale_y + self.offset_y,
        )


class DisplayCalibrator:
    """Calibrates display coordinate mapping."""

    def __init__(self, display_id: int = 0):
        """Initialize calibrator for a display.

        Args:
            display_id: Display identifier.
        """
        self.display_id = display_id
        self._points: List[CalibrationPoint] = []
        self._matrix = CalibrationMatrix()
        self._calibrated = False

    def add_point(
        self,
        screen_x: float,
        screen_y: float,
        display_x: float,
        display_y: float,
    ) -> None:
        """Add a calibration point pair.

        Args:
            screen_x: Screen coordinate X.
            screen_y: Screen coordinate Y.
            display_x: Known display coordinate X.
            display_y: Known display coordinate Y.
        """
        self._points.append(
            CalibrationPoint(screen_x, screen_y, display_x, display_y)
        )

    def calibrate(self) -> bool:
        """Compute calibration matrix from points.

        Returns:
            True if calibration successful (at least 2 points).
        """
        if len(self._points) < 2:
            return False

        mean_sx = sum(p.screen_x for p in self._points) / len(self._points)
        mean_sy = sum(p.screen_y for p in self._points) / len(self._points)
        mean_dx = sum(p.display_x for p in self._points) / len(self._points)
        mean_dy = sum(p.display_y for p in self._points) / len(self._points)

        var_sx = sum((p.screen_x - mean_sx) ** 2 for p in self._points)
        var_sy = sum((p.screen_y - mean_sy) ** 2 for p in self._points)

        if var_sx > 0:
            self._matrix.scale_x = sum(
                (p.screen_x - mean_sx) * (p.display_x - mean_dx) for p in self._points
            ) / var_sx
        if var_sy > 0:
            self._matrix.scale_y = sum(
                (p.screen_y - mean_sy) * (p.display_y - mean_dy) for p in self._points
            ) / var_sy

        self._matrix.offset_x = mean_dx - mean_sx * self._matrix.scale_x
        self._matrix.offset_y = mean_dy - mean_sy * self._matrix.scale_y
        self._calibrated = True
        return True

    def map_point(self, x: float, y: float) -> Tuple[float, float]:
        """Map a point using the calibration matrix.

        Args:
            x: Screen X coordinate.
            y: Screen Y coordinate.

        Returns:
            Tuple of (calibrated_x, calibrated_y).
        """
        if not self._calibrated:
            return (x, y)
        return self._matrix.map(x, y)

    def reset(self) -> None:
        """Reset calibration."""
        self._points.clear()
        self._matrix = CalibrationMatrix()
        self._calibrated = False

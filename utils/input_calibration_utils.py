"""Input calibration utilities for calibrating mouse/click accuracy.

This module provides utilities for calibrating input positions
to account for display scaling, DPI differences, and other factors
that may cause click position inaccuracies.
"""

from __future__ import annotations

import platform
from dataclasses import dataclass
from typing import Optional


IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"
IS_WINDOWS = platform.system() == "Windows"


@dataclass
class CalibrationOffset:
    """Represents a calibration offset for input coordinates."""
    offset_x: int
    offset_y: int
    scale_x: float = 1.0
    scale_y: float = 1.0
    
    def apply(self, x: int, y: int) -> tuple[int, int]:
        """Apply calibration to coordinates.
        
        Args:
            x: Input X coordinate.
            y: Input Y coordinate.
        
        Returns:
            Calibrated (x, y) coordinates.
        """
        calibrated_x = int(x * self.scale_x + self.offset_x)
        calibrated_y = int(y * self.scale_y + self.offset_y)
        return (calibrated_x, calibrated_y)


class InputCalibrator:
    """Calibrates input coordinates for accuracy."""
    
    def __init__(self):
        self._offset = CalibrationOffset(0, 0)
        self._display_scale: float = 1.0
        self._detect_display_scale()
    
    def _detect_display_scale(self) -> None:
        """Detect the display scale factor."""
        if IS_MACOS:
            try:
                import subprocess
                result = subprocess.run(
                    ["system_profiler", "SPDisplaysDataType", "-json"],
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                import json
                data = json.loads(result.stdout)
                displays = data.get("SPDisplaysDataType", [])
                if isinstance(displays, list) and displays:
                    self._display_scale = displays[0].get("ScaleFactor", 1.0)
            except Exception:
                self._display_scale = 1.0
        
        elif IS_WINDOWS:
            try:
                import ctypes
                user32 = ctypes.windll.user32
                user32.SetProcessDPIAware()
                hdc = user32.GetDC(0)
                dpi = ctypes.windll.gdi32.GetDeviceCaps(hdc, 88)  # LOGPIXELSX
                user32.ReleaseDC(0, hdc)
                self._display_scale = dpi / 96.0
            except Exception:
                self._display_scale = 1.0
    
    def calibrate_point(
        self,
        expected_x: int,
        expected_y: int,
        actual_x: int,
        actual_y: int,
    ) -> None:
        """Record a calibration point for offset calculation.
        
        Args:
            expected_x: Expected X coordinate (where you clicked).
            expected_y: Expected Y coordinate (where you clicked).
            actual_x: Actual X coordinate (where it landed).
            actual_y: Actual Y coordinate (where it landed).
        """
        # Simple offset calibration
        self._offset = CalibrationOffset(
            offset_x=expected_x - actual_x,
            offset_y=expected_y - actual_y,
        )
    
    def calibrate_with_scale(
        self,
        point1_expected: tuple[int, int],
        point1_actual: tuple[int, int],
        point2_expected: tuple[int, int],
        point2_actual: tuple[int, int],
    ) -> None:
        """Calibrate using two points for affine transformation.
        
        Args:
            point1_expected: First expected point (x, y).
            point1_actual: First actual point (x, y).
            point2_expected: Second expected point (x, y).
            point2_actual: Second actual point (x, y).
        """
        ex1, ey1 = point1_expected
        ax1, ay1 = point1_actual
        ex2, ey2 = point2_expected
        ax2, ay2 = point2_actual
        
        # Calculate scale factors
        dx_expected = ex2 - ex1
        dy_expected = ey2 - ey1
        dx_actual = ax2 - ax1
        dy_actual = ay2 - ay1
        
        scale_x = dx_expected / dx_actual if dx_actual != 0 else 1.0
        scale_y = dy_expected / dy_actual if dy_actual != 0 else 1.0
        
        # Calculate offset using first point
        offset_x = ex1 - int(ax1 * scale_x)
        offset_y = ey1 - int(ay1 * scale_y)
        
        self._offset = CalibrationOffset(
            offset_x=offset_x,
            offset_y=offset_y,
            scale_x=scale_x,
            scale_y=scale_y,
        )
    
    def calibrate_auto(self) -> bool:
        """Attempt automatic calibration using system information.
        
        Returns:
            True if auto-calibration was successful.
        """
        # Use display scale for automatic calibration
        if self._display_scale != 1.0:
            self._offset = CalibrationOffset(
                offset_x=0,
                offset_y=0,
                scale_x=self._display_scale,
                scale_y=self._display_scale,
            )
            return True
        return False
    
    def get_calibrated_coordinates(self, x: int, y: int) -> tuple[int, int]:
        """Get calibrated coordinates for an input position.
        
        Args:
            x: Input X coordinate.
            y: Input Y coordinate.
        
        Returns:
            Calibrated (x, y) coordinates.
        """
        return self._offset.apply(x, y)
    
    def get_offset(self) -> CalibrationOffset:
        """Get the current calibration offset."""
        return self._offset
    
    def reset(self) -> None:
        """Reset calibration to default (no calibration)."""
        self._offset = CalibrationOffset(0, 0)


# Global calibrator instance
_global_calibrator: Optional[InputCalibrator] = None


def get_calibrator() -> InputCalibrator:
    """Get the global input calibrator instance."""
    global _global_calibrator
    if _global_calibrator is None:
        _global_calibrator = InputCalibrator()
    return _global_calibrator

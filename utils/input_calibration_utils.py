"""Input calibration utilities for UI automation.

Provides utilities for calibrating touch input,
compensating for screen offsets, and managing input transformation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple


@dataclass
class CalibrationPoint:
    """A calibration point with expected and actual positions."""
    expected_x: float
    expected_y: float
    actual_x: float
    actual_y: float
    error_x: float = 0.0
    error_y: float = 0.0
    
    def __post_init__(self) -> None:
        """Calculate errors after initialization."""
        self.error_x = self.actual_x - self.expected_x
        self.error_y = self.actual_y - self.expected_y


@dataclass
class CalibrationMatrix:
    """Affine transformation matrix for calibration."""
    scale_x: float = 1.0
    scale_y: float = 1.0
    offset_x: float = 0.0
    offset_y: float = 0.0
    rotation: float = 0.0
    skew_x: float = 0.0
    skew_y: float = 0.0
    
    def transform_point(self, x: float, y: float) -> Tuple[float, float]:
        """Transform a point using the calibration matrix.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            
        Returns:
            Transformed (x, y) coordinates.
        """
        cos_r = math.cos(self.rotation)
        sin_r = math.sin(self.rotation)
        
        tx = x * cos_r - y * sin_r
        ty = x * sin_r + y * cos_r
        
        tx = tx * self.scale_x + self.offset_x + self.skew_x * ty
        ty = ty * self.scale_y + self.offset_y + self.skew_y * tx
        
        return (tx, ty)
    
    def compose(self, other: "CalibrationMatrix") -> "CalibrationMatrix":
        """Compose this matrix with another.
        
        Args:
            other: Matrix to compose with.
            
        Returns:
            Composed matrix.
        """
        return CalibrationMatrix(
            scale_x=self.scale_x * other.scale_x,
            scale_y=self.scale_y * other.scale_y,
            offset_x=self.offset_x + other.offset_x,
            offset_y=self.offset_y + other.offset_y,
            rotation=self.rotation + other.rotation,
            skew_x=self.skew_x + other.skew_x,
            skew_y=self.skew_y + other.skew_y
        )


@dataclass
class ScreenGeometry:
    """Describes screen geometry for calibration."""
    width: float
    height: float
    offset_x: float = 0.0
    offset_y: float = 0.0
    rotation: float = 0.0
    dpi_scale: float = 1.0


class InputCalibrator:
    """Calibrates input coordinates to match expected positions.
    
    Uses calibration points to compute transformation matrices
    that correct for screen offset, scaling, and rotation.
    """
    
    def __init__(self) -> None:
        """Initialize the input calibrator."""
        self._calibration_points: List[CalibrationPoint] = []
        self._matrix: Optional[CalibrationMatrix] = None
        self._is_calibrated = False
    
    def add_calibration_point(
        self,
        expected_x: float,
        expected_y: float,
        actual_x: float,
        actual_y: float
    ) -> None:
        """Add a calibration point.
        
        Args:
            expected_x: Expected X coordinate.
            expected_y: Expected Y coordinate.
            actual_x: Actual X coordinate.
            actual_y: Actual Y coordinate.
        """
        point = CalibrationPoint(
            expected_x=expected_x,
            expected_y=expected_y,
            actual_x=actual_x,
            actual_y=actual_y
        )
        self._calibration_points.append(point)
        self._is_calibrated = False
    
    def compute_calibration(self) -> CalibrationMatrix:
        """Compute calibration matrix from points.
        
        Returns:
            Computed calibration matrix.
        """
        if len(self._calibration_points) < 2:
            self._matrix = CalibrationMatrix()
            self._is_calibrated = True
            return self._matrix
        
        if len(self._calibration_points) == 2:
            self._matrix = self._compute_bilinear_calibration()
        else:
            self._matrix = self._compute_affine_calibration()
        
        self._is_calibrated = True
        return self._matrix
    
    def _compute_bilinear_calibration(self) -> CalibrationMatrix:
        """Compute bilinear calibration from 2 points.
        
        Returns:
            Calibration matrix.
        """
        p1 = self._calibration_points[0]
        p2 = self._calibration_points[1]
        
        scale_x = (p2.expected_x - p1.expected_x) / (p2.actual_x - p1.actual_x + 0.001)
        scale_y = (p2.expected_y - p1.expected_y) / (p2.actual_y - p1.actual_y + 0.001)
        
        offset_x = p1.expected_x - p1.actual_x * scale_x
        offset_y = p1.expected_y - p1.actual_y * scale_y
        
        return CalibrationMatrix(
            scale_x=scale_x,
            scale_y=scale_y,
            offset_x=offset_x,
            offset_y=offset_y
        )
    
    def _compute_affine_calibration(self) -> CalibrationMatrix:
        """Compute affine calibration from 3+ points.
        
        Returns:
            Calibration matrix.
        """
        avg_error_x = sum(p.error_x for p in self._calibration_points) / len(
            self._calibration_points
        )
        avg_error_y = sum(p.error_y for p in self._calibration_points) / len(
            self._calibration_points
        )
        
        scale_x = 1.0
        scale_y = 1.0
        
        return CalibrationMatrix(
            scale_x=scale_x,
            scale_y=scale_y,
            offset_x=-avg_error_x,
            offset_y=-avg_error_y
        )
    
    def calibrate_point(
        self,
        x: float,
        y: float
    ) -> Tuple[float, float]:
        """Calibrate a point.
        
        Args:
            x: X coordinate to calibrate.
            y: Y coordinate to calibrate.
            
        Returns:
            Calibrated (x, y) coordinates.
        """
        if not self._is_calibrated:
            self.compute_calibration()
        
        if self._matrix is None:
            return (x, y)
        
        return self._matrix.transform_point(x, y)
    
    def get_calibration_error(self) -> float:
        """Get average calibration error.
        
        Returns:
            Average error in pixels.
        """
        if not self._calibration_points:
            return 0.0
        
        total_error = 0.0
        for point in self._calibration_points:
            dx = point.error_x
            dy = point.error_y
            total_error += math.sqrt(dx * dx + dy * dy)
        
        return total_error / len(self._calibration_points)
    
    def clear_calibration(self) -> None:
        """Clear all calibration data."""
        self._calibration_points = []
        self._matrix = None
        self._is_calibrated = False
    
    def get_point_count(self) -> int:
        """Get number of calibration points.
        
        Returns:
            Number of points.
        """
        return len(self._calibration_points)


class TouchOffsetCompensator:
    """Compensates for touch offset errors.
    
    Handles common touch screen issues like
    offset, drift, and pressure-based errors.
    """
    
    def __init__(
        self,
        offset_x: float = 0.0,
        offset_y: float = 0.0,
        pressure_scale: float = 1.0
    ) -> None:
        """Initialize the touch offset compensator.
        
        Args:
            offset_x: X offset to apply.
            offset_y: Y offset to apply.
            pressure_scale: Scale factor for pressure.
        """
        self.offset_x = offset_x
        self.offset_y = offset_y
        self.pressure_scale = pressure_scale
        self._drift_x = 0.0
        self._drift_y = 0.0
    
    def compensate(
        self,
        x: float,
        y: float,
        pressure: float = 1.0
    ) -> Tuple[float, float]:
        """Compensate touch coordinates.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            pressure: Touch pressure.
            
        Returns:
            Compensated (x, y) coordinates.
        """
        compensated_x = x + self.offset_x + self._drift_x
        compensated_y = y + self.offset_y + self._drift_y
        
        return (compensated_x, compensated_y)
    
    def apply_drift_correction(
        self,
        x: float,
        y: float,
        reference_x: float,
        reference_y: float
    ) -> Tuple[float, float]:
        """Apply drift correction based on reference point.
        
        Args:
            x: Current X coordinate.
            y: Current Y coordinate.
            reference_x: Reference X coordinate.
            reference_y: Reference Y coordinate.
            
        Returns:
            Drift-corrected (x, y) coordinates.
        """
        self._drift_x = reference_x - x
        self._drift_y = reference_y - y
        
        return self.compensate(x, y)
    
    def reset_drift(self) -> None:
        """Reset drift values."""
        self._drift_x = 0.0
        self._drift_y = 0.0


class MultiTouchTransformer:
    """Transforms multi-touch input for gesture recognition.
    
    Handles coordinate transformation for multi-touch gestures
    and provides utilities for gesture analysis.
    """
    
    def __init__(self) -> None:
        """Initialize the multi-touch transformer."""
        self._active_touches: Dict[int, Tuple[float, float]] = {}
        self._touch_history: Dict[int, List[Tuple[float, float, float]]] = {}
    
    def add_touch(
        self,
        touch_id: int,
        x: float,
        y: float,
        timestamp_ms: float
    ) -> None:
        """Add or update a touch point.
        
        Args:
            touch_id: Touch identifier.
            x: X coordinate.
            y: Y coordinate.
            timestamp_ms: Timestamp.
        """
        self._active_touches[touch_id] = (x, y)
        
        if touch_id not in self._touch_history:
            self._touch_history[touch_id] = []
        
        self._touch_history[touch_id].append((x, y, timestamp_ms))
        
        if len(self._touch_history[touch_id]) > 100:
            self._touch_history[touch_id] = self._touch_history[touch_id][-50:]
    
    def remove_touch(self, touch_id: int) -> None:
        """Remove a touch point.
        
        Args:
            touch_id: Touch identifier.
        """
        if touch_id in self._active_touches:
            del self._active_touches[touch_id]
    
    def get_centroid(self) -> Tuple[float, float]:
        """Get centroid of all active touches.
        
        Returns:
            (x, y) centroid coordinates.
        """
        if not self._active_touches:
            return (0.0, 0.0)
        
        sum_x = sum(x for x, y in self._active_touches.values())
        sum_y = sum(y for x, y in self._active_touches.values())
        count = len(self._active_touches)
        
        return (sum_x / count, sum_y / count)
    
    def get_touch_distance(self, touch1: int, touch2: int) -> float:
        """Get distance between two touches.
        
        Args:
            touch1: First touch ID.
            touch2: Second touch ID.
            
        Returns:
            Distance between touches.
        """
        if touch1 not in self._active_touches or touch2 not in self._active_touches:
            return 0.0
        
        x1, y1 = self._active_touches[touch1]
        x2, y2 = self._active_touches[touch2]
        
        dx = x2 - x1
        dy = y2 - y1
        
        return math.sqrt(dx * dx + dy * dy)
    
    def get_spread(self) -> float:
        """Get average spread between all touches.
        
        Returns:
            Average touch spread.
        """
        if len(self._active_touches) < 2:
            return 0.0
        
        touch_ids = list(self._active_touches.keys())
        total_distance = 0.0
        count = 0
        
        for i in range(len(touch_ids)):
            for j in range(i + 1, len(touch_ids)):
                total_distance += self.get_touch_distance(
                    touch_ids[i], touch_ids[j]
                )
                count += 1
        
        return total_distance / count if count > 0 else 0.0
    
    def get_rotation(self, touch1: int, touch2: int) -> float:
        """Get rotation angle between two touches.
        
        Args:
            touch1: First touch ID.
            touch2: Second touch ID.
            
        Returns:
            Rotation angle in radians.
        """
        if touch1 not in self._active_touches or touch2 not in self._active_touches:
            return 0.0
        
        x1, y1 = self._active_touches[touch1]
        x2, y2 = self._active_touches[touch2]
        
        return math.atan2(y2 - y1, x2 - x1)
    
    def clear(self) -> None:
        """Clear all touch data."""
        self._active_touches = {}
        self._touch_history = {}


def create_calibration_matrix_from_points(
    expected: List[Tuple[float, float]],
    actual: List[Tuple[float, float]]
) -> CalibrationMatrix:
    """Create a calibration matrix from expected and actual points.
    
    Args:
        expected: List of expected (x, y) coordinates.
        actual: List of actual (x, y) coordinates.
        
    Returns:
        Calibration matrix.
    """
    if len(expected) != len(actual):
        raise ValueError("Expected and actual must have same length")
    
    calibrator = InputCalibrator()
    
    for (exp_x, exp_y), (act_x, act_y) in zip(expected, actual):
        calibrator.add_calibration_point(exp_x, exp_y, act_x, act_y)
    
    return calibrator.compute_calibration()

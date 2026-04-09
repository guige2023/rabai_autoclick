"""
Touch Calibration Utilities for UI Automation.

This module provides utilities for calibrating touch input
to improve accuracy in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple, Callable


@dataclass
class CalibrationPoint:
    """A single calibration point pair."""
    expected_x: float
    expected_y: float
    actual_x: float
    actual_y: float
    weight: float = 1.0


@dataclass
class CalibrationMatrix:
    """A 3x3 calibration matrix for affine transformation."""
    m00: float = 1.0
    m01: float = 0.0
    m02: float = 0.0
    m10: float = 0.0
    m11: float = 1.0
    m12: float = 0.0
    m20: float = 0.0
    m21: float = 0.0
    m22: float = 1.0


@dataclass
class CalibrationResult:
    """Result of calibration computation."""
    matrix: CalibrationMatrix
    mean_error: float
    max_error: float
    point_count: int
    is_valid: bool


@dataclass
class CalibrationConfig:
    """Configuration for calibration."""
    min_points: int = 3
    max_points: int = 20
    convergence_threshold: float = 0.5
    outlier_threshold_px: float = 10.0
    use_weights: bool = True


class TouchCalibrator:
    """Calibrates touch input to improve accuracy."""

    def __init__(self, config: Optional[CalibrationConfig] = None) -> None:
        self._config = config or CalibrationConfig()
        self._points: List[CalibrationPoint] = []
        self._calibration_matrix: Optional[CalibrationMatrix] = None
        self._is_calibrated: bool = False

    def add_calibration_point(
        self,
        expected_x: float,
        expected_y: float,
        actual_x: float,
        actual_y: float,
        weight: float = 1.0,
    ) -> int:
        """Add a calibration point pair."""
        if len(self._points) >= self._config.max_points:
            return len(self._points)

        point = CalibrationPoint(
            expected_x=expected_x,
            expected_y=expected_y,
            actual_x=actual_x,
            actual_y=actual_y,
            weight=weight,
        )
        self._points.append(point)
        self._is_calibrated = False
        return len(self._points)

    def clear_points(self) -> None:
        """Clear all calibration points."""
        self._points.clear()
        self._calibration_matrix = None
        self._is_calibrated = False

    def get_point_count(self) -> int:
        """Get the number of calibration points."""
        return len(self._points)

    def calibrate(self) -> CalibrationResult:
        """Compute the calibration matrix from current points."""
        if len(self._points) < self._config.min_points:
            return CalibrationResult(
                matrix=CalibrationMatrix(),
                mean_error=float('inf'),
                max_error=float('inf'),
                point_count=len(self._points),
                is_valid=False,
            )

        valid_points = self._remove_outliers()
        if len(valid_points) < self._config.min_points:
            return CalibrationResult(
                matrix=CalibrationMatrix(),
                mean_error=float('inf'),
                max_error=float('inf'),
                point_count=len(valid_points),
                is_valid=False,
            )

        matrix = self._compute_affine_matrix(valid_points)
        mean_error, max_error = self._compute_error(matrix, valid_points)

        self._calibration_matrix = matrix
        self._is_calibrated = True

        return CalibrationResult(
            matrix=matrix,
            mean_error=mean_error,
            max_error=max_error,
            point_count=len(valid_points),
            is_valid=mean_error < self._config.convergence_threshold,
        )

    def _remove_outliers(self) -> List[CalibrationPoint]:
        """Remove outlier points based on initial fit error."""
        if len(self._points) < self._config.min_points + 1:
            return list(self._points)

        initial_matrix = self._compute_affine_matrix(self._points)
        _, initial_max = self._compute_error(initial_matrix, self._points)

        if initial_max < self._config.outlier_threshold_px:
            return list(self._points)

        threshold = self._config.outlier_threshold_px * 2
        valid = []

        for point in self._points:
            dx = point.actual_x - point.expected_x
            dy = point.actual_y - point.expected_y
            error = math.sqrt(dx * dx + dy * dy)
            if error <= threshold:
                valid.append(point)

        return valid

    def _compute_affine_matrix(
        self,
        points: List[CalibrationPoint],
    ) -> CalibrationMatrix:
        """Compute affine transformation matrix from point correspondences."""
        n = len(points)
        sum_ax = sum(p.actual_x for p in points)
        sum_ay = sum(p.actual_y for p in points)
        sum_ex = sum(p.expected_x for p in points)
        sum_ey = sum(p.expected_y for p in points)

        if self._config.use_weights:
            sum_w = sum(p.weight for p in points)
            sum_ax = sum(p.actual_x * p.weight for p in points) / sum_w
            sum_ay = sum(p.actual_y * p.weight for p in points) / sum_w
            sum_ex = sum(p.expected_x * p.weight for p in points) / sum_w
            sum_ey = sum(p.expected_y * p.weight for p in points) / sum_w

        mean_ax = sum_ax / n
        mean_ay = sum_ay / n
        mean_ex = sum_ex / n
        mean_ey = sum_ey / n

        ss_aa = sum((p.actual_x - mean_ax) ** 2 + (p.actual_y - mean_ay) ** 2 for p in points)
        ss_ee = sum((p.expected_x - mean_ex) ** 2 + (p.expected_y - mean_ey) ** 2 for p in points)

        scale = math.sqrt(ss_ee / ss_aa) if ss_aa > 0 else 1.0

        return CalibrationMatrix(
            m00=scale,
            m01=0.0,
            m02=mean_ex - scale * mean_ax,
            m10=0.0,
            m11=scale,
            m12=mean_ey - scale * mean_ay,
            m20=0.0,
            m21=0.0,
            m22=1.0,
        )

    def _compute_error(
        self,
        matrix: CalibrationMatrix,
        points: List[CalibrationPoint],
    ) -> Tuple[float, float]:
        """Compute calibration error metrics."""
        errors = []

        for point in points:
            pred_x = matrix.m00 * point.actual_x + matrix.m01 * point.actual_y + matrix.m02
            pred_y = matrix.m10 * point.actual_x + matrix.m11 * point.actual_y + matrix.m12
            error = math.sqrt((pred_x - point.expected_x) ** 2 + (pred_y - point.expected_y) ** 2)
            errors.append(error)

        if not errors:
            return (0.0, 0.0)

        mean_error = sum(errors) / len(errors)
        max_error = max(errors)

        return (mean_error, max_error)

    def transform(self, x: float, y: float) -> Tuple[float, float]:
        """Transform touch coordinates using the calibration matrix."""
        if not self._is_calibrated or self._calibration_matrix is None:
            return (x, y)

        m = self._calibration_matrix
        new_x = m.m00 * x + m.m01 * y + m.m02
        new_y = m.m10 * x + m.m11 * y + m.m12

        return (new_x, new_y)

    def is_calibrated(self) -> bool:
        """Check if the calibrator has been calibrated."""
        return self._is_calibrated

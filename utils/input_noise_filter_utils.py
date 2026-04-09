"""
Input noise filter utilities.

This module provides noise filtering utilities for raw input data,
including Kalman filters, outlier detection, and signal smoothing.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional, Callable
from dataclasses import dataclass, field


# Type aliases
Point2D = Tuple[float, float]
InputPoint = Tuple[float, float, float]  # x, y, timestamp_ms


@dataclass
class KalmanFilterState:
    """State of a 1D Kalman filter."""
    estimate: float = 0.0
    error_covariance: float = 1.0
    process_noise: float = 0.1
    measurement_noise: float = 1.0


@dataclass
class FilteredResult:
    """Result of filtering an input point."""
    x: float
    y: float
    timestamp_ms: float
    is_outlier: bool = False
    confidence: float = 1.0


class KalmanFilter2D:
    """2D Kalman filter for input smoothing."""

    def __init__(self, process_noise: float = 0.1, measurement_noise: float = 1.0):
        self._state_x = KalmanFilterState(process_noise=process_noise, measurement_noise=measurement_noise)
        self._state_y = KalmanFilterState(process_noise=process_noise, measurement_noise=measurement_noise)
        self._initialized = False

    def update(self, x: float, y: float) -> Tuple[float, float]:
        """
        Update filter with new measurement.

        Args:
            x: X coordinate.
            y: Y coordinate.

        Returns:
            Filtered (x, y).
        """
        if not self._initialized:
            self._state_x.estimate = x
            self._state_y.estimate = y
            self._initialized = True
            return x, y

        # Update X
        self._state_x.error_covariance += self._state_x.process_noise
        kalman_gain_x = self._state_x.error_covariance / (self._state_x.error_covariance + self._state_x.measurement_noise)
        self._state_x.estimate += kalman_gain_x * (x - self._state_x.estimate)
        self._state_x.error_covariance *= (1.0 - kalman_gain_x)

        # Update Y
        self._state_y.error_covariance += self._state_y.process_noise
        kalman_gain_y = self._state_y.error_covariance / (self._state_y.error_covariance + self._state_y.measurement_noise)
        self._state_y.estimate += kalman_gain_y * (y - self._state_y.estimate)
        self._state_y.error_covariance *= (1.0 - kalman_gain_y)

        return self._state_x.estimate, self._state_y.estimate

    def reset(self) -> None:
        """Reset filter state."""
        self._initialized = False
        self._state_x = KalmanFilterState()
        self._state_y = KalmanFilterState()


def moving_average_filter(
    points: List[Point2D],
    window_size: int = 3,
) -> List[Point2D]:
    """
    Apply moving average filter to trajectory.

    Args:
        points: Input trajectory.
        window_size: Filter window size (must be odd).

    Returns:
        Filtered trajectory.
    """
    if window_size < 1:
        return points[:]
    if window_size % 2 == 0:
        window_size += 1
    if len(points) < window_size:
        return points[:]

    half = window_size // 2
    result: List[Point2D] = []

    for i in range(len(points)):
        start = max(0, i - half)
        end = min(len(points), i + half + 1)
        window = points[start:end]
        avg_x = sum(p[0] for p in window) / len(window)
        avg_y = sum(p[1] for p in window) / len(window)
        result.append((avg_x, avg_y))

    return result


def exponential_moving_average_filter(
    points: List[Point2D],
    alpha: float = 0.3,
) -> List[Point2D]:
    """
    Apply exponential moving average filter.

    Args:
        points: Input trajectory.
        alpha: Smoothing factor (0-1, lower = smoother).

    Returns:
        Filtered trajectory.
    """
    if not points or alpha <= 0 or alpha > 1:
        return points[:]

    result: List[Point2D] = [points[0]]
    for point in points[1:]:
        prev = result[-1]
        filtered_x = alpha * point[0] + (1 - alpha) * prev[0]
        filtered_y = alpha * point[1] + (1 - alpha) * prev[1]
        result.append((filtered_x, filtered_y))

    return result


def detect_outliers(
    points: List[Point2D],
    threshold_std: float = 2.0,
) -> List[bool]:
    """
    Detect outlier points based on velocity discontinuities.

    Args:
        points: Input trajectory.
        threshold_std: Number of standard deviations for outlier threshold.

    Returns:
        List of booleans indicating if each point is an outlier.
    """
    if len(points) < 3:
        return [False] * len(points)

    # Compute velocities
    velocities: List[float] = []
    for i in range(1, len(points)):
        dx = points[i][0] - points[i - 1][0]
        dy = points[i][1] - points[i - 1][1]
        velocities.append(math.sqrt(dx * dx + dy * dy))

    if not velocities:
        return [False] * len(points)

    mean_vel = sum(velocities) / len(velocities)
    variance = sum((v - mean_vel) ** 2 for v in velocities) / len(velocities)
    std_vel = math.sqrt(variance)

    threshold = threshold_std * std_vel

    # Mark outliers
    outliers = [False]  # First point is never an outlier
    for i in range(1, len(velocities) + 1):
        if i < len(velocities) and velocities[i - 1] > mean_vel + threshold:
            outliers.append(True)
        else:
            outliers.append(False)

    # Pad if needed
    while len(outliers) < len(points):
        outliers.append(False)

    return outliers


def filter_outliers(
    points: List[Point2D],
    threshold_std: float = 2.0,
) -> List[Point2D]:
    """
    Remove outlier points from trajectory.

    Args:
        points: Input trajectory.
        threshold_std: Outlier detection threshold.

    Returns:
        Filtered trajectory with outliers removed.
    """
    outliers = detect_outliers(points, threshold_std)
    return [p for p, is_outlier in zip(points, outliers) if not is_outlier]


def apply_savgol_filter(
    points: List[Point2D],
    window_size: int = 5,
    polynomial_order: int = 2,
) -> List[Point2D]:
    """
    Apply Savitzky-Golay filter for smoothing.

    Args:
        points: Input trajectory.
        window_size: Must be odd and >= polynomial_order + 2.
        polynomial_order: Polynomial order.

    Returns:
        Filtered trajectory.
    """
    if len(points) < window_size:
        return points[:]
    if window_size % 2 == 0:
        window_size += 1
    if polynomial_order >= window_size:
        polynomial_order = window_size - 1

    half = window_size // 2
    result: List[Point2D] = []

    for i in range(len(points)):
        start = max(0, i - half)
        end = min(len(points), i + half + 1)
        window = points[start:end]
        n = len(window)

        # Fit polynomial in x and y separately (simplified)
        xs = [p[0] for p in window]
        ys = [p[1] for p in window]
        indices = list(range(n))

        # Simple linear fit for smoothing
        sum_x = sum(indices)
        sum_x2 = sum(i ** 2 for i in indices)
        sum_yx = sum(ys[j] * indices[j] for j in range(n))
        sum_y = sum(ys)
        sum_xy = sum(xs[j] * indices[j] for j in range(n))

        denom = n * sum_x2 - sum_x * sum_x
        if abs(denom) < 1e-10:
            result.append((sum_x / n, sum_y / n))
        else:
            a_x = (n * sum_xy - sum_x * sum_y) / denom
            b_x = (sum_y - a_x * sum_x) / n
            a_y = (n * sum_yx - sum_x * sum_y) / denom
            b_y = (sum_y - a_y * sum_x) / n
            mid = indices[n // 2]
            result.append((a_x * mid + b_x, a_y * mid + b_y))

    return result

"""Input filtering utilities for noise reduction and validation."""

from typing import Callable, List, Optional, Tuple, Any
import numpy as np


Point = Tuple[float, float]


def median_filter_points(
    points: List[Point],
    window_size: int = 3
) -> List[Point]:
    """Apply median filter to smooth noisy points.
    
    Args:
        points: Input points.
        window_size: Filter window size (odd).
    
    Returns:
        Filtered points.
    """
    if len(points) < window_size:
        return points
    if window_size % 2 == 0:
        window_size += 1
    half = window_size // 2
    filtered = []
    for i in range(len(points)):
        start = max(0, i - half)
        end = min(len(points), i + half + 1)
        window = sorted(points[start:end], key=lambda p: (p[0], p[1]))
        filtered.append(window[len(window) // 2])
    return filtered


def kalman_filter_point(
    measurement: Point,
    estimate: Optional[Point] = None,
    process_variance: float = 0.1,
    measurement_variance: float = 0.1
) -> Point:
    """Apply Kalman filter to a point measurement.
    
    Args:
        measurement: Observed point.
        estimate: Previous estimate (for state persistence).
        process_variance: Process noise.
        measurement_variance: Measurement noise.
    
    Returns:
        Filtered point estimate.
    """
    if estimate is None:
        return measurement
    kalman_gain = measurement_variance / (measurement_variance + process_variance)
    x = estimate[0] + kalman_gain * (measurement[0] - estimate[0])
    y = estimate[1] + kalman_gain * (measurement[1] - estimate[1])
    return (x, y)


def outlier_filter_points(
    points: List[Point],
    threshold: float = 50.0
) -> List[Point]:
    """Remove outlier points based on distance threshold.
    
    Args:
        points: Input points.
        threshold: Max distance from neighbors.
    
    Returns:
        Filtered points with outliers removed.
    """
    if len(points) < 3:
        return points
    filtered = [points[0]]
    for i in range(1, len(points) - 1):
        prev = filtered[-1]
        curr = points[i]
        dist = np.sqrt((curr[0] - prev[0]) ** 2 + (curr[1] - prev[1]) ** 2)
        if dist < threshold:
            filtered.append(curr)
    filtered.append(points[-1])
    return filtered


def validate_click_sequence(
    points: List[Point],
    timestamps: List[float],
    min_interval: float = 0.01,
    max_jump_distance: float = 500.0
) -> bool:
    """Validate a click sequence for plausibility.
    
    Args:
        points: Sequence of click points.
        timestamps: Corresponding timestamps.
        min_interval: Minimum time between clicks.
        max_jump_distance: Maximum jump distance.
    
    Returns:
        True if sequence is valid.
    """
    if len(points) != len(timestamps):
        return False
    for i in range(1, len(points)):
        dt = timestamps[i] - timestamps[i-1]
        if dt < min_interval:
            return False
        dist = np.sqrt((points[i][0] - points[i-1][0]) ** 2 +
                      (points[i][1] - points[i-1][1]) ** 2)
        if dist > max_jump_distance:
            return False
    return True


def velocity_filter(
    points: List[Point],
    timestamps: List[float],
    max_velocity: float = 5000.0
) -> List[Point]:
    """Filter points based on velocity threshold.
    
    Args:
        points: Input points.
        timestamps: Corresponding timestamps.
        max_velocity: Maximum velocity in pixels/second.
    
    Returns:
        Filtered points.
    """
    if len(points) != len(timestamps) or len(points) < 2:
        return points
    filtered = [points[0]]
    for i in range(1, len(points)):
        dt = timestamps[i] - timestamps[i-1]
        if dt <= 0:
            continue
        dist = np.sqrt((points[i][0] - points[i-1][0]) ** 2 +
                      (points[i][1] - points[i-1][1]) ** 2)
        velocity = dist / dt
        if velocity <= max_velocity:
            filtered.append(points[i])
    return filtered


class InputFilter:
    """Configurable input filter pipeline."""

    def __init__(self):
        """Initialize input filter."""
        self._filters: List[Callable[[List[Point], List[float]], List[Point]]] = []
        self._timestamps: Optional[List[float]] = None

    def add_median_filter(self, window_size: int = 3) -> "InputFilter":
        """Add median filter to pipeline."""
        def filter_fn(points, ts):
            return median_filter_points(points, window_size)
        self._filters.append(filter_fn)
        return self

    def add_outlier_filter(self, threshold: float = 50.0) -> "InputFilter":
        """Add outlier filter to pipeline."""
        def filter_fn(points, ts):
            return outlier_filter_points(points, threshold)
        self._filters.append(filter_fn)
        return self

    def add_velocity_filter(self, max_velocity: float = 5000.0) -> "InputFilter":
        """Add velocity filter to pipeline."""
        def filter_fn(points, ts):
            return velocity_filter(points, ts, max_velocity)
        self._filters.append(filter_fn)
        return self

    def apply(self, points: List[Point], timestamps: List[float]) -> List[Point]:
        """Apply all filters in sequence.
        
        Args:
            points: Input points.
            timestamps: Timestamps.
        
        Returns:
            Filtered points.
        """
        result = points
        for f in self._filters:
            result = f(result, timestamps)
        return result

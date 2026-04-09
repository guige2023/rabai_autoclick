"""
Input trajectory smoothing utilities.

This module provides smoothing algorithms for noisy input trajectories,
including Savitzky-Golay filters, moving averages, and exponential smoothing.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Callable, Optional
from dataclasses import dataclass, field


# Type aliases
Point2D = Tuple[float, float]
Trajectory = List[Point2D]
SmoothFunc = Callable[[Trajectory], Trajectory]


@dataclass
class MovingAverageConfig:
    """Configuration for moving average smoothing."""
    window_size: int = 5
    center: bool = True


@dataclass
class SavitzkyGolayConfig:
    """Configuration for Savitzky-Golay filter."""
    window_size: int = 5
    polynomial_order: int = 2


@dataclass
class ExponentialSmoothConfig:
    """Configuration for exponential smoothing."""
    alpha: float = 0.3


def moving_average_smooth(trajectory: Trajectory, config: Optional[MovingAverageConfig] = None) -> Trajectory:
    """
    Smooth a trajectory using a simple moving average.

    Args:
        trajectory: List of (x, y) coordinate tuples.
        config: Moving average configuration. Defaults to window_size=5, centered.

    Returns:
        Smoothed trajectory of the same length.

    Example:
        >>> traj = [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]
        >>> smoothed = moving_average_smooth(traj)
    """
    if config is None:
        config = MovingAverageConfig()
    if len(trajectory) < config.window_size:
        return trajectory[:]

    half = config.window_size // 2
    result: Trajectory = []

    for i in range(len(trajectory)):
        if config.center:
            start = max(0, i - half)
            end = min(len(trajectory), i + half + 1)
        else:
            start = max(0, i - config.window_size + 1)
            end = i + 1

        window = trajectory[start:end]
        avg_x = sum(p[0] for p in window) / len(window)
        avg_y = sum(p[1] for p in window) / len(window)
        result.append((avg_x, avg_y))

    return result


def savitzky_golay_smooth(trajectory: Trajectory, config: Optional[SavitzkyGolayConfig] = None) -> Trajectory:
    """
    Smooth a trajectory using Savitzky-Golay filter.

    Args:
        trajectory: List of (x, y) coordinate tuples.
        config: Savitzky-Golay configuration.

    Returns:
        Smoothed trajectory.

    Note:
        Uses a simplified polynomial fitting approach.
    """
    if config is None:
        config = SavitzkyGolayConfig()
    if len(trajectory) < config.window_size:
        return trajectory[:]

    half = config.window_size // 2
    result: Trajectory = []

    for i in range(len(trajectory)):
        start = max(0, i - half)
        end = min(len(trajectory), i + half + 1)
        window = trajectory[start:end]
        n = len(window)

        # Fit polynomial using least squares (simplified for speed)
        xs = [p[0] for p in window]
        ys = [p[1] for p in window]
        indices = list(range(n))
        sum_x = sum(indices)
        sum_x2 = sum(i ** 2 for i in indices)
        sum_yx = sum(ys[j] * indices[j] for j in range(n))
        sum_y = sum(ys)

        denom = n * sum_x2 - sum_x * sum_x
        if abs(denom) < 1e-10:
            result.append((sum_x / n, sum_y / n))
        else:
            a = (n * sum_yx - sum_y * sum_x) / denom
            b = (sum_y - a * sum_x) / n
            mid = indices[n // 2]
            fit_y = a * mid + b
            fit_x = sum_x / n
            result.append((fit_x, fit_y))

    return result


def exponential_smooth(trajectory: Trajectory, config: Optional[ExponentialSmoothConfig] = None) -> Trajectory:
    """
    Smooth a trajectory using exponential moving average.

    Args:
        trajectory: List of (x, y) coordinate tuples.
        config: Exponential smoothing configuration with alpha.

    Returns:
        Smoothed trajectory.
    """
    if config is None:
        config = ExponentialSmoothConfig()
    alpha = config.alpha
    if not trajectory:
        return []
    result: Trajectory = [trajectory[0]]

    for point in trajectory[1:]:
        prev = result[-1]
        smoothed_x = alpha * point[0] + (1 - alpha) * prev[0]
        smoothed_y = alpha * point[1] + (1 - alpha) * prev[1]
        result.append((smoothed_x, smoothed_y))

    return result


def compute_trajectory_curvature(trajectory: Trajectory) -> List[float]:
    """
    Compute curvature at each point in a trajectory.

    Args:
        trajectory: List of (x, y) coordinate tuples.

    Returns:
        List of curvature values (higher = sharper turn).
    """
    if len(trajectory) < 3:
        return [0.0] * len(trajectory)

    curvatures: List[float] = []
    for i in range(len(trajectory)):
        p0 = trajectory[max(0, i - 1)]
        p1 = trajectory[i]
        p2 = trajectory[min(len(trajectory) - 1, i + 1)]

        v1 = (p1[0] - p0[0], p1[1] - p0[1])
        v2 = (p2[0] - p1[0], p2[1] - p1[1])

        cross = v1[0] * v2[1] - v1[1] * v2[0]
        mag1 = math.sqrt(v1[0] ** 2 + v1[1] ** 2)
        mag2 = math.sqrt(v2[0] ** 2 + v2[1] ** 2)

        if mag1 < 1e-10 or mag2 < 1e-10:
            curvatures.append(0.0)
        else:
            curvatures.append(abs(cross) / (mag1 * mag2))

    return curvatures


def resample_trajectory(trajectory: Trajectory, target_count: int) -> Trajectory:
    """
    Resample a trajectory to a target number of points.

    Args:
        trajectory: Input trajectory.
        target_count: Desired number of output points.

    Returns:
        Resampled trajectory with exactly target_count points.
    """
    if target_count < 2 or len(trajectory) < 2:
        return trajectory[:]
    if len(trajectory) == target_count:
        return trajectory[:]

    # Compute cumulative arc length
    distances = [0.0]
    for i in range(1, len(trajectory)):
        dx = trajectory[i][0] - trajectory[i - 1][0]
        dy = trajectory[i][1] - trajectory[i - 1][1]
        distances.append(distances[-1] + math.sqrt(dx * dx + dy * dy))

    total_length = distances[-1]
    if total_length < 1e-10:
        return [trajectory[0]] * target_count

    # Interpolate at evenly spaced arc lengths
    step = total_length / (target_count - 1)
    result: Trajectory = []

    for i in range(target_count):
        target_dist = i * step
        # Binary search for segment
        lo, hi = 0, len(distances) - 1
        while lo < hi - 1:
            mid = (lo + hi) // 2
            if distances[mid] <= target_dist:
                lo = mid
            else:
                hi = mid

        seg_dist = target_dist - distances[lo]
        seg_len = distances[lo + 1] - distances[lo]
        if seg_len < 1e-10:
            result.append(trajectory[lo])
        else:
            t = seg_dist / seg_len
            x = trajectory[lo][0] + t * (trajectory[lo + 1][0] - trajectory[lo][0])
            y = trajectory[lo][1] + t * (trajectory[lo + 1][1] - trajectory[lo][1])
            result.append((x, y))

    return result


def remove_trajectory_outliers(trajectory: Trajectory, threshold: float = 10.0) -> Trajectory:
    """
    Remove outlier points from a trajectory based on velocity discontinuities.

    Args:
        trajectory: Input trajectory.
        threshold: Speed ratio threshold for outlier detection.

    Returns:
        Filtered trajectory with outliers removed.
    """
    if len(trajectory) < 3:
        return trajectory[:]

    result: Trajectory = [trajectory[0]]
    speeds: List[float] = []

    for i in range(1, len(trajectory)):
        dx = trajectory[i][0] - trajectory[i - 1][0]
        dy = trajectory[i][1] - trajectory[i - 1][1]
        speeds.append(math.sqrt(dx * dx + dy * dy))

    median_speed = sorted(speeds)[len(speeds) // 2] if speeds else 0.0

    for i in range(1, len(trajectory)):
        dx = trajectory[i][0] - trajectory[i - 1][0]
        dy = trajectory[i][1] - trajectory[i - 1][1]
        speed = math.sqrt(dx * dx + dy * dy)
        if speed < threshold * max(median_speed, 1.0):
            result.append(trajectory[i])

    return result

"""Path smoothing utilities for natural motion paths."""

from typing import List, Tuple, Callable, Optional
import numpy as np


Point = Tuple[float, float]


def smooth_path_moving_average(
    points: List[Point],
    window_size: int = 5
) -> List[Point]:
    """Smooth path using moving average.
    
    Args:
        points: Input path points.
        window_size: Smoothing window size.
    
    Returns:
        Smoothed path points.
    """
    if len(points) < window_size:
        return points
    smoothed = []
    half = window_size // 2
    for i in range(len(points)):
        start = max(0, i - half)
        end = min(len(points), i + half + 1)
        window = points[start:end]
        avg_x = sum(p[0] for p in window) / len(window)
        avg_y = sum(p[1] for p in window) / len(window)
        smoothed.append((avg_x, avg_y))
    return smoothed


def smooth_path_gaussian(
    points: List[Point],
    sigma: float = 2.0
) -> List[Point]:
    """Smooth path using Gaussian filter.
    
    Args:
        points: Input path points.
        sigma: Gaussian kernel sigma.
    
    Returns:
        Smoothed path points.
    """
    if len(points) < 3:
        return points
    xs = np.array([p[0] for p in points])
    ys = np.array([p[1] for p in points])
    size = int(6 * sigma + 1)
    if size % 2 == 0:
        size += 1
    x_smoothed = np.convolve(xs, np.ones(size) / size, mode='same')
    y_smoothed = np.convolve(ys, np.ones(size) / size, mode='same')
    return [(x_smoothed[i], y_smoothed[i]) for i in range(len(points))]


def smooth_path_savitzky_golay(
    points: List[Point],
    window_size: int = 5,
    poly_order: int = 2
) -> List[Point]:
    """Smooth path using Savitzky-Golay filter.
    
    Args:
        points: Input path points.
        window_size: Must be odd.
        poly_order: Polynomial order.
    
    Returns:
        Smoothed path points.
    """
    if len(points) < window_size:
        return points
    xs = np.array([p[0] for p in points])
    ys = np.array([p[1] for p in points])
    half = window_size // 2
    padded_x = np.pad(xs, (half, half), mode='edge')
    padded_y = np.pad(ys, (half, half), mode='edge')
    x_smoothed = _savitzky_golay(padded_x, window_size, poly_order)[half:-half]
    y_smoothed = _savitzky_golay(padded_y, window_size, poly_order)[half:-half]
    return [(x_smoothed[i], y_smoothed[i]) for i in range(len(points))]


def _savitzky_golay(
    y: np.ndarray,
    window_size: int,
    poly_order: int
) -> np.ndarray:
    """Compute Savitzky-Golay coefficients and apply."""
    half = window_size // 2
    A = np.array([[i ** k for k in range(poly_order + 1)] for i in range(-half, half + 1)])
    coeffs = np.linalg.lstsq(A, np.eye(poly_order + 1), rcond=None)[0][0]
    return np.convolve(y, coeffs, mode='same')


def resample_path(
    points: List[Point],
    num_points: int
) -> List[Point]:
    """Resample path to have exactly num_points.
    
    Args:
        points: Input path points.
        num_points: Target number of points.
    
    Returns:
        Resampled path with exactly num_points.
    """
    if len(points) == 0:
        return []
    if len(points) == 1:
        return [points[0]] * num_points
    if len(points) == num_points:
        return points
    distances = [0.0]
    for i in range(1, len(points)):
        dx = points[i][0] - points[i-1][0]
        dy = points[i][1] - points[i-1][1]
        distances.append(distances[-1] + np.sqrt(dx * dx + dy * dy))
    total_length = distances[-1]
    if total_length == 0:
        return [points[0]] * num_points
    target_dists = np.linspace(0, total_length, num_points)
    result = []
    for td in target_dists:
        for i in range(1, len(distances)):
            if distances[i] >= td:
                ratio = (td - distances[i-1]) / (distances[i] - distances[i-1]) if distances[i] != distances[i-1] else 0
                x = points[i-1][0] + ratio * (points[i][0] - points[i-1][0])
                y = points[i-1][1] + ratio * (points[i][1] - points[i-1][1])
                result.append((x, y))
                break
    return result


def simplify_path(
    points: List[Point],
    tolerance: float = 1.0
) -> List[Point]:
    """Simplify path using Ramer-Douglas-Peucker algorithm.
    
    Args:
        points: Input path points.
        tolerance: Simplification tolerance.
    
    Returns:
        Simplified path.
    """
    if len(points) < 3:
        return points
    max_dist = 0.0
    max_idx = 0
    start = points[0]
    end = points[-1]
    for i in range(1, len(points) - 1):
        d = perpendicular_distance(points[i], start, end)
        if d > max_dist:
            max_dist = d
            max_idx = i
    if max_dist > tolerance:
        left = simplify_path(points[:max_idx+1], tolerance)
        right = simplify_path(points[max_idx:], tolerance)
        return left[:-1] + right
    return [start, end]


def perpendicular_distance(point: Point, line_start: Point, line_end: Point) -> float:
    """Calculate perpendicular distance from point to line."""
    dx = line_end[0] - line_start[0]
    dy = line_end[1] - line_start[1]
    if dx == 0 and dy == 0:
        return np.sqrt((point[0] - line_start[0]) ** 2 + (point[1] - line_start[1]) ** 2)
    t = max(0, min(1, ((point[0] - line_start[0]) * dx + (point[1] - line_start[1]) * dy) / (dx * dx + dy * dy)))
    proj_x = line_start[0] + t * dx
    proj_y = line_start[1] + t * dy
    return np.sqrt((point[0] - proj_x) ** 2 + (point[1] - proj_y) ** 2)

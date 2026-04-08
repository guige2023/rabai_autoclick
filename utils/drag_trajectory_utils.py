"""Drag trajectory utilities.

This module provides utilities for generating and executing
smooth drag trajectories.
"""

from __future__ import annotations

import math
from typing import Callable, List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class Point2D:
    """A 2D point."""
    x: float
    y: float

    def distance_to(self, other: "Point2D") -> float:
        dx = self.x - other.x
        dy = self.y - other.y
        return math.sqrt(dx * dx + dy * dy)

    def lerp(self, other: "Point2D", t: float) -> "Point2D":
        return Point2D(
            x=self.x + (other.x - self.x) * t,
            y=self.y + (other.y - self.y) * t,
        )


@dataclass
class TrajectoryConfig:
    """Configuration for trajectory generation."""
    points_per_segment: int = 10
    smoothing_factor: float = 0.5
    overshoot: float = 0.0
    snap_to_grid: Optional[Tuple[int, int]] = None


def linear_trajectory(
    start: Tuple[float, float],
    end: Tuple[float, float],
    num_points: int = 10,
) -> List[Point2D]:
    """Generate a linear trajectory between two points.

    Args:
        start: Start coordinates.
        end: End coordinates.
        num_points: Number of points to generate.

    Returns:
        List of points along the trajectory.
    """
    s = Point2D(start[0], start[1])
    e = Point2D(end[0], end[1])
    return [s.lerp(e, t / (num_points - 1)) for t in range(num_points)]


def bezier_trajectory(
    start: Tuple[float, float],
    control: Tuple[float, float],
    end: Tuple[float, float],
    num_points: int = 20,
) -> List[Point2D]:
    """Generate a quadratic bezier trajectory.

    Args:
        start: Start coordinates.
        control: Control point coordinates.
        end: End coordinates.
        num_points: Number of points to generate.

    Returns:
        List of points along the trajectory.
    """
    def bezier(t: float) -> Point2D:
        t2 = 1 - t
        return Point2D(
            x=t2 * t2 * start[0] + 2 * t2 * t * control[0] + t * t * end[0],
            y=t2 * t2 * start[1] + 2 * t2 * t * control[1] + t * t * end[1],
        )
    return [bezier(t / (num_points - 1)) for t in range(num_points)]


def curved_trajectory(
    start: Tuple[float, float],
    end: Tuple[float, float],
    curve_height: float = 50.0,
    num_points: int = 20,
) -> List[Point2D]:
    """Generate a curved trajectory with automatic control point.

    Args:
        start: Start coordinates.
        end: End coordinates.
        curve_height: Height of the curve perpendicular to the line.
        num_points: Number of points to generate.

    Returns:
        List of points along the trajectory.
    """
    mx = (start[0] + end[0]) / 2
    my = (start[1] + end[1]) / 2
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    length = math.sqrt(dx * dx + dy * dy)
    if length == 0:
        length = 1.0
    cx = mx - (dy / length) * curve_height
    cy = my + (dx / length) * curve_height
    return bezier_trajectory(start, (cx, cy), end, num_points)


def smooth_trajectory(
    points: List[Point2D],
    iterations: int = 3,
) -> List[Point2D]:
    """Smooth a trajectory using Chaikin subdivision.

    Args:
        points: Original trajectory points.
        iterations: Number of smoothing iterations.

    Returns:
        Smoothed trajectory points.
    """
    result = points[:]
    for _ in range(iterations):
        new_points: List[Point2D] = [result[0]]
        for i in range(len(result) - 1):
            p0 = result[i]
            p1 = result[i + 1]
            new_points.append(Point2D(
                x=p0.x * 0.75 + p1.x * 0.25,
                y=p0.y * 0.75 + p1.y * 0.25,
            ))
            new_points.append(Point2D(
                x=p0.x * 0.25 + p1.x * 0.75,
                y=p0.y * 0.25 + p1.y * 0.75,
            ))
        new_points.append(result[-1])
        result = new_points
    return result


def resample_trajectory(
    points: List[Point2D],
    target_count: int,
) -> List[Point2D]:
    """Resample trajectory to a specific number of points.

    Args:
        points: Original trajectory points.
        target_count: Desired number of points.

    Returns:
        Resampled trajectory.
    """
    if len(points) <= 1:
        return points[:]
    if len(points) == target_count:
        return points[:]

    total_length = sum(
        points[i].distance_to(points[i + 1])
        for i in range(len(points) - 1)
    )
    step = total_length / (target_count - 1)

    result = [points[0]]
    accumulated = 0.0
    j = 1

    for i in range(1, target_count - 1):
        target = step * i
        while j < len(points):
            seg_len = points[j - 1].distance_to(points[j])
            if accumulated + seg_len >= target:
                t = (target - accumulated) / seg_len if seg_len > 0 else 0
                result.append(Point2D(
                    x=points[j - 1].x + (points[j].x - points[j - 1].x) * t,
                    y=points[j - 1].y + (points[j].y - points[j - 1].y) * t,
                ))
                break
            accumulated += seg_len
            j += 1

    result.append(points[-1])
    return result


__all__ = [
    "Point2D",
    "TrajectoryConfig",
    "linear_trajectory",
    "bezier_trajectory",
    "curved_trajectory",
    "smooth_trajectory",
    "resample_trajectory",
]

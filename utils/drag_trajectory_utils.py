"""
Drag Trajectory Utilities

Provides utilities for computing and executing
drag trajectories in UI automation.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable
import math


@dataclass
class Point:
    """2D point for trajectory."""
    x: float
    y: float


class TrajectoryType(Enum):
    """Types of drag trajectories."""
    LINEAR = auto()
    EASE_IN = auto()
    EASE_OUT = auto()
    EASE_IN_OUT = auto()
    BEZIER = auto()


from enum import Enum, auto


@dataclass
class TrajectoryConfig:
    """Configuration for a drag trajectory."""
    trajectory_type: TrajectoryType = TrajectoryType.LINEAR
    steps: int = 20
    duration_ms: float = 500.0


class DragTrajectory:
    """
    Computes drag trajectories for smooth animations.
    
    Supports various trajectory types including
    linear, ease-in, ease-out, and bezier curves.
    """

    def __init__(
        self,
        start: Point,
        end: Point,
        config: TrajectoryConfig | None = None,
    ) -> None:
        self._start = start
        self._end = end
        self._config = config or TrajectoryConfig()
        self._cached_points: list[Point] | None = None

    def compute(self) -> list[Point]:
        """Compute all trajectory points."""
        if self._cached_points is not None:
            return self._cached_points

        points = []
        steps = self._config.steps

        for i in range(steps + 1):
            t = i / steps
            x, y = self._apply_easing(t)
            points.append(Point(x, y))

        self._cached_points = points
        return points

    def _apply_easing(self, t: float) -> tuple[float, float]:
        """Apply easing function to get point at t."""
        eased_t = t

        if self._config.trajectory_type == TrajectoryType.EASE_IN:
            eased_t = t * t
        elif self._config.trajectory_type == TrajectoryType.EASE_OUT:
            eased_t = 1 - (1 - t) * (1 - t)
        elif self._config.trajectory_type == TrajectoryType.EASE_IN_OUT:
            eased_t = 2 * t * t if t < 0.5 else 1 - pow(-2 * t + 2, 2) / 2
        elif self._config.trajectory_type == TrajectoryType.BEZIER:
            eased_t = self._cubic_bezier(t, 0.25, 0.1, 0.25, 1.0)

        x = self._start.x + (self._end.x - self._start.x) * eased_t
        y = self._start.y + (self._end.y - self._start.y) * eased_t
        return x, y

    def _cubic_bezier(
        self,
        t: float,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
    ) -> float:
        """Compute cubic bezier value at t."""
        cx = 3 * x1
        bx = 3 * (x2 - x1) - cx
        ax = 1 - cx - bx
        cy = 3 * y1
        by = 3 * (y2 - y1) - cy
        ay = 1 - cy - by

        def sample(t: float) -> float:
            return ((ax * t + bx) * t + cx) * t

        return ((ay * t + by) * t + cy) * t

    def get_interpolator(self) -> Callable[[float], Point]:
        """Get a function that interpolates at t."""
        def interpolate(t: float) -> Point:
            t = max(0.0, min(1.0, t))
            x, y = self._apply_easing(t)
            return Point(x, y)
        return interpolate


def compute_linear_trajectory(
    start: tuple[float, float],
    end: tuple[float, float],
    steps: int = 20,
) -> list[tuple[float, float]]:
    """Compute a linear trajectory between two points."""
    trajectory = DragTrajectory(
        Point(start[0], start[1]),
        Point(end[0], end[1]),
        TrajectoryConfig(trajectory_type=TrajectoryType.LINEAR, steps=steps),
    )
    points = trajectory.compute()
    return [(p.x, p.y) for p in points]

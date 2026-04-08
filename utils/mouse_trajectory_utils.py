"""
Mouse Trajectory Utilities

Provides utilities for calculating and smoothing
mouse trajectories in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable
import math


@dataclass
class TrajectoryPoint:
    """Point in a mouse trajectory."""
    x: float
    y: float
    t: float


class MouseTrajectory:
    """
    Calculates mouse trajectories for smooth movement.
    
    Supports various interpolation methods
    and trajectory smoothing.
    """

    def __init__(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
    ) -> None:
        self._start_x = start_x
        self._start_y = start_y
        self._end_x = end_x
        self._end_y = end_y
        self._cached_points: list[TrajectoryPoint] | None = None

    def calculate(
        self,
        steps: int = 20,
        easing: Callable[[float], float] | None = None,
    ) -> list[TrajectoryPoint]:
        """
        Calculate trajectory points.
        
        Args:
            steps: Number of points.
            easing: Optional easing function.
            
        Returns:
            List of TrajectoryPoint.
        """
        if self._cached_points:
            return self._cached_points

        points = []
        for i in range(steps + 1):
            t = i / steps
            if easing:
                t = easing(t)
            x = self._start_x + (self._end_x - self._start_x) * t
            y = self._start_y + (self._end_y - self._start_y) * t
            points.append(TrajectoryPoint(x=x, y=y, t=t))

        self._cached_points = points
        return points

    def get_points(self) -> list[tuple[float, float]]:
        """Get trajectory as simple (x, y) tuples."""
        points = self.calculate()
        return [(p.x, p.y) for p in points]


def linear_easing(t: float) -> float:
    """Linear easing function."""
    return t


def ease_in_quad(t: float) -> float:
    """Ease-in quadratic easing."""
    return t * t


def ease_out_quad(t: float) -> float:
    """Ease-out quadratic easing."""
    return 1 - (1 - t) * (1 - t)


def ease_in_out_quad(t: float) -> float:
    """Ease-in-out quadratic easing."""
    if t < 0.5:
        return 2 * t * t
    return 1 - pow(-2 * t + 2, 2) / 2

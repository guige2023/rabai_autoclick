"""
Drag trajectory generation utilities for UI automation.

This module provides utilities for generating smooth, natural-looking
drag trajectories with various path types and easing functions.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Tuple, Callable, Optional


@dataclass
class Point:
    """2D point with optional timestamp."""
    x: float
    y: float
    timestamp: float = 0.0


@dataclass
class TrajectoryConfig:
    """
    Configuration for trajectory generation.

    Attributes:
        num_points: Number of points in the trajectory.
        duration: Total duration in seconds.
        ease_func: Easing function to apply.
        snap_to_grid: Optional grid size to snap points to.
    """
    num_points: int = 50
    duration: float = 0.5
    ease_func: Callable[[float], float] = field(default=lambda t: t)
    snap_to_grid: Optional[float] = None


class TrajectoryGenerator:
    """
    Generates drag trajectories for automation.

    Supports linear, curved, and custom path trajectories
    with configurable easing.
    """

    def __init__(self) -> None:
        self._config = TrajectoryConfig()

    def set_config(self, config: TrajectoryConfig) -> TrajectoryGenerator:
        """Update trajectory configuration."""
        self._config = config
        return self

    def set_num_points(self, num: int) -> TrajectoryGenerator:
        """Set number of trajectory points."""
        self._config.num_points = max(2, num)
        return self

    def set_duration(self, duration: float) -> TrajectoryGenerator:
        """Set trajectory duration in seconds."""
        self._config.duration = max(0.01, duration)
        return self

    def set_easing(self, func: Callable[[float], float]) -> TrajectoryGenerator:
        """Set easing function."""
        self._config.ease_func = func
        return self

    def linear(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
    ) -> List[Point]:
        """Generate linear trajectory from (x1,y1) to (x2,y2)."""
        points: List[Point] = []
        step = 1.0 / (self._config.num_points - 1)

        for i in range(self._config.num_points):
            t = self._config.ease_func(i * step)
            x = x1 + (x2 - x1) * t
            y = y1 + (y2 - y1) * t

            if self._config.snap_to_grid:
                x = round(x / self._config.snap_to_grid) * self._config.snap_to_grid
                y = round(y / self._config.snap_to_grid) * self._config.snap_to_grid

            points.append(Point(
                x=x,
                y=y,
                timestamp=i * self._config.duration / (self._config.num_points - 1),
            ))

        return points

    def curved(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        control_x: float,
        control_y: float,
    ) -> List[Point]:
        """Generate quadratic Bezier curve trajectory."""
        points: List[Point] = []
        step = 1.0 / (self._config.num_points - 1)

        for i in range(self._config.num_points):
            t = self._config.ease_func(i * step)
            x, y = self._quadratic_bezier(x1, y1, control_x, control_y, x2, y2, t)

            if self._config.snap_to_grid:
                x = round(x / self._config.snap_to_grid) * self._config.snap_to_grid
                y = round(y / self._config.snap_to_grid) * self._config.snap_to_grid

            points.append(Point(
                x=x,
                y=y,
                timestamp=i * self._config.duration / (self._config.num_points - 1),
            ))

        return points

    def arc(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        arc_height: float = 50.0,
    ) -> List[Point]:
        """Generate arc-shaped trajectory."""
        points: List[Point] = []
        step = 1.0 / (self._config.num_points - 1)

        # Calculate control point for arc
        mid_x = (x1 + x2) / 2
        mid_y = (y1 + y2) / 2

        # Perpendicular direction
        dx = x2 - x1
        dy = y2 - y1
        length = math.sqrt(dx * dx + dy * dy)
        if length == 0:
            length = 1

        control_x = mid_x - (dy / length) * arc_height
        control_y = mid_y + (dx / length) * arc_height

        return self.curved(x1, y1, x2, y2, control_x, control_y)

    def smooth(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        smoothness: float = 0.5,
    ) -> List[Point]:
        """
        Generate smooth trajectory with configurable smoothness.

        smoothness: 0.0 = straight line, 1.0 = maximum curve.
        """
        # Calculate perpendicular offset for control point
        dx = x2 - x1
        dy = y2 - y1
        length = math.sqrt(dx * dx + dy * dy)

        if length == 0:
            return self.linear(x1, y1, x2, y2)

        # Perpendicular direction
        offset = length * smoothness * 0.25
        control_x = (x1 + x2) / 2 - (dy / length) * offset
        control_y = (y1 + y2) / 2 + (dx / length) * offset

        return self.curved(x1, y1, x2, y2, control_x, control_y)

    def polyline(
        self,
        points: List[Tuple[float, float]],
    ) -> List[Point]:
        """
        Generate trajectory through multiple waypoints.

        Interpolates smoothly between waypoints.
        """
        if len(points) < 2:
            return []

        result: List[Point] = []
        total_length = 0.0
        segments: List[Tuple[float, float, float]] = []

        # Calculate segment lengths
        for i in range(len(points) - 1):
            x1, y1 = points[i]
            x2, y2 = points[i + 1]
            dx = x2 - x1
            dy = y2 - y1
            length = math.sqrt(dx * dx + dy * dy)
            segments.append((x1, y1, length))
            total_length += length

        if total_length == 0:
            return []

        # Generate points
        current_segment = 0
        accumulated = 0.0
        points_per_segment = max(1, self._config.num_points // len(segments))

        for i in range(self._config.num_points):
            t = i / (self._config.num_points - 1)
            distance = t * total_length

            # Find correct segment
            seg_length = 0.0
            for j, (x1, y1, length) in enumerate(segments):
                if distance <= seg_length + length or j == len(segments) - 1:
                    local_t = (distance - seg_length) / length if length > 0 else 0
                    local_t = max(0, min(1, local_t))
                    local_t = self._config.ease_func(local_t)

                    x2, y2 = points[j + 1]
                    x = x1 + (x2 - x1) * local_t
                    y = y1 + (y2 - y1) * local_t

                    if self._config.snap_to_grid:
                        x = round(x / self._config.snap_to_grid) * self._config.snap_to_grid
                        y = round(y / self._config.snap_to_grid) * self._config.snap_to_grid

                    result.append(Point(
                        x=x,
                        y=y,
                        timestamp=i * self._config.duration / (self._config.num_points - 1),
                    ))
                    break
                seg_length += length

        return result

    @staticmethod
    def _quadratic_bezier(
        x1: float,
        y1: float,
        cx: float,
        cy: float,
        x2: float,
        y2: float,
        t: float,
    ) -> Tuple[float, float]:
        """Calculate point on quadratic Bezier curve."""
        mt = 1 - t
        x = mt * mt * x1 + 2 * mt * t * cx + t * t * x2
        y = mt * mt * y1 + 2 * mt * t * cy + t * t * y2
        return (x, y)


# Pre-built easing functions
def ease_linear(t: float) -> float:
    """Linear easing."""
    return t


def ease_in_quad(t: float) -> float:
    """Quadratic ease-in."""
    return t * t


def ease_out_quad(t: float) -> float:
    """Quadratic ease-out."""
    return t * (2 - t)


def ease_in_out_quad(t: float) -> float:
    """Quadratic ease-in-out."""
    if t < 0.5:
        return 2 * t * t
    return -1 + (4 - 2 * t) * t


def ease_in_cubic(t: float) -> float:
    """Cubic ease-in."""
    return t * t * t


def ease_out_cubic(t: float) -> float:
    """Cubic ease-out."""
    return (t - 1) ** 3 + 1


def ease_in_out_cubic(t: float) -> float:
    """Cubic ease-in-out."""
    if t < 0.5:
        return 4 * t * t * t
    return 1 - ((-2 * t + 2) ** 3) / 2


def ease_in_sine(t: float) -> float:
    """Sine ease-in."""
    return 1 - math.cos(t * math.pi / 2)


def ease_out_sine(t: float) -> float:
    """Sine ease-out."""
    return math.sin(t * math.pi / 2)


def ease_in_out_sine(t: float) -> float:
    """Sine ease-in-out."""
    return -(math.cos(math.pi * t) - 1) / 2

"""
Drag Trajectory Action Module

Provides smooth drag path generation with bezier curves,
physics-based trajectories, and multi-point path planning.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class TrajectoryType(Enum):
    """Types of drag trajectories."""

    LINEAR = "linear"
    BEZIER = "bezier"
    CURVED = "curved"
    PHYSICS = "physics"
    BOUNCE = "bounce"


@dataclass
class Point2D:
    """2D point."""

    x: float
    y: float

    def distance_to(self, other: "Point2D") -> float:
        """Calculate distance to another point."""
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)

    def interpolate_to(self, other: "Point2D", t: float) -> "Point2D":
        """Linear interpolation towards another point."""
        return Point2D(
            x=self.x + (other.x - self.x) * t,
            y=self.y + (other.y - self.y) * t,
        )


@dataclass
class BezierPoint:
    """Control point for bezier curve."""

    point: Point2D
    control_in: Optional[Point2D] = None
    control_out: Optional[Point2D] = None


@dataclass
class DragTrajectoryConfig:
    """Configuration for drag trajectory."""

    trajectory_type: TrajectoryType = TrajectoryType.BEZIER
    duration: float = 0.5
    steps: int = 50
    tension: float = 0.5
    velocity: float = 1.0
    friction: float = 0.95
    bounce_damping: float = 0.7


class TrajectoryGenerator:
    """
    Generates smooth drag trajectories for automation.

    Supports linear, bezier, curved, physics-based,
    and bounce trajectories with configurable parameters.
    """

    def __init__(
        self,
        config: Optional[DragTrajectoryConfig] = None,
    ):
        self.config = config or DragTrajectoryConfig()

    def generate(
        self,
        start: Tuple[float, float],
        end: Tuple[float, float],
        waypoints: Optional[List[Tuple[float, float]]] = None,
    ) -> List[Point2D]:
        """
        Generate a drag trajectory.

        Args:
            start: Start coordinates (x, y)
            end: End coordinates (x, y)
            waypoints: Optional intermediate points

        Returns:
            List of points forming the trajectory
        """
        if self.config.trajectory_type == TrajectoryType.LINEAR:
            return self._linear_trajectory(start, end)
        elif self.config.trajectory_type == TrajectoryType.BEZIER:
            return self._bezier_trajectory(start, end, waypoints)
        elif self.config.trajectory_type == TrajectoryType.CURVED:
            return self._curved_trajectory(start, end)
        elif self.config.trajectory_type == TrajectoryType.PHYSICS:
            return self._physics_trajectory(start, end)
        elif self.config.trajectory_type == TrajectoryType.BOUNCE:
            return self._bounce_trajectory(start, end)
        else:
            return self._linear_trajectory(start, end)

    def _linear_trajectory(
        self,
        start: Tuple[float, float],
        end: Tuple[float, float],
    ) -> List[Point2D]:
        """Generate linear trajectory."""
        points = []
        start_pt = Point2D(start[0], start[1])
        end_pt = Point2D(end[0], end[1])

        for i in range(self.config.steps):
            t = i / (self.config.steps - 1)
            points.append(start_pt.interpolate_to(end_pt, t))

        return points

    def _bezier_trajectory(
        self,
        start: Tuple[float, float],
        end: Tuple[float, float],
        waypoints: Optional[List[Tuple[float, float]]] = None,
    ) -> List[Point2D]:
        """Generate bezier curve trajectory."""
        points = []

        all_points = [Point2D(start[0], start[1])]
        if waypoints:
            all_points.extend(Point2D(w[0], w[1]) for w in waypoints)
        all_points.append(Point2D(end[0], end[1]))

        for i in range(self.config.steps):
            t = i / (self.config.steps - 1)
            point = self._cubic_bezier(all_points, t)
            points.append(point)

        return points

    def _cubic_bezier(
        self,
        points: List[Point2D],
        t: float,
    ) -> Point2D:
        """Calculate point on cubic bezier curve."""
        n = len(points) - 1
        if n == 0:
            return points[0]

        result = Point2D(0, 0)
        for i, p in enumerate(points):
            coeff = self._binomial(n, i) * (1 - t) ** (n - i) * t**i
            result.x += coeff * p.x
            result.y += coeff * p.y

        return result

    def _binomial(self, n: int, k: int) -> float:
        """Calculate binomial coefficient."""
        if k < 0 or k > n:
            return 0
        if k == 0 or k == n:
            return 1

        result = 1
        for i in range(min(k, n - k)):
            result = result * (n - i) // (i + 1)

        return result

    def _curved_trajectory(
        self,
        start: Tuple[float, float],
        end: Tuple[float, float],
    ) -> List[Point2D]:
        """Generate curved trajectory with gravity effect."""
        points = []
        start_pt = Point2D(start[0], start[1])
        end_pt = Point2D(end[0], end[1])

        mid_x = (start_pt.x + end_pt.x) / 2
        mid_y = min(start_pt.y, end_pt.y) - 100 * self.config.tension

        for i in range(self.config.steps):
            t = i / (self.config.steps - 1)

            p1 = start_pt.interpolate_to(Point2D(mid_x, mid_y), t)
            p2 = Point2D(mid_x, mid_y).interpolate_to(end_pt, t)
            point = p1.interpolate_to(p2, t)

            points.append(point)

        return points

    def _physics_trajectory(
        self,
        start: Tuple[float, float],
        end: Tuple[float, float],
    ) -> List[Point2D]:
        """Generate physics-based trajectory with velocity and friction."""
        points = []

        start_pt = Point2D(start[0], start[1])
        end_pt = Point2D(end[0], end[1])

        velocity = Point2D(
            (end_pt.x - start_pt.x) * self.config.velocity,
            (end_pt.y - start_pt.y) * self.config.velocity,
        )

        current = Point2D(start_pt.x, start_pt.y)
        points.append(current)

        for _ in range(self.config.steps - 1):
            velocity.x *= self.config.friction
            velocity.y *= self.config.friction

            current = Point2D(
                current.x + velocity.x,
                current.y + velocity.y,
            )
            points.append(current)

            dist = current.distance_to(end_pt)
            if dist < 5:
                break

        while len(points) < self.config.steps:
            points.append(end_pt)

        return points

    def _bounce_trajectory(
        self,
        start: Tuple[float, float],
        end: Tuple[float, float],
    ) -> List[Point2D]:
        """Generate bounce trajectory."""
        points = []

        start_pt = Point2D(start[0], start[1])
        end_pt = Point2D(end[0], end[1])

        bounce_count = 3
        mid_y = min(start_pt.y, end_pt.y) - 50

        for i in range(self.config.steps):
            t = i / (self.config.steps - 1)

            x = start_pt.x + (end_pt.x - start_pt.x) * t

            bounce_phase = t * bounce_count
            bounce_y = abs(math.sin(bounce_phase * math.pi))
            base_y = start_pt.y + (end_pt.y - start_pt.y) * t
            y = base_y + (mid_y - base_y) * bounce_y

            points.append(Point2D(x, y))

        return points

    def generate_spiral(
        self,
        center: Tuple[float, float],
        start_radius: float,
        end_radius: float,
        rotations: float = 1.5,
    ) -> List[Point2D]:
        """
        Generate spiral trajectory.

        Args:
            center: Center coordinates
            start_radius: Starting radius
            end_radius: Ending radius
            rotations: Number of rotations

        Returns:
            List of points forming spiral
        """
        points = []
        cx, cy = center

        for i in range(self.config.steps):
            t = i / (self.config.steps - 1)
            angle = t * rotations * 2 * math.pi
            radius = start_radius + (end_radius - start_radius) * t

            x = cx + radius * math.cos(angle)
            y = cy + radius * math.sin(angle)

            points.append(Point2D(x, y))

        return points

    def smooth_trajectory(
        self,
        points: List[Point2D],
        iterations: int = 2,
    ) -> List[Point2D]:
        """
        Smooth a trajectory using moving average.

        Args:
            points: Input points
            iterations: Number of smoothing passes

        Returns:
            Smoothed points
        """
        if len(points) < 3:
            return points

        smoothed = points

        for _ in range(iterations):
            result = [smoothed[0]]

            for i in range(1, len(smoothed) - 1):
                avg_x = (smoothed[i - 1].x + smoothed[i].x + smoothed[i + 1].x) / 3
                avg_y = (smoothed[i - 1].y + smoothed[i].y + smoothed[i + 1].y) / 3
                result.append(Point2D(avg_x, avg_y))

            result.append(smoothed[-1])
            smoothed = result

        return smoothed


def create_trajectory_generator(
    config: Optional[DragTrajectoryConfig] = None,
) -> TrajectoryGenerator:
    """Factory function to create a TrajectoryGenerator."""
    return TrajectoryGenerator(config=config)

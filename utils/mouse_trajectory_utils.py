"""Mouse trajectory generation utilities.

Generates natural-feeling mouse trajectories with configurable speed,
acceleration, and noise profiles. Trajectories are used to make
automation actions feel human-like rather than robotic.

Example:
    >>> from utils.mouse_trajectory_utils import TrajectoryGenerator, BezierCurve
    >>> gen = TrajectoryGenerator()
    >>> points = gen.generate((0, 0), (500, 300), steps=50)
    >>> for x, y in points:
    ...     move_mouse(int(x), int(y))
"""
from __future__ import annotations

import math
import random
from typing import Callable

__all__ = [
    "TrajectoryGenerator",
    "BezierCurve",
    "CatmullRomSpline",
]


class BezierCurve:
    """Cubic Bezier curve for smooth mouse movement.

    Example:
        >>> curve = BezierCurve((0, 0), (100, 100), (200, 50), (300, 300))
        >>> for t in [0.0, 0.5, 1.0]:
        ...     print(curve.point(t))
    """

    def __init__(
        self,
        p0: tuple[float, float],
        p1: tuple[float, float],
        p2: tuple[float, float],
        p3: tuple[float, float],
    ) -> None:
        self.p0 = p0
        self.p1 = p1
        self.p2 = p2
        self.p3 = p3

    def point(self, t: float) -> tuple[float, float]:
        """Return the point at normalized parameter t.

        Args:
            t: Parameter in [0.0, 1.0].

        Returns:
            (x, y) coordinate on the curve.
        """
        t = max(0.0, min(1.0, t))
        mt = 1.0 - t
        mt2 = mt * mt
        mt3 = mt2 * mt
        t2 = t * t
        t3 = t2 * t
        x = (
            mt3 * self.p0[0]
            + 3 * mt2 * t * self.p1[0]
            + 3 * mt * t2 * self.p2[0]
            + t3 * self.p3[0]
        )
        y = (
            mt3 * self.p0[1]
            + 3 * mt2 * t * self.p1[1]
            + 3 * mt * t2 * self.p2[1]
            + t3 * self.p3[1]
        )
        return (x, y)

    def derivative(self, t: float) -> tuple[float, float]:
        """Return the tangent vector at parameter t."""
        t = max(0.0, min(1.0, t))
        mt = 1.0 - t
        dx = (
            3 * mt2 * (self.p1[0] - self.p0[0])
            + 6 * mt * t * (self.p2[0] - self.p1[0])
            + 3 * t2 * (self.p3[0] - self.p2[0])
        )
        dy = (
            3 * mt2 * (self.p1[1] - self.p0[1])
            + 6 * mt * t * (self.p2[1] - self.p1[1])
            + 3 * t2 * (self.p3[1] - self.p2[1])
        )
        return (dx, dy)


class CatmullRomSpline:
    """Catmull-Rom spline through a sequence of control points.

    Produces smooth natural-looking curves through waypoints.

    Example:
        >>> points = [(0, 0), (100, 50), (200, 20), (300, 100)]
        >>> spline = CatmullRomSpline(points, segments=10)
        >>> for pt in spline.get_points():
        ...     print(pt)
    """

    def __init__(
        self,
        points: list[tuple[float, float]],
        segments: int = 20,
        alpha: float = 0.5,
    ) -> None:
        self.points = points
        self.segments = segments
        self.alpha = alpha

    def _catmull_rom(
        self,
        p0: tuple[float, float],
        p1: tuple[float, float],
        p2: tuple[float, float],
        p3: tuple[float, float],
        t: float,
    ) -> tuple[float, float]:
        """Compute Catmull-Rom interpolation at t."""
        t2 = t * t
        t3 = t2 * t
        x = (
            0.5
            * (
                2.0 * p1[0]
                + (-p0[0] + p2[0]) * t
                + (2.0 * p0[0] - 5.0 * p1[0] + 4.0 * p2[0] - p3[0]) * t2
                + (-p0[0] + 3.0 * p1[0] - 3.0 * p2[0] + p3[0]) * t3
            )
        )
        y = (
            0.5
            * (
                2.0 * p1[1]
                + (-p0[1] + p2[1]) * t
                + (2.0 * p0[1] - 5.0 * p1[1] + 4.0 * p2[1] - p3[1]) * t2
                + (-p0[1] + 3.0 * p1[1] - 3.0 * p2[1] + p3[1]) * t3
            )
        )
        return (x, y)

    def get_points(self) -> list[tuple[float, float]]:
        """Return all interpolated points along the spline."""
        if len(self.points) < 2:
            return list(self.points)
        pts = []
        pts.append(self.points[0])
        for i in range(len(self.points) - 1):
            p0 = self.points[max(0, i - 1)]
            p1 = self.points[i]
            p2 = self.points[i + 1]
            p3 = self.points[min(len(self.points) - 1, i + 2)]
            for j in range(self.segments):
                t = j / self.segments
                pts.append(self._catmull_rom(p0, p1, p2, p3, t))
        pts.append(self.points[-1])
        return pts


class TrajectoryGenerator:
    """Generates natural-feeling mouse trajectories.

    Provides multiple algorithms for generating mouse movement paths:
    - bezier: Cubic Bezier curves with random control points
    - spline: Catmull-Rom spline through waypoints
    - accelerated: Constant acceleration/deceleration
    - with_noise: Trajectory with random micro-jitter

    Example:
        >>> gen = TrajectoryGenerator(trajectory_type="bezier")
        >>> path = gen.generate((0, 0), (400, 300), steps=40)
    """

    def __init__(
        self,
        trajectory_type: str = "bezier",
        noise: float = 0.0,
        seed: int | None = None,
    ) -> None:
        self.trajectory_type = trajectory_type
        self.noise = noise
        self._rng = random.Random(seed)

    def generate(
        self,
        start: tuple[float, float],
        end: tuple[float, float],
        steps: int = 30,
    ) -> list[tuple[float, float]]:
        """Generate a trajectory path from start to end.

        Args:
            start: Starting (x, y) coordinate.
            end: Ending (x, y) coordinate.
            steps: Number of points in the generated path.

        Returns:
            List of (x, y) coordinate tuples.
        """
        if self.trajectory_type == "bezier":
            return self._generate_bezier(start, end, steps)
        elif self.trajectory_type == "spline":
            return self._generate_spline(start, end, steps)
        elif self.trajectory_type == "linear":
            return self._generate_linear(start, end, steps)
        elif self.trajectory_type == "accelerated":
            return self._generate_accelerated(start, end, steps)
        else:
            return self._generate_bezier(start, end, steps)

    def _generate_bezier(
        self,
        start: tuple[float, float],
        end: tuple[float, float],
        steps: int,
    ) -> list[tuple[float, float]]:
        """Generate path using cubic Bezier with random control points."""
        mid_x = (start[0] + end[0]) / 2
        mid_y = (start[1] + end[1]) / 2
        offset = self._rng.uniform(50, 150)
        p1 = (
            mid_x + self._rng.uniform(-offset, offset),
            mid_y + self._rng.uniform(-offset, offset),
        )
        p2 = (
            mid_x + self._rng.uniform(-offset, offset),
            mid_y + self._rng.uniform(-offset, offset),
        )
        curve = BezierCurve(start, p1, p2, end)
        return [curve.point(t / (steps - 1)) for t in range(steps)]

    def _generate_spline(
        self,
        start: tuple[float, float],
        end: tuple[float, float],
        steps: int,
    ) -> list[tuple[float, float]]:
        """Generate path using Catmull-Rom spline."""
        mid = ((start[0] + end[0]) / 2, (start[1] + end[1]) / 2)
        waypoints = [start, mid, end]
        spline = CatmullRomSpline(waypoints, segments=max(1, steps // 2))
        return spline.get_points()[:steps]

    def _generate_linear(
        self,
        start: tuple[float, float],
        end: tuple[float, float],
        steps: int,
    ) -> list[tuple[float, float]]:
        """Generate a straight line path."""
        return [
            (
                start[0] + (end[0] - start[0]) * t / max(1, steps - 1),
                start[1] + (end[1] - start[1]) * t / max(1, steps - 1),
            )
            for t in range(steps)
        ]

    def _generate_accelerated(
        self,
        start: tuple[float, float],
        end: tuple[float, float],
        steps: int,
    ) -> list[tuple[float, float]]:
        """Generate path with smooth acceleration/deceleration."""
        points = []
        for i in range(steps):
            t = i / max(1, steps - 1)
            # Smoothstep easing
            eased = t * t * (3 - 2 * t)
            x = start[0] + (end[0] - start[0]) * eased
            y = start[1] + (end[1] - start[1]) * eased
            points.append((x, y))
        return points

    def add_noise(
        self,
        points: list[tuple[float, float]],
    ) -> list[tuple[float, float]]:
        """Add micro-jitter noise to a trajectory.

        Args:
            points: Original trajectory points.

        Returns:
            Points with added noise.
        """
        if self.noise <= 0:
            return points
        return [
            (
                x + self._rng.gauss(0, self.noise),
                y + self._rng.gauss(0, self.noise),
            )
            for x, y in points
        ]

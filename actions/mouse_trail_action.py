"""
Mouse Trail Action Module.

Simulates natural mouse movement with bezier curves,
variable speed, and micro-jitter for human-like behavior.
"""

import math
import random
import time
from typing import Callable, Optional, Tuple


class Point:
    """A 2D point with optional timestamp."""

    def __init__(self, x: float, y: float, t: Optional[float] = None):
        """
        Initialize point.

        Args:
            x: X coordinate.
            y: Y coordinate.
            t: Optional timestamp.
        """
        self.x = x
        self.y = y
        self.t = t if t is not None else time.time()

    def __repr__(self) -> str:
        return f"Point({self.x:.1f}, {self.y:.1f})"


class MouseTrail:
    """Generates natural mouse movement paths."""

    JITTER_AMOUNT = 2.0
    MIN_SPEED = 200.0
    MAX_SPEED = 600.0

    def __init__(
        self,
        base_speed: Optional[float] = None,
        jitter: float = JITTER_AMOUNT,
    ):
        """
        Initialize mouse trail generator.

        Args:
            base_speed: Base movement speed in pixels/second.
            jitter: Random jitter amount.
        """
        self.base_speed = base_speed or 400.0
        self.jitter = jitter

    def generate_path(
        self,
        start: Tuple[float, float],
        end: Tuple[float, float],
        duration: Optional[float] = None,
    ) -> list[Point]:
        """
        Generate a natural mouse path between two points.

        Args:
            start: Starting coordinates (x, y).
            end: Ending coordinates (x, y).
            duration: Optional movement duration.

        Returns:
            List of Points forming the path.
        """
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        distance = math.sqrt(dx * dx + dy * dy)

        if distance < 1:
            return [Point(start[0], start[1]), Point(end[0], end[1])]

        if duration is None:
            speed = self.base_speed + random.uniform(-self.jitter * 10, self.jitter * 10)
            duration = distance / speed

        num_points = max(int(duration * 60), 10)

        control = self._generate_control_points(start, end)

        path = []
        start_time = time.time()

        for i in range(num_points + 1):
            t = i / num_points
            point = self._bezier_point(start, control, end, t)
            point.t = start_time + t * duration
            path.append(point)

        return path

    def generate_bezier_path(
        self,
        start: Tuple[float, float],
        end: Tuple[float, float],
        cp1: Tuple[float, float],
        cp2: Tuple[float, float],
        num_points: int = 50,
    ) -> list[Point]:
        """
        Generate a bezier curve path with explicit control points.

        Args:
            start: Start point.
            end: End point.
            cp1: First control point.
            cp2: Second control point.
            num_points: Number of points in path.

        Returns:
            List of Points.
        """
        path = []
        start_time = time.time()
        duration = num_points / 60.0

        for i in range(num_points + 1):
            t = i / num_points
            point = self._cubic_bezier(start, cp1, cp2, end, t)
            point.t = start_time + t * duration
            path.append(point)

        return path

    def _generate_control_points(
        self,
        start: Tuple[float, float],
        end: Tuple[float, float],
    ) -> Tuple[Tuple[float, float], Tuple[float, float]]:
        """Generate bezier control points for natural curve."""
        dx = end[0] - start[0]
        dy = end[1] - start[1]

        mid_x = start[0] + dx * 0.5
        mid_y = start[1] + dy * 0.5

        perp_x = -dy * 0.2 * random.choice([-1, 1])
        perp_y = dx * 0.2 * random.choice([-1, 1])

        cp1 = (
            start[0] + dx * 0.25 + perp_x + random.uniform(-5, 5),
            start[1] + dy * 0.25 + perp_y + random.uniform(-5, 5),
        )
        cp2 = (
            start[0] + dx * 0.75 + perp_x + random.uniform(-5, 5),
            start[1] + dy * 0.75 + perp_y + random.uniform(-5, 5),
        )

        return (cp1, cp2)

    def _bezier_point(
        self,
        start: Tuple[float, float],
        control: Tuple[Tuple[float, float], Tuple[float, float]],
        end: Tuple[float, float],
        t: float,
    ) -> Point:
        """Calculate point on quadratic bezier curve."""
        cp1, cp2 = control
        x = self._cubic_bezier_single(start[0], cp1[0], cp2[0], end[0], t)
        y = self._cubic_bezier_single(start[1], cp1[1], cp2[1], end[1], t)
        return Point(x, y)

    @staticmethod
    def _cubic_bezier(
        start: Tuple[float, float],
        cp1: Tuple[float, float],
        cp2: Tuple[float, float],
        end: Tuple[float, float],
        t: float,
    ) -> Point:
        """Calculate point on cubic bezier curve."""
        x = MouseTrail._cubic_bezier_single(start[0], cp1[0], cp2[0], end[0], t)
        y = MouseTrail._cubic_bezier_single(start[1], cp1[1], cp2[1], end[1], t)
        return Point(x, y)

    @staticmethod
    def _cubic_bezier_single(
        p0: float, p1: float, p2: float, p3: float, t: float
    ) -> float:
        """Single-axis cubic bezier calculation."""
        mt = 1 - t
        return mt * mt * mt * p0 + 3 * mt * mt * t * p1 + 3 * mt * t * t * p2 + t * t * t * p3

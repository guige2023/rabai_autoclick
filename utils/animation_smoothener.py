"""
Animation smoothener for interpolating motion paths.

Smooths jagged or irregular animation trajectories
into fluid motion curves.

Author: AutoClick Team
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable


@dataclass
class Point:
    """A 2D point with optional timestamp."""

    x: float
    y: float
    t: float = 0.0


class AnimationSmoothener:
    """
    Smooths animation paths using various interpolation methods.

    Reduces jitter and normalizes irregular timing in
    mouse/keyboard recording playback.

    Example:
        smoother = AnimationSmoothener(method="bezier")
        smooth_path = smoother.smooth(raw_points)
        for point in smooth_path:
            move_to(point.x, point.y)
    """

    def __init__(
        self,
        method: str = "bezier",
        tension: float = 0.5,
        normalize_timing: bool = True,
        target_fps: float = 60.0,
    ) -> None:
        """
        Initialize smoothener.

        Args:
            method: Smoothing method ("bezier", "catmullrom", "moving_average")
            tension: Curve tension (0.0-1.0)
            normalize_timing: Whether to resample to consistent FPS
            target_fps: Target frames per second for normalization
        """
        self._method = method
        self._tension = tension
        self._normalize_timing = normalize_timing
        self._target_fps = target_fps

    def smooth(self, points: list[Point]) -> list[Point]:
        """
        Smooth a series of animation points.

        Args:
            points: Raw animation points

        Returns:
            Smoothed animation points
        """
        if len(points) < 3:
            return points.copy()

        if self._normalize_timing:
            points = self._normalize_fps(points)

        if self._method == "bezier":
            return self._smooth_bezier(points)
        elif self._method == "catmullrom":
            return self._smooth_catmullrom(points)
        elif self._method == "moving_average":
            return self._smooth_moving_average(points)
        return points

    def _normalize_fps(self, points: list[Point]) -> list[Point]:
        """Resample points to target FPS."""
        if len(points) < 2:
            return points.copy()

        start_t = points[0].t
        end_t = points[-1].t
        duration = end_t - start_t

        if duration <= 0:
            return points.copy()

        num_frames = max(int(duration * self._target_fps), len(points))
        step = duration / num_frames

        resampled = [points[0]]

        for i in range(1, num_frames):
            t = start_t + i * step
            point = self._interpolate_at_t(points, t)
            resampled.append(point)

        resampled.append(points[-1])
        return resampled

    def _interpolate_at_t(self, points: list[Point], t: float) -> Point:
        """Find interpolated point at time t."""
        for i in range(len(points) - 1):
            if points[i].t <= t <= points[i + 1].t:
                p1, p2 = points[i], points[i + 1]
                alpha = (t - p1.t) / (p2.t - p1.t) if p2.t != p1.t else 0.0
                return Point(
                    x=p1.x + (p2.x - p1.x) * alpha,
                    y=p1.y + (p2.y - p1.y) * alpha,
                    t=t,
                )
        return points[-1]

    def _smooth_bezier(self, points: list[Point]) -> list[Point]:
        """Apply Bezier smoothing."""
        if len(points) < 3:
            return points

        smoothed = [points[0]]
        num插值 = len(points) * 3

        for i in range(len(points) - 1):
            p0 = points[max(0, i - 1)]
            p1 = points[i]
            p2 = points[i + 1]
            p3 = points[min(len(points) - 1, i + 2)]

            for j in range(3):
                t = j / 3
                pt = self._cubic_bezier(p0, p1, p2, p3, t)
                smoothed.append(pt)

        smoothed.append(points[-1])
        return smoothed

    def _cubic_bezier(self, p0: Point, p1: Point, p2: Point, p3: Point, t: float) -> Point:
        """Calculate point on cubic Bezier curve."""
        t2 = t * t
        t3 = t2 * t
        mt = 1 - t
        mt2 = mt * mt
        mt3 = mt2 * mt

        return Point(
            x=mt3 * p0.x + 3 * mt2 * t * p1.x + 3 * mt * t2 * p2.x + t3 * p3.x,
            y=mt3 * p0.y + 3 * mt2 * t * p1.y + 3 * mt * t2 * p2.y + t3 * p3.y,
            t=p1.t + (p2.t - p1.t) * t,
        )

    def _smooth_catmullrom(self, points: list[Point]) -> list[Point]:
        """Apply Catmull-Rom spline smoothing."""
        if len(points) < 4:
            return points

        smoothed = [points[0]]
        segments = 10

        for i in range(1, len(points) - 2):
            p0, p1, p2, p3 = points[i - 1], points[i], points[i + 1], points[i + 2]

            for j in range(segments):
                t = j / segments
                pt = self._catmullrom_point(p0, p1, p2, p3, t, self._tension)
                smoothed.append(pt)

        smoothed.append(points[-1])
        return smoothed

    def _catmullrom_point(
        self,
        p0: Point,
        p1: Point,
        p2: Point,
        p3: Point,
        t: float,
        tension: float,
    ) -> Point:
        """Calculate point on Catmull-Rom spline."""
        t2 = t * t
        t3 = t2 * t

        alpha = tension

        x = 0.5 * (
            (2 * p1.x)
            + (-p0.x + p2.x) * t * alpha
            + (2 * p0.x - 5 * p1.x + 4 * p2.x - p3.x) * t2 * alpha
            + (-p0.x + 3 * p1.x - 3 * p2.x + p3.x) * t3 * alpha
        )

        y = 0.5 * (
            (2 * p1.y)
            + (-p0.y + p2.y) * t * alpha
            + (2 * p0.y - 5 * p1.y + 4 * p2.y - p3.y) * t2 * alpha
            + (-p0.y + 3 * p1.y - 3 * p2.y + p3.y) * t3 * alpha
        )

        time = p1.t + (p2.t - p1.t) * t

        return Point(x=x, y=y, t=time)

    def _smooth_moving_average(self, points: list[Point], window: int = 5) -> list[Point]:
        """Apply moving average smoothing."""
        if len(points) <= window:
            return points

        half = window // 2
        smoothed = []

        for i in range(len(points)):
            start = max(0, i - half)
            end = min(len(points), i + half + 1)
            window_points = points[start:end]

            avg_x = sum(p.x for p in window_points) / len(window_points)
            avg_y = sum(p.y for p in window_points) / len(window_points)

            smoothed.append(Point(x=avg_x, y=avg_y, t=points[i].t))

        return smoothed


def calculate_velocity(points: list[Point]) -> list[float]:
    """Calculate velocity (pixels per second) at each point."""
    velocities = []

    for i, point in enumerate(points):
        if i == 0:
            dt = point.t - points[i].t
            if dt > 0:
                dx = point.x - points[i].x
                dy = point.y - points[i].y
                velocities.append(math.sqrt(dx * dx + dy * dy) / dt)
            else:
                velocities.append(0.0)
        else:
            dt = point.t - points[i - 1].t
            if dt > 0:
                dx = point.x - points[i - 1].x
                dy = point.y - points[i - 1].y
                velocities.append(math.sqrt(dx * dx + dy * dy) / dt)
            else:
                velocities.append(velocities[-1] if velocities else 0.0)

    return velocities

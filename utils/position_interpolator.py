"""
Position interpolator for smooth transitions between states.

Provides various interpolation algorithms for
animating between UI positions.

Author: AutoClick Team
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable


class InterpolationType(Enum):
    """Supported interpolation algorithms."""

    LINEAR = auto()
    EASE_IN = auto()
    EASE_OUT = auto()
    EASE_IN_OUT = auto()
    BOUNCE = auto()
    ELASTIC = auto()
    SPRING = auto()


@dataclass
class Vector2:
    """2D vector for position calculations."""

    x: float
    y: float

    def __add__(self, other: "Vector2") -> "Vector2":
        return Vector2(self.x + other.x, self.y + other.y)

    def __sub__(self, other: "Vector2") -> "Vector2":
        return Vector2(self.x - other.x, self.y - other.y)

    def __mul__(self, scalar: float) -> "Vector2":
        return Vector2(self.x * scalar, self.y * scalar)

    def lerp(self, target: "Vector2", t: float) -> "Vector2":
        """Linear interpolation to target."""
        return self + (target - self) * t

    def distance_to(self, other: "Vector2") -> float:
        """Euclidean distance to another vector."""
        dx = other.x - self.x
        dy = other.y - self.y
        return math.sqrt(dx * dx + dy * dy)


class PositionInterpolator:
    """
    Interpolates positions between start and end states.

    Supports multiple easing functions and animation curves.

    Example:
        interp = PositionInterpolator(start=Vector2(0, 0), end=Vector2(100, 100))
        for t in range(11):
            pos = interp.get_position(t / 10, InterpolationType.EASE_OUT)
            move_to(pos.x, pos.y)
    """

    def __init__(
        self,
        start: Vector2 | tuple[float, float],
        end: Vector2 | tuple[float, float],
    ) -> None:
        """
        Initialize interpolator with start and end positions.

        Args:
            start: Starting position
            end: Ending position
        """
        if isinstance(start, tuple):
            start = Vector2(start[0], start[1])
        if isinstance(end, tuple):
            end = Vector2(end[0], end[1])

        self._start = start
        self._end = end

    def get_position(self, t: float, interpolation: InterpolationType = InterpolationType.LINEAR) -> Vector2:
        """
        Get interpolated position at time t.

        Args:
            t: Time value 0.0-1.0
            interpolation: Easing function to use

        Returns:
            Interpolated position vector
        """
        t = max(0.0, min(1.0, t))

        eased_t = self._apply_easing(t, interpolation)

        return self._start.lerp(self._end, eased_t)

    def _apply_easing(self, t: float, interpolation: InterpolationType) -> float:
        """Apply easing function to time value."""
        if interpolation == InterpolationType.LINEAR:
            return t
        elif interpolation == InterpolationType.EASE_IN:
            return t * t
        elif interpolation == InterpolationType.EASE_OUT:
            return 1 - (1 - t) * (1 - t)
        elif interpolation == InterpolationType.EASE_IN_OUT:
            return 2 * t * t if t < 0.5 else 1 - pow(-2 * t + 2, 2) / 2
        elif interpolation == InterpolationType.BOUNCE:
            return self._bounce_out(t)
        elif interpolation == InterpolationType.ELASTIC:
            return self._elastic_out(t)
        elif interpolation == InterpolationType.SPRING:
            return self._spring(t)
        return t

    def _bounce_out(self, t: float) -> float:
        """Bounce easing function."""
        n1 = 7.5625
        d1 = 2.75

        if t < 1 / d1:
            return n1 * t * t
        elif t < 2 / d1:
            t -= 1.5 / d1
            return n1 * t * t + 0.75
        elif t < 2.5 / d1:
            t -= 2.25 / d1
            return n1 * t * t + 0.9375
        else:
            t -= 2.625 / d1
            return n1 * t * t + 0.984375

    def _elastic_out(self, t: float) -> float:
        """Elastic easing function."""
        if t == 0 or t == 1:
            return t
        return math.pow(2, -10 * t) * math.sin((t - 0.1) * 5 * math.pi) + 1

    def _spring(self, t: float) -> float:
        """Spring physics easing."""
        return 1 - math.exp(-6 * t) * math.cos(10 * t)


def interpolate_path(
    points: list[Vector2],
    num_steps: int,
    interpolation: InterpolationType = InterpolationType.EASE_IN_OUT,
) -> list[Vector2]:
    """
    Interpolate a smooth path through multiple points.

    Args:
        points: Waypoints to interpolate through
        num_steps: Number of interpolated points per segment
        interpolation: Easing to apply

    Returns:
        List of interpolated positions forming a smooth path
    """
    if len(points) < 2:
        return points.copy()

    result: list[Vector2] = []

    for i in range(len(points) - 1):
        start = points[i]
        end = points[i + 1]

        for step in range(num_steps):
            t = step / num_steps

            if interpolation != InterpolationType.LINEAR:
                t = _apply_easing_only(t, interpolation)

            result.append(start.lerp(end, t))

    result.append(points[-1])
    return result


def _apply_easing_only(t: float, interpolation: InterpolationType) -> float:
    """Apply easing to time without PositionInterpolator."""
    if interpolation == InterpolationType.LINEAR:
        return t
    elif interpolation == InterpolationType.EASE_IN:
        return t * t
    elif interpolation == InterpolationType.EASE_OUT:
        return 1 - (1 - t) * (1 - t)
    elif interpolation == InterpolationType.EASE_IN_OUT:
        return 2 * t * t if t < 0.5 else 1 - pow(-2 * t + 2, 2) / 2
    return t

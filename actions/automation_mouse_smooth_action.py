"""
Smooth mouse movement automation with easing curves.

Provides smooth mouse movement with configurable easing functions,
path interpolation, and trajectory control for natural-looking automation.

Author: Aito Auto Agent
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class EasingType(Enum):
    """Easing curve types for smooth motion."""
    LINEAR = auto()
    EASE_IN = auto()
    EASE_OUT = auto()
    EASE_IN_OUT = auto()
    QUAD_IN = auto()
    QUAD_OUT = auto()
    QUAD_IN_OUT = auto()
    CUBIC_IN = auto()
    CUBIC_OUT = auto()
    CUBIC_IN_OUT = auto()
    QUART_IN = auto()
    QUART_OUT = auto()
    QUART_IN_OUT = auto()
    QUINT_IN = auto()
    QUINT_OUT = auto()
    QUINT_IN_OUT = auto()
    SINE_IN = auto()
    SINE_OUT = auto()
    SINE_IN_OUT = auto()
    CIRC_IN = auto()
    CIRC_OUT = auto()
    CIRC_IN_OUT = auto()
    ELASTIC_IN = auto()
    ELASTIC_OUT = auto()
    ELASTIC_IN_OUT = auto()
    BOUNCE_IN = auto()
    BOUNCE_OUT = auto()
    BOUNCE_IN_OUT = auto()


@dataclass
class Point:
    """2D point representation."""
    x: float
    y: float

    def __add__(self, other: Point) -> Point:
        return Point(self.x + other.x, self.y + other.y)

    def __sub__(self, other: Point) -> Point:
        return Point(self.x - other.x, self.y - other.y)

    def __mul__(self, scalar: float) -> Point:
        return Point(self.x * scalar, self.y * scalar)

    def distance_to(self, other: Point) -> float:
        """Calculate Euclidean distance to another point."""
        dx = self.x - other.x
        dy = self.y - other.y
        return (dx * dx + dy * dy) ** 0.5


@dataclass
class SmoothMoveConfig:
    """Configuration for smooth mouse movement."""
    duration_ms: float = 300.0
    easing: EasingType = EasingType.EASE_OUT
    steps: int = 30
    overshoot: float = 0.0
    bounce_count: int = 0


class EasingFunctions:
    """Collection of easing functions for smooth motion."""

    @staticmethod
    def linear(t: float) -> float:
        return t

    @staticmethod
    def ease_in(t: float) -> float:
        return t * t

    @staticmethod
    def ease_out(t: float) -> float:
        return t * (2 - t)

    @staticmethod
    def ease_in_out(t: float) -> float:
        return t * t * (3 - 2 * t)

    @staticmethod
    def quad_in(t: float) -> float:
        return t ** 2

    @staticmethod
    def quad_out(t: float) -> float:
        return 1 - (1 - t) ** 2

    @staticmethod
    def quad_in_out(t: float) -> float:
        return 2 * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 2 / 2

    @staticmethod
    def cubic_in(t: float) -> float:
        return t ** 3

    @staticmethod
    def cubic_out(t: float) -> float:
        return 1 - (1 - t) ** 3

    @staticmethod
    def cubic_in_out(t: float) -> float:
        return 4 * t * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 3 / 2

    @staticmethod
    def quart_in(t: float) -> float:
        return t ** 4

    @staticmethod
    def quart_out(t: float) -> float:
        return 1 - (1 - t) ** 4

    @staticmethod
    def quart_in_out(t: float) -> float:
        return 8 * t ** 4 if t < 0.5 else 1 - (-2 * t + 2) ** 4 / 2

    @staticmethod
    def quint_in(t: float) -> float:
        return t ** 5

    @staticmethod
    def quint_out(t: float) -> float:
        return 1 - (1 - t) ** 5

    @staticmethod
    def quint_in_out(t: float) -> float:
        return 16 * t ** 5 if t < 0.5 else 1 - (-2 * t + 2) ** 5 / 2

    @staticmethod
    def sine_in(t: float) -> float:
        return 1 - __import__('math').cos(t * __import__('math').pi / 2)

    @staticmethod
    def sine_out(t: float) -> float:
        return __import__('math').sin(t * __import__('math').pi / 2)

    @staticmethod
    def sine_in_out(t: float) -> float:
        return -(math.cos(math.pi * t) - 1) / 2

    @staticmethod
    def circ_in(t: float) -> float:
        return 1 - math.sqrt(1 - t ** 2)

    @staticmethod
    def circ_out(t: float) -> float:
        return math.sqrt(1 - (1 - t) ** 2)

    @staticmethod
    def circ_in_out(t: float) -> float:
        return (1 - math.sqrt(1 - (2 * t) ** 2)) / 2 if t < 0.5 else \
               (1 + math.sqrt(1 - (-2 * t + 2) ** 2)) / 2

    @staticmethod
    def elastic_in(t: float) -> float:
        if t == 0 or t == 1:
            return t
        return -2 ** (10 * t - 10) * math.sin((t * 10 - 10.75) * (2 * math.pi) / 3)

    @staticmethod
    def elastic_out(t: float) -> float:
        if t == 0 or t == 1:
            return t
        return 2 ** (-10 * t) * math.sin((t * 10 - 0.75) * (2 * math.pi) / 3) + 1

    @staticmethod
    def elastic_in_out(t: float) -> float:
        if t == 0 or t == 1:
            return t
        if t < 0.5:
            return -(2 ** (20 * t - 10) * math.sin((20 * t - 11.125) * (2 * math.pi) / 4.5)) / 2
        return (2 ** (-20 * t + 10) * math.sin((20 * t - 11.125) * (2 * math.pi) / 4.5)) / 2 + 1

    @staticmethod
    def bounce_out(t: float) -> float:
        if t < 1 / 2.75:
            return 7.5625 * t ** 2
        elif t < 2 / 2.75:
            t -= 1.5 / 2.75
            return 7.5625 * t ** 2 + 0.75
        elif t < 2.5 / 2.75:
            t -= 2.25 / 2.75
            return 7.5625 * t ** 2 + 0.9375
        else:
            t -= 2.625 / 2.75
            return 7.5625 * t ** 2 + 0.984375

    @staticmethod
    def bounce_in(t: float) -> float:
        return 1 - EasingFunctions.bounce_out(1 - t)

    @staticmethod
    def bounce_in_out(t: float) -> float:
        return (EasingFunctions.bounce_in(2 * t) if t < 0.5
                else 1 - EasingFunctions.bounce_out(2 - 2 * t) / 2)


@dataclass
class TrajectoryPoint:
    """A point along a mouse trajectory."""
    point: Point
    timestamp: float
    easing_value: float = 1.0


import math


class SmoothMouseMover:
    """
    Smooth mouse movement with configurable easing curves.

    Example:
        mover = SmoothMouseMover()
        mover.move_to(Point(100, 200), SmoothMoveConfig(
            duration_ms=500,
            easing=EasingType.CUBIC_OUT
        ))
    """

    def __init__(self):
        self._easing_map: dict[EasingType, Callable[[float], float]] = {
            EasingType.LINEAR: EasingFunctions.linear,
            EasingType.EASE_IN: EasingFunctions.ease_in,
            EasingType.EASE_OUT: EasingFunctions.ease_out,
            EasingType.EASE_IN_OUT: EasingFunctions.ease_in_out,
            EasingType.QUAD_IN: EasingFunctions.quad_in,
            EasingType.QUAD_OUT: EasingFunctions.quad_out,
            EasingType.QUAD_IN_OUT: EasingFunctions.quad_in_out,
            EasingType.CUBIC_IN: EasingFunctions.cubic_in,
            EasingType.CUBIC_OUT: EasingFunctions.cubic_out,
            EasingType.CUBIC_IN_OUT: EasingFunctions.cubic_in_out,
            EasingType.QUART_IN: EasingFunctions.quart_in,
            EasingType.QUART_OUT: EasingFunctions.quart_out,
            EasingType.QUART_IN_OUT: EasingFunctions.quart_in_out,
            EasingType.QUINT_IN: EasingFunctions.quint_in,
            EasingType.QUINT_OUT: EasingFunctions.quint_out,
            EasingType.QUINT_IN_OUT: EasingFunctions.quint_in_out,
            EasingType.SINE_IN: EasingFunctions.sine_in,
            EasingType.SINE_OUT: EasingFunctions.sine_out,
            EasingType.SINE_IN_OUT: EasingFunctions.sine_in_out,
            EasingType.CIRC_IN: EasingFunctions.circ_in,
            EasingType.CIRC_OUT: EasingFunctions.circ_out,
            EasingType.CIRC_IN_OUT: EasingFunctions.circ_in_out,
            EasingType.ELASTIC_IN: EasingFunctions.elastic_in,
            EasingType.ELASTIC_OUT: EasingFunctions.elastic_out,
            EasingType.ELASTIC_IN_OUT: EasingFunctions.elastic_in_out,
            EasingType.BOUNCE_IN: EasingFunctions.bounce_in,
            EasingType.BOUNCE_OUT: EasingFunctions.bounce_out,
            EasingType.BOUNCE_IN_OUT: EasingFunctions.bounce_in_out,
        }
        self._current_position: Optional[Point] = None
        self._trajectory_history: list[list[TrajectoryPoint]] = []

    def get_easing_func(self, easing_type: EasingType) -> Callable[[float], float]:
        """Get the easing function for a given type."""
        return self._easing_map.get(easing_type, EasingFunctions.linear)

    def generate_trajectory(
        self,
        start: Point,
        end: Point,
        config: SmoothMoveConfig
    ) -> list[TrajectoryPoint]:
        """Generate a smooth trajectory from start to end point."""
        easing_func = self.get_easing_func(config.easing)
        trajectory = []
        start_time = time.time()

        for step in range(config.steps + 1):
            t = step / config.steps
            eased_t = easing_func(t)

            x = start.x + (end.x - start.x) * eased_t
            y = start.y + (end.y - start.y) * eased_t

            trajectory.append(TrajectoryPoint(
                point=Point(x, y),
                timestamp=start_time + (config.duration_ms / 1000) * t,
                easing_value=eased_t
            ))

        return trajectory

    def move_to(
        self,
        target: Point,
        config: Optional[SmoothMoveConfig] = None,
        current_pos_func: Optional[Callable[[], Point]] = None
    ) -> list[TrajectoryPoint]:
        """
        Move smoothly from current position to target.

        Args:
            target: Target point to move to
            config: Movement configuration
            current_pos_func: Function to get current mouse position

        Returns:
            Generated trajectory points
        """
        if config is None:
            config = SmoothMoveConfig()

        if current_pos_func:
            self._current_position = current_pos_func()

        if self._current_position is None:
            self._current_position = Point(0, 0)

        trajectory = self.generate_trajectory(self._current_position, target, config)

        for tp in trajectory:
            pass

        self._trajectory_history.append(trajectory)
        self._current_position = target

        return trajectory

    def move_with_waypoints(
        self,
        waypoints: list[Point],
        config: Optional[SmoothMoveConfig] = None
    ) -> list[list[TrajectoryPoint]]:
        """
        Move through a series of waypoints.

        Args:
            waypoints: List of waypoints to traverse
            config: Movement configuration

        Returns:
            List of trajectories for each segment
        """
        if config is None:
            config = SmoothMoveConfig()

        all_trajectories = []
        current = self._current_position or Point(0, 0)

        for waypoint in waypoints:
            trajectory = self.generate_trajectory(current, waypoint, config)
            all_trajectories.append(trajectory)
            current = waypoint

        self._trajectory_history.extend(all_trajectories)
        if waypoints:
            self._current_position = waypoints[-1]

        return all_trajectories

    def get_trajectory_history(self) -> list[list[TrajectoryPoint]]:
        """Get history of all trajectories generated."""
        return self._trajectory_history

    def reset_position(self, position: Optional[Point] = None) -> None:
        """Reset the current position tracker."""
        self._current_position = position

    def get_total_distance(self, trajectory: list[TrajectoryPoint]) -> float:
        """Calculate total distance traveled along a trajectory."""
        if len(trajectory) < 2:
            return 0.0

        total = 0.0
        for i in range(1, len(trajectory)):
            total += trajectory[i].point.distance_to(trajectory[i - 1].point)
        return total

    def get_average_speed(
        self,
        trajectory: list[TrajectoryPoint]
    ) -> float:
        """Calculate average speed along trajectory in pixels per second."""
        distance = self.get_total_distance(trajectory)
        if len(trajectory) < 2:
            return 0.0

        duration = trajectory[-1].timestamp - trajectory[0].timestamp
        if duration <= 0:
            return 0.0

        return distance / duration


class BezierSplineMover:
    """
    Mouse mover using Bezier spline interpolation.

    Provides more natural curves through control points.
    """

    def __init__(self):
        self.mover = SmoothMouseMover()

    def move_with_control_points(
        self,
        start: Point,
        end: Point,
        control1: Point,
        control2: Optional[Point] = None,
        config: Optional[SmoothMoveConfig] = None
    ) -> list[TrajectoryPoint]:
        """Move using cubic Bezier spline through control points."""
        if config is None:
            config = SmoothMoveConfig()

        trajectory = []
        start_time = time.time()

        for step in range(config.steps + 1):
            t = step / config.steps

            if control2 is None:
                control2 = control1

            u = 1 - t
            tt = t * t
            uu = u * u
            uuu = uu * u
            ttt = tt * t

            x = (uuu * start.x +
                 3 * uu * t * control1.x +
                 3 * u * tt * control2.x +
                 ttt * end.x)

            y = (uuu * start.y +
                 3 * uu * t * control1.y +
                 3 * u * tt * control2.y +
                 ttt * end.y)

            trajectory.append(TrajectoryPoint(
                point=Point(x, y),
                timestamp=start_time + (config.duration_ms / 1000) * t
            ))

        self.mover._trajectory_history.append(trajectory)
        self.mover._current_position = end

        return trajectory


def create_smooth_mover() -> SmoothMouseMover:
    """Factory function to create a smooth mouse mover."""
    return SmoothMouseMover()


def create_bezier_mover() -> BezierSplineMover:
    """Factory function to create a Bezier spline mover."""
    return BezierSplineMover()

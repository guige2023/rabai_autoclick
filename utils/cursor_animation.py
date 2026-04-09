"""
Cursor Animation Module.

Provides utilities for animating cursor movements with various easing
functions, trajectory generation, and cursor trail effects.
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from typing import Callable, Protocol


logger = logging.getLogger(__name__)


class EasingFunction(Protocol):
    """Protocol for cursor easing functions."""

    def __call__(self, t: float) -> float:
        """
        Apply easing to a normalized time value.

        Args:
            t: Normalized time value (0.0 to 1.0).

        Returns:
            Eased time value.
        """
        ...


@dataclass
class CursorPosition:
    """Represents a cursor position at a point in time."""
    x: float
    y: float
    timestamp: float


@dataclass
class CursorTrajectory:
    """A trajectory of cursor positions over time."""
    points: list[CursorPosition] = field(default_factory=list)

    def add_point(self, x: float, y: float, timestamp: float | None = None) -> None:
        """
        Add a point to the trajectory.

        Args:
            x: X coordinate.
            y: Y coordinate.
            timestamp: Optional timestamp (defaults to current time).
        """
        if timestamp is None:
            timestamp = time.time()

        self.points.append(CursorPosition(x, y, timestamp))

    def get_point_at(self, time_target: float) -> CursorPosition | None:
        """
        Get interpolated position at a specific time.

        Args:
            time_target: Target timestamp.

        Returns:
            Interpolated CursorPosition or None.
        """
        if not self.points:
            return None

        if time_target <= self.points[0].timestamp:
            return self.points[0]

        if time_target >= self.points[-1].timestamp:
            return self.points[-1]

        for i in range(len(self.points) - 1):
            p1 = self.points[i]
            p2 = self.points[i + 1]

            if p1.timestamp <= time_target <= p2.timestamp:
                t = (time_target - p1.timestamp) / (p2.timestamp - p1.timestamp)
                x = p1.x + t * (p2.x - p1.x)
                y = p1.y + t * (p2.y - p1.y)

                return CursorPosition(x, y, time_target)

        return None

    def total_duration(self) -> float:
        """Get total duration of the trajectory."""
        if len(self.points) < 2:
            return 0.0

        return self.points[-1].timestamp - self.points[0].timestamp

    def total_distance(self) -> float:
        """Get total distance traveled."""
        if len(self.points) < 2:
            return 0.0

        distance = 0.0
        for i in range(len(self.points) - 1):
            dx = self.points[i + 1].x - self.points[i].x
            dy = self.points[i + 1].y - self.points[i].y
            distance += math.sqrt(dx * dx + dy * dy)

        return distance


class EasingFunctions:
    """Collection of common easing functions."""

    @staticmethod
    def linear(t: float) -> float:
        """Linear easing (no easing)."""
        return t

    @staticmethod
    def ease_in_quad(t: float) -> float:
        """Quadratic ease-in."""
        return t * t

    @staticmethod
    def ease_out_quad(t: float) -> float:
        """Quadratic ease-out."""
        return t * (2 - t)

    @staticmethod
    def ease_in_out_quad(t: float) -> float:
        """Quadratic ease-in-out."""
        return 2 * t * t if t < 0.5 else -1 + (4 - 2 * t) * t

    @staticmethod
    def ease_in_cubic(t: float) -> float:
        """Cubic ease-in."""
        return t * t * t

    @staticmethod
    def ease_out_cubic(t: float) -> float:
        """Cubic ease-out."""
        return (t - 1) ** 3 + 1

    @staticmethod
    def ease_in_out_cubic(t: float) -> float:
        """Cubic ease-in-out."""
        return 4 * t * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 3 / 2

    @staticmethod
    def ease_in_sine(t: float) -> float:
        """Sine ease-in."""
        return 1 - math.cos(t * math.pi / 2)

    @staticmethod
    def ease_out_sine(t: float) -> float:
        """Sine ease-out."""
        return math.sin(t * math.pi / 2)

    @staticmethod
    def ease_in_out_sine(t: float) -> float:
        """Sine ease-in-out."""
        return -(math.cos(math.pi * t) - 1) / 2

    @staticmethod
    def ease_out_elastic(t: float) -> float:
        """Elastic ease-out."""
        if t == 0:
            return 0.0
        if t == 1:
            return 1.0

        p = 0.3
        s = p / 4

        return math.pow(2, -10 * t) * math.sin((t - s) * (2 * math.pi) / p) + 1

    @staticmethod
    def ease_out_bounce(t: float) -> float:
        """Bounce ease-out."""
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


class CursorAnimator:
    """
    Animates cursor movements along trajectories with easing.

    Example:
        >>> animator = CursorAnimator()
        >>> animator.move_to(100, 200, duration=0.5)
        >>> animator.start()
    """

    def __init__(
        self,
        easing: EasingFunction | None = None,
        frame_rate: float = 60.0
    ) -> None:
        """
        Initialize the cursor animator.

        Args:
            easing: Easing function to use (defaults to ease_out_quad).
            frame_rate: Target frame rate for animation.
        """
        self.easing = easing or EasingFunctions.ease_out_quad
        self.frame_rate = frame_rate
        self.frame_duration = 1.0 / frame_rate

        self._current_x: float = 0.0
        self._current_y: float = 0.0
        self._is_animating: bool = False
        self._trajectory: CursorTrajectory | None = None

    def move_to(
        self,
        target_x: float,
        target_y: float,
        duration: float,
        start_x: float | None = None,
        start_y: float | None = None
    ) -> CursorTrajectory:
        """
        Create a trajectory for moving cursor to a target position.

        Args:
            target_x: Target X coordinate.
            target_y: Target Y coordinate.
            duration: Animation duration in seconds.
            start_x: Starting X coordinate (defaults to current).
            start_y: Starting Y coordinate (defaults to current).

        Returns:
            CursorTrajectory for the movement.
        """
        start = start_x if start_x is not None else self._current_x
        start_y_coord = start_y if start_y is not None else self._current_y

        trajectory = CursorTrajectory()
        trajectory.add_point(start, start_y_coord)

        num_frames = int(duration * self.frame_rate)

        for i in range(1, num_frames + 1):
            t = i / num_frames
            eased_t = self.easing(t)

            x = start + (target_x - start) * eased_t
            y = start_y_coord + (target_y - start_y_coord) * eased_t

            timestamp = time.time() + (i / num_frames) * duration
            trajectory.add_point(x, y, timestamp)

        self._trajectory = trajectory
        self._is_animating = True

        return trajectory

    def curve_to(
        self,
        control_x: float,
        control_y: float,
        target_x: float,
        target_y: float,
        duration: float,
        num_segments: int = 50
    ) -> CursorTrajectory:
        """
        Create a quadratic Bezier curve trajectory.

        Args:
            control_x: Control point X coordinate.
            control_y: Control point Y coordinate.
            target_x: Target X coordinate.
            target_y: Target Y coordinate.
            duration: Animation duration in seconds.
            num_segments: Number of curve segments.

        Returns:
            CursorTrajectory for the curve.
        """
        trajectory = CursorTrajectory()
        trajectory.add_point(self._current_x, self._current_y)

        for i in range(1, num_segments + 1):
            t = i / num_segments
            eased_t = self.easing(t)

            x = (1 - eased_t) ** 2 * self._current_x + \
                2 * (1 - eased_t) * eased_t * control_x + \
                eased_t ** 2 * target_x

            y = (1 - eased_t) ** 2 * self._current_y + \
                2 * (1 - eased_t) * eased_t * control_y + \
                eased_t ** 2 * target_y

            timestamp = time.time() + (i / num_segments) * duration
            trajectory.add_point(x, y, timestamp)

        self._trajectory = trajectory
        self._is_animating = True

        return trajectory

    def get_current_position(self) -> tuple[float, float]:
        """
        Get the current cursor position.

        Returns:
            Tuple of (x, y) coordinates.
        """
        return (self._current_x, self._current_y)

    def update(self, x: float, y: float) -> None:
        """
        Update the current cursor position.

        Args:
            x: New X coordinate.
            y: New Y coordinate.
        """
        self._current_x = x
        self._current_y = y

    def is_animating(self) -> bool:
        """Check if animation is in progress."""
        return self._is_animating

    def stop(self) -> None:
        """Stop the current animation."""
        self._is_animating = False

    def get_trajectory(self) -> CursorTrajectory | None:
        """Get the current trajectory."""
        return self._trajectory


class CursorTrailEffect:
    """
    Creates a trailing effect for cursor movements.

    Stores recent cursor positions and provides interpolation for
    smooth trail rendering.
    """

    def __init__(self, max_trail_length: int = 10) -> None:
        """
        Initialize the cursor trail effect.

        Args:
            max_trail_length: Maximum number of positions in trail.
        """
        self.max_trail_length = max_trail_length
        self._trail: list[CursorPosition] = []
        self._fade_factor: float = 0.8

    def add_position(self, x: float, y: float) -> None:
        """
        Add a position to the trail.

        Args:
            x: X coordinate.
            y: Y coordinate.
        """
        self._trail.append(CursorPosition(x, y, time.time()))

        while len(self._trail) > self.max_trail_length:
            self._trail.pop(0)

    def get_trail_points(self) -> list[CursorPosition]:
        """
        Get all trail points with interpolated positions.

        Returns:
            List of CursorPosition objects.
        """
        return list(self._trail)

    def get_opacities(self) -> list[float]:
        """
        Get opacity values for trail points based on age.

        Returns:
            List of opacity values (0.0 to 1.0).
        """
        if not self._trail:
            return []

        now = time.time()
        opacities: list[float] = []

        for point in self._trail:
            age = now - point.timestamp
            opacity = max(0.0, 1.0 - age * self._fade_factor)
            opacities.append(opacity)

        return opacities

    def clear(self) -> None:
        """Clear the trail."""
        self._trail.clear()

    def set_fade_factor(self, factor: float) -> None:
        """
        Set the fade factor for trail opacity.

        Args:
            factor: Fade rate (higher = faster fade).
        """
        self._fade_factor = factor


class TrajectoryGenerator:
    """
    Generates various cursor trajectory patterns.
    """

    @staticmethod
    def generate_circle(
        center_x: float,
        center_y: float,
        radius: float,
        duration: float,
        num_points: int = 100,
        easing: EasingFunction | None = None
    ) -> CursorTrajectory:
        """
        Generate a circular trajectory.

        Args:
            center_x: Center X coordinate.
            center_y: Center Y coordinate.
            radius: Circle radius.
            duration: Animation duration.
            num_points: Number of points.
            easing: Optional easing function.

        Returns:
            CursorTrajectory following a circle.
        """
        easing = easing or EasingFunctions.linear
        trajectory = CursorTrajectory()

        for i in range(num_points):
            t = i / num_points
            eased_t = easing(t)

            angle = eased_t * 2 * math.pi

            x = center_x + radius * math.cos(angle)
            y = center_y + radius * math.sin(angle)

            timestamp = time.time() + t * duration
            trajectory.add_point(x, y, timestamp)

        return trajectory

    @staticmethod
    def generate_spiral(
        center_x: float,
        center_y: float,
        start_radius: float,
        end_radius: float,
        duration: float,
        num_points: int = 100,
        easing: EasingFunction | None = None
    ) -> CursorTrajectory:
        """
        Generate a spiral trajectory.

        Args:
            center_x: Center X coordinate.
            center_y: Center Y coordinate.
            start_radius: Starting radius.
            end_radius: Ending radius.
            duration: Animation duration.
            num_points: Number of points.
            easing: Optional easing function.

        Returns:
            CursorTrajectory following a spiral.
        """
        easing = easing or EasingFunctions.linear
        trajectory = CursorTrajectory()

        for i in range(num_points):
            t = i / num_points
            eased_t = easing(t)

            angle = eased_t * 4 * math.pi
            radius = start_radius + (end_radius - start_radius) * eased_t

            x = center_x + radius * math.cos(angle)
            y = center_y + radius * math.sin(angle)

            timestamp = time.time() + t * duration
            trajectory.add_point(x, y, timestamp)

        return trajectory

    @staticmethod
    def generate_wave(
        start_x: float,
        start_y: float,
        end_x: float,
        amplitude: float,
        frequency: float,
        duration: float,
        num_points: int = 100,
        easing: EasingFunction | None = None
    ) -> CursorTrajectory:
        """
        Generate a wave trajectory.

        Args:
            start_x: Starting X coordinate.
            start_y: Starting Y coordinate.
            end_x: Ending X coordinate.
            amplitude: Wave amplitude.
            frequency: Wave frequency.
            duration: Animation duration.
            num_points: Number of points.
            easing: Optional easing function.

        Returns:
            CursorTrajectory following a wave.
        """
        easing = easing or EasingFunctions.linear
        trajectory = CursorTrajectory()

        for i in range(num_points):
            t = i / num_points
            eased_t = easing(t)

            x = start_x + (end_x - start_x) * eased_t
            y = start_y + amplitude * math.sin(frequency * eased_t * 2 * math.pi)

            timestamp = time.time() + t * duration
            trajectory.add_point(x, y, timestamp)

        return trajectory

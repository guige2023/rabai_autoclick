"""
Mouse Speed and Acceleration Utilities for UI Automation

Provides configurable mouse speed curves, acceleration profiles,
and natural movement simulation for realistic cursor behavior.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable


class SpeedProfile(Enum):
    """Predefined mouse speed profiles."""
    INSTANT = auto()
    LINEAR = auto()
    EASE_OUT = auto()
    EASE_IN = auto()
    EASE_IN_OUT = auto()
    NATURAL = auto()
    SPRING = auto()


@dataclass
class MouseSpeedConfig:
    """Configuration for mouse speed simulation."""
    profile: SpeedProfile = SpeedProfile.NATURAL
    base_speed: float = 500.0  # pixels per second
    acceleration: float = 2.0
    max_speed: float = 2000.0
    min_duration: float = 0.05  # minimum movement duration


@dataclass
class TrajectoryPoint:
    """A single point in a mouse trajectory."""
    x: float
    y: float
    timestamp: float


def get_speed_function(profile: SpeedProfile) -> Callable[[float], float]:
    """
    Get the speed function for a given profile.

    Args:
        profile: SpeedProfile enum value

    Returns:
        Function that takes progress (0-1) and returns speed multiplier
    """
    speed_functions: dict[SpeedProfile, Callable[[float], float]] = {
        SpeedProfile.INSTANT: lambda p: 1.0,
        SpeedProfile.LINEAR: lambda p: p,
        SpeedProfile.EASE_OUT: lambda p: 1.0 - (1.0 - p) ** 2,
        SpeedProfile.EASE_IN: lambda p: p ** 2,
        SpeedProfile.EASE_IN_OUT: lambda p: 2 * p if p < 0.5 else 1.0 - (-2 * p + 2) ** 2 / 2,
        SpeedProfile.NATURAL: lambda p: _natural_curve(p),
        SpeedProfile.SPRING: lambda p: _spring_curve(p),
    }
    return speed_functions.get(profile, lambda p: p)


def _natural_curve(progress: float) -> float:
    """Natural curve that simulates human mouse movement."""
    # Starts slow, speeds up in middle, slows at end
    if progress < 0.3:
        return (progress / 0.3) ** 1.5
    elif progress < 0.7:
        return 0.3 + (progress - 0.3) / 0.4
    else:
        return 0.7 + (1.0 - (1.0 - (progress - 0.7) / 0.3) ** 2) * 0.3


def _spring_curve(progress: float) -> float:
    """Spring-like curve with overshoot."""
    if progress >= 1.0:
        return 1.0
    # Damped spring oscillation
    omega = 10.0
    zeta = 0.5
    return 1.0 - math.exp(-zeta * omega * progress) * math.cos(omega * math.sqrt(1 - zeta**2) * progress)


class MouseSpeedSimulator:
    """
    Simulates mouse movement with configurable speed and acceleration.

    Produces natural-looking mouse trajectories that mimic human
    movement patterns for more realistic automation.
    """

    def __init__(self, config: MouseSpeedConfig | None = None) -> None:
        self.config = config or MouseSpeedConfig()

    def calculate_trajectory(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        num_points: int = 50,
    ) -> list[TrajectoryPoint]:
        """
        Calculate a mouse trajectory with speed profiling.

        Args:
            start_x: Starting X coordinate
            start_y: Starting Y coordinate
            end_x: Ending X coordinate
            end_y: Ending Y coordinate
            num_points: Number of intermediate points

        Returns:
            List of TrajectoryPoint objects forming the trajectory
        """
        distance = math.sqrt((end_x - start_x) ** 2 + (end_y - start_y) ** 2)

        if distance < 1.0:
            return [TrajectoryPoint(x=start_x, y=start_y, timestamp=time.time())]

        # Calculate duration based on distance and speed
        duration = max(
            self.config.min_duration,
            distance / self.config.base_speed,
        )

        speed_func = get_speed_function(self.config.profile)
        points: list[TrajectoryPoint] = []
        base_time = time.time()

        for i in range(num_points + 1):
            progress = i / num_points

            # Apply speed profile
            speed_progress = speed_func(progress)

            # Interpolate position
            x = start_x + (end_x - start_x) * speed_progress
            y = start_y + (end_y - start_y) * speed_progress

            # Timestamp progresses non-linearly too
            timestamp = base_time + duration * progress

            points.append(TrajectoryPoint(x=x, y=y, timestamp=timestamp))

        return points

    def get_mouse_events(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        interval: float = 0.016,
    ) -> list[tuple[float, float, float]]:
        """
        Get mouse move events at regular intervals.

        Args:
            start_x: Starting X coordinate
            start_y: Starting Y coordinate
            end_x: Ending X coordinate
            end_y: Ending Y coordinate
            interval: Time between events in seconds

        Returns:
            List of (x, y, timestamp) tuples
        """
        trajectory = self.calculate_trajectory(start_x, start_y, end_x, end_y)
        if not trajectory:
            return []

        events: list[tuple[float, float, float]] = []
        last_time = trajectory[0].timestamp

        for point in trajectory:
            if point.timestamp >= last_time + interval:
                events.append((point.x, point.y, point.timestamp))
                last_time = point.timestamp

        return events

    def set_profile(self, profile: SpeedProfile) -> None:
        """Change the speed profile."""
        self.config.profile = profile

    def set_base_speed(self, speed: float) -> None:
        """Set the base speed in pixels per second."""
        self.config.base_speed = max(1.0, min(speed, 10000.0))


def apply_curved_path(
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
    curve_strength: float = 0.3,
) -> tuple[float, float]:
    """
    Apply a slight curve to a straight line path.

    Args:
        start_x: Starting X coordinate
        start_y: Starting Y coordinate
        end_x: Ending X coordinate
        end_y: Ending Y coordinate
        curve_strength: Curve intensity (-1.0 to 1.0)

    Returns:
        Tuple of (control_x, control_y) for the curve control point
    """
    mid_x = (start_x + end_x) / 2
    mid_y = (start_y + end_y) / 2

    # Perpendicular offset
    dx = end_x - start_x
    dy = end_y - start_y
    length = math.sqrt(dx**2 + dy**2) or 1.0

    # Perpendicular direction
    perp_x = -dy / length
    perp_y = dx / length

    control_x = mid_x + perp_x * length * curve_strength
    control_y = mid_y + perp_y * length * curve_strength

    return control_x, control_y


def bezier_point(
    t: float,
    p0: tuple[float, float],
    p1: tuple[float, float],
    p2: tuple[float, float],
) -> tuple[float, float]:
    """
    Calculate point on a quadratic Bezier curve.

    Args:
        t: Parameter (0.0 to 1.0)
        p0: Start point (x, y)
        p1: Control point (x, y)
        p2: End point (x, y)

    Returns:
        Point on curve at parameter t
    """
    x = (1 - t) ** 2 * p0[0] + 2 * (1 - t) * t * p1[0] + t**2 * p2[0]
    y = (1 - t) ** 2 * p0[1] + 2 * (1 - t) * t * p1[1] + t**2 * p2[1]
    return x, y

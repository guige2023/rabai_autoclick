"""Mouse Velocity and Trajectory Utilities.

Analyzes and generates mouse movement trajectories with velocity profiles.
Supports Bezier curves, natural-looking movements, and velocity-based animations.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class VelocityProfile:
    """Velocity profile for mouse movement.

    Attributes:
        initial_velocity: Starting velocity in pixels per second.
        peak_velocity: Maximum velocity during movement.
        final_velocity: Ending velocity in pixels per second.
        acceleration: Acceleration rate (pixels per second squared).
        deceleration: Deceleration rate (pixels per second squared).
    """

    initial_velocity: float = 50.0
    peak_velocity: float = 500.0
    final_velocity: float = 10.0
    acceleration: float = 2000.0
    deceleration: float = 3000.0


@dataclass
class TrajectoryPoint:
    """A point along a mouse trajectory.

    Attributes:
        x: X coordinate.
        y: Y coordinate.
        timestamp: Time offset in milliseconds from trajectory start.
        velocity: Instantaneous velocity at this point.
    """

    x: float
    y: float
    timestamp: int
    velocity: float


class VelocityCalculator:
    """Calculates velocity metrics for mouse movements.

    Example:
        calc = VelocityCalculator()
        velocity = calc.calculate_instant_velocity(points[0], points[1], dt_ms)
    """

    @staticmethod
    def calculate_instant_velocity(
        p1: tuple[float, float],
        p2: tuple[float, float],
        dt_ms: int,
    ) -> float:
        """Calculate instantaneous velocity between two points.

        Args:
            p1: First point (x, y).
            p2: Second point (x, y).
            dt_ms: Time difference in milliseconds.

        Returns:
            Velocity in pixels per second.
        """
        if dt_ms == 0:
            return 0.0
        dx = p2[0] - p1[0]
        dy = p2[1] - p1[1]
        distance = math.sqrt(dx * dx + dy * dy)
        return distance / (dt_ms / 1000.0)

    @staticmethod
    def calculate_average_velocity(
        points: list[tuple[float, float]],
        timestamps: list[int],
    ) -> float:
        """Calculate average velocity over a trajectory.

        Args:
            points: List of (x, y) coordinates.
            timestamps: List of timestamps in milliseconds.

        Returns:
            Average velocity in pixels per second.
        """
        if len(points) < 2:
            return 0.0

        total_distance = sum(
            math.sqrt(sum((a - b) ** 2 for a, b in zip(p1, p2)))
            for p1, p2 in zip(points[:-1], points[1:])
        )

        total_time = (timestamps[-1] - timestamps[0]) / 1000.0
        if total_time == 0:
            return 0.0
        return total_distance / total_time

    @staticmethod
    def smooth_velocity(
        velocities: list[float],
        window_size: int = 3,
    ) -> list[float]:
        """Apply moving average smoothing to velocity data.

        Args:
            velocities: List of velocity values.
            window_size: Size of smoothing window (must be odd).

        Returns:
            Smoothed velocity values.
        """
        if window_size % 2 == 0:
            window_size += 1
        if window_size < 3:
            return velocities

        half = window_size // 2
        smoothed = []
        for i in range(len(velocities)):
            start = max(0, i - half)
            end = min(len(velocities), i + half + 1)
            smoothed.append(sum(velocities[start:end]) / (end - start))
        return smoothed


class TrajectoryGenerator:
    """Generates natural-looking mouse trajectories.

    Uses velocity profiles to create human-like movements.

    Example:
        generator = TrajectoryGenerator()
        points = generator.generate(
            start=(100, 100),
            end=(500, 400),
            duration_ms=500,
        )
    """

    def __init__(self, profile: Optional[VelocityProfile] = None):
        """Initialize generator with velocity profile.

        Args:
            profile: Velocity profile to use. Uses defaults if None.
        """
        self.profile = profile or VelocityProfile()

    def generate(
        self,
        start: tuple[float, float],
        end: tuple[float, float],
        duration_ms: int,
        num_points: int = 50,
    ) -> list[TrajectoryPoint]:
        """Generate a trajectory between two points.

        Args:
            start: Starting coordinates.
            end: Ending coordinates.
            duration_ms: Total movement duration in milliseconds.
            num_points: Number of points in the trajectory.

        Returns:
            List of TrajectoryPoint objects.
        """
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        total_distance = math.sqrt(dx * dx + dy * dy)

        if total_distance < 1:
            return [
                TrajectoryPoint(
                    x=start[0], y=start[1], timestamp=0, velocity=0
                )
            ]

        points = []
        velocities = self._generate_velocity_profile(duration_ms, total_distance)
        timestamps = self._generate_timestamps(duration_ms, num_points)

        cumulative_dist = 0.0
        for i, t in enumerate(timestamps):
            progress = t / duration_ms
            x = start[0] + dx * progress
            y = start[1] + dy * progress

            if i > 0:
                prev = points[i - 1]
                dt = t - prev.timestamp
                if dt > 0:
                    dist = math.sqrt((x - prev.x) ** 2 + (y - prev.y) ** 2)
                    cumulative_dist += dist
                    velocity = dist / (dt / 1000.0)
                else:
                    velocity = prev.velocity
            else:
                velocity = velocities[i] if i < len(velocities) else 0

            points.append(TrajectoryPoint(x=x, y=y, timestamp=t, velocity=velocity))

        return points

    def _generate_velocity_profile(
        self,
        duration_ms: int,
        distance: float,
    ) -> list[float]:
        """Generate velocity values along the trajectory.

        Args:
            duration_ms: Total duration in milliseconds.
            distance: Total distance to travel.

        Returns:
            List of velocity values.
        """
        p = self.profile
        if distance < 10:
            return [p.final_velocity] * 10

        accel_time = (p.peak_velocity - p.initial_velocity) / p.acceleration
        decel_time = (p.peak_velocity - p.final_velocity) / p.deceleration

        accel_dist = 0.5 * p.acceleration * accel_time ** 2
        decel_dist = 0.5 * p.deceleration * decel_time ** 2
        cruise_dist = distance - accel_dist - decel_dist
        cruise_time = cruise_dist / p.peak_velocity if p.peak_velocity > 0 else 0

        total_time = accel_time + cruise_time + decel_time
        if total_time > 0:
            scale = duration_ms / 1000.0 / total_time
            accel_time *= scale
            cruise_time *= scale
            decel_time *= scale

        velocities = []
        t = 0.0
        while t < duration_ms / 1000.0:
            if t < accel_time:
                vel = p.initial_velocity + p.acceleration * t
            elif t < accel_time + cruise_time:
                vel = p.peak_velocity
            else:
                remaining = duration_ms / 1000.0 - t
                vel = max(p.final_velocity, p.peak_velocity - p.deceleration * remaining)
            velocities.append(min(vel, p.peak_velocity * 1.2))
            t += 0.01

        return velocities

    def _generate_timestamps(
        self,
        duration_ms: int,
        num_points: int,
    ) -> list[int]:
        """Generate timestamp values for trajectory points.

        Args:
            duration_ms: Total duration in milliseconds.
            num_points: Number of points.

        Returns:
            List of timestamp values.
        """
        return [int(i * duration_ms / (num_points - 1)) for i in range(num_points)]

    def generate_with_noise(
        self,
        start: tuple[float, float],
        end: tuple[float, float],
        duration_ms: int,
        noise_amount: float = 2.0,
        seed: Optional[int] = None,
    ) -> list[TrajectoryPoint]:
        """Generate trajectory with added noise for naturalness.

        Args:
            start: Starting coordinates.
            end: Ending coordinates.
            duration_ms: Total movement duration in milliseconds.
            noise_amount: Maximum pixel displacement from noise.
            seed: Random seed for reproducibility.

        Returns:
            List of TrajectoryPoint with added noise.
        """
        import random
        rng = random.Random(seed)

        base_trajectory = self.generate(start, end, duration_ms)
        noisy_trajectory = []

        mid_x = (start[0] + end[0]) / 2
        mid_y = (start[1] + end[1]) / 2

        for point in base_trajectory:
            progress = point.timestamp / duration_ms
            envelope = math.sin(progress * math.pi)

            noise_x = rng.uniform(-noise_amount, noise_amount) * envelope
            noise_y = rng.uniform(-noise_amount, noise_amount) * envelope

            dist_to_mid = math.sqrt((point.x - mid_x) ** 2 + (point.y - mid_y) ** 2)
            max_offset = noise_amount * envelope
            if dist_to_mid > 0:
                scale = min(max_offset / dist_to_mid, 1.0)
                noise_x = (point.x - mid_x) * scale * rng.uniform(-1, 1)
                noise_y = (point.y - mid_y) * scale * rng.uniform(-1, 1)

            noisy_trajectory.append(
                TrajectoryPoint(
                    x=point.x + noise_x,
                    y=point.y + noise_y,
                    timestamp=point.timestamp,
                    velocity=point.velocity,
                )
            )

        return noisy_trajectory


class BezierTrajectoryGenerator:
    """Generates smooth trajectories using Bezier curves.

    Example:
        gen = BezierTrajectoryGenerator()
        points = gen.cubic_bezier(start, end, control1, control2, num_points=50)
    """

    @staticmethod
    def linear(
        start: tuple[float, float],
        end: tuple[float, float],
        num_points: int = 50,
    ) -> list[tuple[float, float]]:
        """Generate linear interpolation between points.

        Args:
            start: Starting coordinates.
            end: Ending coordinates.
            num_points: Number of interpolation points.

        Returns:
            List of (x, y) coordinates.
        """
        points = []
        for i in range(num_points):
            t = i / (num_points - 1)
            x = start[0] + (end[0] - start[0]) * t
            y = start[1] + (end[1] - start[1]) * t
            points.append((x, y))
        return points

    @staticmethod
    def quadratic(
        start: tuple[float, float],
        control: tuple[float, float],
        end: tuple[float, float],
        num_points: int = 50,
    ) -> list[tuple[float, float]]:
        """Generate quadratic Bezier curve.

        Args:
            start: Starting coordinates.
            control: Control point coordinates.
            end: Ending coordinates.
            num_points: Number of interpolation points.

        Returns:
            List of (x, y) coordinates.
        """
        points = []
        for i in range(num_points):
            t = i / (num_points - 1)
            one_mt = 1 - t
            x = one_mt ** 2 * start[0] + 2 * one_mt * t * control[0] + t ** 2 * end[0]
            y = one_mt ** 2 * start[1] + 2 * one_mt * t * control[1] + t ** 2 * end[1]
            points.append((x, y))
        return points

    @staticmethod
    def cubic(
        start: tuple[float, float],
        control1: tuple[float, float],
        control2: tuple[float, float],
        end: tuple[float, float],
        num_points: int = 50,
    ) -> list[tuple[float, float]]:
        """Generate cubic Bezier curve.

        Args:
            start: Starting coordinates.
            control1: First control point.
            control2: Second control point.
            end: Ending coordinates.
            num_points: Number of interpolation points.

        Returns:
            List of (x, y) coordinates.
        """
        points = []
        for i in range(num_points):
            t = i / (num_points - 1)
            one_mt = 1 - t
            x = (
                one_mt ** 3 * start[0]
                + 3 * one_mt ** 2 * t * control1[0]
                + 3 * one_mt * t ** 2 * control2[0]
                + t ** 3 * end[0]
            )
            y = (
                one_mt ** 3 * start[1]
                + 3 * one_mt ** 2 * t * control1[1]
                + 3 * one_mt * t ** 2 * control2[1]
                + t ** 3 * end[1]
            )
            points.append((x, y))
        return points

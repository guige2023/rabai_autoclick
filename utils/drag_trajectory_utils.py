"""
Drag trajectory utilities for computing smooth drag paths.

Provides trajectory computation with path smoothing,
waypoint interpolation, and physics-based movement.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class Waypoint:
    """A waypoint in a drag trajectory."""
    x: float
    y: float
    arrival_time_ms: float = 0.0


@dataclass
class TrajectoryPoint:
    """A point along the computed trajectory."""
    x: float
    y: float
    timestamp_ms: float
    velocity_x: float = 0.0
    velocity_y: float = 0.0


class DragTrajectoryComputer:
    """Computes smooth drag trajectories."""

    def __init__(
        self,
        interpolation_mode: str = "linear",  # "linear", "cubic", "bezier"
        velocity_profile: str = "constant",  # "constant", "ease_in", "ease_out", "ease_in_out"
    ):
        self.interpolation_mode = interpolation_mode
        self.velocity_profile = velocity_profile

    def compute_trajectory(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        duration_ms: float,
        num_points: int = 50,
    ) -> list[TrajectoryPoint]:
        """Compute a smooth trajectory from start to end.

        Args:
            start_x, start_y: Starting position
            end_x, end_y: Ending position
            duration_ms: Total duration
            num_points: Number of points in the trajectory

        Returns:
            List of TrajectoryPoints with positions and timestamps
        """
        trajectory = []
        prev_x, prev_y, prev_t = start_x, start_y, 0.0

        for i in range(num_points + 1):
            t = i / num_points

            # Apply velocity profile
            t = self._apply_velocity_profile(t)

            # Interpolate position
            x = start_x + (end_x - start_x) * t
            y = start_y + (end_y - start_y) * t

            timestamp_ms = duration_ms * t

            # Compute velocity
            dt = timestamp_ms - prev_t
            vx = (x - prev_x) / dt * 1000 if dt > 0 else 0.0
            vy = (y - prev_y) / dt * 1000 if dt > 0 else 0.0

            trajectory.append(TrajectoryPoint(
                x=x, y=y,
                timestamp_ms=timestamp_ms,
                velocity_x=vx,
                velocity_y=vy,
            ))

            prev_x, prev_y, prev_t = x, y, timestamp_ms

        return trajectory

    def compute_curved_trajectory(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        control_x: float,
        control_y: float,
        duration_ms: float,
        num_points: int = 50,
    ) -> list[TrajectoryPoint]:
        """Compute a curved trajectory using quadratic bezier.

        Args:
            control_x, control_y: Control point for the curve
        """
        trajectory = []
        prev_x, prev_y, prev_t = start_x, start_y, 0.0

        for i in range(num_points + 1):
            t = i / num_points
            t = self._apply_velocity_profile(t)

            # Quadratic bezier
            one_minus_t = 1 - t
            x = one_minus_t ** 2 * start_x + 2 * one_minus_t * t * control_x + t ** 2 * end_x
            y = one_minus_t ** 2 * start_y + 2 * one_minus_t * t * control_y + t ** 2 * end_y

            timestamp_ms = duration_ms * t

            dt = timestamp_ms - prev_t
            vx = (x - prev_x) / dt * 1000 if dt > 0 else 0.0
            vy = (y - prev_y) / dt * 1000 if dt > 0 else 0.0

            trajectory.append(TrajectoryPoint(
                x=x, y=y,
                timestamp_ms=timestamp_ms,
                velocity_x=vx,
                velocity_y=vy,
            ))

            prev_x, prev_y, prev_t = x, y, timestamp_ms

        return trajectory

    def compute_waypoint_trajectory(
        self,
        waypoints: list[Waypoint],
        duration_ms: float,
        num_points: int = 50,
    ) -> list[TrajectoryPoint]:
        """Compute trajectory through waypoints."""
        if len(waypoints) < 2:
            return []

        trajectory = []
        total_distance = self._compute_total_distance(waypoints)

        prev_x, prev_y, prev_t = waypoints[0].x, waypoints[0].y, 0.0

        for i in range(num_points + 1):
            t = i / num_points
            t = self._apply_velocity_profile(t)

            x, y = self._position_at_t(waypoints, t, total_distance)
            timestamp_ms = duration_ms * t

            dt = timestamp_ms - prev_t
            vx = (x - prev_x) / dt * 1000 if dt > 0 else 0.0
            vy = (y - prev_y) / dt * 1000 if dt > 0 else 0.0

            trajectory.append(TrajectoryPoint(
                x=x, y=y,
                timestamp_ms=timestamp_ms,
                velocity_x=vx,
                velocity_y=vy,
            ))

            prev_x, prev_y, prev_t = x, y, timestamp_ms

        return trajectory

    def _apply_velocity_profile(self, t: float) -> float:
        """Apply velocity/easing profile to normalized time."""
        if self.velocity_profile == "constant":
            return t
        elif self.velocity_profile == "ease_in":
            return t * t
        elif self.velocity_profile == "ease_out":
            return 1 - (1 - t) ** 2
        elif self.velocity_profile == "ease_in_out":
            return 2 * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 2 / 2
        return t

    def _compute_total_distance(self, waypoints: list[Waypoint]) -> float:
        """Compute total distance through waypoints."""
        total = 0.0
        for i in range(1, len(waypoints)):
            dx = waypoints[i].x - waypoints[i-1].x
            dy = waypoints[i].y - waypoints[i-1].y
            total += math.hypot(dx, dy)
        return total

    def _position_at_t(
        self,
        waypoints: list[Waypoint],
        t: float,
        total_distance: float,
    ) -> tuple[float, float]:
        """Get position at normalized time t."""
        if total_distance == 0:
            return waypoints[0].x, waypoints[0].y

        target_dist = t * total_distance
        accumulated = 0.0

        for i in range(1, len(waypoints)):
            dx = waypoints[i].x - waypoints[i-1].x
            dy = waypoints[i].y - waypoints[i-1].y
            segment_dist = math.hypot(dx, dy)

            if accumulated + segment_dist >= target_dist:
                segment_t = (target_dist - accumulated) / segment_dist
                x = waypoints[i-1].x + dx * segment_t
                y = waypoints[i-1].y + dy * segment_t
                return x, y

            accumulated += segment_dist

        return waypoints[-1].x, waypoints[-1].y


__all__ = ["DragTrajectoryComputer", "TrajectoryPoint", "Waypoint"]

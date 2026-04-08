"""Mouse trajectory utilities for UI automation.

Provides mouse path generation, smoothing, and analysis
for natural-looking mouse movement.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TrajectoryPoint:
    """A point in a mouse trajectory."""
    x: float
    y: float
    timestamp: float = 0.0

    def distance_to(self, other: TrajectoryPoint) -> float:
        """Euclidean distance to another point."""
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)


@dataclass
class MouseTrajectory:
    """A mouse movement trajectory.

    Attributes:
        points: List of trajectory points.
        start_x: Starting X coordinate.
        start_y: Starting Y coordinate.
        end_x: Ending X coordinate.
        end_y: Ending Y coordinate.
    """
    points: list[TrajectoryPoint] = field(default_factory=list)
    start_x: float = 0.0
    start_y: float = 0.0
    end_x: float = 0.0
    end_y: float = 0.0

    @property
    def path_length(self) -> float:
        """Total path length of the trajectory."""
        if len(self.points) < 2:
            return 0.0
        return sum(
            self.points[i].distance_to(self.points[i + 1])
            for i in range(len(self.points) - 1)
        )

    @property
    def displacement(self) -> float:
        """Direct distance from start to end."""
        return math.sqrt(
            (self.end_x - self.start_x) ** 2
            + (self.end_y - self.start_y) ** 2
        )

    @property
    def straightness(self) -> float:
        """Ratio of displacement to path length (1.0 = perfectly straight)."""
        path_len = self.path_length
        if path_len == 0:
            return 1.0
        return self.displacement / path_len


def generate_linear_trajectory(
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
    num_points: int = 10,
) -> MouseTrajectory:
    """Generate a straight-line trajectory."""
    trajectory = MouseTrajectory(
        start_x=start_x, start_y=start_y,
        end_x=end_x, end_y=end_y,
    )

    for i in range(num_points + 1):
        t = i / num_points
        x = start_x + (end_x - start_x) * t
        y = start_y + (end_y - start_y) * t
        trajectory.points.append(TrajectoryPoint(x=x, y=y))

    return trajectory


def generate_bezier_trajectory(
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
    control_x: float,
    control_y: float,
    num_points: int = 20,
) -> MouseTrajectory:
    """Generate a cubic Bezier curve trajectory."""
    trajectory = MouseTrajectory(
        start_x=start_x, start_y=start_y,
        end_x=end_x, end_y=end_y,
    )

    for i in range(num_points + 1):
        t = i / num_points
        u = 1 - t
        x = (u ** 3) * start_x + 3 * (u ** 2) * t * control_x \
            + 3 * u * (t ** 2) * control_x + (t ** 3) * end_x
        y = (u ** 3) * start_y + 3 * (u ** 2) * t * control_y \
            + 3 * u * (t ** 2) * control_y + (t ** 3) * end_y
        trajectory.points.append(TrajectoryPoint(x=x, y=y))

    return trajectory


def generate_arc_trajectory(
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
    arc_height: float = 50.0,
    num_points: int = 20,
) -> MouseTrajectory:
    """Generate an arc-shaped trajectory."""
    trajectory = MouseTrajectory(
        start_x=start_x, start_y=start_y,
        end_x=end_x, end_y=end_y,
    )

    mid_x = (start_x + end_x) / 2
    mid_y = (start_y + end_y) / 2

    dx = end_x - start_x
    dy = end_y - start_y
    length = math.sqrt(dx * dx + dy * dy)

    if length == 0:
        return trajectory

    nx = -dy / length * arc_height
    ny = dx / length * arc_height
    control_x = mid_x + nx
    control_y = mid_y + ny

    return generate_bezier_trajectory(
        start_x, start_y, end_x, end_y,
        control_x, control_y, num_points,
    )


def add_jitter(
    trajectory: MouseTrajectory,
    jitter_amount: float = 2.0,
) -> MouseTrajectory:
    """Add random jitter to a trajectory."""
    new_trajectory = MouseTrajectory(
        start_x=trajectory.start_x,
        start_y=trajectory.start_y,
        end_x=trajectory.end_x,
        end_y=trajectory.end_y,
    )

    for point in trajectory.points:
        jx = random.uniform(-jitter_amount, jitter_amount)
        jy = random.uniform(-jitter_amount, jitter_amount)
        new_trajectory.points.append(TrajectoryPoint(
            x=point.x + jx,
            y=point.y + jy,
            timestamp=point.timestamp,
        ))

    return new_trajectory


def smooth_trajectory(
    trajectory: MouseTrajectory,
    window_size: int = 3,
) -> MouseTrajectory:
    """Smooth a trajectory using moving average."""
    if len(trajectory.points) <= window_size:
        return trajectory

    new_trajectory = MouseTrajectory(
        start_x=trajectory.start_x,
        start_y=trajectory.start_y,
        end_x=trajectory.end_x,
        end_y=trajectory.end_y,
    )

    half = window_size // 2
    for i in range(len(trajectory.points)):
        start_i = max(0, i - half)
        end_i = min(len(trajectory.points), i + half + 1)
        window = trajectory.points[start_i:end_i]

        avg_x = sum(p.x for p in window) / len(window)
        avg_y = sum(p.y for p in window) / len(window)

        new_trajectory.points.append(TrajectoryPoint(
            x=avg_x,
            y=avg_y,
            timestamp=trajectory.points[i].timestamp,
        ))

    return new_trajectory


def resample_trajectory(
    trajectory: MouseTrajectory,
    num_points: int,
) -> MouseTrajectory:
    """Resample trajectory to a specific number of points."""
    if len(trajectory.points) < 2 or num_points < 2:
        return trajectory

    new_trajectory = MouseTrajectory(
        start_x=trajectory.start_x,
        start_y=trajectory.start_y,
        end_x=trajectory.end_x,
        end_y=trajectory.end_y,
    )

    segment_lengths = [
        trajectory.points[i].distance_to(trajectory.points[i + 1])
        for i in range(len(trajectory.points) - 1)
    ]
    total_length = sum(segment_lengths)

    if total_length == 0:
        return trajectory

    target_spacing = total_length / (num_points - 1)

    accumulated = 0.0
    seg_idx = 0
    remaining_in_seg = segment_lengths[0] if segment_lengths else 0.0

    new_trajectory.points.append(trajectory.points[0])

    for _ in range(num_points - 2):
        while seg_idx < len(segment_lengths) - 1 and accumulated + remaining_in_seg < target_spacing:
            accumulated += remaining_in_seg
            seg_idx += 1
            remaining_in_seg = segment_lengths[seg_idx]

        t = (target_spacing - accumulated) / remaining_in_seg if remaining_in_seg > 0 else 0
        p1 = trajectory.points[seg_idx]
        p2 = trajectory.points[seg_idx + 1]

        new_x = p1.x + (p2.x - p1.x) * t
        new_y = p1.y + (p2.y - p1.y) * t
        new_trajectory.points.append(TrajectoryPoint(x=new_x, y=new_y))

        accumulated = 0.0
        remaining_in_seg = segment_lengths[seg_idx] * (1 - t)

    new_trajectory.points.append(trajectory.points[-1])
    return new_trajectory

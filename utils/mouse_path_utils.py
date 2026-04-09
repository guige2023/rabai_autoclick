"""
Mouse path generation utilities for smooth cursor movements.

This module provides utilities for generating natural-looking mouse
paths with acceleration, deceleration, and curved trajectories.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import List, Tuple, Callable, Optional


@dataclass
class PathPoint:
    """A point in a mouse path with timing and button state."""
    x: float
    y: float
    timestamp: float = 0.0
    button_down: bool = False


class MousePathGenerator:
    """
    Generates natural mouse movement paths.

    Supports various movement styles including smooth curves,
    acceleration profiles, and realistic micro-movements.
    """

    def __init__(self) -> None:
        self._min_distance: float = 1.0
        self._noise_amplitude: float = 2.0

    def set_noise(self, amplitude: float) -> MousePathGenerator:
        """Set micro-movement noise amplitude."""
        self._noise_amplitude = max(0.0, amplitude)
        return self

    def generate_linear(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        duration: float = 0.3,
        num_points: int = 30,
    ) -> List[PathPoint]:
        """Generate a straight-line mouse path."""
        points: List[PathPoint] = []
        start_time = time.time()

        for i in range(num_points + 1):
            t = i / num_points
            x = x1 + (x2 - x1) * t
            y = y1 + (y2 - y1) * t

            # Add micro-noise for natural feel
            x += self._add_noise()
            y += self._add_noise()

            points.append(PathPoint(
                x=x,
                y=y,
                timestamp=start_time + t * duration,
            ))

        return points

    def generate_curved(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        control_x: float,
        control_y: float,
        duration: float = 0.3,
        num_points: int = 30,
    ) -> List[PathPoint]:
        """Generate a Bezier curve mouse path."""
        points: List[PathPoint] = []
        start_time = time.time()

        for i in range(num_points + 1):
            t = i / num_points
            # Cubic Bezier
            mt = 1 - t
            x = mt**3 * x1 + 3*mt**2*t * control_x + 3*mt*t**2 * control_x + t**3 * x2
            y = mt**3 * y1 + 3*mt**2*t * control_y + 3*mt*t**2 * control_y + t**3 * y2

            x += self._add_noise()
            y += self._add_noise()

            points.append(PathPoint(
                x=x,
                y=y,
                timestamp=start_time + t * duration,
            ))

        return points

    def generate_arc(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        arc_height: float = 50.0,
        duration: float = 0.3,
        num_points: int = 30,
    ) -> List[PathPoint]:
        """Generate an arc-shaped mouse path."""
        mid_x = (x1 + x2) / 2
        mid_y = (y1 + y2) / 2

        dx = x2 - x1
        dy = y2 - y1
        length = math.sqrt(dx*dx + dy*dy)
        if length == 0:
            length = 1

        # Perpendicular offset for arc
        offset = arc_height
        control_x = mid_x - (dy / length) * offset
        control_y = mid_y + (dx / length) * offset

        return self.generate_curved(x1, y1, x2, y2, control_x, control_y, duration, num_points)

    def generate_exponential(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        duration: float = 0.3,
        num_points: int = 30,
        smoothness: float = 0.5,
    ) -> List[PathPoint]:
        """Generate mouse path with exponential easing."""
        points: List[PathPoint] = []
        start_time = time.time()

        for i in range(num_points + 1):
            t = i / num_points
            # Exponential ease-out
            eased = 1 - math.exp(-t * 5 * smoothness + (1 - smoothness))
            eased = max(0.0, min(1.0, eased))

            x = x1 + (x2 - x1) * eased
            y = y1 + (y2 - y1) * eased

            x += self._add_noise()
            y += self._add_noise()

            points.append(PathPoint(
                x=x,
                y=y,
                timestamp=start_time + t * duration,
            ))

        return points

    def generate_human_like(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        duration: float = 0.5,
        num_points: int = 40,
    ) -> List[PathPoint]:
        """
        Generate human-like mouse movement with realistic characteristics.

        Includes:
        - Initial hesitation (reaction time)
        - Acceleration phase
        - Cruising
        - Deceleration phase
        - Small overshoot correction
        """
        points: List[PathPoint] = []
        start_time = time.time()

        dx = x2 - x1
        dy = y2 - y1
        distance = math.sqrt(dx*dx + dy*dy)

        # Human reaction time ~150-300ms
        reaction_time = 0.15
        actual_movement_time = duration - reaction_time

        for i in range(num_points + 1):
            raw_t = i / num_points

            # Add reaction delay at start
            if raw_t < 0.1:
                t = 0.0
            else:
                t = (raw_t - 0.1) / 0.9

            # Easing with slight overshoot
            eased = self._human_easing(t)

            # Add small secondary movement (correction)
            correction = math.sin(t * math.pi * 3) * 3 * (1 - t)

            x = x1 + dx * eased + dy / distance * correction if distance > 0 else x1
            y = y1 + dy * eased - dx / distance * correction if distance > 0 else y1

            timestamp = start_time + raw_t * duration

            points.append(PathPoint(
                x=x,
                y=y,
                timestamp=timestamp,
            ))

        return points

    def _human_easing(self, t: float) -> float:
        """Easing function mimicking human motor control."""
        # Quick acceleration, gradual deceleration
        if t < 0.3:
            return 0.5 * (t / 0.3) ** 2
        elif t < 0.7:
            return 0.5 + 0.5 * (t - 0.3) / 0.4
        else:
            progress = (t - 0.7) / 0.3
            return 1.0 - 0.5 * (1 - progress) ** 2

    def _add_noise(self) -> float:
        """Add small random offset for natural movement."""
        import random
        return (random.random() - 0.5) * 2 * self._noise_amplitude


class MousePathSmoother:
    """
    Smooths raw mouse paths to reduce jitter and improve naturalness.
    """

    def __init__(self) -> None:
        self._smoothing_factor: float = 0.3

    def set_smoothing(self, factor: float) -> MousePathSmoother:
        """Set smoothing factor (0.0-1.0)."""
        self._smoothing_factor = max(0.0, min(1.0, factor))
        return self

    def smooth(self, points: List[PathPoint]) -> List[PathPoint]:
        """Apply smoothing to path points."""
        if len(points) < 3:
            return points

        smoothed: List[PathPoint] = [points[0]]

        for i in range(1, len(points) - 1):
            prev = smoothed[-1]
            curr = points[i]
            next_p = points[i + 1]

            x = prev.x + self._smoothing_factor * (
                (curr.x + next_p.x) / 2 - prev.x
            )
            y = prev.y + self._smoothing_factor * (
                (curr.y + next_p.y) / 2 - prev.y
            )

            smoothed.append(PathPoint(
                x=x,
                y=y,
                timestamp=curr.timestamp,
                button_down=curr.button_down,
            ))

        smoothed.append(points[-1])
        return smoothed


def resample_path(
    points: List[PathPoint],
    num_points: int,
) -> List[PathPoint]:
    """
    Resample a path to have exactly num_points.

    Useful for normalizing paths before comparison or playback.
    """
    if not points:
        return []
    if len(points) == 1:
        return [points[0]] * num_points
    if len(points) == num_points:
        return points

    result: List[PathPoint] = []
    total_length = calculate_path_length(points)

    for i in range(num_points):
        target_distance = (i / (num_points - 1)) * total_length
        point = get_point_at_distance(points, target_distance)
        result.append(point)

    return result


def calculate_path_length(points: List[PathPoint]) -> float:
    """Calculate total length of a path."""
    if len(points) < 2:
        return 0.0

    total = 0.0
    for i in range(1, len(points)):
        dx = points[i].x - points[i-1].x
        dy = points[i].y - points[i-1].y
        total += math.sqrt(dx*dx + dy*dy)

    return total


def get_point_at_distance(
    points: List[PathPoint],
    target_distance: float,
) -> PathPoint:
    """Get the point at a specific distance along the path."""
    if not points:
        return PathPoint(0, 0)
    if len(points) == 1:
        return points[0]

    accumulated = 0.0

    for i in range(1, len(points)):
        dx = points[i].x - points[i-1].x
        dy = points[i].y - points[i-1].y
        segment_length = math.sqrt(dx*dx + dy*dy)

        if accumulated + segment_length >= target_distance:
            # Interpolate within this segment
            t = (target_distance - accumulated) / segment_length if segment_length > 0 else 0
            return PathPoint(
                x=points[i-1].x + dx * t,
                y=points[i-1].y + dy * t,
                timestamp=points[i-1].timestamp + t * (points[i].timestamp - points[i-1].timestamp),
            )

        accumulated += segment_length

    return points[-1]

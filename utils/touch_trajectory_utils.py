"""
Touch Trajectory Utilities for UI Automation.

This module provides utilities for recording, analyzing,
and smoothing touch trajectories in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple, Callable


@dataclass
class TrajectoryPoint:
    """A point in a touch trajectory."""
    x: float
    y: float
    timestamp: float
    pressure: float = 0.5
    velocity_x: float = 0.0
    velocity_y: float = 0.0
    speed: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class TrajectorySegment:
    """A segment of a trajectory between two points."""
    start: TrajectoryPoint
    end: TrajectoryPoint
    distance: float
    duration: float
    avg_speed: float
    direction: float


class TrajectoryRecorder:
    """Records touch trajectories for analysis and replay."""

    def __init__(self) -> None:
        self._points: List[TrajectoryPoint] = []
        self._is_recording: bool = False

    def start_recording(self) -> None:
        """Start recording a new trajectory."""
        self._points.clear()
        self._is_recording = True

    def add_point(
        self,
        x: float,
        y: float,
        timestamp: Optional[float] = None,
        pressure: float = 0.5,
    ) -> None:
        """Add a point to the trajectory."""
        if not self._is_recording:
            return

        if timestamp is None:
            timestamp = time.time()

        vx, vy, speed = 0.0, 0.0, 0.0
        if self._points:
            last = self._points[-1]
            dt = timestamp - last.timestamp
            if dt > 0:
                vx = (x - last.x) / dt
                vy = (y - last.y) / dt
                speed = math.sqrt(vx * vx + vy * vy)

        point = TrajectoryPoint(
            x=x,
            y=y,
            timestamp=timestamp,
            pressure=pressure,
            velocity_x=vx,
            velocity_y=vy,
            speed=speed,
        )
        self._points.append(point)

    def stop_recording(self) -> List[TrajectoryPoint]:
        """Stop recording and return the trajectory."""
        self._is_recording = False
        return list(self._points)

    def get_points(self) -> List[TrajectoryPoint]:
        """Get all recorded points."""
        return list(self._points)

    def clear(self) -> None:
        """Clear all recorded points."""
        self._points.clear()
        self._is_recording = False

    def get_total_distance(self) -> float:
        """Calculate total distance traveled."""
        if len(self._points) < 2:
            return 0.0

        total = 0.0
        for i in range(1, len(self._points)):
            dx = self._points[i].x - self._points[i - 1].x
            dy = self._points[i].y - self._points[i - 1].y
            total += math.sqrt(dx * dx + dy * dy)

        return total

    def get_total_duration(self) -> float:
        """Get total duration of the trajectory."""
        if len(self._points) < 2:
            return 0.0

        return self._points[-1].timestamp - self._points[0].timestamp

    def get_average_speed(self) -> float:
        """Calculate average speed across the trajectory."""
        distance = self.get_total_distance()
        duration = self.get_total_duration()
        return distance / duration if duration > 0 else 0.0

    def get_segments(self) -> List[TrajectorySegment]:
        """Get all trajectory segments."""
        if len(self._points) < 2:
            return []

        segments = []
        for i in range(1, len(self._points)):
            p0 = self._points[i - 1]
            p1 = self._points[i]
            dx = p1.x - p0.x
            dy = p1.y - p0.y
            distance = math.sqrt(dx * dx + dy * dy)
            duration = p1.timestamp - p0.timestamp
            avg_speed = distance / duration if duration > 0 else 0.0
            direction = math.atan2(dy, dx)

            segments.append(
                TrajectorySegment(
                    start=p0,
                    end=p1,
                    distance=distance,
                    duration=duration,
                    avg_speed=avg_speed,
                    direction=direction,
                )
            )

        return segments

    def get_bounding_box(self) -> Tuple[float, float, float, float]:
        """Get bounding box as (min_x, min_y, max_x, max_y)."""
        if not self._points:
            return (0.0, 0.0, 0.0, 0.0)

        xs = [p.x for p in self._points]
        ys = [p.y for p in self._points]
        return (min(xs), min(ys), max(xs), max(ys))

    def is_recording(self) -> bool:
        """Check if currently recording."""
        return self._is_recording


class TrajectorySmoother:
    """Smooths trajectories using various algorithms."""

    @staticmethod
    def moving_average(
        points: List[TrajectoryPoint],
        window_size: int = 3,
    ) -> List[TrajectoryPoint]:
        """Apply moving average smoothing to a trajectory."""
        if len(points) <= window_size:
            return list(points)

        smoothed = []
        half = window_size // 2

        for i in range(len(points)):
            start = max(0, i - half)
            end = min(len(points), i + half + 1)
            window = points[start:end]

            avg_x = sum(p.x for p in window) / len(window)
            avg_y = sum(p.y for p in window) / len(window)

            new_point = TrajectoryPoint(
                x=avg_x,
                y=avg_y,
                timestamp=points[i].timestamp,
                pressure=points[i].pressure,
            )
            smoothed.append(new_point)

        return smoothed

    @staticmethod
    def gaussian_smooth(
        points: List[TrajectoryPoint],
        sigma: float = 1.0,
    ) -> List[TrajectoryPoint]:
        """Apply Gaussian smoothing to a trajectory."""
        if len(points) < 3 or sigma <= 0:
            return list(points)

        smoothed = []
        k = int(3 * sigma + 0.5)

        for i in range(len(points)):
            start = max(0, i - k)
            end = min(len(points), i + k + 1)
            window = points[start:end]

            weights = []
            for j in range(start, end):
                dist = abs(j - i)
                weights.append(math.exp(-(dist * dist) / (2 * sigma * sigma)))

            total_weight = sum(weights)
            avg_x = sum(p.x * w for p, w in zip(window, weights)) / total_weight
            avg_y = sum(p.y * w for p, w in zip(window, weights)) / total_weight

            smoothed.append(
                TrajectoryPoint(
                    x=avg_x,
                    y=avg_y,
                    timestamp=points[i].timestamp,
                    pressure=points[i].pressure,
                )
            )

        return smoothed

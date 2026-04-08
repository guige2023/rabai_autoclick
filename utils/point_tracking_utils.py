"""Point tracking utilities.

This module provides utilities for tracking point movements,
computing velocities, and analyzing movement patterns.
"""

from __future__ import annotations

import math
import time
from typing import List, Optional, Tuple
from dataclasses import dataclass, field


@dataclass
class TrackedPoint:
    """A tracked point with position and timestamp."""
    x: float
    y: float
    timestamp: float
    pressure: float = 1.0


@dataclass
class PointTrajectory:
    """A trajectory of tracked points."""
    points: List[TrackedPoint] = field(default_factory=list)

    def add(self, point: TrackedPoint) -> None:
        self.points.append(point)

    def clear(self) -> None:
        self.points.clear()

    @property
    def is_empty(self) -> bool:
        return len(self.points) == 0

    @property
    def start_point(self) -> Optional[Tuple[float, float]]:
        if self.points:
            return (self.points[0].x, self.points[0].y)
        return None

    @property
    def end_point(self) -> Optional[Tuple[float, float]]:
        if self.points:
            p = self.points[-1]
            return (p.x, p.y)
        return None

    def total_distance(self) -> float:
        if len(self.points) < 2:
            return 0.0
        return sum(
            _distance(self.points[i], self.points[i + 1])
            for i in range(len(self.points) - 1)
        )

    def average_velocity(self) -> float:
        if len(self.points) < 2:
            return 0.0
        total_dist = self.total_distance()
        duration = self.points[-1].timestamp - self.points[0].timestamp
        if duration == 0:
            return 0.0
        return total_dist / duration

    def instantaneous_velocity(self, index: int) -> float:
        if index <= 0 or index >= len(self.points):
            return 0.0
        p1 = self.points[index - 1]
        p2 = self.points[index]
        dist = _distance(p1, p2)
        dt = p2.timestamp - p1.timestamp
        if dt == 0:
            return 0.0
        return dist / dt

    def direction_at(self, index: int) -> Optional[float]:
        if index <= 0 or index >= len(self.points):
            return None
        p1 = self.points[index - 1]
        p2 = self.points[index]
        dx = p2.x - p1.x
        dy = p2.y - p1.y
        return math.atan2(dy, dx)


def _distance(p1: TrackedPoint, p2: TrackedPoint) -> float:
    dx = p2.x - p1.x
    dy = p2.y - p1.y
    return math.sqrt(dx * dx + dy * dy)


class PointTracker:
    """Tracks point movements over time."""

    def __init__(self, max_points: int = 1000) -> None:
        self._trajectories: List[PointTrajectory] = []
        self._current_trajectory: Optional[PointTrajectory] = None
        self._tracking = False
        self._max_points = max_points

    def start_tracking(self) -> PointTrajectory:
        trajectory = PointTrajectory()
        self._current_trajectory = trajectory
        self._tracking = True
        return trajectory

    def add_point(
        self,
        x: float,
        y: float,
        timestamp: Optional[float] = None,
        pressure: float = 1.0,
    ) -> None:
        if not self._tracking or not self._current_trajectory:
            return
        if timestamp is None:
            timestamp = time.time()
        point = TrackedPoint(x=x, y=y, timestamp=timestamp, pressure=pressure)
        self._current_trajectory.add(point)
        if len(self._current_trajectory.points) > self._max_points:
            self._current_trajectory.points.pop(0)

    def stop_tracking(self) -> Optional[PointTrajectory]:
        self._tracking = False
        if self._current_trajectory:
            self._trajectories.append(self._current_trajectory)
            result = self._current_trajectory
            self._current_trajectory = None
            return result
        return None

    @property
    def current_trajectory(self) -> Optional[PointTrajectory]:
        return self._current_trajectory

    @property
    def trajectories(self) -> List[PointTrajectory]:
        return self._trajectories.copy()

    def clear(self) -> None:
        self._trajectories.clear()
        self._current_trajectory = None


__all__ = [
    "TrackedPoint",
    "PointTrajectory",
    "PointTracker",
]

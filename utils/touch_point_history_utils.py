"""
Touch point history utilities for tracking touch trajectories.

Provides touch point history management with smoothing,
trajectory analysis, and velocity computation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TouchPoint:
    """A single touch point with timestamp."""
    x: float
    y: float
    timestamp_ms: float
    pressure: float = 1.0
    finger_id: int = 0
    phase: str = "moved"  # "began", "moved", "ended", "cancelled"


@dataclass
class TouchTrajectory:
    """A trajectory of touch points."""
    finger_id: int
    points: list[TouchPoint] = field(default_factory=list)

    def add(self, point: TouchPoint) -> None:
        self.points.append(point)

    @property
    def is_complete(self) -> bool:
        return len(self.points) > 0 and self.points[-1].phase in ("ended", "cancelled")

    @property
    def start_point(self) -> Optional[TouchPoint]:
        return self.points[0] if self.points else None

    @property
    def end_point(self) -> Optional[TouchPoint]:
        return self.points[-1] if self.points else None

    def total_distance(self) -> float:
        """Compute total distance traveled."""
        if len(self.points) < 2:
            return 0.0
        total = 0.0
        for i in range(1, len(self.points)):
            dx = self.points[i].x - self.points[i-1].x
            dy = self.points[i].y - self.points[i-1].y
            total += math.hypot(dx, dy)
        return total

    def duration_ms(self) -> float:
        """Get total duration in milliseconds."""
        if len(self.points) < 2:
            return 0.0
        return self.points[-1].timestamp_ms - self.points[0].timestamp_ms

    def average_velocity(self) -> float:
        """Compute average velocity (pixels/ms)."""
        duration = self.duration_ms()
        if duration <= 0:
            return 0.0
        return self.total_distance() / duration

    def instantaneous_velocity(self, index: int) -> float:
        """Compute instantaneous velocity at a point index."""
        if index < 1 or index >= len(self.points):
            return 0.0
        p1 = self.points[index - 1]
        p2 = self.points[index]
        dx = p2.x - p1.x
        dy = p2.y - p1.y
        dt = p2.timestamp_ms - p1.timestamp_ms
        if dt <= 0:
            return 0.0
        return math.hypot(dx, dy) / dt


class TouchHistoryManager:
    """Manages touch history for multiple fingers."""

    def __init__(self, max_points_per_finger: int = 1000):
        self.max_points_per_finger = max_points_per_finger
        self._trajectories: dict[int, TouchTrajectory] = {}

    def begin_touch(self, finger_id: int, x: float, y: float, timestamp_ms: float, pressure: float = 1.0) -> None:
        """Start a new touch trajectory."""
        traj = TouchTrajectory(finger_id=finger_id)
        traj.add(TouchPoint(x=x, y=y, timestamp_ms=timestamp_ms, pressure=pressure, finger_id=finger_id, phase="began"))
        self._trajectories[finger_id] = traj

    def update_touch(self, finger_id: int, x: float, y: float, timestamp_ms: float, pressure: float = 1.0) -> None:
        """Update an existing touch trajectory."""
        if finger_id not in self._trajectories:
            self.begin_touch(finger_id, x, y, timestamp_ms, pressure)
            return

        traj = self._trajectories[finger_id]
        traj.add(TouchPoint(x=x, y=y, timestamp_ms=timestamp_ms, pressure=pressure, finger_id=finger_id, phase="moved"))

        if len(traj.points) > self.max_points_per_finger:
            traj.points = traj.points[-self.max_points_per_finger:]

    def end_touch(self, finger_id: int, x: float, y: float, timestamp_ms: float) -> Optional[TouchTrajectory]:
        """End a touch trajectory and return it."""
        if finger_id not in self._trajectories:
            return None

        traj = self._trajectories[finger_id]
        traj.add(TouchPoint(x=x, y=y, timestamp_ms=timestamp_ms, finger_id=finger_id, phase="ended"))
        return traj

    def get_trajectory(self, finger_id: int) -> Optional[TouchTrajectory]:
        """Get a trajectory by finger ID."""
        return self._trajectories.get(finger_id)

    def get_active_trajectories(self) -> list[TouchTrajectory]:
        """Get all active (not yet ended) trajectories."""
        return [t for t in self._trajectories.values() if not t.is_complete]

    def get_all_trajectories(self) -> list[TouchTrajectory]:
        """Get all trajectories."""
        return list(self._trajectories.values())

    def clear(self) -> None:
        """Clear all trajectories."""
        self._trajectories.clear()

    def clear_ended(self) -> None:
        """Clear ended trajectories."""
        self._trajectories = {k: v for k, v in self._trajectories.items() if not v.is_complete}


__all__ = ["TouchHistoryManager", "TouchTrajectory", "TouchPoint"]

"""
Touch point tracker utilities.

Track touch points over time for gesture analysis.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TrackedPoint:
    """A tracked touch point."""
    finger_id: int
    x: float
    y: float
    timestamp: float
    pressure: float = 1.0
    velocity_x: float = 0.0
    velocity_y: float = 0.0


@dataclass
class TouchTrail:
    """Trail of tracked points for a finger."""
    finger_id: int
    points: list[TrackedPoint] = field(default_factory=list)
    
    def add_point(self, x: float, y: float, timestamp: float, pressure: float = 1.0) -> None:
        """Add a point to the trail."""
        velocity_x, velocity_y = 0.0, 0.0
        
        if self.points:
            last = self.points[-1]
            dt = timestamp - last.timestamp
            if dt > 0:
                velocity_x = (x - last.x) / dt
                velocity_y = (y - last.y) / dt
        
        point = TrackedPoint(
            finger_id=self.finger_id,
            x=x,
            y=y,
            timestamp=timestamp,
            pressure=pressure,
            velocity_x=velocity_x,
            velocity_y=velocity_y
        )
        self.points.append(point)
    
    def get_length(self) -> float:
        """Get total path length."""
        if len(self.points) < 2:
            return 0.0
        
        total = 0.0
        for i in range(1, len(self.points)):
            dx = self.points[i].x - self.points[i-1].x
            dy = self.points[i].y - self.points[i-1].y
            total += math.sqrt(dx * dx + dy * dy)
        
        return total
    
    def get_displacement(self) -> tuple[float, float]:
        """Get total displacement (start to end)."""
        if len(self.points) < 2:
            return 0.0, 0.0
        
        return (
            self.points[-1].x - self.points[0].x,
            self.points[-1].y - self.points[0].y
        )
    
    def get_average_velocity(self) -> float:
        """Get average velocity."""
        if len(self.points) < 2:
            return 0.0
        
        total = 0.0
        for point in self.points:
            total += math.sqrt(point.velocity_x ** 2 + point.velocity_y ** 2)
        
        return total / len(self.points)
    
    def clear(self) -> None:
        """Clear all points."""
        self.points.clear()


class TouchPointTracker:
    """Track multiple touch points over time."""
    
    def __init__(self, max_points_per_trail: int = 1000):
        self._trails: dict[int, TouchTrail] = {}
        self._max_points = max_points_per_trail
    
    def track_point(
        self,
        finger_id: int,
        x: float,
        y: float,
        timestamp: float,
        pressure: float = 1.0
    ) -> None:
        """Track a new touch point."""
        if finger_id not in self._trails:
            self._trails[finger_id] = TouchTrail(finger_id=finger_id)
        
        trail = self._trails[finger_id]
        trail.add_point(x, y, timestamp, pressure)
        
        if len(trail.points) > self._max_points:
            trail.points.pop(0)
    
    def end_trail(self, finger_id: int) -> Optional[TouchTrail]:
        """End tracking for a finger and return the trail."""
        if finger_id in self._trails:
            trail = self._trails[finger_id]
            del self._trails[finger_id]
            return trail
        return None
    
    def get_trail(self, finger_id: int) -> Optional[TouchTrail]:
        """Get trail for a finger."""
        return self._trails.get(finger_id)
    
    def get_all_trails(self) -> list[TouchTrail]:
        """Get all active trails."""
        return list(self._trails.values())
    
    def clear(self) -> None:
        """Clear all trails."""
        self._trails.clear()

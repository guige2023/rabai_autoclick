"""Touch point management utilities."""

from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass, field
import time


@dataclass
class TouchPoint:
    """Represents a single touch point."""
    id: int
    x: float
    y: float
    pressure: float = 1.0
    timestamp: float = field(default_factory=time.time)
    active: bool = True

    def distance_to(self, other: "TouchPoint") -> float:
        """Calculate distance to another touch point."""
        dx = self.x - other.x
        dy = self.y - other.y
        return (dx * dx + dy * dy) ** 0.5

    def midpoint_to(self, other: "TouchPoint") -> Tuple[float, float]:
        """Get midpoint between this and another touch point."""
        return ((self.x + other.x) / 2, (self.y + other.y) / 2)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "x": self.x,
            "y": self.y,
            "pressure": self.pressure,
            "timestamp": self.timestamp,
            "active": self.active,
        }


class TouchSequence:
    """Tracks a sequence of touch points over time."""

    def __init__(self, max_points: int = 1000):
        """Initialize touch sequence.
        
        Args:
            max_points: Maximum points to retain.
        """
        self.max_points = max_points
        self.points: List[TouchPoint] = []

    def add_point(self, point: TouchPoint) -> None:
        """Add a touch point to the sequence."""
        self.points.append(point)
        if len(self.points) > self.max_points:
            self.points = self.points[-self.max_points:]

    def get_points_by_id(self, touch_id: int) -> List[TouchPoint]:
        """Get all points for a specific touch ID."""
        return [p for p in self.points if p.id == touch_id]

    def get_velocity(self, touch_id: int) -> Optional[float]:
        """Calculate average velocity for a touch ID."""
        pts = self.get_points_by_id(touch_id)
        if len(pts) < 2:
            return None
        total_dist = sum(pts[i].distance_to(pts[i+1]) for i in range(len(pts) - 1))
        total_time = pts[-1].timestamp - pts[0].timestamp
        if total_time == 0:
            return None
        return total_dist / total_time

    def get_duration(self, touch_id: int) -> Optional[float]:
        """Get duration of a touch sequence."""
        pts = self.get_points_by_id(touch_id)
        if len(pts) < 2:
            return None
        return pts[-1].timestamp - pts[0].timestamp

    def get_displacement(self, touch_id: int) -> Optional[Tuple[float, float]]:
        """Get total displacement for a touch ID."""
        pts = self.get_points_by_id(touch_id)
        if len(pts) < 2:
            return None
        return (pts[-1].x - pts[0].x, pts[-1].y - pts[0].y)

    def clear(self) -> None:
        """Clear all points."""
        self.points.clear()


class MultiTouchTracker:
    """Tracks multiple simultaneous touches."""

    def __init__(self):
        """Initialize multi-touch tracker."""
        self.active_touches: Dict[int, TouchPoint] = {}
        self.sequence = TouchSequence()

    def touch_down(self, touch_id: int, x: float, y: float, pressure: float = 1.0) -> TouchPoint:
        """Record touch down event.
        
        Args:
            touch_id: Touch identifier.
            x, y: Coordinates.
            pressure: Touch pressure.
        
        Returns:
            Created touch point.
        """
        point = TouchPoint(id=touch_id, x=x, y=y, pressure=pressure)
        self.active_touches[touch_id] = point
        self.sequence.add_point(point)
        return point

    def touch_move(self, touch_id: int, x: float, y: float, pressure: float = 1.0) -> Optional[TouchPoint]:
        """Record touch move event.
        
        Args:
            touch_id: Touch identifier.
            x, y: New coordinates.
            pressure: Touch pressure.
        
        Returns:
            Updated touch point or None if not tracked.
        """
        if touch_id not in self.active_touches:
            return None
        point = TouchPoint(id=touch_id, x=x, y=y, pressure=pressure)
        self.active_touches[touch_id] = point
        self.sequence.add_point(point)
        return point

    def touch_up(self, touch_id: int) -> Optional[TouchPoint]:
        """Record touch up event.
        
        Args:
            touch_id: Touch identifier.
        
        Returns:
            Final touch point or None if not tracked.
        """
        if touch_id not in self.active_touches:
            return None
        point = self.active_touches[touch_id]
        point.active = False
        del self.active_touches[touch_id]
        return point

    def get_active_count(self) -> int:
        """Get number of active touches."""
        return len(self.active_touches)

    def get_pinch_distance(self) -> Optional[float]:
        """Get distance between two active touches (for pinch detection)."""
        if len(self.active_touches) < 2:
            return None
        pts = list(self.active_touches.values())
        return pts[0].distance_to(pts[1])

    def reset(self) -> None:
        """Reset all tracking data."""
        self.active_touches.clear()
        self.sequence.clear()

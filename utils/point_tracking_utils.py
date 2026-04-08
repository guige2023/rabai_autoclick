"""Point tracking and gesture recognition utilities.

Provides tools for tracking mouse movement paths,
recognizing simple gestures, and analyzing point
sequences for automation trigger patterns.

Example:
    >>> from utils.point_tracking_utils import PointTracker, GestureRecognizer
    >>> tracker = PointTracker(max_points=100)
    >>> tracker.add(100, 100)
    >>> tracker.add(150, 150)
    >>> gesture = GestureRecognizer.recognize(tracker.points)
    >>> print(f"Detected: {gesture}")
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Optional

__all__ = [
    "Point3D",
    "PointTracker",
    "GestureRecognizer",
    "GestureType",
    "MovementAnalyzer",
]


@dataclass
class Point3D:
    """A timestamped 2D point."""

    x: float
    y: float
    t: float = field(default_factory=time.monotonic)

    def distance_to(self, other: "Point3D") -> float:
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)


class GestureType:
    """Known gesture types."""

    UNKNOWN = "unknown"
    STRAIGHT_LINE = "straight"
    CURVE = "curve"
    ZIGZAG = "zigzag"
    CIRCLE = "circle"
    SPIRAL = "spiral"
    SCROLL_UP = "scroll_up"
    SCROLL_DOWN = "scroll_down"
    SCROLL_LEFT = "scroll_left"
    SCROLL_RIGHT = "scroll_right"
    CLICK = "click"
    DOUBLE_CLICK = "double_click"
    DRAG = "drag"


class PointTracker:
    """Tracks a sequence of mouse/touch points over time.

    Example:
        >>> tracker = PointTracker(max_points=200)
        >>> tracker.add(100, 200)
        >>> tracker.add(105, 210)
        >>> print(f"Distance: {tracker.total_distance():.1f}")
    """

    def __init__(self, max_points: int = 500):
        self.max_points = max_points
        self._points: list[Point3D] = []
        self._start_time: Optional[float] = None

    def add(self, x: float, y: float, t: Optional[float] = None) -> None:
        """Add a point to the track.

        Args:
            x: X coordinate.
            y: Y coordinate.
            t: Optional timestamp (defaults to time.monotonic).
        """
        if t is None:
            t = time.monotonic()

        if self._start_time is None:
            self._start_time = t

        self._points.append(Point3D(x=x, y=y, t=t))
        if len(self._points) > self.max_points:
            self._points.pop(0)

    def clear(self) -> None:
        """Clear all tracked points."""
        self._points = []
        self._start_time = None

    @property
    def points(self) -> list[Point3D]:
        return list(self._points)

    @property
    def count(self) -> int:
        return len(self._points)

    @property
    def duration(self) -> float:
        """Total tracking duration in seconds."""
        if len(self._points) < 2:
            return 0.0
        return self._points[-1].t - self._points[0].t

    def total_distance(self) -> float:
        """Total path distance traveled."""
        if len(self._points) < 2:
            return 0.0
        return sum(
            self._points[i].distance_to(self._points[i + 1])
            for i in range(len(self._points) - 1)
        )

    def average_speed(self) -> float:
        """Average speed in pixels per second."""
        dur = self.duration
        if dur == 0:
            return 0.0
        return self.total_distance() / dur

    def bounding_box(self) -> tuple[float, float, float, float]:
        """Get the bounding box of tracked points.

        Returns:
            (min_x, min_y, max_x, max_y).
        """
        if not self._points:
            return (0, 0, 0, 0)
        xs = [p.x for p in self._points]
        ys = [p.y for p in self._points]
        return (min(xs), min(ys), max(xs), max(ys))

    def direction_at(self, index: int) -> Optional[tuple[float, float]]:
        """Get the direction vector at a point index.

        Returns:
            (dx, dy) normalized direction, or None.
        """
        if index <= 0 or index >= len(self._points) - 1:
            return None
        p_prev = self._points[index - 1]
        p_next = self._points[index + 1]
        dx = p_next.x - p_prev.x
        dy = p_next.y - p_prev.y
        dist = math.sqrt(dx * dx + dy * dy)
        if dist == 0:
            return None
        return (dx / dist, dy / dist)

    def is_straight(self, tolerance: float = 0.1) -> bool:
        """Check if the path is approximately a straight line.

        Args:
            tolerance: Maximum deviation from straight line (0-1).

        Returns:
            True if the path is approximately linear.
        """
        if len(self._points) < 3:
            return True

        p0 = self._points[0]
        pn = self._points[-1]
        direct_dist = p0.distance_to(pn)

        if direct_dist < 5:
            return True

        path_dist = self.total_distance()
        linearity = direct_dist / path_dist if path_dist > 0 else 1.0

        return linearity >= (1.0 - tolerance)

    def dominant_direction(self) -> Optional[str]:
        """Get the dominant movement direction.

        Returns:
            'up', 'down', 'left', 'right', or None.
        """
        if len(self._points) < 2:
            return None

        p0 = self._points[0]
        pn = self._points[-1]
        dx = pn.x - p0.x
        dy = pn.y - p0.y

        if abs(dx) < 10 and abs(dy) < 10:
            return None

        if abs(dx) > abs(dy):
            return "right" if dx > 0 else "left"
        else:
            return "down" if dy > 0 else "up"


class GestureRecognizer:
    """Recognizes gesture types from point sequences."""

    @staticmethod
    def recognize(points: list[Point3D]) -> str:
        """Recognize a gesture from a list of points.

        Args:
            points: List of Point3D objects.

        Returns:
            Gesture type string.
        """
        if len(points) < 2:
            return GestureType.UNKNOWN

        tracker = PointTracker()
        for p in points:
            tracker.add(p.x, p.y, p.t)

        # Check for scroll gestures based on direction
        if tracker.duration < 1.0 and tracker.total_distance() > 50:
            direction = tracker.dominant_direction()
            if direction == "up":
                return GestureType.SCROLL_UP
            elif direction == "down":
                return GestureType.SCROLL_DOWN
            elif direction == "left":
                return GestureType.SCROLL_LEFT
            elif direction == "right":
                return GestureType.SCROLL_RIGHT

        # Check for click (very short movement)
        if tracker.duration < 0.5 and tracker.total_distance() < 10:
            return GestureType.CLICK

        # Check for straight line
        if tracker.is_straight(tolerance=0.15):
            return GestureType.STRAIGHT_LINE

        # Check for circle (rough heuristic)
        if GestureRecognizer._is_circular(points):
            return GestureType.CIRCLE

        return GestureType.CURVE

    @staticmethod
    def _is_circular(points: list[Point3D], tolerance: float = 0.2) -> bool:
        """Detect if a path forms a roughly circular shape."""
        if len(points) < 10:
            return False

        # Compute centroid
        cx = sum(p.x for p in points) / len(points)
        cy = sum(p.y for p in points) / len(points)

        # Compute radius variance
        radii = [math.sqrt((p.x - cx) ** 2 + (p.y - cy) ** 2) for p in points]
        avg_radius = sum(radii) / len(radii)

        if avg_radius < 20:
            return False

        variance = sum((r - avg_radius) ** 2 for r in radii) / len(radii)
        normalized_variance = math.sqrt(variance) / avg_radius

        return normalized_variance < tolerance


class MovementAnalyzer:
    """Analyzes movement patterns for abnormality detection."""

    def __init__(self, baseline: list[Point3D]):
        self.baseline = baseline
        self.baseline_speed = self._compute_speed_profile(baseline)

    def _compute_speed_profile(self, points: list[Point3D]) -> dict:
        tracker = PointTracker()
        for p in points:
            tracker.add(p.x, p.y, p.t)
        return {
            "avg_speed": tracker.average_speed(),
            "total_distance": tracker.total_distance(),
            "duration": tracker.duration,
        }

    def compare(self, current: list[Point3D]) -> dict:
        """Compare current movement against baseline.

        Returns:
            Dictionary with similarity metrics.
        """
        current_profile = self._compute_speed_profile(current)

        speed_ratio = (
            current_profile["avg_speed"] / max(self.baseline_speed["avg_speed"], 0.1)
        )
        distance_ratio = (
            current_profile["total_distance"] / max(self.baseline_speed["total_distance"], 0.1)
        )

        return {
            "speed_ratio": speed_ratio,
            "distance_ratio": distance_ratio,
            "is_human_like": 0.2 < speed_ratio < 5.0,
        }

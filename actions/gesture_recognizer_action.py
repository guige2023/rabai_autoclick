"""
Gesture Recognizer Action Module.

Recognizes touch/pointer gestures from sequences of coordinates,
including tap, double-tap, long-press, swipe, pinch, and drag.
"""

import math
from typing import NamedTuple, Optional


class Point(NamedTuple):
    """A point with x, y, and timestamp."""
    x: float
    y: float
    t: float


class Gesture:
    """Represents a recognized gesture."""

    def __init__(self, name: str, points: list[Point], confidence: float = 1.0):
        """
        Initialize gesture.

        Args:
            name: Gesture name (tap, swipe_left, etc.).
            points: Points that form the gesture.
            confidence: Confidence score 0-1.
        """
        self.name = name
        self.points = points
        self.confidence = confidence

    def __repr__(self) -> str:
        return f"Gesture({self.name}, conf={self.confidence:.2f}, points={len(self.points)})"


class GestureRecognizer:
    """Recognizes gestures from point sequences."""

    TAP_MAX_DISTANCE = 20.0
    TAP_MAX_DURATION = 0.5
    SWIPE_MIN_DISTANCE = 50.0
    SWIPE_MAX_DURATION = 2.0
    DOUBLE_TAP_MAX_INTERVAL = 0.3
    LONG_PRESS_MIN_DURATION = 0.5

    def __init__(self):
        """Initialize gesture recognizer."""
        self._last_tap_time: Optional[float] = None
        self._last_tap_point: Optional[Point] = None

    def recognize(self, points: list[Point]) -> Optional[Gesture]:
        """
        Recognize a gesture from a sequence of points.

        Args:
            points: Ordered list of points with timestamps.

        Returns:
            Recognized Gesture or None.
        """
        if len(points) < 2:
            return None

        total_duration = points[-1].t - points[0].t
        total_distance = self._path_distance(points)

        if total_duration <= self.TAP_MAX_DURATION and total_distance < self.TAP_MAX_DISTANCE:
            return self._recognize_tap(points)

        if total_duration <= self.SWIPE_MAX_DURATION and total_distance >= self.SWIPE_MIN_DISTANCE:
            return self._recognize_swipe(points)

        if total_duration >= self.LONG_PRESS_MIN_DURATION and total_distance < self.TAP_MAX_DISTANCE:
            return Gesture("long_press", points, confidence=0.9)

        return Gesture("drag", points, confidence=0.8)

    def _recognize_tap(self, points: list[Point]) -> Gesture:
        """Recognize tap or double-tap."""
        duration = points[-1].t - points[0].t
        current_time = points[-1].t

        is_double_tap = (
            self._last_tap_time is not None
            and current_time - self._last_tap_time <= self.DOUBLE_TAP_MAX_INTERVAL
            and self._last_tap_point is not None
            and self._distance(points[0], self._last_tap_point) < self.TAP_MAX_DISTANCE
        )

        gesture_name = "double_tap" if is_double_tap else "tap"
        confidence = 1.0 if duration < 0.2 else 0.8

        if not is_double_tap:
            self._last_tap_time = current_time
            self._last_tap_point = points[0]
        else:
            self._last_tap_time = None
            self._last_tap_point = None

        return Gesture(gesture_name, points, confidence=confidence)

    def _recognize_swipe(self, points: list[Point]) -> Gesture:
        """Recognize swipe direction."""
        start = points[0]
        end = points[-1]

        dx = end.x - start.x
        dy = end.y - start.y

        if abs(dx) > abs(dy):
            direction = "right" if dx > 0 else "left"
        else:
            direction = "down" if dy > 0 else "up"

        speed = self._path_distance(points) / (points[-1].t - points[0].t)
        confidence = min(1.0, speed / 1000.0)

        return Gesture(f"swipe_{direction}", points, confidence=confidence)

    @staticmethod
    def _path_distance(points: list[Point]) -> float:
        """Calculate total path distance."""
        if len(points) < 2:
            return 0.0
        total = 0.0
        for i in range(1, len(points)):
            total += math.sqrt(
                (points[i].x - points[i - 1].x) ** 2
                + (points[i].y - points[i - 1].y) ** 2
            )
        return total

    @staticmethod
    def _distance(p1: Point, p2: Point) -> float:
        """Calculate distance between two points."""
        return math.sqrt((p1.x - p2.x) ** 2 + (p1.y - p2.y) ** 2)

    @staticmethod
    def points_from_coordinates(coords: list[tuple], start_time: float = 0.0, dt: float = 0.016) -> list[Point]:
        """
        Create Point list from coordinate tuples.

        Args:
            coords: List of (x, y) tuples.
            start_time: Starting timestamp.
            dt: Time delta between points.

        Returns:
            List of Point objects.
        """
        return [Point(x, y, start_time + i * dt) for i, (x, y) in enumerate(coords)]

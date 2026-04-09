"""
Gesture Recognition Utilities for UI Automation

Provides gesture recognition for touch/trackpad input patterns
including taps, swipes, pinches, and custom gesture templates.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class GestureType(Enum):
    """Supported gesture types."""
    TAP = auto()
    DOUBLE_TAP = auto()
    LONG_PRESS = auto()
    SWIPE_LEFT = auto()
    SWIPE_RIGHT = auto()
    SWIPE_UP = auto()
    SWIPE_DOWN = auto()
    PINCH_IN = auto()
    PINCH_OUT = auto()
    ROTATE = auto()
    CUSTOM = auto()


@dataclass
class Point:
    """2D point representation."""
    x: float
    y: float
    timestamp: float = field(default_factory=time.time)

    def distance_to(self, other: Point) -> float:
        """Calculate Euclidean distance to another point."""
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)

    def angle_to(self, other: Point) -> float:
        """Calculate angle in radians to another point."""
        return math.atan2(other.y - self.y, other.x - self.x)


@dataclass
class GestureTemplate:
    """Template for matching custom gestures."""
    name: str
    points: list[Point]
    tolerance: float = 20.0
    point_order_matters: bool = True


@dataclass
class RecognizedGesture:
    """Result of gesture recognition."""
    gesture_type: GestureType
    confidence: float
    start_point: Point
    end_point: Point
    duration: float
    metadata: dict = field(default_factory=dict)


class GestureRecognizer:
    """
    Recognizes gestures from a sequence of touch/pointer points.

    Supports common gestures (tap, swipe, pinch) and custom template matching.
    """

    TAP_DISTANCE_THRESHOLD = 30.0
    TAP_TIME_THRESHOLD = 0.3
    LONG_PRESS_TIME = 0.5
    SWIPE_DISTANCE_THRESHOLD = 100.0

    def __init__(self) -> None:
        self._custom_templates: list[GestureTemplate] = []
        self._current_points: list[Point] = []
        self._is_recognizing = False

    def add_template(self, template: GestureTemplate) -> None:
        """Register a custom gesture template for matching."""
        self._custom_templates.append(template)

    def start_recognition(self) -> None:
        """Begin recording points for gesture recognition."""
        self._current_points = []
        self._is_recognizing = True

    def add_point(self, x: float, y: float) -> None:
        """Add a point to the current gesture sequence."""
        if self._is_recognizing:
            self._current_points.append(Point(x=x, y=y, timestamp=time.time()))

    def end_recognition(self) -> Optional[RecognizedGesture]:
        """End recording and return recognized gesture."""
        self._is_recognizing = False
        if not self._current_points:
            return None
        return self._recognize()

    def _recognize(self) -> Optional[RecognizedGesture]:
        """Internal gesture recognition logic."""
        points = self._current_points
        if len(points) < 2:
            return None

        start = points[0]
        end = points[-1]
        duration = end.timestamp - start.timestamp
        distance = start.distance_to(end)

        # Double tap detection
        if len(points) == 2 and duration < self.TAP_TIME_THRESHOLD:
            return RecognizedGesture(
                gesture_type=GestureType.TAP,
                confidence=0.95,
                start_point=start,
                end_point=end,
                duration=duration,
            )

        # Swipe detection
        if distance > self.SWIPE_DISTANCE_THRESHOLD:
            angle = start.angle_to(end)
            gesture_type = self._angle_to_swipe_type(angle)
            velocity = distance / duration if duration > 0 else 0
            return RecognizedGesture(
                gesture_type=gesture_type,
                confidence=min(1.0, distance / 200.0),
                start_point=start,
                end_point=end,
                duration=duration,
                metadata={"velocity": velocity, "angle": math.degrees(angle)},
            )

        # Long press detection
        if duration > self.LONG_PRESS_TIME and distance < self.TAP_DISTANCE_THRESHOLD:
            return RecognizedGesture(
                gesture_type=GestureType.LONG_PRESS,
                confidence=0.85,
                start_point=start,
                end_point=end,
                duration=duration,
            )

        return None

    def _angle_to_swipe_type(self, angle: float) -> GestureType:
        """Convert angle to swipe direction."""
        angle_deg = math.degrees(angle) % 360
        if 45 <= angle_deg < 135:
            return GestureType.SWIPE_DOWN
        elif 135 <= angle_deg < 225:
            return GestureType.SWIPE_LEFT
        elif 225 <= angle_deg < 315:
            return GestureType.SWIPE_UP
        else:
            return GestureType.SWIPE_RIGHT

    def recognize_from_points(self, points: list[tuple[float, float]]) -> Optional[RecognizedGesture]:
        """
        Recognize gesture from a list of (x, y) tuples.

        Args:
            points: List of (x, y) coordinate tuples

        Returns:
            RecognizedGesture if recognized, None otherwise
        """
        self._current_points = [
            Point(x=x, y=y, timestamp=time.time() + i * 0.01)
            for i, (x, y) in enumerate(points)
        ]
        return self._recognize()


def match_gesture_template(
    gesture_points: list[Point],
    template: GestureTemplate,
) -> float:
    """
    Match a gesture against a template and return confidence score.

    Args:
        gesture_points: Points from the performed gesture
        template: Template to match against

    Returns:
        Confidence score between 0.0 and 1.0
    """
    if len(gesture_points) < 2 or len(template.points) < 2:
        return 0.0

    # Normalize gesture points
    normalized = _normalize_points(gesture_points)
    normalized_template = _normalize_points(template.points)

    if template.point_order_matters:
        return _sequential_match(normalized, normalized_template, template.tolerance)
    else:
        return _best_order_match(normalized, normalized_template, template.tolerance)


def _normalize_points(points: list[Point]) -> list[Point]:
    """Normalize points to 0-1 range for scale-invariant matching."""
    if not points:
        return []

    min_x = min(p.x for p in points)
    max_x = max(p.x for p in points)
    min_y = min(p.y for p in points)
    max_y = max(p.y for p in points)

    range_x = max_x - min_x or 1.0
    range_y = max_y - min_y or 1.0

    return [
        Point(x=(p.x - min_x) / range_x, y=(p.y - min_y) / range_y, timestamp=p.timestamp)
        for p in points
    ]


def _sequential_match(
    gesture: list[Point],
    template: list[Point],
    tolerance: float,
) -> float:
    """Match gesture to template in order."""
    if len(template) == 0:
        return 0.0

    total_error = 0.0
    template_idx = 0
    step = len(gesture) / len(template)

    for i, point in enumerate(gesture):
        expected_idx = min(int(i * step), len(template) - 1)
        expected = template[expected_idx]
        total_error += point.distance_to(expected)

    avg_error = total_error / len(gesture) if gesture else 0.0
    confidence = max(0.0, 1.0 - (avg_error / tolerance))
    return confidence


def _best_order_match(
    gesture: list[Point],
    template: list[Point],
    tolerance: float,
) -> float:
    """Match gesture to template allowing any point order."""
    if not template:
        return 0.0

    total_error = 0.0
    for g_point in gesture:
        min_dist = min(p.distance_to(g_point) for p in template)
        total_error += min_dist

    avg_error = total_error / len(gesture) if gesture else 0.0
    confidence = max(0.0, 1.0 - (avg_error / tolerance))
    return confidence

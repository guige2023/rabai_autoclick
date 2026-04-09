"""
Touch gesture recognition and generation utilities.

This module provides utilities for recognizing and generating
touch gestures including taps, swipes, pinches, and rotations.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Tuple, Dict, Any
from enum import Enum, auto


class GestureType(Enum):
    """Types of touch gestures."""
    TAP = auto()
    DOUBLE_TAP = auto()
    LONG_PRESS = auto()
    SWIPE_LEFT = auto()
    SWIPE_RIGHT = auto()
    SWIPE_UP = auto()
    SWIPE_DOWN = auto()
    PINCH = auto()
    ROTATE = auto()
    DRAG = auto()
    PAN = auto()


@dataclass
class TouchPoint:
    """
    Represents a single touch point.

    Attributes:
        x: X coordinate.
        y: Y coordinate.
        pressure: Touch pressure (0.0-1.0).
        radius: Touch radius.
        timestamp: When the touch occurred.
        finger_id: Identifier for multi-touch tracking.
    """
    x: float
    y: float
    pressure: float = 1.0
    radius: float = 1.0
    timestamp: float = field(default_factory=time.time)
    finger_id: int = 0


@dataclass
class Gesture:
    """
    Represents a complete gesture.

    Attributes:
        gesture_type: Type of the gesture.
        points: Sequence of touch points.
        duration: Total gesture duration in seconds.
        start_point: First touch point.
        end_point: Last touch point.
        centroid: Center point of the gesture.
    """
    gesture_type: GestureType
    points: List[TouchPoint] = field(default_factory=list)
    duration: float = 0.0
    start_point: Optional[TouchPoint] = None
    end_point: Optional[TouchPoint] = None
    centroid: Optional[Tuple[float, float]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.points:
            if not self.start_point:
                self.start_point = self.points[0]
            if not self.end_point:
                self.end_point = self.points[-1]
            if not self.duration and len(self.points) > 1:
                self.duration = self.points[-1].timestamp - self.points[0].timestamp
            if not self.centroid:
                self.centroid = self._calculate_centroid()

    def _calculate_centroid(self) -> Tuple[float, float]:
        """Calculate the centroid of all touch points."""
        if not self.points:
            return (0.0, 0.0)
        total_x = sum(p.x for p in self.points)
        total_y = sum(p.y for p in self.points)
        return (total_x / len(self.points), total_y / len(self.points))


class GestureRecognizer:
    """
    Recognizes gesture types from touch point sequences.

    Uses heuristics and pattern matching to classify gestures.
    """

    # Thresholds for gesture recognition
    TAP_MAX_DURATION: float = 0.3
    TAP_MAX_DISTANCE: float = 30.0
    SWIPE_MIN_DISTANCE: float = 100.0
    SWIPE_MAX_DURATION: float = 1.0
    LONG_PRESS_MIN_DURATION: float = 0.5
    PINCH_MIN_DISTANCE: float = 50.0

    def recognize(self, points: List[TouchPoint]) -> Gesture:
        """
        Recognize gesture type from touch points.

        Returns a Gesture with classified gesture_type.
        """
        if not points:
            return Gesture(gesture_type=GestureType.TAP)

        duration = points[-1].timestamp - points[0].timestamp
        distance = self._calculate_distance(points[0], points[-1])

        # Check for tap
        if duration < self.TAP_MAX_DURATION and distance < self.TAP_MAX_DISTANCE:
            return Gesture(
                gesture_type=GestureType.TAP,
                points=points,
                duration=duration,
            )

        # Check for long press
        if duration > self.LONG_PRESS_MIN_DURATION and distance < self.TAP_MAX_DISTANCE:
            return Gesture(
                gesture_type=GestureType.LONG_PRESS,
                points=points,
                duration=duration,
            )

        # Check for swipe
        if distance > self.SWIPE_MIN_DISTANCE and duration < self.SWIPE_MAX_DURATION:
            direction = self._get_swipe_direction(points[0], points[-1])
            return Gesture(
                gesture_type=direction,
                points=points,
                duration=duration,
                metadata={"distance": distance, "direction": direction.name},
            )

        # Default to drag
        return Gesture(
            gesture_type=GestureType.DRAG,
            points=points,
            duration=duration,
        )

    def _calculate_distance(self, p1: TouchPoint, p2: TouchPoint) -> float:
        """Calculate Euclidean distance between two touch points."""
        dx = p2.x - p1.x
        dy = p2.y - p1.y
        return math.sqrt(dx * dx + dy * dy)

    def _get_swipe_direction(self, start: TouchPoint, end: TouchPoint) -> GestureType:
        """Determine swipe direction from start to end."""
        dx = end.x - start.x
        dy = end.y - start.y

        if abs(dx) > abs(dy):
            return GestureType.SWIPE_RIGHT if dx > 0 else GestureType.SWIPE_LEFT
        else:
            return GestureType.SWIPE_DOWN if dy > 0 else GestureType.SWIPE_UP


class GestureGenerator:
    """
    Generates touch point sequences for gesture playback.

    Creates smooth, natural-looking touch sequences from
    high-level gesture descriptions.
    """

    def __init__(self, sampling_rate: float = 60.0) -> None:
        self._sampling_rate = sampling_rate

    def generate_swipe(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        duration: float = 0.5,
        finger_id: int = 0,
    ) -> List[TouchPoint]:
        """Generate touch points for a swipe gesture."""
        points: List[TouchPoint] = []
        num_points = int(duration * self._sampling_rate)
        start_time = time.time()

        for i in range(num_points + 1):
            t = i / num_points
            # Apply easing for natural feel
            eased_t = self._ease_out_quad(t)
            x = x1 + (x2 - x1) * eased_t
            y = y1 + (y2 - y1) * eased_t

            points.append(TouchPoint(
                x=x,
                y=y,
                timestamp=start_time + t * duration,
                finger_id=finger_id,
            ))

        return points

    def generate_tap(
        self,
        x: float,
        y: float,
        duration: float = 0.1,
        finger_id: int = 0,
    ) -> List[TouchPoint]:
        """Generate touch points for a tap gesture."""
        start_time = time.time()
        return [
            TouchPoint(x=x, y=y, timestamp=start_time, finger_id=finger_id),
            TouchPoint(x=x, y=y, timestamp=start_time + duration, finger_id=finger_id),
        ]

    def generate_long_press(
        self,
        x: float,
        y: float,
        duration: float = 1.0,
        finger_id: int = 0,
    ) -> List[TouchPoint]:
        """Generate touch points for a long press gesture."""
        points: List[TouchPoint] = []
        start_time = time.time()

        # Down
        points.append(TouchPoint(x=x, y=y, timestamp=start_time, finger_id=finger_id))

        # Hold at position
        hold_duration = duration - 0.05
        num_hold_points = max(1, int(hold_duration * self._sampling_rate))
        for i in range(num_hold_points):
            t = i / num_hold_points
            points.append(TouchPoint(
                x=x, y=y,
                timestamp=start_time + 0.025 + t * hold_duration,
                finger_id=finger_id,
            ))

        # Up
        points.append(TouchPoint(
            x=x, y=y,
            timestamp=start_time + duration,
            finger_id=finger_id,
        ))

        return points

    def generate_pinch(
        self,
        center_x: float,
        center_y: float,
        start_distance: float,
        end_distance: float,
        duration: float = 0.5,
        finger_id_start: int = 0,
        finger_id_end: int = 1,
    ) -> Tuple[List[TouchPoint], List[TouchPoint]]:
        """Generate touch point pairs for a pinch gesture."""
        points1: List[TouchPoint] = []
        points2: List[TouchPoint] = []
        num_points = int(duration * self._sampling_rate)
        start_time = time.time()

        for i in range(num_points + 1):
            t = i / num_points
            eased_t = self._ease_in_out_quad(t)
            current_distance = start_distance + (end_distance - start_distance) * eased_t
            half = current_distance / 2

            timestamp = start_time + t * duration

            # First finger on left
            points1.append(TouchPoint(
                x=center_x - half,
                y=center_y,
                timestamp=timestamp,
                finger_id=finger_id_start,
            ))

            # Second finger on right
            points2.append(TouchPoint(
                x=center_x + half,
                y=center_y,
                timestamp=timestamp,
                finger_id=finger_id_end,
            ))

        return points1, points2

    def _ease_out_quad(self, t: float) -> float:
        """Quadratic ease-out."""
        return -t * (t - 2)

    def _ease_in_out_quad(self, t: float) -> float:
        """Quadratic ease-in-out."""
        if t < 0.5:
            return 2 * t * t
        return -1 + (4 - 2 * t) * t


def interpolate_points(
    p1: TouchPoint,
    p2: TouchPoint,
    num_points: int,
) -> List[TouchPoint]:
    """Generate intermediate points between two touch points."""
    points: List[TouchPoint] = []
    for i in range(num_points + 1):
        t = i / num_points
        points.append(TouchPoint(
            x=p1.x + (p2.x - p1.x) * t,
            y=p1.y + (p2.y - p1.y) * t,
            pressure=p1.pressure + (p2.pressure - p1.pressure) * t,
            radius=p1.radius + (p2.radius - p1.radius) * t,
            timestamp=p1.timestamp + (p2.timestamp - p1.timestamp) * t,
            finger_id=p1.finger_id,
        ))
    return points

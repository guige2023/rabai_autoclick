"""Gesture recognition utilities.

This module provides utilities for recognizing gestures from
input sequences such as mouse drags and touch patterns.
"""

from __future__ import annotations

from typing import List, NamedTuple, Optional, Tuple
from dataclasses import dataclass
from enum import Enum, auto
import math


class GestureType(Enum):
    """Recognized gesture types."""
    TAP = auto()
    DOUBLE_TAP = auto()
    LONG_PRESS = auto()
    SWIPE_LEFT = auto()
    SWIPE_RIGHT = auto()
    SWIPE_UP = auto()
    SWIPE_DOWN = auto()
    PINCH = auto()
    ZOOM = auto()
    DRAG = auto()
    ROTATE = auto()
    UNKNOWN = auto()


class Point(NamedTuple):
    """A 2D point."""
    x: float
    y: float

    def distance_to(self, other: "Point") -> float:
        """Calculate Euclidean distance to another point."""
        dx = self.x - other.x
        dy = self.y - other.y
        return math.sqrt(dx * dx + dy * dy)

    def angle_to(self, other: "Point") -> float:
        """Calculate angle in degrees to another point."""
        return math.degrees(math.atan2(other.y - self.y, other.x - self.x))


@dataclass
class Gesture:
    """A recognized gesture."""
    gesture_type: GestureType
    points: List[Point]
    duration: float
    metadata: dict

    @property
    def start_point(self) -> Optional[Point]:
        """Get the starting point."""
        return self.points[0] if self.points else None

    @property
    def end_point(self) -> Optional[Point]:
        """Get the ending point."""
        return self.points[-1] if self.points else None

    @property
    def total_distance(self) -> float:
        """Calculate total distance traveled."""
        if len(self.points) < 2:
            return 0.0
        return sum(
            self.points[i].distance_to(self.points[i + 1])
            for i in range(len(self.points) - 1)
        )


@dataclass
class GestureConfig:
    """Configuration for gesture recognition."""
    swipe_min_distance: float = 50.0
    swipe_max_duration: float = 1.0
    tap_max_distance: float = 10.0
    tap_max_duration: float = 0.5
    long_press_min_duration: float = 0.5
    double_tap_max_interval: float = 0.3


def recognize_swipe(
    points: List[Point],
    duration: float,
    config: Optional[GestureConfig] = None,
) -> Optional[GestureType]:
    """Recognize swipe direction from points.

    Args:
        points: List of points in the gesture.
        duration: Total duration in seconds.
        config: Gesture configuration.

    Returns:
        GestureType or None if not a swipe.
    """
    cfg = config or GestureConfig()
    if len(points) < 2:
        return None

    start = points[0]
    end = points[-1]
    distance = start.distance_to(end)

    if distance < cfg.swipe_min_distance:
        return None
    if duration > cfg.swipe_max_duration:
        return None

    angle = start.angle_to(end)

    if -45 <= angle < 45:
        return GestureType.SWIPE_RIGHT
    elif 45 <= angle < 135:
        return GestureType.SWIPE_DOWN
    elif -135 <= angle < -45:
        return GestureType.SWIPE_UP
    else:
        return GestureType.SWIPE_LEFT


def recognize_tap(
    points: List[Point],
    duration: float,
    config: Optional[GestureConfig] = None,
) -> Optional[GestureType]:
    """Recognize tap from points.

    Args:
        points: List of points in the gesture.
        duration: Total duration in seconds.
        config: Gesture configuration.

    Returns:
        GestureType.TAP or None if not a tap.
    """
    cfg = config or GestureConfig()
    if len(points) < 2:
        return None

    start = points[0]
    end = points[-1]
    distance = start.distance_to(end)

    if distance < cfg.tap_max_distance and duration < cfg.tap_max_duration:
        return GestureType.TAP
    return None


def recognize_long_press(
    points: List[Point],
    duration: float,
    config: Optional[GestureConfig] = None,
) -> Optional[GestureType]:
    """Recognize long press from points.

    Args:
        points: List of points in the gesture.
        duration: Total duration in seconds.
        config: Gesture configuration.

    Returns:
        GestureType.LONG_PRESS or None.
    """
    cfg = config or GestureConfig()
    if len(points) == 1 and duration >= cfg.long_press_min_duration:
        return GestureType.LONG_PRESS
    return None


def recognize_gesture(
    points: List[Point],
    duration: float,
    config: Optional[GestureConfig] = None,
) -> Gesture:
    """Recognize a gesture from points and duration.

    Args:
        points: List of points in the gesture.
        duration: Total duration in seconds.
        config: Gesture configuration.

    Returns:
        Gesture with recognized type.
    """
    cfg = config or GestureConfig()

    for recognizer in [recognize_long_press, recognize_tap, recognize_swipe]:
        result = recognizer(points, duration, cfg)
        if result is not None:
            return Gesture(
                gesture_type=result,
                points=points,
                duration=duration,
                metadata={},
            )

    return Gesture(
        gesture_type=GestureType.UNKNOWN,
        points=points,
        duration=duration,
        metadata={},
    )


def resample_points(points: List[Point], num_points: int) -> List[Point]:
    """Resample gesture points to a fixed count.

    Args:
        points: Original points.
        num_points: Target number of points.

    Returns:
        Resampled points list.
    """
    if len(points) <= 1:
        return points[:]
    if len(points) == num_points:
        return points[:]

    total_dist = sum(
        points[i].distance_to(points[i + 1])
        for i in range(len(points) - 1)
    )
    step = total_dist / (num_points - 1)

    result = [points[0]]
    accumulated = 0.0
    j = 1

    for i in range(num_points - 1):
        target = step * (i + 1)
        while j < len(points) and accumulated + points[j - 1].distance_to(points[j]) < target:
            accumulated += points[j - 1].distance_to(points[j])
            j += 1
        if j < len(points):
            result.append(points[j])
        else:
            result.append(points[-1])

    return result


__all__ = [
    "GestureType",
    "Point",
    "Gesture",
    "GestureConfig",
    "recognize_swipe",
    "recognize_tap",
    "recognize_long_press",
    "recognize_gesture",
    "resample_points",
]

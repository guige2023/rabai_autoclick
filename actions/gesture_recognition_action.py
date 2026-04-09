"""Gesture recognition action for UI automation.

Recognizes and classifies touch/pointer gestures:
- Tap, double-tap, long-press
- Swipe (directional)
- Pinch (zoom)
- Rotate
- Drag trajectories
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable


class GestureType(Enum):
    """Recognized gesture types."""
    TAP = auto()
    DOUBLE_TAP = auto()
    LONG_PRESS = auto()
    SWIPE_UP = auto()
    SWIPE_DOWN = auto()
    SWIPE_LEFT = auto()
    SWIPE_RIGHT = auto()
    PINCH_IN = auto()
    PINCH_OUT = auto()
    ROTATE_CW = auto()  # Clockwise
    ROTATE_CCW = auto()  # Counter-clockwise
    DRAG = auto()
    PAN = auto()
    UNKNOWN = auto()


@dataclass
class Point:
    """2D point with timestamp."""
    x: float
    y: float
    timestamp: float = 0.0


@dataclass
class GestureSample:
    """A gesture sample (sequence of points)."""
    points: list[Point] = field(default_factory=list)

    def add_point(self, x: float, y: float, timestamp: float | None = None) -> None:
        if timestamp is None:
            timestamp = time.time()
        self.points.append(Point(x, y, timestamp))

    @property
    def start(self) -> Point | None:
        return self.points[0] if self.points else None

    @property
    def end(self) -> Point | None:
        return self.points[-1] if self.points else None

    @property
    def duration(self) -> float:
        if len(self.points) < 2:
            return 0.0
        return self.end.timestamp - self.start.timestamp

    def total_distance(self) -> float:
        """Total path distance."""
        dist = 0.0
        for i in range(1, len(self.points)):
            dx = self.points[i].x - self.points[i-1].x
            dy = self.points[i].y - self.points[i-1].y
            dist += math.sqrt(dx * dx + dy * dy)
        return dist

    def displacement(self) -> tuple[float, float]:
        """Net displacement (dx, dy) from start to end."""
        if len(self.points) < 2:
            return (0.0, 0.0)
        return (self.end.x - self.start.x, self.end.y - self.start.y)

    def direction_angle(self) -> float:
        """Direction angle in radians from start to end."""
        dx, dy = self.displacement()
        return math.atan2(dy, dx)

    def dominant_direction(self) -> GestureType | None:
        """Get dominant swipe direction if gesture is a swipe."""
        dx, dy = self.displacement()
        angle = self.direction_angle()

        # Check if it's roughly a cardinal direction
        swipe_threshold = 0.7  # cos(45°) threshold
        cos_a = math.cos(angle)

        if abs(cos_a) > swipe_threshold:
            if dx > 0:
                return GestureType.SWIPE_RIGHT
            else:
                return GestureType.SWIPE_LEFT
        elif abs(math.sin(angle)) > swipe_threshold:
            if dy > 0:
                return GestureType.SWIPE_DOWN
            else:
                return GestureType.SWIPE_UP
        return None

    def average_velocity(self) -> float:
        """Average velocity in pixels per second."""
        if self.duration == 0:
            return 0.0
        return self.total_distance() / self.duration


@dataclass
class PinchSample:
    """Pinch gesture sample (two fingers)."""
    finger1: list[Point] = field(default_factory=list)
    finger2: list[Point] = field(default_factory=list)

    def initial_distance(self) -> float:
        if not self.finger1 or not self.finger2:
            return 0.0
        p1 = self.finger1[0]
        p2 = self.finger2[0]
        return math.sqrt((p2.x - p1.x) ** 2 + (p2.y - p1.y) ** 2)

    def final_distance(self) -> float:
        if not self.finger1 or not self.finger2:
            return 0.0
        p1 = self.finger1[-1]
        p2 = self.finger2[-1]
        return math.sqrt((p2.x - p1.x) ** 2 + (p2.y - p1.y) ** 2)

    def scale_factor(self) -> float:
        """Ratio of final to initial distance."""
        init = self.initial_distance()
        if init == 0:
            return 1.0
        return self.final_distance() / init


@dataclass
class RotationSample:
    """Rotation gesture sample."""
    center: Point
    finger1: list[Point] = field(default_factory=list)
    finger2: list[Point] = field(default_factory=list)

    def initial_angle(self) -> float:
        if not self.finger1 or not self.finger2:
            return 0.0
        p1 = self.finger1[0]
        p2 = self.finger2[0]
        return math.atan2(p2.y - self.center.y, p2.x - self.center.x)

    def final_angle(self) -> float:
        if not self.finger1 or not self.finger2:
            return 0.0
        p1 = self.finger1[-1]
        p2 = self.finger2[-1]
        return math.atan2(p2.y - self.center.y, p2.x - self.center.x)

    def rotation_delta(self) -> float:
        """Rotation in radians (positive = CW)."""
        return self.final_angle() - self.initial_angle()


@dataclass
class RecognizedGesture:
    """Result of gesture recognition."""
    gesture_type: GestureType
    confidence: float  # 0.0 to 1.0
    sample: GestureSample | PinchSample | RotationSample | None = None
    metadata: dict = field(default_factory=dict)


class GestureRecognizer:
    """Recognizes gestures from input samples.

    Configurable thresholds for:
    - Tap detection (movement tolerance, time limits)
    - Swipe detection (minimum distance, velocity)
    - Long-press detection (duration)
    - Pinch/rotation thresholds
    """

    def __init__(
        self,
        tap_max_distance: float = 20.0,
        tap_max_duration: float = 0.3,
        double_tap_max_interval: float = 0.4,
        swipe_min_distance: float = 50.0,
        swipe_min_velocity: float = 200.0,
        long_press_min_duration: float = 0.5,
        pinch_scale_threshold: float = 0.1,
        rotation_threshold: float = 0.1,
    ):
        self.tap_max_distance = tap_max_distance
        self.tap_max_duration = tap_max_duration
        self.double_tap_max_interval = double_tap_max_interval
        self.swipe_min_distance = swipe_min_distance
        self.swipe_min_velocity = swipe_min_velocity
        self.long_press_min_duration = long_press_min_duration
        self.pinch_scale_threshold = pinch_scale_threshold
        self.rotation_threshold = rotation_threshold

        self._last_tap_time: float = 0.0
        self._last_tap_pos: tuple[float, float] = (0.0, 0.0)

    def recognize(self, sample: GestureSample) -> RecognizedGesture:
        """Recognize gesture from sample.

        Args:
            sample: Gesture sample to classify

        Returns:
            Recognized gesture with confidence
        """
        if len(sample.points) < 2:
            return RecognizedGesture(GestureType.UNKNOWN, 0.0, sample)

        # Check for long press
        if self._is_long_press(sample):
            return RecognizedGesture(
                GestureType.LONG_PRESS,
                0.95,
                sample,
                {"duration": sample.duration},
            )

        # Check for swipe
        if self._is_swipe(sample):
            direction = sample.dominant_direction()
            return RecognizedGesture(
                direction or GestureType.SWIPE_RIGHT,
                self._swipe_confidence(sample),
                sample,
                {"distance": math.sqrt(sum(d**2 for d in sample.displacement())**2)},
            )

        # Check for tap
        if self._is_tap(sample):
            return self._recognize_tap(sample)

        # Check for drag/pan
        if sample.total_distance() > self.swipe_min_distance:
            return RecognizedGesture(
                GestureType.DRAG,
                min(sample.average_velocity() / 500.0, 1.0),
                sample,
            )

        return RecognizedGesture(GestureType.UNKNOWN, 0.0, sample)

    def recognize_pinch(self, sample: PinchSample) -> RecognizedGesture:
        """Recognize pinch gesture.

        Args:
            sample: Pinch gesture sample

        Returns:
            Recognized pinch gesture
        """
        scale = sample.scale_factor()

        if scale > 1.0 + self.pinch_scale_threshold:
            return RecognizedGesture(
                GestureType.PINCH_OUT,
                min(abs(scale - 1.0) * 2.0, 1.0),
                sample,
                {"scale": scale},
            )
        elif scale < 1.0 - self.pinch_scale_threshold:
            return RecognizedGesture(
                GestureType.PINCH_IN,
                min(abs(scale - 1.0) * 2.0, 1.0),
                sample,
                {"scale": scale},
            )

        return RecognizedGesture(GestureType.UNKNOWN, 0.0, sample)

    def recognize_rotation(self, sample: RotationSample) -> RecognizedGesture:
        """Recognize rotation gesture.

        Args:
            sample: Rotation gesture sample

        Returns:
            Recognized rotation gesture
        """
        delta = sample.rotation_delta()

        if abs(delta) < self.rotation_threshold:
            return RecognizedGesture(GestureType.UNKNOWN, 0.0, sample)

        gesture_type = GestureType.ROTATE_CW if delta > 0 else GestureType.ROTATE_CCW
        confidence = min(abs(delta) / math.pi, 1.0)

        return RecognizedGesture(
            gesture_type,
            confidence,
            sample,
            {"angle": delta},
        )

    def _is_tap(self, sample: GestureSample) -> bool:
        """Check if sample is a tap."""
        if sample.duration > self.tap_max_duration:
            return False
        return sample.total_distance() <= self.tap_max_distance

    def _is_long_press(self, sample: GestureSample) -> bool:
        """Check if sample is a long press."""
        if len(sample.points) > 1:
            return False
        return sample.duration >= self.long_press_min_duration

    def _is_swipe(self, sample: GestureSample) -> bool:
        """Check if sample is a swipe."""
        dx, dy = sample.displacement()
        distance = math.sqrt(dx * dx + dy * dy)

        if distance < self.swipe_min_distance:
            return False

        # Check velocity
        if sample.average_velocity() < self.swipe_min_velocity:
            return False

        # Check straightness (ratio of displacement to path length)
        if sample.total_distance() > 0:
            straightness = distance / sample.total_distance()
            if straightness < 0.7:  # Not straight enough
                return False

        return True

    def _swipe_confidence(self, sample: GestureSample) -> float:
        """Calculate swipe recognition confidence."""
        dx, dy = sample.displacement()
        distance = math.sqrt(dx * dx + dy * dy)

        # Distance score (0-0.4)
        dist_score = min(distance / (self.swipe_min_distance * 2), 0.4)

        # Velocity score (0-0.3)
        vel_score = min(sample.average_velocity() / (self.swipe_min_velocity * 2), 0.3)

        # Straightness score (0-0.3)
        if sample.total_distance() > 0:
            straightness = distance / sample.total_distance()
            straight_score = straightness * 0.3
        else:
            straight_score = 0.0

        return dist_score + vel_score + straight_score

    def _recognize_tap(self, sample: GestureSample) -> RecognizedGesture:
        """Recognize tap type (single, double, triple)."""
        now = time.time()
        pos = (sample.start.x, sample.start.y) if sample.start else (0.0, 0.0)

        # Check for double-tap
        time_since_last = now - self._last_tap_time
        pos_distance = math.sqrt(
            (pos[0] - self._last_tap_pos[0]) ** 2 +
            (pos[1] - self._last_tap_pos[1]) ** 2
        )

        is_double = (
            time_since_last <= self.double_tap_max_interval and
            pos_distance <= self.tap_max_distance * 2
        )

        self._last_tap_time = now
        self._last_tap_pos = pos

        if is_double:
            return RecognizedGesture(
                GestureType.DOUBLE_TAP,
                0.95,
                sample,
                {"interval": time_since_last},
            )

        return RecognizedGesture(GestureType.TAP, 0.9, sample)


def create_gesture_recognizer(**kwargs) -> GestureRecognizer:
    """Create gesture recognizer with options."""
    return GestureRecognizer(**kwargs)

"""
Touch Analyzer Utility

Analyzes touch patterns and gesture trajectories for input automation.
Supports multi-touch gesture recognition and touch classification.

Example:
    >>> analyzer = TouchAnalyzer()
    >>> analyzer.add_touch((100, 200), timestamp=1.0)
    >>> analyzer.add_touch((110, 210), timestamp=1.05)
    >>> gesture = analyzer.classify()
    >>> print(gesture.type)
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class GestureType(Enum):
    """Recognized gesture types."""
    UNKNOWN = "unknown"
    TAP = "tap"
    DOUBLE_TAP = "double_tap"
    LONG_PRESS = "long_press"
    SWIPE_LEFT = "swipe_left"
    SWIPE_RIGHT = "swipe_right"
    SWIPE_UP = "swipe_up"
    SWIPE_DOWN = "swipe_down"
    PINCH = "pinch"
    SPREAD = "spread"
    ROTATE = "rotate"
    DRAG = "drag"
    PAN = "pan"


@dataclass
class TouchPoint:
    """A single touch point."""
    x: float
    y: float
    timestamp: float
    pressure: float = 1.0
    finger_id: int = 0


@dataclass
class GestureResult:
    """Result of gesture classification."""
    type: GestureType
    confidence: float  # 0.0 to 1.0
    start_point: Optional[tuple[float, float]] = None
    end_point: Optional[tuple[float, float]] = None
    distance: float = 0.0
    duration: float = 0.0
    direction: Optional[str] = None
    velocity: float = 0.0


@dataclass
class TouchSequence:
    """A sequence of touch points from one finger."""
    finger_id: int
    points: list[TouchPoint] = field(default_factory=list)

    def add(self, x: float, y: float, timestamp: float, pressure: float = 1.0) -> None:
        """Add a point to the sequence."""
        self.points.append(TouchPoint(x, y, timestamp, pressure, self.finger_id))

    @property
    def start(self) -> Optional[TouchPoint]:
        return self.points[0] if self.points else None

    @property
    def end(self) -> Optional[TouchPoint]:
        return self.points[-1] if self.points else None

    def total_distance(self) -> float:
        """Total path distance traveled."""
        if len(self.points) < 2:
            return 0.0
        dist = 0.0
        for i in range(1, len(self.points)):
            p1, p2 = self.points[i - 1], self.points[i]
            dist += math.sqrt((p2.x - p1.x) ** 2 + (p2.y - p1.y) ** 2)
        return dist

    def duration(self) -> float:
        """Total time duration."""
        if not self.points:
            return 0.0
        return self.end.timestamp - self.start.timestamp

    def average_velocity(self) -> float:
        """Average velocity in pixels per second."""
        dur = self.duration()
        if dur <= 0:
            return 0.0
        return self.total_distance() / dur


class TouchAnalyzer:
    """
    Analyzes touch input and classifies gestures.

    Args:
        tap_threshold: Max movement (px) for a tap gesture.
        swipe_threshold: Min movement (px) for a swipe gesture.
        double_tap_window: Max time (s) between two taps.
        long_press_delay: Min time (s) for a long press.
    """

    def __init__(
        self,
        tap_threshold: float = 10.0,
        swipe_threshold: float = 50.0,
        double_tap_window: float = 0.3,
        long_press_delay: float = 0.5,
    ) -> None:
        self.tap_threshold = tap_threshold
        self.swipe_threshold = swipe_threshold
        self.double_tap_window = double_tap_window
        self.long_press_delay = long_press_delay
        self._sequences: dict[int, TouchSequence] = {}
        self._last_tap: Optional[tuple[float, float, float]] = None  # x, y, time
        self._lock = threading.Lock()
        self._multi_touch_threshold = 2

    def reset(self) -> None:
        """Clear all tracked sequences."""
        with self._lock:
            self._sequences.clear()
            self._last_tap = None

    def add_touch(
        self,
        x: float,
        y: float,
        finger_id: int = 0,
        timestamp: Optional[float] = None,
        pressure: float = 1.0,
    ) -> None:
        """
        Add a touch point.

        Args:
            x: X coordinate.
            y: Y coordinate.
            finger_id: Finger identifier for multi-touch.
            timestamp: Event timestamp (defaults to time.time()).
            pressure: Touch pressure (0.0 to 1.0).
        """
        import time as time_module
        ts = timestamp if timestamp is not None else time_module.time()

        with self._lock:
            if finger_id not in self._sequences:
                self._sequences[finger_id] = TouchSequence(finger_id)
            self._sequences[finger_id].add(x, y, ts, pressure)

    def end_touch(self, finger_id: int, timestamp: Optional[float] = None) -> None:
        """
        Mark the end of a touch sequence for a finger.

        Args:
            finger_id: Finger identifier.
            timestamp: Event timestamp.
        """
        pass  # Sequence automatically ends on classify

    def classify(self) -> GestureResult:
        """
        Classify the current touch sequences as a gesture.

        Returns:
            GestureResult with classification details.
        """
        with self._lock:
            sequences = dict(self._sequences)
            self._sequences.clear()

        if not sequences:
            return GestureResult(GestureType.UNKNOWN, 0.0)

        # Single finger analysis
        if len(sequences) == 1:
            seq = list(sequences.values())[0]
            return self._classify_single(seq)

        # Multi-touch analysis
        return self._classify_multi(list(sequences.values()))

    def _classify_single(self, seq: TouchSequence) -> GestureResult:
        """Classify a single-finger sequence."""
        if len(seq.points) == 0:
            return GestureResult(GestureType.UNKNOWN, 0.0)

        dist = seq.total_distance()
        dur = seq.duration()
        start = seq.start
        end = seq.end

        if start is None or end is None:
            return GestureResult(GestureType.UNKNOWN, 0.0)

        # Tap detection
        if dist < self.tap_threshold:
            if dur < self.long_press_delay:
                # Check double tap
                if (
                    self._last_tap is not None
                    and dur < self.double_tap_window
                    and math.sqrt(
                        (end.x - self._last_tap[0]) ** 2
                        + (end.y - self._last_tap[1]) ** 2
                    ) < self.tap_threshold * 2
                ):
                    self._last_tap = None
                    return GestureResult(
                        GestureType.DOUBLE_TAP,
                        confidence=0.9,
                        start_point=(start.x, start.y),
                        end_point=(end.x, end.y),
                        duration=dur,
                    )
                self._last_tap = (end.x, end.y, end.timestamp)
                return GestureResult(
                    GestureType.TAP,
                    confidence=0.85,
                    start_point=(start.x, start.y),
                    end_point=(end.x, end.y),
                    duration=dur,
                )
            else:
                return GestureResult(
                    GestureType.LONG_PRESS,
                    confidence=0.8,
                    start_point=(start.x, start.y),
                    end_point=(end.x, end.y),
                    duration=dur,
                )

        # Swipe detection
        if dist >= self.swipe_threshold and dur < 1.0:
            dx = end.x - start.x
            dy = end.y - start.y
            angle = math.degrees(math.atan2(dy, dx))

            if abs(dx) > abs(dy):
                if dx > 0:
                    gesture_type = GestureType.SWIPE_RIGHT
                    direction = "right"
                else:
                    gesture_type = GestureType.SWIPE_LEFT
                    direction = "left"
            else:
                if dy > 0:
                    gesture_type = GestureType.SWIPE_DOWN
                    direction = "down"
                else:
                    gesture_type = GestureType.SWIPE_UP
                    direction = "up"

            velocity = dist / dur if dur > 0 else 0
            confidence = min(dist / 100.0, 1.0) * 0.9

            return GestureResult(
                gesture_type,
                confidence,
                start_point=(start.x, start.y),
                end_point=(end.x, end.y),
                distance=dist,
                duration=dur,
                direction=direction,
                velocity=velocity,
            )

        # Drag/Pan
        return GestureResult(
            GestureType.DRAG,
            confidence=0.7,
            start_point=(start.x, start.y),
            end_point=(end.x, end.y),
            distance=dist,
            duration=dur,
            velocity=seq.average_velocity(),
        )

    def _classify_multi(self, sequences: list[TouchSequence]) -> GestureResult:
        """Classify a multi-touch sequence."""
        if len(sequences) < 2:
            return GestureResult(GestureType.UNKNOWN, 0.0)

        # Simple pinch/spread detection
        start_dists: list[float] = []
        end_dists: list[float] = []

        for i in range(1, len(sequences)):
            s0, s1 = sequences[0], sequences[i]
            if s0.start and s1.start:
                start_dists.append(math.sqrt(
                    (s1.start.x - s0.start.x) ** 2 + (s1.start.y - s0.start.y) ** 2
                ))
            if s0.end and s1.end:
                end_dists.append(math.sqrt(
                    (s1.end.x - s0.end.x) ** 2 + (s1.end.y - s0.end.y) ** 2
                ))

        if start_dists and end_dists:
            avg_start = sum(start_dists) / len(start_dists)
            avg_end = sum(end_dists) / len(end_dists)
            ratio = avg_end / avg_start if avg_start > 0 else 1.0

            if ratio > 1.2:
                return GestureResult(GestureType.SPREAD, confidence=0.8, distance=avg_end - avg_start)
            elif ratio < 0.8:
                return GestureResult(GestureType.PINCH, confidence=0.8, distance=avg_start - avg_end)

        return GestureResult(GestureType.UNKNOWN, confidence=0.5)

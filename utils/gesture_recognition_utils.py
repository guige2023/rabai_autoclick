"""Gesture recognition from touch/click sequences."""

from typing import List, Tuple, Optional, Dict, Any
from enum import Enum
import math


Point = Tuple[float, float]


class GestureType(Enum):
    """Recognized gesture types."""
    TAP = "tap"
    DOUBLE_TAP = "double_tap"
    LONG_PRESS = "long_press"
    SWIPE_LEFT = "swipe_left"
    SWIPE_RIGHT = "swipe_right"
    SWIPE_UP = "swipe_up"
    SWIPE_DOWN = "swipe_down"
    PINCH = "pinch"
    ZOOM = "zoom"
    ROTATE = "rotate"
    UNKNOWN = "unknown"


class GestureRecognizer:
    """Recognize gestures from point sequences."""

    def __init__(
        self,
        tap_threshold: float = 20.0,
        swipe_threshold: float = 50.0,
        long_press_duration: float = 0.5,
        double_tap_interval: float = 0.3
    ):
        """Initialize recognizer.
        
        Args:
            tap_threshold: Max distance for tap detection.
            swipe_threshold: Min distance for swipe detection.
            long_press_duration: Min duration for long press.
            double_tap_interval: Max interval between taps.
        """
        self.tap_threshold = tap_threshold
        self.swipe_threshold = swipe_threshold
        self.long_press_duration = long_press_duration
        self.double_tap_interval = double_tap_interval

    def recognize(
        self,
        points: List[Point],
        timestamps: List[float]
    ) -> Tuple[GestureType, Dict[str, Any]]:
        """Recognize gesture from points and timestamps.
        
        Args:
            points: Sequence of touch/click points.
            timestamps: Corresponding timestamps.
        
        Returns:
            Tuple of (gesture type, metadata dict).
        """
        if not points or len(points) < 1:
            return GestureType.UNKNOWN, {}
        if len(points) == 1:
            return self._recognize_single_tap(points[0], timestamps[0])
        if len(points) == 2:
            return self._recognize_two_point_gesture(points, timestamps)
        return self._recognize_multi_point_gesture(points, timestamps)

    def _recognize_single_tap(
        self,
        point: Point,
        timestamp: float
    ) -> Tuple[GestureType, Dict[str, Any]]:
        """Recognize single point gesture."""
        return GestureType.TAP, {"point": point, "timestamp": timestamp}

    def _recognize_two_point_gesture(
        self,
        points: List[Point],
        timestamps: List[float]
    ) -> Tuple[GestureType, Dict[str, Any]]:
        """Recognize two-point gesture like pinch/zoom."""
        p1, p2 = points[0], points[1]
        dx = p2[0] - p1[0]
        dy = p2[1] - p1[1]
        dist = math.sqrt(dx * dx + dy * dy)
        return GestureType.PINCH, {
            "distance": dist,
            "direction": "in" if dist < self.swipe_threshold else "out",
            "points": points
        }

    def _recognize_multi_point_gesture(
        self,
        points: List[Point],
        timestamps: List[float]
    ) -> Tuple[GestureType, Dict[str, Any]]:
        """Recognize multi-point gesture from path."""
        start = points[0]
        end = points[-1]
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        dist = math.sqrt(dx * dx + dy * dy)
        duration = timestamps[-1] - timestamps[0] if len(timestamps) > 1 else 0
        if dist < self.swipe_threshold:
            if duration > self.long_press_duration:
                return GestureType.LONG_PRESS, {"point": start, "duration": duration}
            return GestureType.TAP, {"point": start, "duration": duration}
        angle = math.degrees(math.atan2(dy, dx))
        if abs(dx) > abs(dy):
            if dx > 0:
                return GestureType.SWIPE_RIGHT, {"angle": angle, "distance": dist}
            return GestureType.SWIPE_LEFT, {"angle": angle, "distance": dist}
        else:
            if dy > 0:
                return GestureType.SWIPE_DOWN, {"angle": angle, "distance": dist}
            return GestureType.SWIPE_UP, {"angle": angle, "distance": dist}

    def recognize_from_events(
        self,
        events: List[Dict[str, Any]]
    ) -> Tuple[GestureType, Dict[str, Any]]:
        """Recognize gesture from event list.
        
        Args:
            events: List of dicts with keys: x, y, event_type, timestamp.
        
        Returns:
            Tuple of (gesture type, metadata dict).
        """
        down_points = [(e["x"], e["y"]) for e in events if e.get("event_type") in ("down", "touchstart")]
        down_times = [e["timestamp"] for e in events if e.get("event_type") in ("down", "touchstart")]
        if not down_points:
            return GestureType.UNKNOWN, {}
        return self.recognize(down_points, down_times)

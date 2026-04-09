"""
Gesture recognition utilities for UI automation.

Provides gesture recognition from touch/mouse input sequences,
including tap, double-tap, long-press, swipe, pinch, and rotate gestures.

Author: Auto-generated
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Protocol


class GestureType(Enum):
    """Types of recognized gestures."""
    TAP = auto()
    DOUBLE_TAP = auto()
    LONG_PRESS = auto()
    SWIPE_UP = auto()
    SWIPE_DOWN = auto()
    SWIPE_LEFT = auto()
    SWIPE_RIGHT = auto()
    PINCH_IN = auto()
    PINCH_OUT = auto()
    ROTATE_CW = auto()
    ROTATE_CCW = auto()
    DRAG = auto()
    UNKNOWN = auto()


@dataclass
class TouchPoint:
    """A single touch point with position and timestamp."""
    x: float
    y: float
    timestamp: float
    pressure: float = 1.0
    finger_id: int = 0


@dataclass 
class GestureTemplate:
    """Template for matching gesture patterns."""
    gesture_type: GestureType
    min_points: int
    max_duration: float
    min_distance: float
    name: str = ""


@dataclass
class RecognizedGesture:
    """Result of gesture recognition."""
    gesture_type: GestureType
    confidence: float
    start_point: TouchPoint
    end_point: TouchPoint
    duration: float
    points: list[TouchPoint]
    metadata: dict = field(default_factory=dict)


class GestureRecognizer:
    """
    Recognizes gestures from sequences of touch/mouse input points.
    
    Example:
        recognizer = GestureRecognizer()
        recognizer.add_point(TouchPoint(100, 200, time.time()))
        recognizer.add_point(TouchPoint(150, 250, time.time() + 0.1))
        gesture = recognizer.recognize()
    """
    
    TAP_TEMPLATE = GestureTemplate(
        GestureType.TAP, min_points=1, max_duration=0.3, min_distance=10
    )
    DOUBLE_TAP_TEMPLATE = GestureTemplate(
        GestureType.DOUBLE_TAP, min_points=2, max_duration=0.5, min_distance=10
    )
    LONG_PRESS_TEMPLATE = GestureTemplate(
        GestureType.LONG_PRESS, min_points=1, max_duration=0.5, min_distance=5
    )
    
    SWIPE_THRESHOLD = 50.0
    SWIPE_MAX_DURATION = 1.0
    PINCH_THRESHOLD = 20.0
    ROTATE_THRESHOLD = 15.0
    
    def __init__(self, templates: list[GestureTemplate] | None = None):
        self._points: list[TouchPoint] = []
        self._start_time: float | None = None
        self._templates = templates or [
            self.TAP_TEMPLATE,
            self.DOUBLE_TAP_TEMPLATE,
            self.LONG_PRESS_TEMPLATE,
        ]
    
    def add_point(self, point: TouchPoint) -> None:
        """Add a touch point to the recognition buffer."""
        if self._start_time is None:
            self._start_time = point.timestamp
        self._points.append(point)
    
    def reset(self) -> None:
        """Clear all buffered points."""
        self._points.clear()
        self._start_time = None
    
    def recognize(self) -> RecognizedGesture:
        """
        Recognize the gesture from buffered points.
        
        Returns:
            RecognizedGesture with type and confidence.
        """
        if len(self._points) < 1:
            return RecognizedGesture(
                GestureType.UNKNOWN, 0.0,
                TouchPoint(0, 0, 0), TouchPoint(0, 0, 0), 0.0, []
            )
        
        start = self._points[0]
        end = self._points[-1]
        duration = end.timestamp - (self._start_time or end.timestamp)
        
        gesture_type = self._classify_gesture(start, end, duration)
        confidence = self._calculate_confidence(
            gesture_type, start, end, duration
        )
        
        result = RecognizedGesture(
            gesture_type=gesture_type,
            confidence=confidence,
            start_point=start,
            end_point=end,
            duration=duration,
            points=self._points.copy(),
            metadata=self._extract_metadata(gesture_type, start, end),
        )
        
        return result
    
    def _classify_gesture(
        self, start: TouchPoint, end: TouchPoint, duration: float
    ) -> GestureType:
        """Classify the gesture based on start/end points and duration."""
        dx = end.x - start.x
        dy = end.y - start.y
        distance = math.sqrt(dx * dx + dy * dy)
        
        # Long press detection
        if duration > self.LONG_PRESS_TEMPLATE.max_duration:
            if distance < self.LONG_PRESS_TEMPLATE.min_distance:
                return GestureType.LONG_PRESS
        
        # Swipe detection
        if distance > self.SWIPE_THRESHOLD and duration < self.SWIPE_MAX_DURATION:
            angle = math.degrees(math.atan2(dy, dx))
            
            if -45 <= angle <= 45:
                return GestureType.SWIPE_RIGHT
            elif 45 < angle <= 135:
                return GestureType.SWIPE_DOWN
            elif -135 <= angle < -45:
                return GestureType.SWIPE_UP
            else:
                return GestureType.SWIPE_LEFT
        
        # Tap detection
        if duration < self.TAP_TEMPLATE.max_duration:
            if distance < self.TAP_TEMPLATE.min_distance:
                return GestureType.TAP
        
        return GestureType.UNKNOWN
    
    def _calculate_confidence(
        self, gesture_type: GestureType, start: TouchPoint,
        end: TouchPoint, duration: float
    ) -> float:
        """Calculate confidence score for the recognized gesture."""
        if gesture_type == GestureType.UNKNOWN:
            return 0.0
        
        distance = math.sqrt(
            (end.x - start.x) ** 2 + (end.y - start.y) ** 2
        )
        
        if gesture_type in (GestureType.TAP, GestureType.LONG_PRESS):
            confidence = min(1.0, distance / 20.0)
            if gesture_type == GestureType.LONG_PRESS:
                confidence *= min(1.0, duration / 1.0)
        
        elif gesture_type in (
            GestureType.SWIPE_UP, GestureType.SWIPE_DOWN,
            GestureType.SWIPE_LEFT, GestureType.SWIPE_RIGHT
        ):
            confidence = min(1.0, distance / 100.0)
            confidence *= 0.9 if duration < 0.3 else 0.7
        
        else:
            confidence = 0.5
        
        return confidence
    
    def _extract_metadata(
        self, gesture_type: GestureType, start: TouchPoint, end: TouchPoint
    ) -> dict:
        """Extract additional metadata from the gesture."""
        dx = end.x - start.x
        dy = end.y - start.y
        distance = math.sqrt(dx * dx + dy * dy)
        angle = math.degrees(math.atan2(dy, dx)) if distance > 0 else 0
        
        return {
            "delta_x": dx,
            "delta_y": dy,
            "distance": distance,
            "angle_degrees": angle,
            "start_position": (start.x, start.y),
            "end_position": (end.x, end.y),
        }


class MultiTouchGestureRecognizer:
    """
    Recognizer for multi-touch gestures like pinch and rotate.
    
    Tracks multiple fingers independently and detects
    pinch and rotation gestures.
    """
    
    PINCH_THRESHOLD = 0.7
    
    def __init__(self):
        self._finger_tracks: dict[int, list[TouchPoint]] = {}
        self._start_distance: float | None = None
    
    def add_finger_point(self, finger_id: int, point: TouchPoint) -> None:
        """Add a point for a specific finger."""
        if finger_id not in self._finger_tracks:
            self._finger_tracks[finger_id] = []
        self._finger_tracks[finger_id].append(point)
    
    def recognize_pinch(self) -> tuple[GestureType, float]:
        """
        Recognize pinch gesture.
        
        Returns:
            Tuple of (GestureType.PINCH_IN or PINCH_OUT, scale_factor)
        """
        if len(self._finger_tracks) < 2:
            return GestureType.UNKNOWN, 1.0
        
        tracks = list(self._finger_tracks.values())
        if len(tracks[0]) < 2 or len(tracks[1]) < 2:
            return GestureType.UNKNOWN, 1.0
        
        # Calculate initial distance between fingers
        p0_start = tracks[0][0]
        p1_start = tracks[1][0]
        start_dist = math.sqrt(
            (p0_start.x - p1_start.x) ** 2 +
            (p0_start.y - p1_start.y) ** 2
        )
        
        # Calculate current distance
        p0_end = tracks[0][-1]
        p1_end = tracks[1][-1]
        end_dist = math.sqrt(
            (p0_end.x - p1_end.x) ** 2 +
            (p0_end.y - p1_end.y) ** 2
        )
        
        if start_dist == 0:
            return GestureType.UNKNOWN, 1.0
        
        scale = end_dist / start_dist
        
        if scale < self.PINCH_THRESHOLD:
            return GestureType.PINCH_IN, scale
        elif scale > 1 / self.PINCH_THRESHOLD:
            return GestureType.PINCH_OUT, scale
        
        return GestureType.UNKNOWN, scale
    
    def recognize_rotation(self) -> tuple[GestureType, float]:
        """
        Recognize rotation gesture.
        
        Returns:
            Tuple of (GestureType.ROTATE_CW or ROTATE_CCW, angle_degrees)
        """
        if len(self._finger_tracks) < 2:
            return GestureType.UNKNOWN, 0.0
        
        tracks = list(self._finger_tracks.values())
        if len(tracks[0]) < 2 or len(tracks[1]) < 2:
            return GestureType.UNKNOWN, 0.0
        
        # Calculate initial angle
        p0_start = tracks[0][0]
        p1_start = tracks[1][0]
        start_angle = math.degrees(
            math.atan2(p1_start.y - p0_start.y, p1_start.x - p0_start.x)
        )
        
        # Calculate current angle
        p0_end = tracks[0][-1]
        p1_end = tracks[1][-1]
        end_angle = math.degrees(
            math.atan2(p1_end.y - p0_end.y, p1_end.x - p0_end.x)
        )
        
        delta = end_angle - start_angle
        
        # Normalize to [-180, 180]
        while delta > 180:
            delta -= 360
        while delta < -180:
            delta += 360
        
        if abs(delta) > self.ROTATE_THRESHOLD:
            return (
                GestureType.ROTATE_CW if delta > 0 else GestureType.ROTATE_CCW,
                abs(delta),
            )
        
        return GestureType.UNKNOWN, abs(delta)
    
    def reset(self) -> None:
        """Clear all tracked fingers."""
        self._finger_tracks.clear()
        self._start_distance = None


def create_swipe_gesture(
    start_x: float, start_y: float,
    end_x: float, end_y: float,
    duration: float = 0.3,
    num_points: int = 10
) -> list[TouchPoint]:
    """
    Create a synthetic swipe gesture for testing/replay.
    
    Args:
        start_x: Start X coordinate
        start_y: Start Y coordinate
        end_x: End X coordinate
        end_y: End Y coordinate
        duration: Duration of swipe in seconds
        num_points: Number of intermediate points
    
    Returns:
        List of TouchPoints forming the swipe path
    """
    points = []
    interval = duration / num_points
    start_time = time.time()
    
    for i in range(num_points + 1):
        t = i / num_points
        x = start_x + (end_x - start_x) * t
        y = start_y + (end_y - start_y) * t
        timestamp = start_time + interval * i
        
        points.append(TouchPoint(x, y, timestamp))
    
    return points


def recognize_from_points(
    points: list[tuple[float, float, float]]
) -> RecognizedGesture:
    """
    Convenience function to recognize gesture from raw point tuples.
    
    Args:
        points: List of (x, y, timestamp) tuples
    
    Returns:
        RecognizedGesture result
    """
    touch_points = [
        TouchPoint(x, y, ts) for x, y, ts in points
    ]
    
    recognizer = GestureRecognizer()
    for pt in touch_points:
        recognizer.add_point(pt)
    
    return recognizer.recognize()

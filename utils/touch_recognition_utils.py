"""
Touch gesture recognition utilities.

Recognize common gesture patterns from touch data.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional


class GestureType(Enum):
    """Recognized gesture types."""
    UNKNOWN = auto()
    TAP = auto()
    DOUBLE_TAP = auto()
    LONG_PRESS = auto()
    SWIPE_UP = auto()
    SWIPE_DOWN = auto()
    SWIPE_LEFT = auto()
    SWIPE_RIGHT = auto()
    DRAG = auto()
    PINCH = auto()
    SPREAD = auto()
    ROTATE = auto()
    TWO_FINGER_SWIPE = auto()


@dataclass
class RecognizedGesture:
    """A recognized gesture."""
    gesture_type: GestureType
    confidence: float
    start_point: tuple[float, float]
    end_point: tuple[float, float]
    duration_ms: float
    metadata: dict


@dataclass
class TouchData:
    """Touch input data."""
    x: float
    y: float
    timestamp: float
    finger_id: int = 0
    pressure: float = 1.0


class GestureRecognizer:
    """Recognize gestures from touch data."""
    
    def __init__(self):
        self._tap_threshold = 200
        self._swipe_min_distance = 50
        self._long_press_duration = 500
        self._velocity_threshold = 100
    
    def recognize(self, touches: list[TouchData]) -> RecognizedGesture:
        """Recognize gesture from touch data."""
        if not touches:
            return RecognizedGesture(
                gesture_type=GestureType.UNKNOWN,
                confidence=0.0,
                start_point=(0, 0),
                end_point=(0, 0),
                duration_ms=0,
                metadata={}
            )
        
        sorted_touches = sorted(touches, key=lambda t: t.timestamp)
        
        if len(sorted_touches) == 1:
            return self._recognize_tap_or_press(sorted_touches)
        
        unique_fingers = set(t.finger_id for t in sorted_touches)
        
        if len(unique_fingers) == 1:
            return self._recognize_single_finger_gesture(sorted_touches)
        
        return self._recognize_multi_finger_gesture(sorted_touches, len(unique_fingers))
    
    def _recognize_tap_or_press(self, touches: list[TouchData]) -> RecognizedGesture:
        """Recognize tap or long press from single touch."""
        duration = (touches[-1].timestamp - touches[0].timestamp) * 1000
        
        start = (touches[0].x, touches[0].y)
        end = (touches[-1].x, touches[-1].y)
        
        if duration < self._tap_threshold:
            gesture_type = GestureType.TAP
            confidence = 0.8
        else:
            gesture_type = GestureType.LONG_PRESS
            confidence = 0.7
        
        return RecognizedGesture(
            gesture_type=gesture_type,
            confidence=confidence,
            start_point=start,
            end_point=end,
            duration_ms=duration,
            metadata={"duration": duration}
        )
    
    def _recognize_single_finger_gesture(self, touches: list[TouchData]) -> RecognizedGesture:
        """Recognize gesture from single finger."""
        duration = (touches[-1].timestamp - touches[0].timestamp) * 1000
        
        start = (touches[0].x, touches[0].y)
        end = (touches[-1].x, touches[-1].y)
        
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        distance = math.sqrt(dx * dx + dy * dy)
        
        if distance < self._swipe_min_distance:
            if duration < self._tap_threshold:
                return RecognizedGesture(
                    gesture_type=GestureType.TAP,
                    confidence=0.8,
                    start_point=start,
                    end_point=end,
                    duration_ms=duration,
                    metadata={}
                )
            return RecognizedGesture(
                gesture_type=GestureType.LONG_PRESS,
                confidence=0.7,
                start_point=start,
                end_point=end,
                duration_ms=duration,
                metadata={}
            )
        
        angle = math.degrees(math.atan2(dy, dx))
        
        if -45 <= angle < 45:
            gesture_type = GestureType.SWIPE_RIGHT
        elif 45 <= angle < 135:
            gesture_type = GestureType.SWIPE_DOWN
        elif -135 <= angle < -45:
            gesture_type = GestureType.SWIPE_UP
        else:
            gesture_type = GestureType.SWIPE_LEFT
        
        velocity = distance / duration if duration > 0 else 0
        confidence = min(0.9, velocity / self._velocity_threshold) if velocity > 0 else 0.5
        
        return RecognizedGesture(
            gesture_type=gesture_type,
            confidence=confidence,
            start_point=start,
            end_point=end,
            duration_ms=duration,
            metadata={"distance": distance, "angle": angle, "velocity": velocity}
        )
    
    def _recognize_multi_finger_gesture(
        self,
        touches: list[TouchData],
        finger_count: int
    ) -> RecognizedGesture:
        """Recognize multi-finger gesture."""
        if finger_count == 2:
            return RecognizedGesture(
                gesture_type=GestureType.TWO_FINGER_SWIPE,
                confidence=0.6,
                start_point=(0, 0),
                end_point=(0, 0),
                duration_ms=0,
                metadata={"finger_count": finger_count}
            )
        
        return RecognizedGesture(
            gesture_type=GestureType.UNKNOWN,
            confidence=0.0,
            start_point=(0, 0),
            end_point=(0, 0),
            duration_ms=0,
            metadata={}
        )


class SwipeGestureAnalyzer:
    """Analyze swipe gesture details."""
    
    def __init__(self):
        self._min_swipe_distance = 50
        self._direction_threshold = 0.5
    
    def analyze_swipe(
        self,
        start: tuple[float, float],
        end: tuple[float, float],
        points: list[tuple[float, float]]
    ) -> dict:
        """Analyze swipe gesture in detail."""
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        distance = math.sqrt(dx * dx + dy * dy)
        
        if distance < self._min_swipe_distance:
            return {"valid": False, "reason": "too_short"}
        
        direction = self._get_direction(dx, dy)
        
        curvature = self._calculate_curvature(points) if len(points) > 2 else 0
        
        velocity = self._calculate_average_velocity(points) if len(points) > 1 else 0
        
        return {
            "valid": True,
            "direction": direction,
            "distance": distance,
            "curvature": curvature,
            "average_velocity": velocity,
            "displacement": (dx, dy)
        }
    
    def _get_direction(self, dx: float, dy: float) -> str:
        """Get swipe direction."""
        if abs(dx) > abs(dy):
            return "horizontal" if dx > 0 else "horizontal"
        return "vertical" if dy > 0 else "vertical"
    
    def _calculate_curvature(self, points: list[tuple[float, float]]) -> float:
        """Calculate path curvature."""
        if len(points) < 3:
            return 0
        
        angles = []
        for i in range(1, len(points) - 1):
            v1 = (points[i][0] - points[i-1][0], points[i][1] - points[i-1][1])
            v2 = (points[i+1][0] - points[i][0], points[i+1][1] - points[i][1])
            
            dot = v1[0] * v2[0] + v1[1] * v2[1]
            mag1 = math.sqrt(v1[0]**2 + v1[1]**2)
            mag2 = math.sqrt(v2[0]**2 + v2[1]**2)
            
            if mag1 > 0 and mag2 > 0:
                cos_angle = dot / (mag1 * mag2)
                angles.append(math.acos(max(-1, min(1, cos_angle))))
        
        return sum(angles) / len(angles) if angles else 0
    
    def _calculate_average_velocity(self, points: list[tuple[float, float]]) -> float:
        """Calculate average velocity."""
        if len(points) < 2:
            return 0
        
        total_distance = sum(
            math.sqrt((points[i][0] - points[i-1][0])**2 + (points[i][1] - points[i-1][1])**2)
            for i in range(1, len(points))
        )
        
        return total_distance / len(points)

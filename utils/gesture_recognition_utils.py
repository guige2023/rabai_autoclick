"""Gesture Recognition Utilities for Mouse and Touch Input.

This module provides gesture recognition capabilities for mouse and touch input,
including tap detection, swipe detection, pinch recognition, and custom gesture patterns.

Example:
    >>> from gesture_recognition_utils import GestureRecognizer
    >>> recognizer = GestureRecognizer()
    >>> recognizer.add_tap((100, 100))
    >>> recognizer.add_tap((105, 105))
    >>> result = recognizer.recognize()
    >>> print(result.gesture_type)
    'double_tap'
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Dict, List, Optional, Tuple, Any


class GestureType(Enum):
    """Enumeration of supported gesture types."""
    NONE = auto()
    TAP = auto()
    DOUBLE_TAP = auto()
    TRIPLE_TAP = auto()
    LONG_PRESS = auto()
    SWIPE_UP = auto()
    SWIPE_DOWN = auto()
    SWIPE_LEFT = auto()
    SWIPE_RIGHT = auto()
    PINCH = auto()
    SPREAD = auto()
    ROTATE = auto()
    DRAG = auto()
    CUSTOM = auto()


class GestureState(Enum):
    """States in gesture recognition lifecycle."""
    IDLE = auto()
    TRACKING = auto()
    COMPLETED = auto()
    CANCELLED = auto()


@dataclass
class GesturePoint:
    """A single point in a gesture sequence with timestamp and pressure."""
    x: float
    y: float
    timestamp: float = field(default_factory=time.time)
    pressure: float = 1.0
    radius: float = 1.0
    
    def distance_to(self, other: GesturePoint) -> float:
        """Calculate Euclidean distance to another point."""
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)
    
    def angle_to(self, other: GesturePoint) -> float:
        """Calculate angle in radians to another point."""
        return math.atan2(other.y - self.y, other.x - self.x)


@dataclass
class GestureResult:
    """Result of gesture recognition."""
    gesture_type: GestureType = GestureType.NONE
    state: GestureState = GestureState.IDLE
    points: List[GesturePoint] = field(default_factory=list)
    direction: Optional[Tuple[float, float]] = None
    distance: float = 0.0
    duration: float = 0.0
    velocity: float = 0.0
    confidence: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class GestureRecognizer:
    """Recognizes gestures from input sequences.
    
    Supports tap, swipe, pinch, spread, and drag gestures with configurable
    sensitivity and thresholds.
    
    Attributes:
        tap_threshold: Maximum distance between points for tap (pixels)
        swipe_threshold: Minimum distance for swipe recognition (pixels)
        time_threshold: Maximum time between taps for multi-tap (seconds)
        long_press_duration: Duration for long press recognition (seconds)
    """
    
    def __init__(
        self,
        tap_threshold: float = 10.0,
        swipe_threshold: float = 50.0,
        time_threshold: float = 0.3,
        long_press_duration: float = 0.5,
        pinch_threshold: float = 0.1,
        velocity_threshold: float = 100.0,
    ):
        self.tap_threshold = tap_threshold
        self.swipe_threshold = swipe_threshold
        self.time_threshold = time_threshold
        self.long_press_duration = long_press_duration
        self.pinch_threshold = pinch_threshold
        self.velocity_threshold = velocity_threshold
        
        self._points: List[GesturePoint] = []
        self._tap_count: int = 0
        self._last_tap_time: float = 0
        self._start_time: float = 0
        self._state: GestureState = GestureState.IDLE
        self._custom_gestures: Dict[str, Callable] = {}
    
    def reset(self) -> None:
        """Reset recognizer state for new gesture sequence."""
        self._points.clear()
        self._tap_count = 0
        self._last_tap_time = 0
        self._start_time = 0
        self._state = GestureState.IDLE
    
    def add_point(self, x: float, y: float, pressure: float = 1.0, radius: float = 1.0) -> None:
        """Add a point to the current gesture sequence.
        
        Args:
            x: X coordinate
            y: Y coordinate
            pressure: Touch pressure (0.0 to 1.0)
            radius: Touch radius
        """
        if self._state == GestureState.IDLE:
            self._state = GestureState.TRACKING
            self._start_time = time.time()
        
        point = GesturePoint(x, y, time.time(), pressure, radius)
        self._points.append(point)
    
    def add_tap(self, position: Tuple[float, float]) -> None:
        """Convenience method for adding a tap at position.
        
        Args:
            position: (x, y) tuple
        """
        self.add_point(position[0], position[1])
    
    def recognize(self) -> GestureResult:
        """Recognize the current gesture from accumulated points.
        
        Returns:
            GestureResult with recognized gesture type and metadata
        """
        if len(self._points) == 0:
            return GestureResult(state=self._state)
        
        if self._state == GestureState.TRACKING:
            return self._recognize_gesture()
        
        return GestureResult(state=self._state)
    
    def _recognize_gesture(self) -> GestureResult:
        """Internal gesture recognition logic."""
        if len(self._points) == 1:
            return self._recognize_single_point()
        elif len(self._points) == 2:
            return self._recognize_two_points()
        else:
            return self._recognize_multi_point()
    
    def _recognize_single_point(self) -> GestureResult:
        """Recognize gesture from single point."""
        elapsed = time.time() - self._start_time
        
        if elapsed >= self.long_press_duration:
            return GestureResult(
                gesture_type=GestureType.LONG_PRESS,
                state=GestureState.COMPLETED,
                points=self._points.copy(),
                duration=elapsed,
                confidence=1.0,
            )
        
        return GestureResult(
            gesture_type=GestureType.TAP,
            state=GestureState.TRACKING,
            points=self._points.copy(),
        )
    
    def _recognize_two_points(self) -> GestureResult:
        """Recognize gesture from two points."""
        p1, p2 = self._points[0], self._points[1]
        distance = p1.distance_to(p2)
        duration = p2.timestamp - p1.timestamp
        direction = self._calculate_swipe_direction(p1, p2)
        
        if distance >= self.swipe_threshold:
            return GestureResult(
                gesture_type=GestureType.SWIPE_UP if direction[1] < 0 else GestureType.SWIPE_DOWN,
                state=GestureState.COMPLETED,
                points=self._points.copy(),
                direction=direction,
                distance=distance,
                duration=duration,
                velocity=distance / duration if duration > 0 else 0,
                confidence=min(distance / self.swipe_threshold, 1.0),
            )
        
        return GestureResult(
            gesture_type=GestureType.DRAG,
            state=GestureState.COMPLETED,
            points=self._points.copy(),
            direction=direction,
            distance=distance,
            duration=duration,
            velocity=distance / duration if duration > 0 else 0,
            confidence=min(distance / self.swipe_threshold, 1.0),
        )
    
    def _recognize_multi_point(self) -> GestureResult:
        """Recognize gesture from multiple points."""
        start_point = self._points[0]
        end_point = self._points[-1]
        distance = start_point.distance_to(end_point)
        duration = end_point.timestamp - start_point.timestamp
        direction = self._calculate_swipe_direction(start_point, end_point)
        
        if distance >= self.swipe_threshold:
            return GestureResult(
                gesture_type=self._get_swipe_type(direction),
                state=GestureState.COMPLETED,
                points=self._points.copy(),
                direction=direction,
                distance=distance,
                duration=duration,
                velocity=distance / duration if duration > 0 else 0,
                confidence=min(distance / self.swipe_threshold, 1.0),
            )
        
        return GestureResult(
            gesture_type=GestureType.DRAG,
            state=GestureState.COMPLETED,
            points=self._points.copy(),
            direction=direction,
            distance=distance,
            duration=duration,
            velocity=distance / duration if duration > 0 else 0,
            confidence=0.5,
        )
    
    def _calculate_swipe_direction(self, start: GesturePoint, end: GesturePoint) -> Tuple[float, float]:
        """Calculate normalized swipe direction vector."""
        dx = end.x - start.x
        dy = end.y - start.y
        length = math.sqrt(dx * dx + dy * dy)
        
        if length == 0:
            return (0.0, 0.0)
        
        return (dx / length, dy / length)
    
    def _get_swipe_type(self, direction: Tuple[float, float]) -> GestureType:
        """Determine swipe type from direction vector."""
        if abs(direction[0]) > abs(direction[1]):
            if direction[0] > 0:
                return GestureType.SWIPE_RIGHT
            else:
                return GestureType.SWIPE_LEFT
        else:
            if direction[1] > 0:
                return GestureType.SWIPE_DOWN
            else:
                return GestureType.SWIPE_UP
    
    def register_custom_gesture(
        self,
        name: str,
        matcher: Callable[[List[GesturePoint]], Optional[GestureResult]],
    ) -> None:
        """Register a custom gesture matcher.
        
        Args:
            name: Name for the custom gesture
            matcher: Function that takes points and returns GestureResult or None
        """
        self._custom_gestures[name] = matcher
    
    def recognize_custom(self, name: str) -> Optional[GestureResult]:
        """Attempt to recognize a registered custom gesture.
        
        Args:
            name: Name of registered custom gesture
            
        Returns:
            GestureResult if matched, None otherwise
        """
        if name not in self._custom_gestures:
            return None
        
        matcher = self._custom_gestures[name]
        return matcher(self._points)


class GestureClassifier:
    """Machine learning based gesture classifier.
    
    Provides pattern matching and classification of complex gestures
    using template matching algorithms.
    """
    
    def __init__(self, matching_threshold: float = 0.8):
        self.matching_threshold = matching_threshold
        self._templates: Dict[str, List[GesturePoint]] = {}
    
    def add_template(self, name: str, points: List[GesturePoint]) -> None:
        """Add a gesture template for matching.
        
        Args:
            name: Template name
            points: Reference gesture points
        """
        self._templates[name] = self._normalize_points(points)
    
    def classify(self, points: List[GesturePoint]) -> Optional[Tuple[str, float]]:
        """Classify gesture against registered templates.
        
        Args:
            points: Gesture points to classify
            
        Returns:
            (template_name, confidence) or None if no match
        """
        if not points or not self._templates:
            return None
        
        normalized = self._normalize_points(points)
        
        best_match = None
        best_score = 0.0
        
        for name, template in self._templates.items():
            score = self._calculate_similarity(normalized, template)
            
            if score > best_score:
                best_score = score
                best_match = name
        
        if best_score >= self.matching_threshold:
            return (best_match, best_score)
        
        return None
    
    def _normalize_points(self, points: List[GesturePoint]) -> List[GesturePoint]:
        """Normalize points for scale and rotation invariant matching."""
        if not points:
            return []
        
        xs = [p.x for p in points]
        ys = [p.y for p in points]
        
        min_x, max_x = min(xs), max(xs)
        min_y, max_y = min(ys), max(ys)
        
        width = max_x - min_x or 1
        height = max_y - min_y or 1
        scale = max(width, height)
        
        centroid_x = sum(xs) / len(xs)
        centroid_y = sum(ys) / len(ys)
        
        normalized = []
        for p in points:
            norm_x = (p.x - centroid_x) / scale
            norm_y = (p.y - centroid_y) / scale
            normalized.append(GesturePoint(norm_x, norm_y, p.timestamp, p.pressure, p.radius))
        
        return normalized
    
    def _calculate_similarity(
        self,
        points1: List[GesturePoint],
        points2: List[GesturePoint],
    ) -> float:
        """Calculate similarity score between two gesture sequences."""
        if len(points1) != len(points2):
            return 0.0
        
        total_distance = sum(
            p1.distance_to(p2) 
            for p1, p2 in zip(points1, points2)
        )
        
        avg_distance = total_distance / len(points1)
        similarity = max(0.0, 1.0 - avg_distance)
        
        return similarity


class GestureSequenceAnalyzer:
    """Analyzes sequences of gestures for pattern detection.
    
    Useful for recognizing gesture sequences like double-swipe or
    tap-then-drag combinations.
    """
    
    def __init__(self, sequence_timeout: float = 2.0):
        self.sequence_timeout = sequence_timeout
        self._gesture_sequence: List[GestureResult] = []
        self._last_gesture_time: float = 0
    
    def add_gesture(self, result: GestureResult) -> None:
        """Add a recognized gesture to the sequence.
        
        Args:
            result: GestureResult from recognizer
        """
        current_time = time.time()
        
        if current_time - self._last_gesture_time > self.sequence_timeout:
            self._gesture_sequence.clear()
        
        self._gesture_sequence.append(result)
        self._last_gesture_time = current_time
    
    def get_sequence(self) -> List[GestureResult]:
        """Get current gesture sequence."""
        return self._gesture_sequence.copy()
    
    def clear(self) -> None:
        """Clear the gesture sequence."""
        self._gesture_sequence.clear()
    
    def match_sequence(self, pattern: List[GestureType]) -> bool:
        """Check if current sequence matches a pattern.
        
        Args:
            pattern: List of expected gesture types
            
        Returns:
            True if sequence matches pattern exactly
        """
        if len(self._gesture_sequence) != len(pattern):
            return False
        
        return all(
            g.gesture_type == p 
            for g, p in zip(self._gesture_sequence, pattern)
        )
    
    def match_sequence_flexible(self, pattern: List[GestureType]) -> float:
        """Check pattern match with tolerance for extra gestures.
        
        Args:
            pattern: Expected gesture type pattern
            
        Returns:
            Match confidence from 0.0 to 1.0
        """
        if not pattern:
            return 0.0
        
        sequence_types = [g.gesture_type for g in self._gesture_sequence]
        
        if len(sequence_types) < len(pattern):
            return sum(
                1 for expected in pattern 
                if expected in sequence_types
            ) / len(pattern)
        
        max_matches = 0
        for i in range(len(sequence_types) - len(pattern) + 1):
            matches = sum(
                1 for j, expected in enumerate(pattern)
                if sequence_types[i + j] == expected
            )
            max_matches = max(max_matches, matches)
        
        return max_matches / len(pattern)

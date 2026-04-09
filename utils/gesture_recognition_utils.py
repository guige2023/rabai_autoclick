"""
Gesture Recognition Utilities for UI Automation.

This module provides utilities for recognizing and executing complex
gestures like swipes, pinches, and multi-touch patterns.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


class GestureType(Enum):
    """Types of supported gestures."""
    TAP = auto()
    DOUBLE_TAP = auto()
    LONG_PRESS = auto()
    SWIPE = auto()
    FLING = auto()
    PINCH = auto()
    SPREAD = auto()
    ROTATE = auto()
    DRAG = auto()
    PAN = auto()


@dataclass
class Point:
    """Represents a 2D point with optional timestamp."""
    x: float
    y: float
    timestamp: float = field(default_factory=time.time)
    
    def distance_to(self, other: 'Point') -> float:
        """Calculate Euclidean distance to another point."""
        dx = self.x - other.x
        dy = self.y - other.y
        return math.sqrt(dx * dx + dy * dy)


@dataclass
class GestureTemplate:
    """
    Template for gesture recognition.
    
    Attributes:
        gesture_type: Type of gesture
        points: Normalized points defining the gesture
        tolerance: Matching tolerance threshold
    """
    gesture_type: GestureType
    name: str
    points: list[Point]
    tolerance: float = 0.2
    
    def to_normalized(self, bounds_width: float, bounds_height: float) -> 'GestureTemplate':
        """Return a normalized version of this template."""
        if not self.points:
            return self
        
        min_x = min(p.x for p in self.points)
        max_x = max(p.x for p in self.points)
        min_y = min(p.y for p in self.points)
        max_y = max(p.y for p in self.points)
        
        width = max_x - min_x or 1.0
        height = max_y - min_y or 1.0
        
        normalized_points = [
            Point(
                (p.x - min_x) / width,
                (p.y - min_y) / height,
                p.timestamp
            )
            for p in self.points
        ]
        
        return GestureTemplate(
            gesture_type=self.gesture_type,
            name=self.name,
            points=normalized_points,
            tolerance=self.tolerance
        )


@dataclass
class Gesture:
    """
    Represents a recognized or defined gesture.
    
    Attributes:
        gesture_type: Type of gesture
        points: Points comprising the gesture
        start_point: Gesture start point
        end_point: Gesture end point
        duration_ms: Total gesture duration
        velocity: Gesture velocity if applicable
    """
    gesture_type: GestureType
    points: list[Point]
    start_point: Optional[Point] = None
    end_point: Optional[Point] = None
    duration_ms: float = 0.0
    velocity: float = 0.0
    metadata: dict = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.start_point and self.points:
            self.start_point = self.points[0]
        if not self.end_point and len(self.points) > 1:
            self.end_point = self.points[-1]
    
    @property
    def direction(self) -> Optional[str]:
        """Get swipe direction if applicable."""
        if self.gesture_type != GestureType.SWIPE or not self.start_point or not self.end_point:
            return None
        
        dx = self.end_point.x - self.start_point.x
        dy = self.end_point.y - self.start_point.y
        
        angle = math.degrees(math.atan2(dy, dx))
        
        if -45 <= angle < 45:
            return "right"
        elif 45 <= angle < 135:
            return "down"
        elif -135 <= angle < -45:
            return "up"
        else:
            return "left"
    
    @property
    def distance(self) -> float:
        """Get total gesture distance."""
        if len(self.points) < 2:
            return 0.0
        
        total = 0.0
        for i in range(1, len(self.points)):
            total += self.points[i-1].distance_to(self.points[i])
        return total


class GestureRecognizer:
    """
    Recognizes gestures from point sequences.
    
    Example:
        recognizer = GestureRecognizer()
        gesture = recognizer.recognize(points, GestureType.SWIPE)
    """
    
    def __init__(self):
        self._templates: dict[str, GestureTemplate] = {}
        self._register_default_templates()
    
    def _register_default_templates(self) -> None:
        """Register default gesture templates."""
        # Tap
        self.register_template(GestureTemplate(
            gesture_type=GestureType.TAP,
            name="tap",
            points=[Point(0.5, 0.5)],
            tolerance=0.3
        ))
        
        # Swipe Right
        self.register_template(GestureTemplate(
            gesture_type=GestureType.SWIPE,
            name="swipe_right",
            points=[Point(0.2, 0.5), Point(0.5, 0.5), Point(0.8, 0.5)],
            tolerance=0.25
        ))
        
        # Swipe Left
        self.register_template(GestureTemplate(
            gesture_type=GestureType.SWIPE,
            name="swipe_left",
            points=[Point(0.8, 0.5), Point(0.5, 0.5), Point(0.2, 0.5)],
            tolerance=0.25
        ))
        
        # Swipe Down
        self.register_template(GestureTemplate(
            gesture_type=GestureType.SWIPE,
            name="swipe_down",
            points=[Point(0.5, 0.2), Point(0.5, 0.5), Point(0.5, 0.8)],
            tolerance=0.25
        ))
        
        # Swipe Up
        self.register_template(GestureTemplate(
            gesture_type=GestureType.SWIPE,
            name="swipe_up",
            points=[Point(0.5, 0.8), Point(0.5, 0.5), Point(0.5, 0.2)],
            tolerance=0.25
        ))
    
    def register_template(self, template: GestureTemplate) -> None:
        """Register a gesture template."""
        self._templates[template.name] = template
    
    def recognize(
        self, 
        points: list[Point], 
        expected_type: Optional[GestureType] = None
    ) -> Optional[Gesture]:
        """
        Recognize a gesture from a sequence of points.
        
        Args:
            points: Sequence of points defining the gesture
            expected_type: Optional expected gesture type filter
            
        Returns:
            Recognized Gesture or None if no match
        """
        if len(points) < 2:
            return None
        
        # Determine gesture type based on points
        gesture_type = expected_type or self._classify_gesture(points)
        
        # Calculate duration
        duration_ms = (points[-1].timestamp - points[0].timestamp) * 1000
        
        # Calculate velocity
        total_distance = sum(
            points[i-1].distance_to(points[i]) 
            for i in range(1, len(points))
        )
        velocity = total_distance / (duration_ms / 1000) if duration_ms > 0 else 0
        
        return Gesture(
            gesture_type=gesture_type,
            points=points,
            start_point=points[0],
            end_point=points[-1],
            duration_ms=duration_ms,
            velocity=velocity
        )
    
    def _classify_gesture(self, points: list[Point]) -> GestureType:
        """Classify gesture type based on point analysis."""
        if len(points) == 1:
            return GestureType.TAP
        
        # Analyze gesture characteristics
        total_distance = sum(
            points[i-1].distance_to(points[i]) 
            for i in range(1, len(points))
        )
        
        duration_ms = (points[-1].timestamp - points[0].timestamp) * 1000
        
        # Calculate bounding box
        min_x = min(p.x for p in points)
        max_x = max(p.x for p in points)
        min_y = min(p.y for p in points)
        max_y = max(p.y for p in points)
        
        width = max_x - min_x
        height = max_y - min_y
        aspect_ratio = width / height if height > 0 else 0
        
        # Velocity-based classification
        velocity = total_distance / (duration_ms / 1000) if duration_ms > 0 else 0
        
        if velocity > 1000:
            return GestureType.FLING
        
        if aspect_ratio > 2:
            return GestureType.SWIPE
        elif aspect_ratio < 0.5:
            return GestureType.SWIPE
        else:
            return GestureType.PAN
    
    def match_template(
        self, 
        points: list[Point],
        template_name: str
    ) -> float:
        """
        Match a gesture against a template.
        
        Args:
            points: Gesture points
            template_name: Name of template to match against
            
        Returns:
            Match score (0.0 - 1.0), higher is better
        """
        template = self._templates.get(template_name)
        if not template or len(points) < 2:
            return 0.0
        
        # Normalize both gestures
        normalized_points = self._normalize_points(points)
        
        # Resample to same number of points
        resampled = self._resample_points(normalized_points, len(template.points))
        
        # Calculate distance score
        total_distance = sum(
            resampled[i].distance_to(template.points[i])
            for i in range(len(resampled))
        )
        
        avg_distance = total_distance / len(resampled)
        max_distance = math.sqrt(2)  # Maximum possible normalized distance
        
        score = 1.0 - (avg_distance / max_distance)
        return max(0.0, min(1.0, score))
    
    def _normalize_points(self, points: list[Point]) -> list[Point]:
        """Normalize gesture points to 0-1 range."""
        if not points:
            return []
        
        min_x = min(p.x for p in points)
        max_x = max(p.x for p in points)
        min_y = min(p.y for p in points)
        max_y = max(p.y for p in points)
        
        width = max_x - min_x or 1.0
        height = max_y - min_y or 1.0
        
        return [
            Point((p.x - min_x) / width, (p.y - min_y) / height, p.timestamp)
            for p in points
        ]
    
    def _resample_points(self, points: list[Point], num_points: int) -> list[Point]:
        """Resample gesture to a specific number of points."""
        if len(points) < 2 or num_points < 2:
            return points[:num_points] if points else []
        
        # Calculate total path length
        total_length = sum(
            points[i-1].distance_to(points[i])
            for i in range(1, len(points))
        )
        
        if total_length == 0:
            return points[:num_points]
        
        # Resample at equal intervals
        interval = total_length / (num_points - 1)
        resampled = [points[0]]
        accumulated = 0.0
        i = 1
        
        while len(resampled) < num_points and i < len(points):
            d = points[i-1].distance_to(points[i])
            
            if accumulated + d >= interval:
                # Interpolate new point
                t = (interval - accumulated) / d
                new_point = Point(
                    points[i-1].x + t * (points[i].x - points[i-1].x),
                    points[i-1].y + t * (points[i].y - points[i-1].y),
                    points[i-1].timestamp + t * (points[i].timestamp - points[i-1].timestamp)
                )
                resampled.append(new_point)
                points = [new_point] + points[i:]
                accumulated = 0.0
            else:
                accumulated += d
                i += 1
        
        # Pad with last point if needed
        while len(resampled) < num_points:
            resampled.append(points[-1])
        
        return resampled[:num_points]

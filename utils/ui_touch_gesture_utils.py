"""
UI Touch Gesture Utilities - Touch gesture recognition and generation.

This module provides utilities for recognizing and generating touch
gestures for mobile UI automation, including taps, swipes, pinches,
and custom gesture patterns.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import uuid
import time
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional, Sequence


@dataclass
class TouchPoint:
    """Represents a single touch point in a gesture.
    
    Attributes:
        x: X coordinate.
        y: Y coordinate.
        pressure: Touch pressure (0.0 to 1.0).
        size: Touch size.
        timestamp: Time when point was recorded.
    """
    x: float
    y: float
    pressure: float = 1.0
    size: float = 1.0
    timestamp: float = field(default_factory=time.time)


@dataclass
class TouchStroke:
    """Represents a stroke (finger path) in a gesture.
    
    Attributes:
        id: Unique identifier.
        points: Ordered list of touch points.
        finger_id: Which finger (0 = primary).
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    points: list[TouchPoint] = field(default_factory=list)
    finger_id: int = 0
    
    def add_point(
        self,
        x: float,
        y: float,
        pressure: float = 1.0,
        size: float = 1.0
    ) -> None:
        """Add a point to the stroke."""
        self.points.append(TouchPoint(
            x=x,
            y=y,
            pressure=pressure,
            size=size
        ))
    
    def get_start(self) -> Optional[TouchPoint]:
        """Get starting point."""
        return self.points[0] if self.points else None
    
    def get_end(self) -> Optional[TouchPoint]:
        """Get ending point."""
        return self.points[-1] if self.points else None
    
    def get_duration(self) -> float:
        """Get stroke duration in seconds."""
        if len(self.points) < 2:
            return 0.0
        return self.points[-1].timestamp - self.points[0].timestamp
    
    def get_distance(self) -> float:
        """Get total distance traveled."""
        if len(self.points) < 2:
            return 0.0
        
        total = 0.0
        for i in range(1, len(self.points)):
            dx = self.points[i].x - self.points[i-1].x
            dy = self.points[i].y - self.points[i-1].y
            total += (dx ** 2 + dy ** 2) ** 0.5
        
        return total
    
    def get_velocity(self) -> float:
        """Get average velocity."""
        duration = self.get_duration()
        if duration == 0:
            return 0.0
        return self.get_distance() / duration


@dataclass
class Gesture:
    """Represents a complete touch gesture.
    
    Attributes:
        id: Unique identifier.
        gesture_type: Type of gesture.
        strokes: List of strokes in the gesture.
        start_time: When gesture started.
        metadata: Additional gesture data.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    gesture_type: str = "unknown"
    strokes: list[TouchStroke] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    metadata: dict = field(default_factory=dict)
    
    def add_stroke(self, finger_id: int = 0) -> TouchStroke:
        """Add a new stroke to the gesture."""
        stroke = TouchStroke(finger_id=finger_id)
        self.strokes.append(stroke)
        return stroke
    
    def get_duration(self) -> float:
        """Get gesture duration."""
        if not self.strokes:
            return 0.0
        min_start = min(s.points[0].timestamp for s in self.strokes if s.points)
        max_end = max(s.points[-1].timestamp for s in self.strokes if s.points)
        return max_end - min_start
    
    def get_primary_stroke(self) -> Optional[TouchStroke]:
        """Get the primary (first) stroke."""
        return self.strokes[0] if self.strokes else None


class GestureRecognizer:
    """Recognizes gesture types from touch data.
    
    Provides methods for classifying gestures based on
    stroke patterns and characteristics.
    
    Example:
        >>> recognizer = GestureRecognizer()
        >>> gesture_type = recognizer.recognize(strokes)
    """
    
    TAP_MAX_DURATION = 0.5
    TAP_MAX_DISTANCE = 50.0
    SWIPE_MIN_DISTANCE = 100.0
    LONG_PRESS_MIN_DURATION = 0.5
    
    def recognize(self, gesture: Gesture) -> str:
        """Recognize gesture type.
        
        Args:
            gesture: Gesture to recognize.
            
        Returns:
            Gesture type string.
        """
        if len(gesture.strokes) == 1:
            return self._recognize_single_finger(gesture.strokes[0])
        elif len(gesture.strokes) == 2:
            return self._recognize_two_finger(gesture.strokes)
        return "unknown"
    
    def _recognize_single_finger(self, stroke: TouchStroke) -> str:
        """Recognize single finger gesture."""
        if len(stroke.points) < 2:
            return "tap"
        
        duration = stroke.get_duration()
        distance = stroke.get_distance()
        
        if duration < self.TAP_MAX_DURATION and distance < self.TAP_MAX_DISTANCE:
            if duration > self.LONG_PRESS_MIN_DURATION:
                return "long_press"
            return "tap"
        
        if distance >= self.SWIPE_MIN_DISTANCE:
            direction = self._get_swipe_direction(stroke)
            return f"swipe_{direction}"
        
        return "drag"
    
    def _recognize_two_finger(self, strokes: list[TouchStroke]) -> str:
        """Recognize two finger gesture."""
        if len(strokes) < 2:
            return "unknown"
        
        stroke1 = strokes[0]
        stroke2 = strokes[1]
        
        if len(stroke1.points) >= 2 and len(stroke2.points) >= 2:
            start1 = stroke1.points[0]
            end1 = stroke1.points[-1]
            start2 = stroke2.points[0]
            end2 = stroke2.points[-1]
            
            dist_start = ((start1.x - start2.x) ** 2 + (start1.y - start2.y) ** 2) ** 0.5
            dist_end = ((end1.x - end2.x) ** 2 + (end1.y - end2.y) ** 2) ** 0.5
            
            if dist_end > dist_start * 1.5:
                return "pinch_open"
            elif dist_end < dist_start * 0.5:
                return "pinch_close"
        
        return "two_finger_swipe"
    
    def _get_swipe_direction(self, stroke: TouchStroke) -> str:
        """Get swipe direction."""
        if len(stroke.points) < 2:
            return "unknown"
        
        start = stroke.points[0]
        end = stroke.points[-1]
        
        dx = end.x - start.x
        dy = end.y - start.y
        
        if abs(dx) > abs(dy):
            return "right" if dx > 0 else "left"
        else:
            return "down" if dy > 0 else "up"


class GestureGenerator:
    """Generates touch gestures for playback.
    
    Provides methods for creating standard gestures
    like taps, swipes, and pinches.
    
    Example:
        >>> generator = GestureGenerator()
        >>> gesture = generator.create_swipe(100, 100, 300, 300)
    """
    
    DEFAULT_VELOCITY = 500.0
    POINTS_PER_SECOND = 60
    
    def create_tap(
        self,
        x: float,
        y: float,
        duration: float = 0.1
    ) -> Gesture:
        """Create a tap gesture.
        
        Args:
            x: Tap X coordinate.
            y: Tap Y coordinate.
            duration: Tap duration.
            
        Returns:
            Tap Gesture.
        """
        gesture = Gesture(gesture_type="tap")
        stroke = gesture.add_stroke()
        
        stroke.add_point(x, y)
        stroke.add_point(x, y)
        
        return gesture
    
    def create_swipe(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        duration: float = 0.5
    ) -> Gesture:
        """Create a swipe gesture.
        
        Args:
            start_x: Start X coordinate.
            start_y: Start Y coordinate.
            end_x: End X coordinate.
            end_y: End Y coordinate.
            duration: Swipe duration.
            
        Returns:
            Swipe Gesture.
        """
        gesture = Gesture(gesture_type="swipe")
        stroke = gesture.add_stroke()
        
        num_points = int(duration * self.POINTS_PER_SECOND)
        for i in range(num_points + 1):
            t = i / num_points
            x = start_x + (end_x - start_x) * t
            y = start_y + (end_y - start_y) * t
            stroke.add_point(x, y)
        
        return gesture
    
    def create_drag(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        duration: float = 1.0
    ) -> Gesture:
        """Create a drag gesture.
        
        Args:
            start_x: Start X coordinate.
            start_y: Start Y coordinate.
            end_x: End X coordinate.
            end_y: End Y coordinate.
            duration: Drag duration.
            
        Returns:
            Drag Gesture.
        """
        gesture = Gesture(gesture_type="drag")
        stroke = gesture.add_stroke()
        
        num_points = int(duration * self.POINTS_PER_SECOND)
        for i in range(num_points + 1):
            t = i / num_points
            ease_t = self._ease_out_quad(t)
            x = start_x + (end_x - start_x) * ease_t
            y = start_y + (end_y - start_y) * ease_t
            stroke.add_point(x, y)
        
        return gesture
    
    def create_pinch(
        self,
        center_x: float,
        center_y: float,
        start_distance: float,
        end_distance: float,
        duration: float = 0.5
    ) -> Gesture:
        """Create a pinch gesture.
        
        Args:
            center_x: Center X coordinate.
            center_y: Center Y coordinate.
            start_distance: Starting distance between fingers.
            end_distance: Ending distance between fingers.
            duration: Gesture duration.
            
        Returns:
            Pinch Gesture.
        """
        gesture = Gesture(gesture_type="pinch")
        
        num_points = int(duration * self.POINTS_PER_SECOND)
        
        for finger_id in range(2):
            stroke = gesture.add_stroke(finger_id)
            
            angle = (finger_id * 180) * (3.14159 / 180)
            
            for i in range(num_points + 1):
                t = i / num_points
                distance = start_distance + (end_distance - start_distance) * t
                
                offset_x = (distance / 2) * (1 if finger_id == 0 else -1)
                offset_y = 0
                
                x = center_x + offset_x
                y = center_y + offset_y
                stroke.add_point(x, y)
        
        return gesture
    
    def create_long_press(
        self,
        x: float,
        y: float,
        duration: float = 1.0
    ) -> Gesture:
        """Create a long press gesture.
        
        Args:
            x: Press X coordinate.
            y: Press Y coordinate.
            duration: Press duration.
            
        Returns:
            Long press Gesture.
        """
        gesture = Gesture(gesture_type="long_press")
        stroke = gesture.add_stroke()
        
        num_points = 10
        for i in range(num_points):
            t = i / num_points
            current_duration = duration * t
            x_pos = x + (i % 3 - 1) * 2
            y_pos = y + (i % 3 - 1) * 2
            stroke.add_point(x_pos, y_pos)
        
        stroke.add_point(x, y)
        
        return gesture
    
    @staticmethod
    def _ease_out_quad(t: float) -> float:
        """Ease out quadratic function."""
        return t * (2 - t)


class GesturePlayer:
    """Plays back recorded gestures.
    
    Provides methods for executing gestures with configurable
    speed and timing.
    
    Example:
        >>> player = GesturePlayer()
        >>> player.play(gesture, speed=1.5)
    """
    
    def __init__(self) -> None:
        """Initialize gesture player."""
        self._interrupted: bool = False
    
    def play(
        self,
        gesture: Gesture,
        speed: float = 1.0,
        on_point: Optional[Callable[[TouchPoint, TouchStroke], None]] = None
    ) -> bool:
        """Play a gesture.
        
        Args:
            gesture: Gesture to play.
            speed: Playback speed multiplier.
            on_point: Optional callback for each point.
            
        Returns:
            True if gesture completed fully.
        """
        self._interrupted = False
        
        for stroke in gesture.strokes:
            if self._interrupted:
                return False
            
            if not self._play_stroke(stroke, speed, on_point):
                return False
        
        return True
    
    def interrupt(self) -> None:
        """Interrupt the currently playing gesture."""
        self._interrupted = True
    
    def _play_stroke(
        self,
        stroke: TouchStroke,
        speed: float,
        on_point: Optional[Callable[[TouchPoint, TouchStroke], None]]
    ) -> bool:
        """Play a single stroke."""
        if len(stroke.points) < 2:
            return True
        
        for i in range(len(stroke.points) - 1):
            if self._interrupted:
                return False
            
            p1 = stroke.points[i]
            p2 = stroke.points[i + 1]
            
            dx = p2.x - p1.x
            dy = p2.y - p1.y
            distance = (dx ** 2 + dy ** 2) ** 0.5
            
            if distance > 0:
                velocity = stroke.get_velocity() if stroke.get_velocity() > 0 else self.DEFAULT_VELOCITY
                duration = distance / velocity
                duration = duration / speed
            else:
                duration = (p2.timestamp - p1.timestamp) / speed
            
            duration = max(0.001, duration)
            
            if on_point:
                on_point(p2, stroke)
        
        return True

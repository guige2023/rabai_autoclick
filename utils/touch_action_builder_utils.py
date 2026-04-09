"""
Touch action builder and trajectory utilities.

Provides a builder pattern for constructing touch gestures
with multiple points and trajectories.

Author: Auto-generated
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable


class TouchPhase(Enum):
    """Phase of a touch gesture."""
    BEGAN = auto()
    MOVED = auto()
    STATIONARY = auto()
    ENDED = auto()
    CANCELLED = auto()


@dataclass
class TouchPoint:
    """A touch point with position and metadata."""
    x: float
    y: float
    timestamp: float
    pressure: float = 1.0
    radius_x: float = 1.0
    radius_y: float = 1.0
    azimuth: float = 0.0
    altitude: float = 0.0
    finger_id: int = 0


@dataclass
class TouchAction:
    """A touch action in a gesture sequence."""
    phase: TouchPhase
    point: TouchPoint
    duration_ms: float = 0


@dataclass
class TouchGesture:
    """A complete touch gesture."""
    actions: list[TouchAction] = field(default_factory=list)
    gesture_type: str = "custom"
    metadata: dict = field(default_factory=dict)
    
    def add_action(self, action: TouchAction) -> None:
        """Add an action to the gesture."""
        self.actions.append(action)
    
    def duration_ms(self) -> float:
        """Total duration of the gesture."""
        if not self.actions:
            return 0
        return (self.actions[-1].point.timestamp - self.actions[0].point.timestamp) * 1000


class TouchActionBuilder:
    """
    Builder for constructing touch gestures.
    
    Example:
        builder = TouchActionBuilder()
        gesture = (builder
            .start(100, 100)
            .move_to(200, 200, duration=0.3)
            .move_to(300, 100, duration=0.2)
            .release()
            .build())
    """
    
    def __init__(self, gesture_type: str = "custom"):
        self._gesture_type = gesture_type
        self._actions: list[TouchAction] = []
        self._start_time: float | None = None
        self._current_x: float = 0
        self._current_y: float = 0
        self._finger_id: int = 0
    
    def start(
        self,
        x: float,
        y: float,
        finger_id: int = 0,
        pressure: float = 1.0,
    ) -> TouchActionBuilder:
        """
        Start a touch gesture at position.
        
        Args:
            x, y: Starting position
            finger_id: ID for this finger (for multi-touch)
            pressure: Touch pressure
            
        Returns:
            Self for chaining
        """
        self._start_time = time.time()
        self._current_x = x
        self._current_y = y
        self._finger_id = finger_id
        
        point = TouchPoint(
            x=x, y=y,
            timestamp=0,
            pressure=pressure,
            finger_id=finger_id,
        )
        
        self._actions.append(TouchAction(phase=TouchPhase.BEGAN, point=point))
        return self
    
    def move_to(
        self,
        x: float,
        y: float,
        duration: float = 0.1,
        num_points: int = 10,
    ) -> TouchActionBuilder:
        """
        Add a move action to position.
        
        Args:
            x, y: Target position
            duration: Duration in seconds
            num_points: Number of intermediate points
            
        Returns:
            Self for chaining
        """
        if self._start_time is None:
            raise ValueError("Must call start() before move_to()")
        
        for i in range(num_points):
            t = i / num_points
            px = self._current_x + (x - self._current_x) * t
            py = self._current_y + (y - self._current_y) * t
            elapsed = duration * t
            timestamp = self._start_time + elapsed
            
            point = TouchPoint(
                x=px, y=py,
                timestamp=timestamp,
                finger_id=self._finger_id,
            )
            
            self._actions.append(TouchAction(
                phase=TouchPhase.MOVED,
                point=point,
                duration_ms=duration * 1000 / num_points,
            ))
        
        self._current_x = x
        self._current_y = y
        return self
    
    def move_by(
        self,
        dx: float,
        dy: float,
        duration: float = 0.1,
        num_points: int = 10,
    ) -> TouchActionBuilder:
        """Move relative to current position."""
        return self.move_to(
            self._current_x + dx,
            self._current_y + dy,
            duration=duration,
            num_points=num_points,
        )
    
    def release(self) -> TouchActionBuilder:
        """Release the touch."""
        if self._start_time is None:
            raise ValueError("Must call start() before release()")
        
        point = TouchPoint(
            x=self._current_x,
            y=self._current_y,
            timestamp=time.time(),
            finger_id=self._finger_id,
        )
        
        self._actions.append(TouchAction(phase=TouchPhase.ENDED, point=point))
        return self
    
    def tap(
        self,
        x: float,
        y: float,
        duration: float = 0.1,
    ) -> TouchActionBuilder:
        """Add a tap gesture at position."""
        self.start(x, y)
        self.move_to(x, y, duration=0.001, num_points=1)
        self.release()
        return self
    
    def long_press(
        self,
        x: float,
        y: float,
        duration: float = 0.5,
    ) -> TouchActionBuilder:
        """Add a long press gesture."""
        self.start(x, y)
        time.sleep(duration)
        self.release()
        return self
    
    def swipe(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        duration: float = 0.3,
    ) -> TouchActionBuilder:
        """Add a swipe gesture from start to end."""
        self.start(start_x, start_y)
        self.move_to(end_x, end_y, duration=duration, num_points=20)
        self.release()
        return self
    
    def build(self) -> TouchGesture:
        """Build and return the gesture."""
        gesture = TouchGesture(
            gesture_type=self._gesture_type,
            actions=list(self._actions),
        )
        return gesture
    
    def reset(self) -> TouchActionBuilder:
        """Reset the builder."""
        self._actions.clear()
        self._start_time = None
        self._current_x = 0
        self._current_y = 0
        return self


class MultiTouchGestureBuilder:
    """
    Builder for multi-touch gestures.
    
    Example:
        builder = MultiTouchGestureBuilder()
        builder.add_finger(1).start(100, 100).move_to(200, 200).release()
        builder.add_finger(2).start(300, 100).move_to(200, 200).release()
        gesture = builder.build()
    """
    
    def __init__(self):
        self._builders: dict[int, TouchActionBuilder] = {}
        self._current_finger: int = 0
    
    def add_finger(self, finger_id: int) -> MultiTouchGestureBuilder:
        """Add a finger to the gesture."""
        self._builders[finger_id] = TouchActionBuilder()
        self._current_finger = finger_id
        return self
    
    def start(
        self,
        x: float,
        y: float,
        pressure: float = 1.0,
    ) -> MultiTouchGestureBuilder:
        """Start touch for current finger."""
        if self._current_finger not in self._builders:
            self.add_finger(self._current_finger)
        self._builders[self._current_finger].start(x, y, pressure=pressure)
        return self
    
    def move_to(
        self,
        x: float,
        y: float,
        duration: float = 0.1,
    ) -> MultiTouchGestureBuilder:
        """Move current finger."""
        self._builders[self._current_finger].move_to(x, y, duration=duration)
        return self
    
    def release(self) -> MultiTouchGestureBuilder:
        """Release current finger."""
        self._builders[self._current_finger].release()
        return self
    
    def build(self) -> list[TouchGesture]:
        """Build all finger gestures."""
        return [b.build() for b in self._builders.values()]


def create_drag_trajectory(
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
    num_points: int = 20,
    curve_factor: float = 0.0,
) -> list[tuple[float, float]]:
    """
    Create a curved drag trajectory.
    
    Args:
        start_x, start_y: Start position
        end_x, end_y: End position
        num_points: Number of points in trajectory
        curve_factor: Curvature (-1.0 to 1.0)
        
    Returns:
        List of (x, y) points
    """
    points = []
    
    for i in range(num_points + 1):
        t = i / num_points
        
        # Linear interpolation
        x = start_x + (end_x - start_x) * t
        y = start_y + (end_y - start_y) * t
        
        # Add curve
        if curve_factor != 0:
            mid_x = (start_x + end_x) / 2
            mid_y = (start_y + end_y) / 2
            
            # Perpendicular offset
            dx = end_x - start_x
            dy = end_y - start_y
            length = math.sqrt(dx * dx + dy * dy)
            
            if length > 0:
                nx = -dy / length
                ny = dx / length
                
                # Parabolic curve
                curve = 4 * t * (1 - t) * curve_factor
                x += nx * curve * length * 0.3
                y += ny * curve * length * 0.3
        
        points.append((x, y))
    
    return points

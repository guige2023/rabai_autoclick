"""
Touch Simulation Utilities for UI Automation

Provides realistic touch simulation including multi-touch,
touch sequences, and touch pressure simulation.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


class TouchAction(Enum):
    """Types of touch actions."""
    DOWN = auto()
    MOVE = auto()
    UP = auto()
    CANCEL = auto()


@dataclass
class TouchPoint:
    """Represents a single touch point."""
    x: float
    y: float
    pressure: float = 1.0
    radius: float = 5.0
    timestamp: float = field(default_factory=time.time)
    action: TouchAction = TouchAction.MOVE


@dataclass
class TouchEvent:
    """A complete touch event with multiple touch points."""
    event_id: int
    points: list[TouchPoint]
    action: TouchAction
    timestamp: float = field(default_factory=time.time)


@dataclass
class TouchSequence:
    """A sequence of touch events forming a gesture."""
    events: list[TouchEvent]
    duration: float = 0.0


class TouchSimulator:
    """
    Simulates realistic multi-touch input for automation testing.

    Supports single-finger gestures, multi-touch patterns,
    and configurable touch parameters (pressure, size, timing).
    """

    DEFAULT_PRESSURE = 0.8
    DEFAULT_RADIUS = 8.0
    DEFAULT_DELAY = 0.016  # ~60fps

    def __init__(self) -> None:
        self._next_event_id = 0
        self._current_touches: dict[int, TouchPoint] = {}
        self._event_callback: Optional[callable] = None
        self._sequence: list[TouchEvent] = []

    def set_event_callback(self, callback: callable) -> None:
        """
        Set callback for touch events.

        Callback receives TouchEvent objects.
        """
        self._event_callback = callback

    def begin_touch(self, touch_id: int, x: float, y: float) -> TouchEvent:
        """
        Start a new touch at the given coordinates.

        Args:
            touch_id: Unique identifier for this touch point
            x: X coordinate
            y: Y coordinate

        Returns:
            TouchEvent with DOWN action
        """
        point = TouchPoint(
            x=x, y=y,
            pressure=self.DEFAULT_PRESSURE,
            radius=self.DEFAULT_RADIUS,
            action=TouchAction.DOWN,
        )
        self._current_touches[touch_id] = point
        event = TouchEvent(
            event_id=self._next_event_id,
            points=[point],
            action=TouchAction.DOWN,
        )
        self._next_event_id += 1
        self._sequence.append(event)
        self._dispatch_event(event)
        return event

    def move_touch(
        self,
        touch_id: int,
        x: float,
        y: float,
        pressure: Optional[float] = None,
    ) -> TouchEvent:
        """
        Move an existing touch point.

        Args:
            touch_id: Touch identifier from begin_touch
            x: New X coordinate
            y: New Y coordinate
            pressure: Optional pressure value (0.0-1.0)

        Returns:
            TouchEvent with MOVE action, or None if touch not found
        """
        if touch_id not in self._current_touches:
            return None

        point = TouchPoint(
            x=x, y=y,
            pressure=pressure or self.DEFAULT_PRESSURE,
            radius=self.DEFAULT_RADIUS,
            action=TouchAction.MOVE,
        )
        self._current_touches[touch_id] = point
        event = TouchEvent(
            event_id=self._next_event_id,
            points=[point],
            action=TouchAction.MOVE,
        )
        self._next_event_id += 1
        self._sequence.append(event)
        self._dispatch_event(event)
        return event

    def end_touch(self, touch_id: int) -> Optional[TouchEvent]:
        """
        End a touch.

        Args:
            touch_id: Touch identifier from begin_touch

        Returns:
            TouchEvent with UP action, or None if touch not found
        """
        if touch_id not in self._current_touches:
            return None

        last_point = self._current_touches[touch_id]
        point = TouchPoint(
            x=last_point.x, y=last_point.y,
            pressure=0.0,
            radius=self.DEFAULT_RADIUS,
            action=TouchAction.UP,
        )
        event = TouchEvent(
            event_id=self._next_event_id,
            points=[point],
            action=TouchAction.UP,
        )
        self._next_event_id += 1
        del self._current_touches[touch_id]
        self._sequence.append(event)
        self._dispatch_event(event)
        return event

    def _dispatch_event(self, event: TouchEvent) -> None:
        """Dispatch event to callback if registered."""
        if self._event_callback:
            self._event_callback(event)

    def get_sequence(self) -> TouchSequence:
        """Get the recorded touch sequence."""
        if not self._sequence:
            return TouchSequence(events=[])
        return TouchSequence(
            events=self._sequence,
            duration=self._sequence[-1].timestamp - self._sequence[0].timestamp,
        )

    def clear_sequence(self) -> None:
        """Clear the recorded sequence."""
        self._sequence = []
        self._current_touches = {}

    def simulate_tap(
        self,
        x: float,
        y: float,
        duration: float = 0.1,
    ) -> TouchSequence:
        """
        Simulate a tap gesture at the given coordinates.

        Args:
            x: X coordinate
            y: Y coordinate
            duration: Tap duration in seconds

        Returns:
            TouchSequence representing the tap
        """
        self.clear_sequence()
        touch_id = 1

        self.begin_touch(touch_id, x, y)
        time.sleep(duration)
        self.end_touch(touch_id)

        return self.get_sequence()

    def simulate_swipe(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        duration: float = 0.5,
        num_points: int = 20,
    ) -> TouchSequence:
        """
        Simulate a swipe gesture.

        Args:
            start_x: Starting X coordinate
            start_y: Starting Y coordinate
            end_x: Ending X coordinate
            end_y: Ending Y coordinate
            duration: Total swipe duration in seconds
            num_points: Number of intermediate points

        Returns:
            TouchSequence representing the swipe
        """
        self.clear_sequence()
        touch_id = 1

        self.begin_touch(touch_id, start_x, start_y)
        time.sleep(0.02)

        interval = duration / num_points
        for i in range(1, num_points + 1):
            progress = i / num_points
            x = start_x + (end_x - start_x) * progress
            y = start_y + (end_y - start_y) * progress
            self.move_touch(touch_id, x, y)
            if i < num_points:
                time.sleep(interval)

        self.end_touch(touch_id)
        return self.get_sequence()

    def simulate_pinch(
        self,
        center_x: float,
        center_y: float,
        start_distance: float,
        end_distance: float,
        duration: float = 0.5,
    ) -> TouchSequence:
        """
        Simulate a pinch gesture (zoom).

        Args:
            center_x: Center point X coordinate
            center_y: Center point Y coordinate
            start_distance: Initial distance between fingers
            end_distance: Final distance between fingers
            duration: Total gesture duration in seconds

        Returns:
            TouchSequence representing the pinch
        """
        self.clear_sequence()

        # Calculate initial finger positions
        angle = 0.0  # degrees
        import math
        rad = math.radians(angle)

        touch_id_1 = 1
        touch_id_2 = 2

        x1 = center_x + start_distance / 2 * math.cos(rad)
        y1 = center_y + start_distance / 2 * math.sin(rad)
        x2 = center_x - start_distance / 2 * math.cos(rad)
        y2 = center_y - start_distance / 2 * math.sin(rad)

        self.begin_touch(touch_id_1, x1, y1)
        self.begin_touch(touch_id_2, x2, y2)
        time.sleep(0.02)

        num_steps = 20
        interval = duration / num_steps
        for i in range(1, num_steps + 1):
            progress = i / num_steps
            distance = start_distance + (end_distance - start_distance) * progress

            x1 = center_x + distance / 2 * math.cos(rad)
            y1 = center_y + distance / 2 * math.sin(rad)
            x2 = center_x - distance / 2 * math.cos(rad)
            y2 = center_y - distance / 2 * math.sin(rad)

            self.move_touch(touch_id_1, x1, y1)
            self.move_touch(touch_id_2, x2, y2)
            if i < num_steps:
                time.sleep(interval)

        self.end_touch(touch_id_1)
        self.end_touch(touch_id_2)

        return self.get_sequence()

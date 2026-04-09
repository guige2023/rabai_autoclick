"""
Touch Simulator Action Module.

Simulates touch events including tap, long press, swipe,
pinch, and multi-touch gestures for mobile automation.
"""

import math
import time
from typing import Callable, Optional, Tuple


class TouchEvent:
    """Represents a touch event."""

    def __init__(
        self,
        event_type: str,
        x: float,
        y: float,
        finger_id: int = 0,
        pressure: float = 1.0,
        timestamp: Optional[float] = None,
    ):
        """
        Initialize touch event.

        Args:
            event_type: 'down', 'move', 'up', 'cancel'.
            x: X coordinate.
            y: Y coordinate.
            finger_id: Finger identifier for multi-touch.
            pressure: Touch pressure (0-1).
            timestamp: Optional timestamp.
        """
        self.event_type = event_type
        self.x = x
        self.y = y
        self.finger_id = finger_id
        self.pressure = pressure
        self.timestamp = timestamp if timestamp is not None else time.time()


class TouchSimulator:
    """Simulates touch events for mobile automation."""

    def __init__(self, sender: Optional[Callable[[TouchEvent], None]] = None):
        """
        Initialize touch simulator.

        Args:
            sender: Optional function to send touch events to system.
        """
        self._sender = sender

    def tap(
        self,
        x: float,
        y: float,
        duration: float = 0.1,
        finger_id: int = 0,
    ) -> list[TouchEvent]:
        """
        Simulate a tap gesture.

        Args:
            x: Tap X coordinate.
            y: Tap Y coordinate.
            duration: Tap duration.
            finger_id: Finger identifier.

        Returns:
            List of touch events generated.
        """
        events = []

        events.append(TouchEvent("down", x, y, finger_id))
        time.sleep(duration)
        events.append(TouchEvent("up", x, y, finger_id))

        for event in events:
            self._send(event)

        return events

    def long_press(
        self,
        x: float,
        y: float,
        duration: float = 1.0,
        finger_id: int = 0,
    ) -> list[TouchEvent]:
        """
        Simulate a long press gesture.

        Args:
            x: Press X coordinate.
            y: Press Y coordinate.
            duration: Press duration in seconds.
            finger_id: Finger identifier.

        Returns:
            List of touch events generated.
        """
        events = []

        events.append(TouchEvent("down", x, y, finger_id))
        self._send(events[-1])
        time.sleep(duration)
        events.append(TouchEvent("up", x, y, finger_id))
        self._send(events[-1])

        return events

    def swipe(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        duration: float = 0.5,
        finger_id: int = 0,
    ) -> list[TouchEvent]:
        """
        Simulate a swipe gesture.

        Args:
            start_x: Start X coordinate.
            start_y: Start Y coordinate.
            end_x: End X coordinate.
            end_y: End Y coordinate.
            duration: Swipe duration in seconds.
            finger_id: Finger identifier.

        Returns:
            List of touch events generated.
        """
        events = []
        num_points = max(int(duration * 60), 10)

        events.append(TouchEvent("down", start_x, start_y, finger_id))
        self._send(events[-1])

        for i in range(1, num_points):
            t = i / num_points
            x = start_x + (end_x - start_x) * t
            y = start_y + (end_y - start_y) * t
            events.append(TouchEvent("move", x, y, finger_id))
            self._send(events[-1])

        events.append(TouchEvent("up", end_x, end_y, finger_id))
        self._send(events[-1])

        return events

    def multi_tap(
        self,
        points: list[Tuple[float, float]],
        duration: float = 0.1,
    ) -> list[TouchEvent]:
        """
        Simulate simultaneous taps at multiple points.

        Args:
            points: List of (x, y) coordinates.
            duration: Tap duration.

        Returns:
            List of touch events generated.
        """
        events = []

        for i, (x, y) in enumerate(points):
            events.append(TouchEvent("down", x, y, i))
            self._send(events[-1])

        time.sleep(duration)

        for i, (x, y) in enumerate(points):
            events.append(TouchEvent("up", x, y, i))
            self._send(events[-1])

        return events

    def pinch(
        self,
        center_x: float,
        center_y: float,
        start_distance: float,
        end_distance: float,
        duration: float = 0.5,
    ) -> list[TouchEvent]:
        """
        Simulate a pinch gesture.

        Args:
            center_x: Center X coordinate.
            center_y: Center Y coordinate.
            start_distance: Starting distance between fingers.
            end_distance: Ending distance between fingers.
            duration: Gesture duration.

        Returns:
            List of touch events generated.
        """
        events = []
        num_points = max(int(duration * 60), 10)

        angle = 0
        for i, dist in enumerate(self._interpolate(start_distance, end_distance, num_points)):
            t = i / num_points
            angle += 0.1

            x1 = center_x + dist * math.cos(angle)
            y1 = center_y + dist * math.sin(angle)
            x2 = center_x - dist * math.cos(angle)
            y2 = center_y - dist * math.sin(angle)

            if i == 0:
                events.append(TouchEvent("down", x1, y1, 0))
                events.append(TouchEvent("down", x2, y2, 1))
            else:
                events.append(TouchEvent("move", x1, y1, 0))
                events.append(TouchEvent("move", x2, y2, 1))

            for e in events[-2:]:
                self._send(e)

        events.append(TouchEvent("up", x1, y1, 0))
        events.append(TouchEvent("up", x2, y2, 1))
        for e in events[-2:]:
            self._send(e)

        return events

    def _send(self, event: TouchEvent) -> None:
        """Send a touch event to the system."""
        if self._sender:
            self._sender(event)

    @staticmethod
    def _interpolate(start: float, end: float, steps: int) -> list[float]:
        """Generate interpolated values."""
        return [start + (end - start) * (i / (steps - 1)) for i in range(steps)]

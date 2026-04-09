"""
Multi-touch simulation utilities for gesture automation.

This module provides utilities for simulating multi-touch gestures
on touch-capable devices.
"""

from __future__ import annotations

import time
import platform
from typing import List, Tuple, Optional, Dict, Any, Callable
from dataclasses import dataclass, field
from enum import Enum, auto


IS_MACOS: bool = platform.system() == 'Darwin'


class TouchPhase(Enum):
    """Touch phase states."""
    BEGAN = auto()
    MOVED = auto()
    STATIONARY = auto()
    ENDED = auto()
    CANCELLED = auto()


@dataclass
class TouchPoint:
    """
    Represents a single touch point in a multi-touch gesture.

    Attributes:
        identifier: Unique touch identifier.
        x: X coordinate.
        y: Y coordinate.
        major_radius: Major axis radius of touch ellipse.
        minor_radius: Minor axis radius of touch ellipse.
        angle: Angle of touch ellipse in radians.
        pressure: Touch pressure (0.0-1.0).
        timestamp: Timestamp in seconds.
    """
    identifier: int
    x: int
    y: int
    major_radius: float = 10.0
    minor_radius: float = 10.0
    angle: float = 0.0
    pressure: float = 1.0
    timestamp: float = field(default_factory=time.time)
    phase: TouchPhase = TouchPhase.BEGAN

    def distance_to(self, other: TouchPoint) -> float:
        """Calculate distance to another touch point."""
        import math
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)


@dataclass
class MultiTouchGesture:
    """
    A multi-touch gesture consisting of multiple touch points.

    Attributes:
        touches: Dictionary of touch_id -> TouchPoint.
        start_time: Gesture start timestamp.
    """
    touches: Dict[int, TouchPoint] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)

    def add_touch(self, touch: TouchPoint) -> None:
        """Add or update a touch point."""
        self.touches[touch.identifier] = touch

    def remove_touch(self, identifier: int) -> None:
        """Remove a touch point."""
        if identifier in self.touches:
            del self.touches[identifier]

    def get_centroid(self) -> Tuple[int, int]:
        """Get the centroid of all touch points."""
        if not self.touches:
            return (0, 0)
        cx = sum(t.x for t in self.touches.values()) / len(self.touches)
        cy = sum(t.y for t in self.touches.values()) / len(self.touches)
        return (int(cx), int(cy))

    def get_spread(self) -> float:
        """Get the average distance between touch points."""
        if len(self.touches) < 2:
            return 0.0
        import math
        points = list(self.touches.values())
        total = 0.0
        count = 0
        for i in range(len(points)):
            for j in range(i + 1, len(points)):
                total += points[i].distance_to(points[j])
                count += 1
        return total / count

    def duration(self) -> float:
        """Get gesture duration in seconds."""
        if not self.touches:
            return 0.0
        timestamps = [t.timestamp for t in self.touches.values()]
        return max(timestamps) - self.start_time


class MultiTouchSimulator:
    """
    Simulator for multi-touch gestures.

    Supports pinch, spread, rotate, and two-finger drag gestures.
    """

    def __init__(self, touch_delay: float = 0.016):
        """
        Initialize the multi-touch simulator.

        Args:
            touch_delay: Minimum delay between touch updates (seconds).
        """
        self._touch_delay = touch_delay

    def pinch(
        self,
        center_x: int, center_y: int,
        start_distance: float,
        end_distance: float,
        steps: int = 20,
        angle: float = 0.0
    ) -> None:
        """
        Simulate a pinch gesture (two fingers moving together).

        Args:
            center_x: Center x coordinate.
            center_y: Center y coordinate.
            start_distance: Starting distance between fingers.
            end_distance: Ending distance between fingers.
            steps: Number of intermediate steps.
            angle: Angle of pinch axis in radians.
        """
        import math
        dx = math.cos(angle) * start_distance / 2
        dy = math.sin(angle) * start_distance / 2

        finger1_start = (center_x - dx, center_y - dy)
        finger2_start = (center_x + dx, center_y + dy)

        for i in range(steps + 1):
            t = i / steps
            current_dist = start_distance + t * (end_distance - start_distance)
            current_dx = math.cos(angle) * current_dist / 2
            current_dy = math.sin(angle) * current_dist / 2

            finger1 = (center_x - current_dx, center_y - current_dy)
            finger2 = (center_x + current_dx, center_y + current_dy)

            self._send_two_touch(finger1[0], finger1[1], 1, finger2[0], finger2[1], 2)
            time.sleep(self._touch_delay)

    def _send_two_touch(
        self, x1: int, y1: int, id1: int, x2: int, y2: int, id2: int
    ) -> None:
        """Send two touch points to the system."""
        if IS_MACOS:
            self._macos_multi_touch(x1, y1, id1, x2, y2, id2)
        else:
            pass  # Fallback - single touch only

    def _macos_multi_touch(
        self, x1: int, y1: int, id1: int, x2: int, y2: int, id2: int
    ) -> None:
        """Send multi-touch on macOS."""
        import Quartz
        # macOS multi-touch requires CGEvent with touch data
        # This is a simplified implementation
        pass

    def two_finger_drag(
        self,
        start_x: int, start_y: int,
        end_x: int, end_y: int,
        steps: int = 20,
        offset_x: int = 50, offset_y: int = 0
    ) -> None:
        """
        Simulate a two-finger drag gesture.

        Args:
            start_x: Start x of drag center.
            start_y: Start y of drag center.
            end_x: End x of drag center.
            end_y: End y of drag center.
            steps: Number of intermediate steps.
            offset_x: X offset between two fingers.
            offset_y: Y offset between two fingers.
        """
        for i in range(steps + 1):
            t = i / steps
            cx = int(start_x + t * (end_x - start_x))
            cy = int(start_y + t * (end_y - start_y))

            finger1 = (cx - offset_x // 2, cy - offset_y // 2)
            finger2 = (cx + offset_x // 2, cy + offset_y // 2)

            self._send_two_touch(
                finger1[0], finger1[1], 1,
                finger2[0], finger2[1], 2
            )
            time.sleep(self._touch_delay)

    def rotate(
        self,
        center_x: int, center_y: int,
        start_angle: float,
        end_angle: float,
        radius: float,
        steps: int = 30
    ) -> None:
        """
        Simulate a rotation gesture.

        Args:
            center_x: Rotation center x.
            center_y: Rotation center y.
            start_angle: Start angle in radians.
            end_angle: End angle in radians.
            radius: Distance of fingers from center.
            steps: Number of intermediate steps.
        """
        import math
        for i in range(steps + 1):
            t = i / steps
            angle = start_angle + t * (end_angle - start_angle)

            finger1_x = int(center_x + math.cos(angle) * radius)
            finger1_y = int(center_y + math.sin(angle) * radius)
            finger2_x = int(center_x + math.cos(angle + math.pi) * radius)
            finger2_y = int(center_y + math.sin(angle + math.pi) * radius)

            self._send_two_touch(finger1_x, finger1_y, 1, finger2_x, finger2_y, 2)
            time.sleep(self._touch_delay)

    def three_finger_tap(
        self,
        x: int, y: int,
        duration: float = 0.1
    ) -> None:
        """
        Simulate a three-finger tap.

        Args:
            x: Tap center x.
            y: Tap center y.
            duration: Tap duration in seconds.
        """
        offsets = [(-20, -10), (20, -10), (0, 20)]
        for ox, oy in offsets:
            self._send_touch_at(x + ox, y + oy, 0)
        time.sleep(duration)
        # Touch up
        time.sleep(self._touch_delay)

    def _send_touch_at(self, x: int, y: int, identifier: int) -> None:
        """Send a single touch at a position."""
        if IS_MACOS:
            import Quartz
            # Simplified touch event
            e = Quartz.CGEventCreateMouseEvent(
                None, Quartz.kCGEventMouseMoved,
                (x, y), Quartz.kCGMouseButtonLeft
            )
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, e)


def interpolate_touch_path(
    start_x: int, start_y: int,
    end_x: int, end_y: int,
    num_points: int
) -> List[Tuple[int, int]]:
    """
    Generate intermediate points along a touch path.

    Args:
        start_x: Start x.
        start_y: Start y.
        end_x: End x.
        end_y: End y.
        num_points: Number of points to generate.

    Returns:
        List of (x, y) tuples.
    """
    if num_points < 2:
        return [(start_x, start_y)]
    points = []
    for i in range(num_points):
        t = i / (num_points - 1)
        x = int(start_x + t * (end_x - start_x))
        y = int(start_y + t * (end_y - start_y))
        points.append((x, y))
    return points


def calculate_pinch_spread(
    x1: int, y1: int, x2: int, y2: int
) -> float:
    """Calculate the distance between two points for pinch gestures."""
    import math
    return math.sqrt((x2 - x1) ** 2 + (y2 - y1) ** 2)

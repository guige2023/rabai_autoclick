"""
Drag and Drop Simulator Action Module.

Simulates drag and drop operations with configurable
trajectories, speed, and easing for precise automation.
"""

import math
import time
from typing import Callable, Optional, Tuple


class DragDropSimulator:
    """Simulates drag and drop operations."""

    def __init__(
        self,
        duration: float = 0.5,
        easing: str = "ease_out",
        trajectory: str = "straight",
    ):
        """
        Initialize drag drop simulator.

        Args:
            duration: Drag duration in seconds.
            easing: Easing function name.
            trajectory: Trajectory type.
        """
        self.duration = duration
        self.easing = easing
        self.trajectory = trajectory

    def drag(
        self,
        start: Tuple[int, int],
        end: Tuple[int, int],
        sender: Optional[Callable[[str, int, int], None]] = None,
    ) -> list[Tuple[str, int, int]]:
        """
        Perform a drag operation.

        Args:
            start: Start coordinates (x, y).
            end: End coordinates (x, y).
            sender: Optional function to send mouse events.

        Returns:
            List of (event_type, x, y) tuples.
        """
        events = []
        num_points = max(int(self.duration * 60), 10)

        x1, y1 = start
        x2, y2 = end

        for i in range(num_points + 1):
            t = i / num_points
            eased_t = self._apply_easing(t)

            x = x1 + (x2 - x1) * eased_t
            y = y1 + (y2 - y1) * eased_t

            if i == 0:
                event_type = "mouse_down"
            elif i == num_points:
                event_type = "mouse_up"
            else:
                event_type = "mouse_move"

            events.append((event_type, int(x), int(y)))

            if sender:
                sender(event_type, int(x), int(y))

        return events

    def drag_with_offset(
        self,
        start: Tuple[int, int],
        end: Tuple[int, int],
        offset_x: int = 0,
        offset_y: int = -50,
    ) -> list[Tuple[int, int]]:
        """
        Drag with an offset at the end position.

        Args:
            start: Start coordinates.
            end: End coordinates.
            offset_x: Horizontal offset at end.
            offset_y: Vertical offset at end.

        Returns:
            List of (x, y) coordinates.
        """
        drag_end = (end[0] + offset_x, end[1] + offset_y)
        events = self.drag(start, drag_end)
        return [(x, y) for _, x, y in events]

    def drag_with_waypoint(
        self,
        start: Tuple[int, int],
        waypoint: Tuple[int, int],
        end: Tuple[int, int],
    ) -> list[Tuple[int, int]]:
        """
        Drag through an intermediate waypoint.

        Args:
            start: Start coordinates.
            waypoint: Intermediate point.
            end: End coordinates.

        Returns:
            List of (x, y) coordinates.
        """
        events1 = self.drag(start, waypoint)
        events2 = self.drag(waypoint, end)

        result = [(x, y) for _, x, y in events]
        result.extend([(x, y) for _, x, y in events[1:]])

        return result

    def _apply_easing(self, t: float) -> float:
        """Apply easing function to t."""
        if self.easing == "ease_in":
            return t * t
        elif self.easing == "ease_out":
            return 1 - (1 - t) * (1 - t)
        elif self.easing == "ease_in_out":
            return 2 * t * t if t < 0.5 else 1 - pow(-2 * t + 2, 2) / 2
        elif self.easing == "bounce":
            return self._bounce_ease(t)
        else:
            return t

    @staticmethod
    def _bounce_ease(t: float) -> float:
        """Bounce easing function."""
        if t < 0.5:
            return 8 * t * t * t * t
        else:
            t = 1 - t
            return 1 - 8 * t * t * t * t

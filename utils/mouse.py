"""Mouse utilities for RabAI AutoClick.

Provides:
- Mouse events
- Mouse position tracking
- Click patterns
"""

from dataclasses import dataclass
from enum import Enum
from typing import Callable, List, Optional, Tuple


class MouseButton(Enum):
    """Mouse button identifiers."""
    LEFT = 1
    RIGHT = 2
    MIDDLE = 4
    X1 = 8
    X2 = 16


class MouseEventType(Enum):
    """Mouse event types."""
    MOVE = "move"
    CLICK = "click"
    DOUBLE_CLICK = "double_click"
    DOWN = "down"
    UP = "up"
    WHEEL = "wheel"
    DRAG = "drag"


@dataclass
class MouseEvent:
    """Mouse event data."""
    event_type: MouseEventType
    x: int
    y: int
    button: MouseButton = MouseButton.LEFT
    delta: int = 0
    timestamp: float = 0


@dataclass
class Point:
    """2D point."""
    x: int
    y: int

    def distance_to(self, other: "Point") -> float:
        """Calculate distance to another point.

        Args:
            other: Other point.

        Returns:
            Euclidean distance.
        """
        dx = self.x - other.x
        dy = self.y - other.y
        return (dx * dx + dy * dy) ** 0.5

    def __add__(self, other: "Point") -> "Point":
        """Add two points."""
        return Point(self.x + other.x, self.y + other.y)

    def __sub__(self, other: "Point") -> "Point":
        """Subtract points."""
        return Point(self.x - other.x, self.y - other.y)


@dataclass
class Rectangle:
    """Rectangle defined by top-left and bottom-right corners."""
    x: int
    y: int
    width: int
    height: int

    @property
    def left(self) -> int:
        """Get left edge."""
        return self.x

    @property
    def top(self) -> int:
        """Get top edge."""
        return self.y

    @property
    def right(self) -> int:
        """Get right edge."""
        return self.x + self.width

    @property
    def bottom(self) -> int:
        """Get bottom edge."""
        return self.y + self.height

    def contains(self, x: int, y: int) -> bool:
        """Check if point is inside rectangle.

        Args:
            x: X coordinate.
            y: Y coordinate.

        Returns:
            True if point is inside.
        """
        return self.left <= x <= self.right and self.top <= y <= self.bottom

    def center(self) -> Point:
        """Get center point."""
        return Point(self.x + self.width // 2, self.y + self.height // 2)


class ClickPattern:
    """Pattern of clicks for automation."""

    def __init__(self) -> None:
        """Initialize click pattern."""
        self._clicks: List[tuple] = []

    def add_click(
        self,
        x: int,
        y: int,
        button: MouseButton = MouseButton.LEFT,
        delay: float = 0,
    ) -> "ClickPattern":
        """Add a click to pattern.

        Args:
            x: X coordinate.
            y: Y coordinate.
            button: Mouse button.
            delay: Delay after click in seconds.

        Returns:
            Self for chaining.
        """
        self._clicks.append(("click", x, y, button, delay))
        return self

    def add_double_click(
        self,
        x: int,
        y: int,
        button: MouseButton = MouseButton.LEFT,
        delay: float = 0,
    ) -> "ClickPattern":
        """Add a double click to pattern.

        Args:
            x: X coordinate.
            y: Y coordinate.
            button: Mouse button.
            delay: Delay after click in seconds.

        Returns:
            Self for chaining.
        """
        self._clicks.append(("double_click", x, y, button, delay))
        return self

    def add_drag(
        self,
        start_x: int,
        start_y: int,
        end_x: int,
        end_y: int,
        button: MouseButton = MouseButton.LEFT,
    ) -> "ClickPattern":
        """Add a drag operation.

        Args:
            start_x: Start X coordinate.
            start_y: Start Y coordinate.
            end_x: End X coordinate.
            end_y: End Y coordinate.
            button: Mouse button.

        Returns:
            Self for chaining.
        """
        self._clicks.append(("drag", start_x, start_y, end_x, end_y, button))
        return self

    @property
    def clicks(self) -> List[tuple]:
        """Get all click operations."""
        return self._clicks.copy()

    def clear(self) -> None:
        """Clear all clicks."""
        self._clicks.clear()


class MouseRecorder:
    """Record and playback mouse actions."""

    def __init__(self) -> None:
        """Initialize mouse recorder."""
        self._events: List[MouseEvent] = []
        self._recording = False

    def start_recording(self) -> None:
        """Start recording mouse events."""
        self._events.clear()
        self._recording = True

    def stop_recording(self) -> None:
        """Stop recording mouse events."""
        self._recording = False

    def record_event(self, event: MouseEvent) -> None:
        """Record a mouse event.

        Args:
            event: Mouse event to record.
        """
        if self._recording:
            self._events.append(event)

    @property
    def events(self) -> List[MouseEvent]:
        """Get recorded events."""
        return self._events.copy()

    def clear(self) -> None:
        """Clear recorded events."""
        self._events.clear()

    @property
    def is_recording(self) -> bool:
        """Check if recording."""
        return self._recording


class MouseSimulator:
    """Simulate mouse actions."""

    @staticmethod
    def click(x: int, y: int, button: MouseButton = MouseButton.LEFT) -> bool:
        """Simulate a click.

        Args:
            x: X coordinate.
            y: Y coordinate.
            button: Mouse button.

        Returns:
            True if successful.
        """
        try:
            import win32api
            import win32con
            win32api.SetCursorPos((x, y))
            win32api.mouse_event(win32con.MOUSEEVENTF_LEFTDOWN, 0, 0)
            win32api.mouse_event(win32con.MOUSEEVENTF_LEFTUP, 0, 0)
            return True
        except Exception:
            return False

    @staticmethod
    def double_click(x: int, y: int, button: MouseButton = MouseButton.LEFT) -> bool:
        """Simulate a double click.

        Args:
            x: X coordinate.
            y: Y coordinate.
            button: Mouse button.

        Returns:
            True if successful.
        """
        try:
            import win32api
            import win32con
            win32api.SetCursorPos((x, y))
            for _ in range(2):
                win32api.mouse_event(win32con.MOUSEEVENTF_LEFTDOWN, 0, 0)
                win32api.mouse_event(win32con.MOUSEEVENTF_LEFTUP, 0, 0)
            return True
        except Exception:
            return False

    @staticmethod
    def right_click(x: int, y: int) -> bool:
        """Simulate a right click.

        Args:
            x: X coordinate.
            y: Y coordinate.

        Returns:
            True if successful.
        """
        try:
            import win32api
            import win32con
            win32api.SetCursorPos((x, y))
            win32api.mouse_event(win32con.MOUSEEVENTF_RIGHTDOWN, 0, 0)
            win32api.mouse_event(win32con.MOUSEEVENTF_RIGHTUP, 0, 0)
            return True
        except Exception:
            return False

    @staticmethod
    def move(x: int, y: int) -> bool:
        """Move mouse cursor.

        Args:
            x: X coordinate.
            y: Y coordinate.

        Returns:
            True if successful.
        """
        try:
            import win32api
            win32api.SetCursorPos((x, y))
            return True
        except Exception:
            return False

    @staticmethod
    def drag(
        start_x: int,
        start_y: int,
        end_x: int,
        end_y: int,
        button: MouseButton = MouseButton.LEFT,
    ) -> bool:
        """Simulate a drag operation.

        Args:
            start_x: Start X coordinate.
            start_y: Start Y coordinate.
            end_x: End X coordinate.
            end_y: End Y coordinate.
            button: Mouse button.

        Returns:
            True if successful.
        """
        try:
            import win32api
            import win32con
            win32api.SetCursorPos((start_x, start_y))
            win32api.mouse_event(win32con.MOUSEEVENTF_LEFTDOWN, 0, 0)
            win32api.SetCursorPos((end_x, end_y))
            win32api.mouse_event(win32con.MOUSEEVENTF_LEFTUP, 0, 0)
            return True
        except Exception:
            return False


class MousePositionTracker:
    """Track mouse position over time."""

    def __init__(self, interval: float = 0.1) -> None:
        """Initialize tracker.

        Args:
            interval: Sampling interval in seconds.
        """
        self._interval = interval
        self._positions: List[Tuple[int, int, float]] = []
        self._running = False

    def start(self) -> None:
        """Start tracking."""
        self._running = True

    def stop(self) -> None:
        """Stop tracking."""
        self._running = False

    def record_position(self, x: int, y: int, timestamp: float) -> None:
        """Record a position.

        Args:
            x: X coordinate.
            y: Y coordinate.
            timestamp: Timestamp.
        """
        if self._running:
            self._positions.append((x, y, timestamp))

    def get_positions(self) -> List[Tuple[int, int, float]]:
        """Get all recorded positions."""
        return self._positions.copy()

    def clear(self) -> None:
        """Clear recorded positions."""
        self._positions.clear()

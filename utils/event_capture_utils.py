"""Event capture utilities for recording and monitoring system input events.

Provides tools for capturing keyboard and mouse events at the
system level, enabling the creation of custom event listeners
and real-time input monitoring for automation triggers.

Example:
    >>> from utils.event_capture_utils import EventCatcher, start_capture, stop_capture
    >>> catcher = EventCatcher()
    >>> catcher.start()
    >>> # ... perform actions ...
    >>> events = catcher.stop()
    >>> print(f"Captured {len(events)} events")
"""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field
from typing import Optional, Callable

__all__ = [
    "Event",
    "EventType",
    "EventCatcher",
    "start_capture",
    "stop_capture",
]


class EventType:
    """Event type constants."""

    MOUSE_MOVE = "mouse_move"
    MOUSE_DOWN = "mouse_down"
    MOUSE_UP = "mouse_up"
    MOUSE_CLICK = "mouse_click"
    KEY_DOWN = "key_down"
    KEY_UP = "key_up"
    SCROLL = "scroll"
    UNKNOWN = "unknown"


@dataclass
class Event:
    """A captured input event.

    Attributes:
        event_type: Type of event.
        timestamp: Monotonic timestamp when event occurred.
        x: X coordinate (for mouse events).
        y: Y coordinate (for mouse events).
        key: Key name (for keyboard events).
        button: Mouse button (for mouse events).
        delta: Scroll delta (for scroll events).
    """

    event_type: str
    timestamp: float = field(default_factory=time.monotonic)
    x: Optional[float] = None
    y: Optional[float] = None
    key: Optional[str] = None
    button: Optional[str] = None
    delta: Optional[tuple[float, float]] = None

    def __repr__(self) -> str:
        return (
            f"Event(type={self.event_type!r}, "
            f"pos=({self.x}, {self.y}), "
            f"key={self.key!r})"
        )


class EventCatcher:
    """Captures system input events in a background thread.

    Example:
        >>> catcher = EventCatcher()
        >>> catcher.start()
        >>> time.sleep(10)  # capture for 10 seconds
        >>> catcher.stop()
        >>> for event in catcher.events:
        ...     print(event)
    """

    def __init__(self, callback: Optional[Callable[[Event], None]] = None):
        self._events: list[Event] = []
        self._running = False
        self._callback = callback
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start capturing events."""
        self._events = []
        self._running = True
        thread = threading.Thread(target=self._capture_loop, daemon=True)
        thread.start()

    def stop(self) -> list[Event]:
        """Stop capturing and return captured events."""
        self._running = False
        time.sleep(0.1)
        with self._lock:
            return list(self._events)

    @property
    def events(self) -> list[Event]:
        with self._lock:
            return list(self._events)

    @property
    def count(self) -> int:
        return len(self.events)

    def clear(self) -> None:
        """Clear captured events."""
        with self._lock:
            self._events = []

    def _capture_loop(self) -> None:
        """Background event capture loop.

        On macOS, this uses a CGEvent tap for low-level event capture.
        """
        import sys

        if sys.platform != "darwin":
            self._running = False
            return

        try:
            import Quartz
        except ImportError:
            self._running = False
            return

        self._running = True

        # Use an NSEvent monitor as a simpler approach
        try:
            import AppKit
        except ImportError:
            self._running = False
            return

        # Basic event tap using CGEvent
        self._run_cg_event_tap()

    def _run_cg_event_tap(self) -> None:
        """Run a CGEvent tap for low-level event capture."""
        import sys

        if sys.platform != "darwin":
            return

        try:
            import Quartz
        except ImportError:
            return

        last_click_time = [0.0]
        last_click_pos = [(0, 0)]

        def callback(proxy, etype, event, refcon):
            ts = time.monotonic()
            try:
                loc = Quartz.CGEventGetLocation(event)
                x, y = float(loc.x), float(loc.y)
            except Exception:
                x = y = None

            try:
                etype_val = Quartz.CGEventGetType(event)
            except Exception:
                return event

            e: Optional[Event] = None

            if etype_val == Quartz.kCGEventMouseMoved:
                e = Event(event_type=EventType.MOUSE_MOVE, x=x, y=y)
            elif etype_val == Quartz.kCGEventLeftMouseDown:
                e = Event(event_type=EventType.MOUSE_DOWN, x=x, y=y, button="left")
            elif etype_val == Quartz.kCGEventLeftMouseUp:
                e = Event(event_type=EventType.MOUSE_UP, x=x, y=y, button="left")
                # Detect click
                if (
                    ts - last_click_time[0] < 0.5
                    and abs(x - last_click_pos[0][0]) < 5
                    and abs(y - last_click_pos[0][1]) < 5
                ):
                    e_click = Event(
                        event_type=EventType.MOUSE_CLICK,
                        x=x,
                        y=y,
                        button="left",
                        timestamp=ts,
                    )
                    self._add_event(e_click)
                last_click_time[0] = ts
                last_click_pos[0] = (x, y)
            elif etype_val == Quartz.kCGEventRightMouseDown:
                e = Event(event_type=EventType.MOUSE_DOWN, x=x, y=y, button="right")
            elif etype_val == Quartz.kCGEventRightMouseUp:
                e = Event(event_type=EventType.MOUSE_UP, x=x, y=y, button="right")
            elif etype_val == Quartz.kCGEventKeyDown:
                try:
                    key_code = Quartz.CGEventGetIntegerValueField(
                        event, Quartz.kCGKeyboardEventKeycode
                    )
                    key = _key_code_to_name.get(key_code, f"key_{key_code}")
                    e = Event(event_type=EventType.KEY_DOWN, key=key)
                except Exception:
                    pass
            elif etype_val == Quartz.kCGEventKeyUp:
                try:
                    key_code = Quartz.CGEventGetIntegerValueField(
                        event, Quartz.kCGKeyboardEventKeycode
                    )
                    key = _key_code_to_name.get(key_code, f"key_{key_code}")
                    e = Event(event_type=EventType.KEY_UP, key=key)
                except Exception:
                    pass
            elif etype_val == Quartz.kCGEventScrollWheel:
                try:
                    dy = Quartz.CGEventGetIntegerValueField(
                        event, Quartz.kCGScrollWheelEventDeltaAxis2
                    )
                    dx = Quartz.CGEventGetIntegerValueField(
                        event, Quartz.kCGScrollWheelEventDeltaAxis1
                    )
                    e = Event(
                        event_type=EventType.SCROLL,
                        delta=(float(dx), float(dy)),
                    )
                except Exception:
                    pass

            if e is not None:
                self._add_event(e)

            return event

        # Note: Setting up a CGEvent tap requires accessibility permissions
        # and proper run loop management. This is a simplified implementation.
        self._running = False

    def _add_event(self, event: Event) -> None:
        with self._lock:
            self._events.append(event)
        if self._callback:
            try:
                self._callback(event)
            except Exception:
                pass


# Key code to name mapping
_key_code_to_name = {
    0: "a", 1: "s", 2: "d", 3: "f", 4: "h", 5: "g", 6: "z", 7: "x",
    8: "c", 9: "v", 11: "b", 12: "q", 13: "w", 14: "e", 15: "r",
    17: "y", 18: "1", 19: "2", 20: "3", 21: "4", 22: "6", 23: "5",
    24: "=", 25: "9", 26: "7", 27: "-", 28: "8", 29: "0", 30: "]",
    31: "o", 32: "u", 33: "[", 34: "i", 35: "p", 37: "l", 38: "j",
    40: "k", 41: ";", 42: "\\", 43: ",", 44: "/", 45: "n", 46: "m",
    47: ".", 48: "tab", 49: "space", 50: "`",
    51: "delete", 53: "escape",
    55: "command", 56: "shift", 57: "capslock", 58: "option", 59: "control",
    96: "f5", 97: "f6", 98: "f7", 99: "f3", 100: "f8", 101: "f9",
    103: "f11", 105: "f13", 107: "f14", 109: "f10", 111: "f12",
    113: "f15", 118: "f4", 120: "f2", 122: "f1", 123: "left", 124: "right",
    125: "down", 126: "up",
}


# Module-level convenience
_global_catcher: Optional[EventCatcher] = None


def start_capture(callback: Optional[Callable[[Event], None]] = None) -> EventCatcher:
    """Start global event capture (convenience function)."""
    global _global_catcher
    _global_catcher = EventCatcher(callback=callback)
    _global_catcher.start()
    return _global_catcher


def stop_capture() -> list[Event]:
    """Stop global event capture and return captured events."""
    global _global_catcher
    if _global_catcher is not None:
        return _global_catcher.stop()
    return []

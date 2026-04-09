"""
Input event dispatch utilities for UI automation.

This module provides utilities for dispatching input events
(keyboard, mouse, touch) to the system in a controlled manner.
"""

from __future__ import annotations

import time
import platform
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from contextlib import contextmanager


IS_MACOS: bool = platform.system() == 'Darwin'


class InputEventType(Enum):
    """Types of input events."""
    KEYBOARD = auto()
    MOUSE_MOVE = auto()
    MOUSE_CLICK = auto()
    MOUSE_DRAG = auto()
    MOUSE_SCROLL = auto()
    TOUCH = auto()
    CUSTOM = auto()


@dataclass
class InputEvent:
    """
    Represents a single input event.

    Attributes:
        event_type: Type of input event.
        x: X coordinate (for pointer events).
        y: Y coordinate (for pointer events).
        button: Mouse button (for click events).
        keycode: Key code (for keyboard events).
        delta: Scroll delta (for scroll events).
        timestamp: Event timestamp.
        metadata: Additional event data.
    """
    event_type: InputEventType
    x: int = 0
    y: int = 0
    button: Optional[str] = None
    keycode: Optional[int] = None
    delta: float = 0.0
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_pointer_event(self) -> bool:
        """Check if this is a pointer (mouse/touch) event."""
        return self.event_type in (
            InputEventType.MOUSE_MOVE,
            InputEventType.MOUSE_CLICK,
            InputEventType.MOUSE_DRAG,
            InputEventType.TOUCH,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize event to dictionary."""
        return {
            'event_type': self.event_type.name,
            'x': self.x,
            'y': self.y,
            'button': self.button,
            'keycode': self.keycode,
            'delta': self.delta,
            'timestamp': self.timestamp,
            'metadata': self.metadata,
        }


@dataclass
class InputSequence:
    """
    A sequence of input events to be dispatched.

    Attributes:
        events: List of input events.
        delay_between: Delay in seconds between events.
        repeat: Number of times to repeat the sequence.
    """
    events: List[InputEvent] = field(default_factory=list)
    delay_between: float = 0.0
    repeat: int = 1

    def add_event(self, event: InputEvent) -> None:
        """Add an event to the sequence."""
        self.events.append(event)

    def add_click(
        self, x: int, y: int,
        button: str = 'left',
        click_count: int = 1
    ) -> None:
        """Add a click event to the sequence."""
        for _ in range(click_count):
            self.events.append(
                InputEvent(
                    event_type=InputEventType.MOUSE_CLICK,
                    x=x, y=y, button=button,
                    metadata={'click_count': click_count}
                )
            )

    def add_move(self, x: int, y: int) -> None:
        """Add a move event to the sequence."""
        self.events.append(
            InputEvent(event_type=InputEventType.MOUSE_MOVE, x=x, y=y)
        )

    def add_keypress(self, keycode: int) -> None:
        """Add a keypress event to the sequence."""
        self.events.append(
            InputEvent(
                event_type=InputEventType.KEYBOARD,
                keycode=keycode
            )
        )

    def add_scroll(self, x: int, y: int, delta: float) -> None:
        """Add a scroll event to the sequence."""
        self.events.append(
            InputEvent(
                event_type=InputEventType.MOUSE_SCROLL,
                x=x, y=y, delta=delta
            )
        )

    def clear(self) -> None:
        """Clear all events from the sequence."""
        self.events.clear()

    def duration_estimate(self) -> float:
        """Estimate total duration of the sequence in seconds."""
        base = sum(self.delay_between for _ in self.events)
        return base * self.repeat


class InputDispatcher:
    """
    Centralized input event dispatcher.

    Provides a unified interface for dispatching various types
    of input events with logging and state tracking.
    """

    def __init__(self, enable_logging: bool = True):
        """
        Initialize the input dispatcher.

        Args:
            enable_logging: Whether to log dispatched events.
        """
        self._enable_logging = enable_logging
        self._event_log: List[InputEvent] = []
        self._dispatch_count: Dict[InputEventType, int] = {}

    def dispatch(self, event: InputEvent) -> None:
        """
        Dispatch a single input event.

        Args:
            event: The input event to dispatch.

        Raises:
            ValueError: If event type is not supported.
        """
        if self._enable_logging:
            self._event_log.append(event)
        self._dispatch_count[event.event_type] = (
            self._dispatch_count.get(event.event_type, 0) + 1
        )

        if event.is_pointer_event():
            self._dispatch_pointer_event(event)
        elif event.event_type == InputEventType.KEYBOARD:
            self._dispatch_keyboard_event(event)
        elif event.event_type == InputEventType.MOUSE_SCROLL:
            self._dispatch_scroll_event(event)
        else:
            raise ValueError(f"Unsupported event type: {event.event_type}")

    def _dispatch_pointer_event(self, event: InputEvent) -> None:
        """Dispatch a pointer event (mouse/touch)."""
        if IS_MACOS:
            import Quartz
            # Move mouse to position
            e = Quartz.CGEventCreateMouseEvent(
                None,
                Quartz.kCGEventMouseMoved,
                (event.x, event.y),
                Quartz.kCGMouseButtonLeft
            )
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, e)

            if event.event_type == InputEventType.MOUSE_CLICK:
                self._macos_click(event)
        else:
            import pyautogui
            pyautogui.moveTo(event.x, event.y, _pause=False)

    def _macos_click(self, event: InputEvent) -> None:
        """Dispatch a click on macOS."""
        import Quartz
        button_map = {
            'left': Quartz.kCGEventLeftMouseDown,
            'right': Quartz.kCGEventRightMouseDown,
            'middle': Quartz.kCGEventOtherMouseDown,
        }
        down_type = button_map.get(event.button, Quartz.kCGEventLeftMouseDown)
        up_type = {
            'left': Quartz.kCGEventLeftMouseUp,
            'right': Quartz.kCGEventRightMouseUp,
            'middle': Quartz.kCGEventOtherMouseUp,
        }.get(event.button, Quartz.kCGEventLeftMouseUp)

        for ev_type in [down_type, up_type]:
            e = Quartz.CGEventCreateMouseEvent(
                None, ev_type, (event.x, event.y),
                Quartz.kCGMouseButtonLeft
            )
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, e)

    def _dispatch_keyboard_event(self, event: InputEvent) -> None:
        """Dispatch a keyboard event."""
        if IS_MACOS:
            import Quartz
            e = Quartz.CGEventCreateKeyboardEvent(
                None,
                event.keycode,
                True
            )
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, e)
        else:
            import pyautogui
            pyautogui.press(event.keycode, _pause=False)

    def _dispatch_scroll_event(self, event: InputEvent) -> None:
        """Dispatch a scroll event."""
        if IS_MACOS:
            import Quartz
            e = Quartz.CGEventCreateScrollMouseEvent(
                None,
                Quartz.kCGScrollMouseEventVersion,
                Quartz.kCGEventSourceStateID,
                int(event.delta),
                int(event.delta),
                0
            )
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, e)
        else:
            import pyautogui
            pyautogui.scroll(int(event.delta), x=event.x, y=event.y, _pause=False)

    def dispatch_sequence(self, sequence: InputSequence) -> None:
        """
        Dispatch an entire input sequence.

        Args:
            sequence: The input sequence to dispatch.
        """
        for _ in range(sequence.repeat):
            for event in sequence.events:
                self.dispatch(event)
                if sequence.delay_between > 0:
                    time.sleep(sequence.delay_between)

    def get_stats(self) -> Dict[str, Any]:
        """Get dispatch statistics."""
        return {
            'total_events': len(self._event_log),
            'by_type': {
                k.name: v for k, v in self._dispatch_count.items()
            },
        }

    def clear_log(self) -> None:
        """Clear the event log."""
        self._event_log.clear()

    @contextmanager
    def dispatch_block(self):
        """
        Context manager for batching dispatches.

        Yields:
            The dispatcher itself.
        """
        yield self


def create_click_sequence(
    points: List[Tuple[int, int]],
    button: str = 'left',
    delay_between: float = 0.1
) -> InputSequence:
    """
    Create a click sequence from a list of points.

    Args:
        points: List of (x, y) coordinates to click.
        button: Mouse button to use.
        delay_between: Delay between clicks.

    Returns:
        InputSequence configured for the clicks.
    """
    seq = InputSequence(delay_between=delay_between)
    for x, y in points:
        seq.add_click(x, y, button=button)
    return seq

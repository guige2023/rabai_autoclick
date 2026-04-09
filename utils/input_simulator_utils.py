"""
Input simulation utilities for UI automation.

This module provides high-level utilities for simulating user input
including keyboard, mouse, and touch events with configurable
timing and behavior.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional, List, Dict, Any, Tuple
from enum import Enum, auto


class InputType(Enum):
    """Types of simulated input."""
    MOUSE_MOVE = auto()
    MOUSE_CLICK = auto()
    MOUSE_DOUBLE_CLICK = auto()
    MOUSE_RIGHT_CLICK = auto()
    MOUSE_DRAG = auto()
    MOUSE_SCROLL = auto()
    KEYBOARD_KEY = auto()
    KEYBOARD_TEXT = auto()
    TOUCH_TAP = auto()
    TOUCH_LONG_PRESS = auto()
    TOUCH_SWIPE = auto()
    TOUCH_DRAG = auto()


@dataclass
class InputEvent:
    """
    Represents a single input event.

    Attributes:
        type: The type of input event.
        position: Optional (x, y) coordinates.
        data: Additional event-specific data.
        delay: Delay before this event in seconds.
        duration: Duration for held/repeated events.
    """
    type: InputType
    position: Optional[Tuple[float, float]] = None
    data: Dict[str, Any] = field(default_factory=dict)
    delay: float = 0.0
    duration: float = 0.0

    def __post_init__(self) -> None:
        if self.delay > 0:
            time.sleep(self.delay)


@dataclass
class InputSequence:
    """
    A sequence of input events to be executed.

    Attributes:
        events: List of input events in sequence.
        repeat: Number of times to repeat the sequence.
        interval: Delay between repetitions.
    """
    events: List[InputEvent] = field(default_factory=list)
    repeat: int = 1
    interval: float = 0.0

    def add(
        self,
        input_type: InputType,
        position: Optional[Tuple[float, float]] = None,
        **kwargs: Any,
    ) -> InputSequence:
        """Add an event to the sequence."""
        self.events.append(InputEvent(type=input_type, position=position, data=kwargs))
        return self

    def click(self, x: float, y: float) -> InputSequence:
        """Add mouse click at position."""
        return self.add(InputType.MOUSE_CLICK, (x, y))

    def double_click(self, x: float, y: float) -> InputSequence:
        """Add mouse double click at position."""
        return self.add(InputType.MOUSE_DOUBLE_CLICK, (x, y))

    def right_click(self, x: float, y: float) -> InputSequence:
        """Add mouse right click at position."""
        return self.add(InputType.MOUSE_RIGHT_CLICK, (x, y))

    def move_to(self, x: float, y: float) -> InputSequence:
        """Add mouse move to position."""
        return self.add(InputType.MOUSE_MOVE, (x, y))

    def scroll(self, dx: float, dy: float) -> InputSequence:
        """Add mouse scroll."""
        return self.add(InputType.MOUSE_SCROLL, data={"dx": dx, "dy": dy})

    def type_text(self, text: str) -> InputSequence:
        """Add keyboard text input."""
        return self.add(InputType.KEYBOARD_TEXT, data={"text": text})

    def press_key(self, key: str) -> InputSequence:
        """Add keyboard key press."""
        return self.add(InputType.KEYBOARD_KEY, data={"key": key})

    def tap(self, x: float, y: float) -> InputSequence:
        """Add touch tap at position."""
        return self.add(InputType.TOUCH_TAP, (x, y))

    def long_press(self, x: float, y: float, duration: float = 1.0) -> InputSequence:
        """Add touch long press at position."""
        return self.add(InputType.TOUCH_LONG_PRESS, (x, y), duration=duration)

    def swipe(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        duration: float = 0.5,
    ) -> InputSequence:
        """Add touch swipe from (x1, y1) to (x2, y2)."""
        return self.add(
            InputType.TOUCH_SWIPE,
            (x1, y1),
            data={"end_x": x2, "end_y": y2, "duration": duration},
        )

    def drag(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        duration: float = 0.5,
    ) -> InputSequence:
        """Add touch drag from (x1, y1) to (x2, y2)."""
        return self.add(
            InputType.TOUCH_DRAG,
            (x1, y1),
            data={"end_x": x2, "end_y": y2, "duration": duration},
        )

    def wait(self, seconds: float) -> InputSequence:
        """Add a delay event."""
        self.events.append(InputEvent(type=InputType.MOUSE_MOVE, delay=seconds))
        return self


class InputSimulator:
    """
    High-level input simulator with configurable backends.

    Executes input sequences using registered input handlers.
    Supports both real hardware simulation (via platform APIs)
    and synthetic events.
    """

    def __init__(self) -> None:
        self._handlers: Dict[InputType, Callable[[InputEvent], None]] = {}
        self._before_handler: Optional[Callable[[InputEvent], None]] = None
        self._after_handler: Optional[Callable[[InputEvent], None]] = None
        self._speed: float = 1.0

    def register_handler(
        self,
        input_type: InputType,
        handler: Callable[[InputEvent], None],
    ) -> InputSimulator:
        """Register a handler for an input type."""
        self._handlers[input_type] = handler
        return self

    def set_before_handler(
        self,
        handler: Callable[[InputEvent], None],
    ) -> InputSimulator:
        """Set handler called before each event."""
        self._before_handler = handler
        return self

    def set_after_handler(
        self,
        handler: Callable[[InputEvent], None],
    ) -> InputSimulator:
        """Set handler called after each event."""
        self._after_handler = handler
        return self

    def set_speed(self, speed: float) -> InputSimulator:
        """Set simulation speed multiplier (1.0 = normal)."""
        self._speed = max(0.1, speed)
        return self

    def execute(self, sequence: InputSequence) -> None:
        """Execute an input sequence."""
        for _ in range(sequence.repeat):
            for event in sequence.events:
                self._execute_event(event)
            if sequence.interval > 0:
                time.sleep(sequence.interval / self._speed)

    def _execute_event(self, event: InputEvent) -> None:
        """Execute a single input event."""
        if event.delay > 0:
            time.sleep(event.delay / self._speed)

        if self._before_handler:
            self._before_handler(event)

        handler = self._handlers.get(event.type)
        if handler:
            handler(event)

        if self._after_handler:
            self._after_handler(event)

    def click_at(self, x: float, y: float) -> None:
        """Convenience method to click at position."""
        self.execute(InputSequence().click(x, y))

    def type_text(self, text: str) -> None:
        """Convenience method to type text."""
        self.execute(InputSequence().type_text(text))


# Gesture generation utilities
def generate_swipe_gesture(
    x1: float,
    y1: float,
    x2: float,
    y2: float,
    steps: int = 20,
) -> List[Tuple[float, float]]:
    """
    Generate touch points for a smooth swipe gesture.

    Returns list of (x, y) positions along the swipe path.
    """
    points: List[Tuple[float, float]] = []
    for i in range(steps + 1):
        t = i / steps
        x = x1 + (x2 - x1) * t
        y = y1 + (y2 - y1) * t
        points.append((x, y))
    return points


def generate_pinch_gesture(
    center_x: float,
    center_y: float,
    start_distance: float,
    end_distance: float,
    steps: int = 20,
) -> List[Tuple[Tuple[float, float], Tuple[float, float]]]:
    """
    Generate touch point pairs for a pinch gesture.

    Returns list of ((x1, y1), (x2, y2)) tuples for two fingers.
    """
    pairs: List[Tuple[Tuple[float, float], Tuple[float, float]]] = []
    for i in range(steps + 1):
        t = i / steps
        distance = start_distance + (end_distance - start_distance) * t
        half = distance / 2
        x1 = center_x - half
        x2 = center_x + half
        pairs.append(((x1, center_y), (x2, center_y)))
    return pairs


def generate_rotate_gesture(
    center_x: float,
    center_y: float,
    radius: float,
    start_angle: float,
    end_angle: float,
    steps: int = 20,
) -> List[Tuple[float, float]]:
    """
    Generate touch points for a rotation gesture.

    Angles are in radians. Positive = counter-clockwise.
    """
    import math
    points: List[Tuple[float, float]] = []
    for i in range(steps + 1):
        t = i / steps
        angle = start_angle + (end_angle - start_angle) * t
        x = center_x + radius * math.cos(angle)
        y = center_y + radius * math.sin(angle)
        points.append((x, y))
    return points

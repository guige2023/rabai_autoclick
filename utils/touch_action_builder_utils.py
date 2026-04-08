"""
Touch action builder utilities for composing complex touch interactions.

Provides a fluent builder API for constructing multi-step
touch actions including taps, swipes, long-presses, and custom sequences.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TouchStep:
    """A single step in a touch action sequence."""
    step_type: str  # "tap", "swipe", "long_press", "wait", "drag"
    x: float = 0
    y: float = 0
    x2: float = 0  # For swipe/drag
    y2: float = 0
    duration_ms: float = 100
    finger_id: int = 0
    metadata: dict = field(default_factory=dict)


@dataclass
class TouchAction:
    """A complete touch action with multiple steps."""
    name: str
    finger_count: int = 1
    steps: list[TouchStep] = field(default_factory=list)


class TouchActionBuilder:
    """Fluent builder for composing touch action sequences."""

    def __init__(self, name: str = ""):
        self._action = TouchAction(name=name)

    def with_name(self, name: str) -> TouchActionBuilder:
        self._action.name = name
        return self

    def with_finger_count(self, count: int) -> TouchActionBuilder:
        self._action.finger_count = count
        return self

    def tap(self, x: float, y: float, finger_id: int = 0) -> TouchActionBuilder:
        """Add a tap step."""
        self._action.steps.append(TouchStep(
            step_type="tap", x=x, y=y, finger_id=finger_id, duration_ms=50
        ))
        return self

    def double_tap(self, x: float, y: float, finger_id: int = 0) -> TouchActionBuilder:
        """Add two rapid taps."""
        self._action.steps.append(TouchStep(
            step_type="tap", x=x, y=y, finger_id=finger_id, duration_ms=50
        ))
        self._action.steps.append(TouchStep(
            step_type="tap", x=x, y=y, finger_id=finger_id, duration_ms=50
        ))
        return self

    def long_press(self, x: float, y: float, duration_ms: float = 500.0, finger_id: int = 0) -> TouchActionBuilder:
        """Add a long press step."""
        self._action.steps.append(TouchStep(
            step_type="long_press", x=x, y=y, finger_id=finger_id, duration_ms=duration_ms
        ))
        return self

    def swipe(
        self,
        x1: float, y1: float,
        x2: float, y2: float,
        duration_ms: float = 200.0,
        finger_id: int = 0,
    ) -> TouchActionBuilder:
        """Add a swipe step."""
        self._action.steps.append(TouchStep(
            step_type="swipe",
            x=x1, y=y1,
            x2=x2, y2=y2,
            finger_id=finger_id,
            duration_ms=duration_ms,
        ))
        return self

    def drag(
        self,
        x1: float, y1: float,
        x2: float, y2: float,
        duration_ms: float = 300.0,
        finger_id: int = 0,
    ) -> TouchActionBuilder:
        """Add a drag step (slow swipe with hold)."""
        self._action.steps.append(TouchStep(
            step_type="drag",
            x=x1, y=y1,
            x2=x2, y2=y2,
            finger_id=finger_id,
            duration_ms=duration_ms,
        ))
        return self

    def wait(self, duration_ms: float = 100.0) -> TouchActionBuilder:
        """Add a wait step between actions."""
        self._action.steps.append(TouchStep(
            step_type="wait", duration_ms=duration_ms
        ))
        return self

    def build(self) -> TouchAction:
        return self._action

    def clear(self) -> TouchActionBuilder:
        self._action = TouchAction(name=self._action.name, finger_count=self._action.finger_count)
        return self


# Preset touch actions
def two_finger_swipe_down(y_start: float, screen_width: float, screen_height: float, duration_ms: float = 200.0) -> TouchAction:
    """Create a two-finger swipe down action."""
    x = screen_width / 2
    return TouchActionBuilder("two_finger_swipe_down") \
        .with_finger_count(2) \
        .swipe(x, y_start, x, y_start - 300, duration_ms, finger_id=0) \
        .build()


def pinch_to_zoom(cx: float, cy: float, start_scale: float, end_scale: float) -> TouchAction:
    """Create a pinch-to-zoom action."""
    distance = 100
    b = TouchActionBuilder("pinch_zoom").with_finger_count(2)

    # Start positions
    x1, y1 = cx - distance, cy
    x2, y2 = cx + distance, cy

    if end_scale > start_scale:  # Zoom in
        b.swipe(x1, y1, x1 - 50, y1, 150, finger_id=0)
        b.swipe(x2, y2, x2 + 50, y2, 150, finger_id=1)
    else:  # Zoom out
        b.swipe(x1, y1, x1 + 50, y1, 150, finger_id=0)
        b.swipe(x2, y2, x2 - 50, y2, 150, finger_id=1)

    return b.build()


__all__ = ["TouchActionBuilder", "TouchAction", "TouchStep", "two_finger_swipe_down", "pinch_to_zoom"]

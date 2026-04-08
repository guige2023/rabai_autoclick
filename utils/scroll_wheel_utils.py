"""
Scroll wheel utilities for mouse wheel interaction.

Provides scroll wheel event handling, momentum scrolling,
and scroll target detection.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class ScrollEvent:
    """A scroll wheel event."""
    x: int
    y: int
    delta_x: int = 0
    delta_y: int = 0
    momentum: float = 0.0
    phase: str = "began"  # began, changed, ended
    timestamp_ms: int = 0


@dataclass
class ScrollState:
    """Current scroll state."""
    x: int
    y: int
    velocity_x: float = 0.0
    velocity_y: float = 0.0
    content_offset_x: float = 0.0
    content_offset_y: float = 0.0
    is_momentum: bool = False


class ScrollWheelHandler:
    """Handles scroll wheel events with momentum."""

    def __init__(
        self,
        friction: float = 0.95,
        min_velocity: float = 0.5,
        on_scroll: Optional[Callable[[ScrollEvent], None]] = None,
    ):
        self.friction = friction
        self.min_velocity = min_velocity
        self.on_scroll = on_scroll
        self._state = ScrollState(x=0, y=0)
        self._momentum_events: list[ScrollEvent] = []

    def handle_event(self, event: ScrollEvent) -> list[ScrollEvent]:
        """Handle a scroll event and return any generated momentum events."""
        self._state.x = event.x
        self._state.y = event.y

        if event.phase in ("began", "changed"):
            self._state.velocity_x = event.delta_x
            self._state.velocity_y = event.delta_y
            self._state.is_momentum = False

        if self.on_scroll:
            self.on_scroll(event)

        if event.phase == "ended":
            return self._start_momentum()

        return [event]

    def _start_momentum(self) -> list[ScrollEvent]:
        """Start momentum scrolling."""
        self._state.is_momentum = True
        events = []
        vx = self._state.velocity_x
        vy = self._state.velocity_y

        while abs(vx) > self.min_velocity or abs(vy) > self.min_velocity:
            vx *= self.friction
            vy *= self.friction

            self._state.content_offset_x += vx
            self._state.content_offset_y += vy

            events.append(ScrollEvent(
                x=self._state.x,
                y=self._state.y,
                delta_x=int(vx),
                delta_y=int(vy),
                momentum=math.hypot(vx, vy),
                phase="changed",
            ))

        events.append(ScrollEvent(
            x=self._state.x,
            y=self._state.y,
            delta_x=0,
            delta_y=0,
            phase="ended",
        ))

        self._state.velocity_x = 0.0
        self._state.velocity_y = 0.0
        self._state.is_momentum = False

        return events

    def scroll_to(
        self,
        target_x: float,
        target_y: float,
        duration_ms: float = 300.0,
    ) -> list[ScrollEvent]:
        """Generate scroll events to move to a target position."""
        dx = target_x - self._state.content_offset_x
        dy = target_y - self._state.content_offset_y

        steps = max(1, int(duration_ms / 16))
        events = []
        for i in range(steps):
            t = i / steps
            eased = 1 - (1 - t) ** 2  # ease_out
            delta_x = int(dx / steps * (2 * (1 - eased)))
            delta_y = int(dy / steps * (2 * (1 - eased)))
            events.append(ScrollEvent(
                x=self._state.x,
                y=self._state.y,
                delta_x=delta_x,
                delta_y=delta_y,
                phase="changed",
            ))

        events.append(ScrollEvent(
            x=self._state.x,
            y=self._state.y,
            phase="ended",
        ))
        return events

    @property
    def state(self) -> ScrollState:
        return self._state


def scroll_by_clicks(
    clicks_x: int = 0,
    clicks_y: int = 0,
    x: int = 0,
    y: int = 0,
) -> ScrollEvent:
    """Create a scroll event from click counts."""
    delta_y = clicks_y * 120  # macOS scroll delta
    delta_x = clicks_x * 120
    return ScrollEvent(
        x=x,
        y=y,
        delta_x=delta_x,
        delta_y=delta_y,
        phase="changed",
    )


__all__ = ["ScrollWheelHandler", "ScrollEvent", "ScrollState", "scroll_by_clicks"]

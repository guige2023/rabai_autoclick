"""UI event detection utilities.

This module provides utilities for detecting and classifying
UI events from event streams.
"""

from __future__ import annotations

import time
from typing import Callable, Dict, List, Optional, TypeVar
from dataclasses import dataclass, field
from enum import Enum, auto


class EventKind(Enum):
    """Kinds of UI events."""
    CLICK = auto()
    DOUBLE_CLICK = auto()
    LONG_PRESS = auto()
    DRAG = auto()
    HOVER = auto()
    SCROLL = auto()
    KEY_TYPING = auto()
    FOCUS = auto()
    BLUR = auto()
    UNKNOWN = auto()


@dataclass
class UIEvent:
    """A UI event."""
    kind: EventKind
    timestamp: float
    x: int = 0
    y: int = 0
    key: str = ""
    button: int = 0
    delta: int = 0
    metadata: Dict = field(default_factory=dict)


class EventDetector:
    """Detects UI event types from event streams."""

    def __init__(
        self,
        double_click_ms: float = 300.0,
        long_press_ms: float = 500.0,
        drag_threshold_px: float = 5.0,
    ) -> None:
        self._double_click_ms = double_click_ms
        self._long_press_ms = long_press_ms
        self._drag_threshold = drag_threshold_px
        self._recent_clicks: List[UIEvent] = []
        self._press_start: Optional[float] = None
        self._press_pos: Optional[tuple[int, int]] = None

    def detect_click(self, event: UIEvent) -> EventKind:
        """Detect if event is part of a click pattern.

        Args:
            event: UI event to analyze.

        Returns:
            EventKind classification.
        """
        self._cleanup_old_clicks(event.timestamp)

        is_double = any(
            abs(event.x - c.x) < 5 and abs(event.y - c.y) < 5
            and (event.timestamp - c.timestamp) * 1000 < self._double_click_ms
            for c in self._recent_clicks
        )

        if is_double:
            return EventKind.DOUBLE_CLICK

        self._recent_clicks.append(event)
        return EventKind.CLICK

    def detect_long_press(self, event: UIEvent) -> bool:
        """Detect long press pattern.

        Args:
            event: UI event (should be press event).

        Returns:
            True if long press detected.
        """
        if self._press_start is None:
            self._press_start = event.timestamp
            self._press_pos = (event.x, event.y)
            return False

        duration_ms = (event.timestamp - self._press_start) * 1000
        if duration_ms >= self._long_press_ms:
            return True
        return False

    def detect_drag(
        self,
        start_x: int,
        start_y: int,
        current_x: int,
        current_y: int,
    ) -> bool:
        """Detect if movement constitutes a drag.

        Args:
            start_x: Start X coordinate.
            start_y: Start Y coordinate.
            current_x: Current X coordinate.
            current_y: Current Y coordinate.

        Returns:
            True if drag threshold exceeded.
        """
        dx = abs(current_x - start_x)
        dy = abs(current_y - start_y)
        return (dx * dx + dy * dy) ** 0.5 > self._drag_threshold

    def detect_scroll(self, events: List[UIEvent]) -> bool:
        """Detect scroll pattern from event sequence.

        Args:
            events: Event sequence to analyze.

        Returns:
            True if scroll pattern detected.
        """
        scroll_events = [e for e in events if e.delta != 0]
        return len(scroll_events) >= 3

    def _cleanup_old_clicks(self, timestamp: float) -> None:
        """Remove old clicks beyond double-click window."""
        cutoff = timestamp - (self._double_click_ms / 1000) * 2
        self._recent_clicks = [
            c for c in self._recent_clicks if c.timestamp > cutoff
        ]

    def reset(self) -> None:
        """Reset detector state."""
        self._recent_clicks.clear()
        self._press_start = None
        self._press_pos = None


__all__ = [
    "EventKind",
    "UIEvent",
    "EventDetector",
]

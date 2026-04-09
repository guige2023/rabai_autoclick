"""
Double Tap Detection Utilities for UI Automation.

This module provides utilities for detecting double tap
gestures in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Callable
from enum import Enum


class TapState(Enum):
    """States in double tap detection."""
    IDLE = "idle"
    FIRST_TAP = "first_tap"
    WAITING_SECOND = "waiting_second"
    SECOND_TAP = "second_tap"


@dataclass
class DoubleTapEvent:
    """Represents a detected double tap gesture."""
    x: float
    y: float
    first_tap_time: float
    second_tap_time: float
    interval_ms: float
    was_recognized: bool = False


@dataclass
class SingleTapEvent:
    """Represents a detected single tap gesture."""
    x: float
    y: float
    duration_ms: float
    timestamp: float


@dataclass
class DoubleTapConfig:
    """Configuration for double tap detection."""
    max_interval_ms: float = 300.0
    max_distance_px: float = 40.0
    max_duration_ms: float = 200.0
    require_both_in_bounds: bool = True


class DoubleTapDetector:
    """Detects single and double tap gestures."""

    def __init__(self, config: Optional[DoubleTapConfig] = None) -> None:
        self._config = config or DoubleTapConfig()
        self._state: TapState = TapState.IDLE
        self._first_tap_x: float = 0.0
        self._first_tap_y: float = 0.0
        self._first_tap_time: float = 0.0
        self._last_tap_time: float = 0.0
        self._on_double_tap_callbacks: List[Callable[[DoubleTapEvent], None]] = []
        self._on_single_tap_callbacks: List[Callable[[SingleTapEvent], None]] = []

    def on_double_tap(self, callback: Callable[[DoubleTapEvent], None]) -> None:
        """Register a callback for double tap events."""
        self._on_double_tap_callbacks.append(callback)

    def on_single_tap(self, callback: Callable[[SingleTapEvent], None]) -> None:
        """Register a callback for single tap events."""
        self._on_single_tap_callbacks.append(callback)

    def tap_down(self, x: float, y: float) -> None:
        """Handle tap down event."""
        current_time = time.time()

        if self._state == TapState.IDLE:
            self._state = TapState.FIRST_TAP
            self._first_tap_x = x
            self._first_tap_y = y
            self._first_tap_time = current_time

        elif self._state == TapState.WAITING_SECOND:
            elapsed = (current_time - self._last_tap_time) * 1000.0
            dx = x - self._first_tap_x
            dy = y - self._first_tap_y
            distance = (dx * dx + dy * dy) ** 0.5

            if elapsed <= self._config.max_interval_ms and distance <= self._config.max_distance_px:
                self._state = TapState.SECOND_TAP
            else:
                self._state = TapState.FIRST_TAP
                self._first_tap_x = x
                self._first_tap_y = y
                self._first_tap_time = current_time

    def tap_up(self, x: float, y: float) -> Optional[Any]:
        """Handle tap up event and return recognized gesture."""
        current_time = time.time()

        if self._state == TapState.FIRST_TAP:
            duration_ms = (current_time - self._first_tap_time) * 1000.0

            if duration_ms <= self._config.max_duration_ms:
                self._state = TapState.WAITING_SECOND
                self._last_tap_time = current_time
                return None
            else:
                self._state = TapState.IDLE
                return self._emit_single_tap(x, y, duration_ms)

        elif self._state == TapState.SECOND_TAP:
            interval_ms = (current_time - self._first_tap_time) * 1000.0
            dx = x - self._first_tap_x
            dy = y - self._first_tap_y
            distance = (dx * dx + dy * dy) ** 0.5

            self._state = TapState.IDLE

            if self._config.require_both_in_bounds and distance > self._config.max_distance_px:
                return self._emit_single_tap(x, y, 0.0)

            event = DoubleTapEvent(
                x=self._first_tap_x,
                y=self._first_tap_y,
                first_tap_time=self._first_tap_time,
                second_tap_time=current_time,
                interval_ms=interval_ms,
                was_recognized=True,
            )

            for callback in self._on_double_tap_callbacks:
                callback(event)

            return event

        elif self._state == TapState.WAITING_SECOND:
            elapsed = (current_time - self._last_tap_time) * 1000.0
            if elapsed > self._config.max_interval_ms:
                duration_ms = (current_time - self._first_tap_time) * 1000.0
                self._state = TapState.IDLE
                return self._emit_single_tap(self._first_tap_x, self._first_tap_y, duration_ms)

        return None

    def _emit_single_tap(self, x: float, y: float, duration_ms: float) -> SingleTapEvent:
        """Emit a single tap event."""
        event = SingleTapEvent(
            x=x,
            y=y,
            duration_ms=duration_ms,
            timestamp=self._first_tap_time if self._state == TapState.IDLE else time.time(),
        )

        for callback in self._on_single_tap_callbacks:
            callback(event)

        return event

    def get_state(self) -> TapState:
        """Get the current tap detection state."""
        return self._state

    def reset(self) -> None:
        """Reset all state."""
        self._state = TapState.IDLE
        self._first_tap_x = 0.0
        self._first_tap_y = 0.0
        self._first_tap_time = 0.0
        self._last_tap_time = 0.0

    def is_waiting_for_second_tap(self) -> bool:
        """Check if waiting for a second tap."""
        return self._state == TapState.WAITING_SECOND

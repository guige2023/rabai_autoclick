"""
Long Press Detection Utilities for UI Automation.

This module provides utilities for detecting and handling
long press gestures in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Callable
from enum import Enum


class LongPressState(Enum):
    """States in long press detection."""
    IDLE = "idle"
    TOUCHING = "touching"
    ACTIVATED = "activated"
    RELEASED = "released"


@dataclass
class LongPressEvent:
    """Represents a long press gesture event."""
    x: float
    y: float
    duration: float
    timestamp: float
    state: LongPressState
    was_activated: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LongPressConfig:
    """Configuration for long press detection."""
    activation_threshold_ms: float = 500.0
    max_movement_radius: float = 20.0
    allow_move_during_press: bool = False
    repeat_activation: bool = False
    repeat_interval_ms: float = 200.0


class LongPressDetector:
    """Detects long press gestures from touch/mouse input."""

    def __init__(
        self,
        config: Optional[LongPressConfig] = None,
    ) -> None:
        self._config = config or LongPressConfig()
        self._state: LongPressState = LongPressState.IDLE
        self._start_x: float = 0.0
        self._start_y: float = 0.0
        self._start_time: float = 0.0
        self._current_x: float = 0.0
        self._current_y: float = 0.0
        self._activation_time: Optional[float] = None
        self._callbacks: Dict[LongPressState, List[Callable]] = {}

    def set_config(self, config: LongPressConfig) -> None:
        """Update the long press configuration."""
        self._config = config

    def on_state_change(
        self,
        state: LongPressState,
        callback: Callable[[LongPressEvent], None],
    ) -> None:
        """Register a callback for state changes."""
        if state not in self._callbacks:
            self._callbacks[state] = []
        self._callbacks[state].append(callback)

    def touch_start(self, x: float, y: float) -> None:
        """Record the start of a potential long press."""
        self._state = LongPressState.TOUCHING
        self._start_x = x
        self._start_y = y
        self._current_x = x
        self._current_y = y
        self._start_time = time.time()
        self._activation_time = None
        self._emit_event()

    def touch_move(self, x: float, y: float) -> None:
        """Update position during a long press."""
        if self._state == LongPressState.IDLE:
            return

        self._current_x = x
        self._current_y = y

        if not self._config.allow_move_during_press:
            if self._has_exceeded_radius():
                self._state = LongPressState.RELEASED
                self._emit_event()
                self.reset()

    def touch_end(self) -> Optional[LongPressEvent]:
        """Record the end of a long press and return the event."""
        if self._state == LongPressState.IDLE:
            return None

        duration = (time.time() - self._start_time) * 1000.0
        was_activated = self._activation_time is not None

        event = LongPressEvent(
            x=self._start_x,
            y=self._start_y,
            duration=duration,
            timestamp=self._start_time,
            state=self._state,
            was_activated=was_activated,
        )

        self._state = LongPressState.RELEASED
        self._emit_event()
        self.reset()
        return event

    def cancel(self) -> None:
        """Cancel the current long press detection."""
        self.reset()

    def reset(self) -> None:
        """Reset all state."""
        self._state = LongPressState.IDLE
        self._activation_time = None

    def check_activation(self) -> bool:
        """Check if long press has been activated based on elapsed time."""
        if self._state != LongPressState.TOUCHING:
            return False

        elapsed = (time.time() - self._start_time) * 1000.0

        if elapsed >= self._config.activation_threshold_ms:
            if self._activation_time is None:
                self._activation_time = time.time()
                self._state = LongPressState.ACTIVATED
                self._emit_event()
            return True

        return False

    def get_state(self) -> LongPressState:
        """Get the current long press state."""
        return self._state

    def get_elapsed_time_ms(self) -> float:
        """Get elapsed time since touch start in milliseconds."""
        if self._state == LongPressState.IDLE:
            return 0.0
        return (time.time() - self._start_time) * 1000.0

    def _has_exceeded_radius(self) -> bool:
        """Check if the touch has moved beyond the allowed radius."""
        dx = self._current_x - self._start_x
        dy = self._current_y - self._start_y
        distance_sq = dx * dx + dy * dy
        return distance_sq > self._config.max_movement_radius ** 2

    def _emit_event(self) -> None:
        """Emit event to registered callbacks."""
        if self._state not in self._callbacks:
            return

        elapsed = (time.time() - self._start_time) * 1000.0 if self._state != LongPressState.IDLE else 0.0

        event = LongPressEvent(
            x=self._start_x,
            y=self._start_y,
            duration=elapsed,
            timestamp=self._start_time,
            state=self._state,
            was_activated=self._activation_time is not None,
        )

        for callback in self._callbacks[self._state]:
            callback(event)


def create_long_press_config(
    activation_threshold_ms: float = 500.0,
    **kwargs: Any,
) -> LongPressConfig:
    """Create a long press configuration with the specified parameters."""
    return LongPressConfig(
        activation_threshold_ms=activation_threshold_ms,
        **kwargs,
    )

"""
Touch Idle Detection Utilities

Detect when a touch device has been idle (no touch events)
for a configurable period, useful for power management and
automation pause/resume.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Optional, List


@dataclass
class IdleState:
    """Current idle state of the touch device."""
    is_idle: bool
    idle_duration_ms: float
    last_touch_timestamp_ms: float
    idle_threshold_ms: float


class TouchIdleDetector:
    """
    Detect when the touch device has been idle for a period.

    Fires callbacks when idle starts and ends, enabling
    power-saving modes or automation pause.
    """

    def __init__(
        self,
        idle_threshold_ms: float = 5000.0,
        wake_threshold_ms: float = 500.0,
    ):
        self.idle_threshold_ms = idle_threshold_ms
        self.wake_threshold_ms = wake_threshold_ms
        self._last_touch_ms = time.time() * 1000
        self._is_idle = False
        self._on_idle_callbacks: List[Callable[[IdleState], None]] = []
        self._on_wake_callbacks: List[Callable[[IdleState], None]] = []

    def record_touch(self, timestamp_ms: Optional[float] = None) -> IdleState:
        """
        Record a touch event, potentially waking from idle.

        Returns the current idle state.
        """
        now = timestamp_ms or time.time() * 1000
        was_idle = self._is_idle

        self._last_touch_ms = now
        self._is_idle = False

        state = self._get_state(now)

        if was_idle and not self._is_idle:
            # Transition: idle -> active (wake)
            for cb in self._on_wake_callbacks:
                cb(state)

        return state

    def check_idle(self, timestamp_ms: Optional[float] = None) -> IdleState:
        """
        Check if the device has become idle.

        This should be called periodically (e.g., by a timer).
        """
        now = timestamp_ms or time.time() * 1000
        was_idle = self._is_idle

        idle_duration = now - self._last_touch_ms
        if idle_duration >= self.idle_threshold_ms:
            self._is_idle = True

        state = self._get_state(now)

        if not was_idle and self._is_idle:
            # Transition: active -> idle
            for cb in self._on_idle_callbacks:
                cb(state)

        return state

    def on_idle(self, callback: Callable[[IdleState], None]) -> None:
        """Register a callback for when idle starts."""
        self._on_idle_callbacks.append(callback)

    def on_wake(self, callback: Callable[[IdleState], None]) -> None:
        """Register a callback for when idle ends (touch resumes)."""
        self._on_wake_callbacks.append(callback)

    def reset(self) -> None:
        """Reset the idle detector."""
        self._last_touch_ms = time.time() * 1000
        self._is_idle = False

    def _get_state(self, now_ms: float) -> IdleState:
        """Get the current idle state."""
        return IdleState(
            is_idle=self._is_idle,
            idle_duration_ms=now_ms - self._last_touch_ms,
            last_touch_timestamp_ms=self._last_touch_ms,
            idle_threshold_ms=self.idle_threshold_ms,
        )

"""
Touch Timing Utilities for UI Automation.

This module provides utilities for managing precise touch timing
and synchronization in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Callable
from enum import Enum


class TimingMode(Enum):
    """Timing modes for touch events."""
    IMMEDIATE = "immediate"
    SCHEDULED = "scheduled"
    SYNCED = "synced"
    FRAME_LOCKED = "frame_locked"


@dataclass
class TouchTimingEvent:
    """A timed touch event."""
    scheduled_time: float
    actual_time: Optional[float] = None
    target_x: float = 0.0
    target_y: float = 0.0
    action: str = "touch"
    completed: bool = False
    jitter_ms: float = 0.0


@dataclass
class TimingStats:
    """Statistics for touch timing accuracy."""
    mean_jitter_ms: float = 0.0
    max_jitter_ms: float = 0.0
    min_jitter_ms: float = 0.0
    std_dev_ms: float = 0.0
    total_events: int = 0
    missed_deadlines: int = 0


@dataclass
class TimingConfig:
    """Configuration for touch timing."""
    mode: TimingMode = TimingMode.IMMEDIATE
    target_fps: float = 60.0
    max_jitter_tolerance_ms: float = 5.0
    scheduling_ahead_ms: float = 10.0
    use_hardware_timer: bool = False


class TouchTimingManager:
    """Manages precise timing for touch events."""

    def __init__(self, config: Optional[TimingConfig] = None) -> None:
        self._config = config or TimingConfig()
        self._schedule: List[TouchTimingEvent] = []
        self._completed: List[TouchTimingEvent] = []
        self._frame_interval: float = 1.0 / self._config.target_fps
        self._last_frame_time: float = 0.0
        self._on_deadline_miss_callbacks: List[Callable[[TouchTimingEvent], None]] = []

    def set_config(self, config: TimingConfig) -> None:
        """Update the timing configuration."""
        self._config = config
        self._frame_interval = 1.0 / config.target_fps

    def on_deadline_miss(
        self,
        callback: Callable[[TouchTimingEvent], None],
    ) -> None:
        """Register a callback for missed deadlines."""
        self._on_deadline_miss_callbacks.append(callback)

    def schedule_touch(
        self,
        x: float,
        y: float,
        delay_ms: float,
        action: str = "touch",
    ) -> TouchTimingEvent:
        """Schedule a touch event for a future time."""
        scheduled_time = time.time() + delay_ms / 1000.0

        event = TouchTimingEvent(
            scheduled_time=scheduled_time,
            target_x=x,
            target_y=y,
            action=action,
        )

        self._schedule.append(event)
        self._schedule.sort(key=lambda e: e.scheduled_time)

        return event

    def process_scheduled(self) -> List[TouchTimingEvent]:
        """Process all events that should have fired by now."""
        current_time = time.time()
        ready: List[TouchTimingEvent] = []

        while self._schedule and self._schedule[0].scheduled_time <= current_time:
            event = self._schedule.pop(0)
            event.actual_time = current_time
            event.completed = True
            event.jitter_ms = (current_time - event.scheduled_time) * 1000.0

            if event.jitter_ms > self._config.max_jitter_tolerance_ms:
                for cb in self._on_deadline_miss_callbacks:
                    cb(event)

            ready.append(event)
            self._completed.append(event)

        return ready

    def get_next_event_time(self) -> Optional[float]:
        """Get the scheduled time of the next event."""
        if self._schedule:
            return self._schedule[0].scheduled_time
        return None

    def get_statistics(self) -> TimingStats:
        """Calculate timing statistics from completed events."""
        if not self._completed:
            return TimingStats()

        jitters = [e.jitter_ms for e in self._completed]
        missed = sum(1 for e in self._completed if e.jitter_ms > self._config.max_jitter_tolerance_ms)

        mean = sum(jitters) / len(jitters)
        variance = sum((j - mean) ** 2 for j in jitters) / len(jitters)
        std_dev = variance ** 0.5

        return TimingStats(
            mean_jitter_ms=mean,
            max_jitter_ms=max(jitters),
            min_jitter_ms=min(jitters),
            std_dev_ms=std_dev,
            total_events=len(self._completed),
            missed_deadlines=missed,
        )

    def sync_to_frame(self) -> float:
        """Synchronize to the next frame time."""
        current_time = time.time()
        elapsed = current_time - self._last_frame_time

        if self._last_frame_time == 0.0:
            self._last_frame_time = current_time
            return current_time

        if elapsed >= self._frame_interval:
            self._last_frame_time = current_time
        else:
            sleep_time = self._frame_interval - elapsed
            time.sleep(sleep_time)
            self._last_frame_time = time.time()

        return self._last_frame_time

    def clear_schedule(self) -> None:
        """Clear all scheduled events."""
        self._schedule.clear()

    def clear_completed(self) -> None:
        """Clear completed event history."""
        self._completed.clear()

    def get_pending_count(self) -> int:
        """Get the number of pending scheduled events."""
        return len(self._schedule)

    def get_frame_interval(self) -> float:
        """Get the frame interval in seconds."""
        return self._frame_interval

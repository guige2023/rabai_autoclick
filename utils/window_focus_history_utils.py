"""
Window focus history utilities.

This module provides utilities for tracking and analyzing window
focus events and focus patterns over time.
"""

from __future__ import annotations

import time
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict


# Type aliases
WindowID = str


@dataclass
class FocusEvent:
    """A single window focus event."""
    window_id: WindowID
    window_title: str
    timestamp: float
    focus_duration_ms: float = 0.0
    event_type: str = "focus"  # focus, blur, activate, deactivate


@dataclass
class FocusPattern:
    """Pattern analysis of focus behavior."""
    total_focus_time_ms: float
    average_focus_duration_ms: float
    focus_count: int
    switch_count: int
    most_focused_window: WindowID
    focus_distribution: Dict[WindowID, float]
    metadata: Dict[str, Any] = field(default_factory=dict)


class WindowFocusTracker:
    """Tracks window focus events and computes patterns."""

    def __init__(self):
        self._events: List[FocusEvent] = []
        self._active_window: Optional[WindowID] = None
        self._focus_start_time: Optional[float] = None
        self._window_times: Dict[WindowID, float] = defaultdict(float)

    def focus_gained(self, window_id: WindowID, window_title: str = "") -> None:
        """
        Record a window gaining focus.

        Args:
            window_id: Unique window identifier.
            window_title: Window title.
        """
        now = time.time()

        # End previous focus
        if self._active_window is not None and self._focus_start_time is not None:
            duration = (now - self._focus_start_time) * 1000.0
            self._window_times[self._active_window] += duration

            self._events.append(FocusEvent(
                window_id=self._active_window,
                window_title="",
                timestamp=self._focus_start_time,
                focus_duration_ms=duration,
                event_type="blur",
            ))

        self._active_window = window_id
        self._focus_start_time = now

        self._events.append(FocusEvent(
            window_id=window_id,
            window_title=window_title,
            timestamp=now,
            event_type="focus",
        ))

    def focus_lost(self, window_id: WindowID) -> None:
        """
        Record a window losing focus.

        Args:
            window_id: Window that lost focus.
        """
        if self._active_window == window_id:
            now = time.time()
            if self._focus_start_time is not None:
                duration = (now - self._focus_start_time) * 1000.0
                self._window_times[window_id] += duration

            self._active_window = None
            self._focus_start_time = None

    def get_focus_pattern(self, window_seconds: Optional[float] = None) -> FocusPattern:
        """
        Compute focus pattern analysis.

        Args:
            window_seconds: Optional time window in seconds.

        Returns:
            FocusPattern with analysis results.
        """
        # Close out current focus
        now = time.time()
        if self._active_window is not None and self._focus_start_time is not None:
            duration = (now - self._focus_start_time) * 1000.0
            self._window_times[self._active_window] += duration

        # Filter by time window if specified
        events = self._events
        if window_seconds is not None:
            cutoff = now - window_seconds
            events = [e for e in events if e.timestamp >= cutoff]

        # Compute pattern
        total_time = sum(self._window_times.values())
        focus_count = sum(1 for e in events if e.event_type == "focus")

        # Count switches
        switch_count = sum(
            1 for i in range(1, len(events))
            if events[i].window_id != events[i - 1].window_id
        )

        # Most focused
        most_focused = max(self._window_times.keys(), key=lambda w: self._window_times[w]) if self._window_times else ""

        # Distribution
        distribution = {
            w: t / total_time if total_time > 0 else 0.0
            for w, t in self._window_times.items()
        }

        avg_duration = total_time / focus_count if focus_count > 0 else 0.0

        return FocusPattern(
            total_focus_time_ms=total_time,
            average_focus_duration_ms=avg_duration,
            focus_count=focus_count,
            switch_count=switch_count,
            most_focused_window=most_focused,
            focus_distribution=distribution,
        )

    def get_recent_events(self, count: int = 10) -> List[FocusEvent]:
        """Get the most recent focus events."""
        return sorted(self._events, key=lambda e: e.timestamp, reverse=True)[:count]

    def get_focus_rank(self) -> List[Tuple[WindowID, float]]:
        """
        Get windows ranked by total focus time.

        Returns:
            List of (window_id, focus_time_ms) sorted by focus time.
        """
        return sorted(self._window_times.items(), key=lambda x: x[1], reverse=True)

    def clear(self) -> None:
        """Clear all focus history."""
        self._events.clear()
        self._active_window = None
        self._focus_start_time = None
        self._window_times.clear()


def estimate_focus_transition_time(
    from_window: WindowID,
    to_window: WindowID,
    historical_data: List[Tuple[float, float]],
) -> float:
    """
    Estimate typical transition time between two windows.

    Args:
        from_window: Source window ID.
        to_window: Target window ID.
        historical_data: List of (focus_gained_time, focus_lost_time) tuples.

    Returns:
        Estimated transition time in milliseconds.
    """
    if not historical_data:
        return 100.0  # Default 100ms

    times = [t[0] - t[1] for t in historical_data if t[0] > t[1]]
    if not times:
        return 100.0

    avg = sum(times) / len(times)
    return max(10.0, min(1000.0, avg * 1000.0))

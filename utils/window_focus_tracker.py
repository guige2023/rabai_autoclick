"""
Window focus tracker for monitoring active window changes.

Tracks which windows gain/lose focus and maintains
a history of focus transitions.

Author: AutoClick Team
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class FocusEvent:
    """Represents a window focus change event."""

    window_id: str
    window_title: str
    timestamp: float
    event_type: str  # "gained" or "lost"


class WindowFocusTracker:
    """
    Tracks window focus changes over time.

    Maintains a history of focus events and supports
    callbacks for focus changes.

    Example:
        tracker = WindowFocusTracker()
        tracker.on_focus_change(lambda event: print(f"Window changed: {event.window_title}"))
        tracker.start()

        # Later...
        tracker.stop()
        history = tracker.get_history()
    """

    def __init__(self, max_history: int = 100) -> None:
        """
        Initialize tracker.

        Args:
            max_history: Maximum focus events to retain
        """
        self._history: list[FocusEvent] = []
        self._max_history = max_history
        self._current_window: str | None = None
        self._callbacks: list[Callable[[FocusEvent], None]] = []
        self._running = False

    @property
    def current_window(self) -> str | None:
        """Get the currently focused window ID."""
        return self._current_window

    @property
    def is_running(self) -> bool:
        """Check if tracker is actively monitoring."""
        return self._running

    def on_focus_change(
        self, callback: Callable[[FocusEvent], None]
    ) -> None:
        """Register a callback for focus change events."""
        self._callbacks.append(callback)

    def record_focus_change(
        self,
        window_id: str,
        window_title: str,
        event_type: str,
    ) -> None:
        """
        Record a focus change event.

        Args:
            window_id: Unique window identifier
            window_title: Human-readable window title
            event_type: "gained" or "lost"
        """
        event = FocusEvent(
            window_id=window_id,
            window_title=window_title,
            timestamp=time.time(),
            event_type=event_type,
        )

        self._history.append(event)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history :]

        self._current_window = window_id if event_type == "gained" else None

        for callback in self._callbacks:
            try:
                callback(event)
            except Exception:
                pass

    def get_history(
        self,
        since: float | None = None,
        window_id: str | None = None,
    ) -> list[FocusEvent]:
        """
        Get focus history with optional filtering.

        Args:
            since: Optional timestamp to filter events after
            window_id: Optional window ID to filter events for

        Returns:
            List of matching focus events
        """
        events = self._history

        if since is not None:
            events = [e for e in events if e.timestamp >= since]

        if window_id is not None:
            events = [e for e in events if e.window_id == window_id]

        return events

    def get_window_duration(self, window_id: str) -> float:
        """
        Calculate total time a window was focused.

        Args:
            window_id: Window to calculate duration for

        Returns:
            Total seconds the window was in focus
        """
        history = self.get_history(window_id=window_id)
        total = 0.0

        for i, event in enumerate(history):
            if event.event_type == "gained":
                end_time = (
                    history[i + 1].timestamp
                    if i + 1 < len(history) and history[i + 1].event_type == "lost"
                    else time.time()
                )
                total += end_time - event.timestamp

        return total

    def get_focus_count(self, window_id: str) -> int:
        """Get number of times a window gained focus."""
        return sum(
            1 for e in self._history if e.window_id == window_id and e.event_type == "gained"
        )

    def clear_history(self) -> None:
        """Clear all focus history."""
        self._history.clear()

    def start(self) -> None:
        """Start tracking (placeholder for polling mechanism)."""
        self._running = True

    def stop(self) -> None:
        """Stop tracking."""
        self._running = False


class FocusTransitionDetector:
    """
    Detects patterns in focus transitions.

    Identifies rapid focus switching, stuck focus,
    and other problematic patterns.
    """

    def __init__(self, threshold_seconds: float = 0.5) -> None:
        """
        Initialize detector.

        Args:
            threshold_seconds: Time below which transitions are considered "rapid"
        """
        self._threshold = threshold_seconds

    def detect_rapid_switching(
        self,
        history: list[FocusEvent],
        min_switches: int = 3,
    ) -> list[tuple[FocusEvent, FocusEvent]]:
        """
        Detect rapid focus switching patterns.

        Args:
            history: Focus event history
            min_switches: Minimum switches to flag

        Returns:
            List of (from, to) event tuples that were rapid
        """
        rapid: list[tuple[FocusEvent, FocusEvent]] = []

        for i in range(len(history) - 1):
            curr = history[i]
            next_event = history[i + 1]

            if curr.event_type == "lost" and next_event.event_type == "gained":
                if next_event.timestamp - curr.timestamp < self._threshold:
                    rapid.append((curr, next_event))

        if len(rapid) >= min_switches:
            return rapid
        return []

    def detect_stuck_focus(
        self,
        history: list[FocusEvent],
        min_duration: float = 300.0,
    ) -> list[FocusEvent]:
        """
        Detect windows that may be stuck in focus.

        Args:
            history: Focus event history
            min_duration: Minimum duration to flag as potentially stuck

        Returns:
            Focus events where window was focused for extended period
        """
        stuck: list[FocusEvent] = []
        now = time.time()

        for i, event in enumerate(history):
            if event.event_type != "gained":
                continue

            end_time = (
                history[i + 1].timestamp
                if i + 1 < len(history) and history[i + 1].event_type == "lost"
                else now
            )

            if end_time - event.timestamp >= min_duration:
                stuck.append(event)

        return stuck

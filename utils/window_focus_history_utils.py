"""
Window Focus History Utilities

Track and query window focus history for understanding user
workflow patterns and debugging focus-related issues.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional, List, Dict
from collections import deque


@dataclass
class FocusEvent:
    """A single window focus change event."""
    window_id: int
    window_title: str
    event_type: str  # 'focused', 'blurred', 'raised'
    timestamp_ms: float = field(default_factory=lambda: time.time() * 1000)


@dataclass
class FocusSession:
    """A continuous focus session for a single window."""
    window_id: int
    window_title: str
    started_at_ms: float
    ended_at_ms: Optional[float] = None
    focus_count: int = 1

    @property
    def duration_ms(self) -> float:
        end = self.ended_at_ms or time.time() * 1000
        return end - self.started_at_ms


class WindowFocusHistory:
    """
    Track window focus events over time and provide
    queries for focus patterns.
    """

    def __init__(self, max_events: int = 1000, max_sessions: int = 500):
        self.max_events = max_events
        self.max_sessions = max_sessions
        self._events: deque[FocusEvent] = deque(maxlen=max_events)
        self._sessions: deque[FocusSession] = deque(maxlen=max_sessions)
        self._current_session: Optional[FocusSession] = None
        self._last_focused_window_id: Optional[int] = None

    def record_focus(self, window_id: int, window_title: str) -> None:
        """Record a window gaining focus."""
        self._end_current_session()

        self._current_session = FocusSession(
            window_id=window_id,
            window_title=window_title,
            started_at_ms=time.time() * 1000,
        )
        self._last_focused_window_id = window_id

        event = FocusEvent(
            window_id=window_id,
            window_title=window_title,
            event_type="focused",
        )
        self._events.append(event)

    def record_blur(self, window_id: int) -> None:
        """Record a window losing focus."""
        if self._last_focused_window_id == window_id:
            self._end_current_session()
            self._last_focused_window_id = None

        # We don't know the new focused window yet
        event = FocusEvent(
            window_id=window_id,
            window_title="",
            event_type="blurred",
        )
        self._events.append(event)

    def _end_current_session(self) -> None:
        """End the current focus session."""
        if self._current_session:
            self._current_session.ended_at_ms = time.time() * 1000
            self._sessions.append(self._current_session)
            self._current_session = None

    def get_current_session(self) -> Optional[FocusSession]:
        """Get the current (ongoing) focus session."""
        return self._current_session

    def get_last_focused_window(self) -> Optional[int]:
        """Get the window ID of the last focused window."""
        return self._last_focused_window_id

    def get_sessions_for_window(self, window_id: int) -> List[FocusSession]:
        """Get all sessions for a specific window."""
        return [
            s for s in self._sessions
            if s.window_id == window_id
        ]

    def get_total_focus_time_ms(self, window_id: int) -> float:
        """Get total accumulated focus time for a window."""
        sessions = self.get_sessions_for_window(window_id)
        return sum(s.duration_ms for s in sessions)

    def get_recent_events(self, count: int = 10) -> List[FocusEvent]:
        """Get the most recent focus events."""
        return list(self._events)[-count:]

    def get_focus_frequency(self, window_id: int, window_ms: float = 60000.0) -> float:
        """
        Get focus frequency for a window within a time window.

        Args:
            window_id: Window to check.
            window_ms: Time window in milliseconds.

        Returns:
            Focus events per minute.
        """
        now = time.time() * 1000
        cutoff = now - window_ms
        recent = [e for e in self._events if e.timestamp_ms >= cutoff and e.window_id == window_id]
        return (len(recent) / window_ms) * 60000.0

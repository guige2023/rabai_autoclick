"""Data Windowing Action module.

Provides sliding and tumbling window operations for data streams.
Supports time-based and count-based windows with various
aggregation strategies.
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class WindowType(Enum):
    """Window types."""

    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


@dataclass
class WindowConfig:
    """Configuration for windowing."""

    window_type: WindowType = WindowType.TUMBLING
    size_seconds: float = 60.0
    slide_seconds: Optional[float] = None
    max_size: int = 1000


@dataclass
class Window(Generic[T]):
    """A data window."""

    window_id: str
    start_time: datetime
    end_time: datetime
    data: list[T] = field(default_factory=list)

    @property
    def size(self) -> int:
        return len(self.data)

    @property
    def duration(self) -> timedelta:
        return self.end_time - self.start_time


class TumblingWindowOperator:
    """Tumbling window operator.

    Non-overlapping windows of fixed size. Each element
    belongs to exactly one window.
    """

    def __init__(self, size_seconds: float):
        self.size_seconds = size_seconds
        self._current_window: deque[dict] = deque()
        self._window_start: Optional[datetime] = None
        self._windows: list[Window] = []

    def add(self, element: dict, timestamp: datetime) -> list[Window]:
        """Add element and return completed windows.

        Args:
            element: Data element
            timestamp: Element timestamp

        Returns:
            List of completed windows
        """
        if self._window_start is None:
            self._window_start = self._truncate_time(timestamp)

        window_end = self._window_start + timedelta(seconds=self.size_seconds)

        if timestamp >= window_end:
            completed = self._emit_window()
            self._window_start = self._truncate_time(timestamp)
            self._current_window.clear()
            completed.append(self._start_new_window(element, timestamp))
            return completed

        self._current_window.append({"element": element, "timestamp": timestamp})
        return []

    def _truncate_time(self, dt: datetime) -> datetime:
        """Truncate time to window boundary."""
        total_seconds = dt.timestamp()
        truncated = int(total_seconds / self.size_seconds) * self.size_seconds
        return datetime.fromtimestamp(truncated)

    def _emit_window(self) -> list[Window]:
        """Emit current window if not empty."""
        if not self._current_window:
            return []

        window = Window(
            window_id=f"tw_{self._window_start.timestamp()}",
            start_time=self._window_start,
            end_time=self._window_start + timedelta(seconds=self.size_seconds),
            data=[item["element"] for item in self._current_window],
        )
        self._windows.append(window)
        return [window]

    def _start_new_window(
        self,
        element: dict,
        timestamp: datetime,
    ) -> Window:
        """Start new window with element."""
        window = Window(
            window_id=f"tw_{self._window_start.timestamp()}",
            start_time=self._window_start,
            end_time=self._window_start + timedelta(seconds=self.size_seconds),
            data=[element],
        )
        self._current_window.append(
            {"element": element, "timestamp": timestamp}
        )
        return window

    def flush(self) -> list[Window]:
        """Flush any remaining elements as final window."""
        if self._current_window:
            window = Window(
                window_id=f"tw_{self._window_start.timestamp()}",
                start_time=self._window_start,
                end_time=self._window_start + timedelta(seconds=self.size_seconds),
                data=[item["element"] for item in self._current_window],
            )
            self._windows.append(window)
            self._current_window.clear()
            return [window]
        return []


class SlidingWindowOperator:
    """Sliding window operator.

    Overlapping windows that slide by a configurable step.
    Each element can belong to multiple windows.
    """

    def __init__(
        self,
        size_seconds: float,
        slide_seconds: float,
        max_windows: int = 100,
    ):
        self.size_seconds = size_seconds
        self.slide_seconds = slide_seconds
        self.max_windows = max_windows

        self._elements: deque[tuple[datetime, Any]] = deque(maxlen=max_windows * 10)
        self._window_starts: deque[datetime] = deque(maxlen=max_windows)
        self._window_id = 0

    def add(self, element: Any, timestamp: datetime) -> None:
        """Add element to sliding windows.

        Args:
            element: Data element
            timestamp: Element timestamp
        """
        self._elements.append((timestamp, element))

        cutoff = timestamp - timedelta(seconds=self.size_seconds)
        self._elements = deque(
            [(t, e) for t, e in self._elements if t >= cutoff],
            maxlen=self._elements.maxlen,
        )

        if not self._window_starts:
            self._add_window_start(timestamp)
        else:
            last_start = self._window_starts[-1]
            next_start = last_start + timedelta(seconds=self.slide_seconds)
            while next_start <= timestamp:
                self._add_window_start(next_start)
                next_start = self._window_starts[-1] + timedelta(
                    seconds=self.slide_seconds
                )

    def _add_window_start(self, start_time: datetime) -> None:
        """Add a window start time."""
        self._window_id += 1
        self._window_starts.append(start_time)

    def get_windows(self, current_time: datetime) -> list[Window]:
        """Get all active windows at current time.

        Args:
            current_time: Reference time

        Returns:
            List of active windows with their data
        """
        windows = []
        cutoff = current_time - timedelta(seconds=self.size_seconds)

        for start in self._window_starts:
            if start < cutoff:
                continue

            end = start + timedelta(seconds=self.size_seconds)
            window_data = [
                elem
                for ts, elem in self._elements
                if start <= ts < end
            ]

            windows.append(
                Window(
                    window_id=f"sw_{self._window_id}_{start.timestamp()}",
                    start_time=start,
                    end_time=end,
                    data=window_data,
                )
            )

        return windows


@dataclass
class SessionWindow:
    """Session window with gap-based closure."""

    window_id: str
    start_time: datetime
    end_time: datetime
    data: list[Any] = field(default_factory=list)
    last_activity: datetime = field(default_factory=time_func)

    def add(self, element: Any, timestamp: datetime) -> None:
        """Add element to session."""
        self.data.append(element)
        self.last_activity = timestamp
        self.end_time = timestamp


def time_func() -> datetime:
    """Get current time."""
    return datetime.now()


class SessionWindowOperator:
    """Session window operator.

    Windows are defined by periods of activity separated
    by gaps exceeding a threshold.
    """

    def __init__(self, gap_seconds: float, max_length_seconds: float = 3600):
        self.gap_seconds = gap_seconds
        self.max_length_seconds = max_length_seconds
        self._current_session: Optional[SessionWindow] = None
        self._sessions: list[SessionWindow] = []
        self._session_id = 0

    def add(self, element: Any, timestamp: datetime) -> list[SessionWindow]:
        """Add element, returning closed sessions.

        Args:
            element: Data element
            timestamp: Element timestamp

        Returns:
            List of closed sessions
        """
        closed = []

        if self._current_session is None:
            self._start_new_session(element, timestamp)
            return closed

        gap = (timestamp - self._current_session.last_activity).total_seconds()

        if gap >= self.gap_seconds:
            closed.append(self._current_session)
            self._start_new_session(element, timestamp)
        elif (
            timestamp - self._current_session.start_time
        ).total_seconds() >= self.max_length_seconds:
            closed.append(self._current_session)
            self._start_new_session(element, timestamp)
        else:
            self._current_session.add(element, timestamp)

        return closed

    def _start_new_session(self, element: Any, timestamp: datetime) -> None:
        """Start new session."""
        self._session_id += 1
        self._current_session = SessionWindow(
            window_id=f"session_{self._session_id}",
            start_time=timestamp,
            end_time=timestamp,
            last_activity=timestamp,
            data=[element],
        )

    def flush(self) -> list[SessionWindow]:
        """Flush current session as final."""
        if self._current_session:
            session = self._current_session
            self._current_session = None
            return [session]
        return []


def window_aggregate(
    window: Window,
    func: Callable[[list], Any],
) -> Any:
    """Apply aggregation function to window data.

    Args:
        window: Source window
        func: Aggregation function

    Returns:
        Aggregated result
    """
    return func(window.data)

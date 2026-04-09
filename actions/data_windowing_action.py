"""Data windowing and time-series aggregation action."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional, Sequence


class WindowType(str, Enum):
    """Type of window."""

    TUMBLING = "tumbling"  # Non-overlapping fixed size
    HOPPING = "hopping"  # Overlapping fixed size
    SLIDING = "sliding"  # Sliding with custom behavior
    SESSION = "session"  # Session-based with gap detection


@dataclass
class WindowConfig:
    """Configuration for a window."""

    window_type: WindowType
    size_seconds: float
    hop_seconds: Optional[float] = None
    session_gap_seconds: Optional[float] = None
    min_samples: int = 1


@dataclass
class Window:
    """A data window."""

    window_id: str
    start_time: datetime
    end_time: datetime
    data: list[Any]
    is_complete: bool = False


@dataclass
class WindowResult:
    """Result of windowing operation."""

    total_windows: int
    complete_windows: int
    incomplete_windows: int
    windows: list[Window]


class DataWindowingAction:
    """Creates windows from streaming or time-series data."""

    def __init__(self):
        """Initialize windowing action."""
        self._window_id_counter = 0

    def _next_window_id(self) -> str:
        """Generate next window ID."""
        self._window_id_counter += 1
        return f"window_{self._window_id_counter}"

    def create_tumbling_windows(
        self,
        data: Sequence[dict[str, Any]],
        timestamp_field: str,
        window_size_seconds: float,
    ) -> WindowResult:
        """Create tumbling (non-overlapping) windows.

        Args:
            data: Time-ordered records.
            timestamp_field: Field containing timestamp.
            window_size_seconds: Size of each window.

        Returns:
            WindowResult with all windows.
        """
        if not data:
            return WindowResult(0, 0, 0, [])

        sorted_data = sorted(data, key=lambda x: x.get(timestamp_field, 0))

        windows: list[Window] = []
        current_window: list[Any] = []
        window_start: Optional[datetime] = None
        window_end_time: Optional[datetime] = None

        for record in sorted_data:
            ts_value = record.get(timestamp_field)
            if isinstance(ts_value, datetime):
                record_time = ts_value
            elif isinstance(ts_value, (int, float)):
                record_time = datetime.fromtimestamp(ts_value)
            else:
                continue

            if window_start is None:
                window_start = record_time
                window_end_time = window_start + timedelta(seconds=window_size_seconds)

            if record_time < window_end_time:
                current_window.append(record)
            else:
                if current_window:
                    windows.append(
                        Window(
                            window_id=self._next_window_id(),
                            start_time=window_start,
                            end_time=window_end_time,
                            data=current_window,
                            is_complete=True,
                        )
                    )
                window_start = record_time
                window_end_time = window_start + timedelta(seconds=window_size_seconds)
                current_window = [record]

        if current_window:
            windows.append(
                Window(
                    window_id=self._next_window_id(),
                    start_time=window_start,
                    end_time=window_end_time,
                    data=current_window,
                    is_complete=True,
                )
            )

        return WindowResult(
            total_windows=len(windows),
            complete_windows=len(windows),
            incomplete_windows=0,
            windows=windows,
        )

    def create_hopping_windows(
        self,
        data: Sequence[dict[str, Any]],
        timestamp_field: str,
        window_size_seconds: float,
        hop_seconds: float,
    ) -> WindowResult:
        """Create hopping (overlapping) windows.

        Args:
            data: Time-ordered records.
            timestamp_field: Field containing timestamp.
            window_size_seconds: Size of each window.
            hop_seconds: Hop size (advance amount).

        Returns:
            WindowResult with all windows.
        """
        if not data:
            return WindowResult(0, 0, 0, [])

        sorted_data = sorted(data, key=lambda x: x.get(timestamp_field, 0))

        first_ts = sorted_data[0].get(timestamp_field, 0)
        if isinstance(first_ts, datetime):
            start_time = first_ts
        else:
            start_time = datetime.fromtimestamp(first_ts)

        last_ts = sorted_data[-1].get(timestamp_field, 0)
        if isinstance(last_ts, datetime):
            end_time = last_ts
        else:
            end_time = datetime.fromtimestamp(last_ts)

        windows: list[Window] = []
        current_start = start_time

        while current_start <= end_time:
            current_end = current_start + timedelta(seconds=window_size_seconds)

            window_data = [
                r
                for r in sorted_data
                if (r.get(timestamp_field) or 0)
                >= current_start.timestamp()
                and (r.get(timestamp_field) or 0) < current_end.timestamp()
            ]

            is_complete = len(window_data) >= 1
            windows.append(
                Window(
                    window_id=self._next_window_id(),
                    start_time=current_start,
                    end_time=current_end,
                    data=window_data,
                    is_complete=is_complete,
                )
            )

            current_start += timedelta(seconds=hop_seconds)

        complete = sum(1 for w in windows if w.is_complete)
        return WindowResult(
            total_windows=len(windows),
            complete_windows=complete,
            incomplete_windows=len(windows) - complete,
            windows=windows,
        )

    def create_session_windows(
        self,
        data: Sequence[dict[str, Any]],
        timestamp_field: str,
        session_gap_seconds: float,
        min_session_size: int = 1,
    ) -> WindowResult:
        """Create session-based windows.

        Args:
            data: Time-ordered records.
            timestamp_field: Field containing timestamp.
            session_gap_seconds: Gap threshold to split sessions.
            min_session_size: Minimum records per session.

        Returns:
            WindowResult with session windows.
        """
        if not data:
            return WindowResult(0, 0, 0, [])

        sorted_data = sorted(data, key=lambda x: x.get(timestamp_field, 0))

        windows: list[Window] = []
        current_session: list[Any] = []
        session_start: Optional[datetime] = None
        last_ts: Optional[datetime] = None

        for record in sorted_data:
            ts_value = record.get(timestamp_field)
            if isinstance(ts_value, datetime):
                record_time = ts_value
            elif isinstance(ts_value, (int, float)):
                record_time = datetime.fromtimestamp(ts_value)
            else:
                continue

            if last_ts is not None:
                gap = (record_time - last_ts).total_seconds()
                if gap >= session_gap_seconds and len(current_session) >= min_session_size:
                    windows.append(
                        Window(
                            window_id=self._next_window_id(),
                            start_time=session_start,
                            end_time=last_ts,
                            data=current_session,
                            is_complete=True,
                        )
                    )
                    current_session = []
                    session_start = None

            current_session.append(record)
            if session_start is None:
                session_start = record_time
            last_ts = record_time

        if len(current_session) >= min_session_size:
            windows.append(
                Window(
                    window_id=self._next_window_id(),
                    start_time=session_start,
                    end_time=last_ts,
                    data=current_session,
                    is_complete=True,
                )
            )

        complete = sum(1 for w in windows if w.is_complete)
        return WindowResult(
            total_windows=len(windows),
            complete_windows=complete,
            incomplete_windows=len(windows) - complete,
            windows=windows,
        )

    def aggregate_window(
        self,
        window: Window,
        agg_func: Callable[[list], float],
        value_field: str,
    ) -> float:
        """Aggregate values within a window."""
        values = [record.get(value_field, 0) for record in window.data]
        return agg_func(values)

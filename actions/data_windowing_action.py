"""Data windowing action for time-series data processing.

Applies windowing operations to time-series data including
tumbling, sliding, and session windows.
"""

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generator, Optional

logger = logging.getLogger(__name__)


class WindowType(Enum):
    """Types of time windows."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    COUNT = "count"


@dataclass
class Window:
    """A data window."""
    window_id: str
    start_time: float
    end_time: float
    data: list[Any]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class WindowingResult:
    """Result of windowing operation."""
    windows: list[Window]
    total_windows: int
    total_records: int
    processing_time_ms: float


@dataclass
class WindowingStats:
    """Statistics for windowing operations."""
    records_processed: int = 0
    windows_created: int = 0
    late_records: int = 0


class DataWindowingAction:
    """Apply windowing to time-series data.

    Args:
        window_type: Type of windowing.
        window_size: Size of window in seconds or count.

    Example:
        >>> windowing = DataWindowingAction(WindowType.TUMBLING, 60)
        >>> result = windowing.apply(data, time_field="timestamp")
    """

    def __init__(
        self,
        window_type: WindowType = WindowType.TUMBLING,
        window_size: float = 60.0,
    ) -> None:
        self.window_type = window_type
        self.window_size = window_size
        self._stats = WindowingStats()
        self._session_gaps: dict[str, float] = {}

    def apply(
        self,
        data: list[dict],
        time_field: str = "timestamp",
        window_size: Optional[float] = None,
    ) -> WindowingResult:
        """Apply windowing to data.

        Args:
            data: List of time-series records.
            time_field: Name of timestamp field.
            window_size: Optional override of window size.

        Returns:
            Windowing result with windows.
        """
        import time as time_module
        start_time = time_module.time()

        size = window_size or self.window_size
        windows: list[Window] = []

        if not data:
            return WindowingResult(
                windows=[],
                total_windows=0,
                total_records=0,
                processing_time_ms=0.0,
            )

        if self.window_type == WindowType.TUMBLING:
            windows = self._tumbling_windows(data, time_field, size)
        elif self.window_type == WindowType.SLIDING:
            windows = self._sliding_windows(data, time_field, size)
        elif self.window_type == WindowType.SESSION:
            windows = self._session_windows(data, time_field, size)
        elif self.window_type == WindowType.COUNT:
            windows = self._count_windows(data, int(size))

        self._stats.records_processed = len(data)
        self._stats.windows_created = len(windows)

        return WindowingResult(
            windows=windows,
            total_windows=len(windows),
            total_records=len(data),
            processing_time_ms=(time_module.time() - start_time) * 1000,
        )

    def _tumbling_windows(
        self,
        data: list[dict],
        time_field: str,
        window_size: float,
    ) -> list[Window]:
        """Create non-overlapping tumbling windows.

        Args:
            data: Time-series data.
            time_field: Timestamp field name.
            window_size: Window size in seconds.

        Returns:
            List of windows.
        """
        if not data:
            return []

        sorted_data = sorted(data, key=lambda x: self._get_timestamp(x, time_field))

        windows: list[Window] = []
        current_window: list[Any] = []
        window_id = 0

        min_ts = self._get_timestamp(sorted_data[0], time_field)
        window_start = min_ts - (min_ts % window_size)
        window_end = window_start + window_size

        for record in sorted_data:
            ts = self._get_timestamp(record, time_field)

            if ts >= window_end:
                if current_window:
                    windows.append(Window(
                        window_id=f"tumbling_{window_id}",
                        start_time=window_start,
                        end_time=window_end,
                        data=current_window.copy(),
                    ))
                    window_id += 1

                while ts >= window_end:
                    window_start = window_end
                    window_end = window_start + window_size

                current_window = []

            current_window.append(record)

        if current_window:
            windows.append(Window(
                window_id=f"tumbling_{window_id}",
                start_time=window_start,
                end_time=window_end,
                data=current_window,
            ))

        return windows

    def _sliding_windows(
        self,
        data: list[dict],
        time_field: str,
        window_size: float,
    ) -> list[Window]:
        """Create overlapping sliding windows.

        Args:
            data: Time-series data.
            time_field: Timestamp field name.
            window_size: Window size in seconds.

        Returns:
            List of windows.
        """
        if not data:
            return []

        sorted_data = sorted(data, key=lambda x: self._get_timestamp(x, time_field))

        windows: list[Window] = []
        window_id = 0
        slide_interval = window_size / 2

        for i, record in enumerate(sorted_data):
            ts = self._get_timestamp(record, time_field)
            window_start = ts
            window_end = ts + window_size

            window_data = [
                r for r in sorted_data
                if window_start <= self._get_timestamp(r, time_field) < window_end
            ]

            windows.append(Window(
                window_id=f"sliding_{window_id}",
                start_time=window_start,
                end_time=window_end,
                data=window_data,
            ))
            window_id += 1

            if i % int(slide_interval) != 0:
                pass

        return windows

    def _session_windows(
        self,
        data: list[dict],
        time_field: str,
        gap_threshold: float,
    ) -> list[Window]:
        """Create session-based windows with gap detection.

        Args:
            data: Time-series data.
            time_field: Timestamp field name.
            gap_threshold: Gap threshold in seconds.

        Returns:
            List of windows.
        """
        if not data:
            return []

        sorted_data = sorted(data, key=lambda x: self._get_timestamp(x, time_field))

        windows: list[Window] = []
        current_window: list[Any] = []
        window_id = 0
        session_start: Optional[float] = None

        for record in sorted_data:
            ts = self._get_timestamp(record, time_field)

            if not current_window:
                current_window = [record]
                session_start = ts
            else:
                last_ts = self._get_timestamp(current_window[-1], time_field)
                gap = ts - last_ts

                if gap > gap_threshold:
                    windows.append(Window(
                        window_id=f"session_{window_id}",
                        start_time=session_start,
                        end_time=last_ts,
                        data=current_window.copy(),
                    ))
                    window_id += 1
                    current_window = [record]
                    session_start = ts
                else:
                    current_window.append(record)

        if current_window and session_start is not None:
            last_ts = self._get_timestamp(current_window[-1], time_field)
            windows.append(Window(
                window_id=f"session_{window_id}",
                start_time=session_start,
                end_time=last_ts,
                data=current_window,
            ))

        return windows

    def _count_windows(
        self,
        data: list[Any],
        window_size: int,
    ) -> list[Window]:
        """Create fixed-size count-based windows.

        Args:
            data: List data.
            window_size: Number of items per window.

        Returns:
            List of windows.
        """
        if not data or window_size <= 0:
            return []

        windows: list[Window] = []
        window_id = 0

        for i in range(0, len(data), window_size):
            chunk = data[i:i + window_size]
            windows.append(Window(
                window_id=f"count_{window_id}",
                start_time=float(i),
                end_time=float(i + len(chunk)),
                data=chunk,
            ))
            window_id += 1

        return windows

    def _get_timestamp(self, record: dict, time_field: str) -> float:
        """Extract timestamp from record.

        Args:
            record: Data record.
            time_field: Field name.

        Returns:
            Timestamp value.
        """
        value = record.get(time_field, 0)

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                pass

        return time.time()

    def aggregate_window(
        self,
        window: Window,
        agg_fn: Callable[[list[Any]], Any],
    ) -> Any:
        """Aggregate data within a window.

        Args:
            window: Window to aggregate.
            agg_fn: Aggregation function.

        Returns:
            Aggregated result.
        """
        return agg_fn(window.data)

    def get_stats(self) -> WindowingStats:
        """Get windowing statistics.

        Returns:
            Current statistics.
        """
        return self._stats

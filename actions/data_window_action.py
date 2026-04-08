"""Data Window Action Module.

Provides time-series windowing with tumbling, sliding,
and session windows for streaming data analysis.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar
from collections import deque

T = TypeVar("T")


class WindowType(Enum):
    """Window type."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


@dataclass
class Window:
    """Data window."""
    window_id: int
    start_time: float
    end_time: float
    items: List[Any] = field(default_factory=list)


class DataWindowAction:
    """Time-series windowing for data streams.

    Example:
        windower = DataWindowAction(
            window_type=WindowType.TUMBLING,
            window_size_seconds=60.0
        )

        for window in windower.process(data_stream):
            result = aggregate(window.items)
    """

    def __init__(
        self,
        window_type: WindowType = WindowType.TUMBLING,
        window_size_seconds: float = 60.0,
        slide_interval_seconds: Optional[float] = None,
        session_gap_seconds: float = 5.0,
    ) -> None:
        self.window_type = window_type
        self.window_size = window_size_seconds
        self.slide_interval = slide_interval_seconds or window_size_seconds
        self.session_gap = session_gap_seconds
        self._buffers: Dict[int, deque] = {}
        self._window_id = 0
        self._last_window_end: Dict[int, float] = {}

    def process(
        self,
        items: List[Dict[str, Any]],
        timestamp_field: str = "timestamp",
    ) -> List[Window]:
        """Process items into windows.

        Args:
            items: List of items with timestamp field
            timestamp_field: Name of timestamp field

        Returns:
            List of Window objects
        """
        if not items:
            return []

        sorted_items = sorted(
            items,
            key=lambda x: x.get(timestamp_field, 0)
        )

        if self.window_type == WindowType.TUMBLING:
            return self._process_tumbling(sorted_items, timestamp_field)
        elif self.window_type == WindowType.SLIDING:
            return self._process_sliding(sorted_items, timestamp_field)
        elif self.window_type == WindowType.SESSION:
            return self._process_session(sorted_items, timestamp_field)

        return []

    def _process_tumbling(
        self,
        items: List[Dict],
        timestamp_field: str,
    ) -> List[Window]:
        """Process with tumbling windows."""
        windows: List[Window] = []

        if not items:
            return windows

        start_ts = items[0].get(timestamp_field, time.time())
        window_end = start_ts + self.window_size

        current_window = Window(
            window_id=self._window_id,
            start_time=start_ts,
            end_time=window_end,
        )

        for item in items:
            ts = item.get(timestamp_field, time.time())

            if ts >= window_end:
                if current_window.items:
                    windows.append(current_window)

                self._window_id += 1
                start_ts = ts
                window_end = start_ts + self.window_size
                current_window = Window(
                    window_id=self._window_id,
                    start_time=start_ts,
                    end_time=window_end,
                )

            current_window.items.append(item)

        if current_window.items:
            windows.append(current_window)

        return windows

    def _process_sliding(
        self,
        items: List[Dict],
        timestamp_field: str,
    ) -> List[Window]:
        """Process with sliding windows."""
        windows: List[Window] = []

        if not items:
            return windows

        start_ts = items[0].get(timestamp_field, time.time())
        window_end = start_ts + self.window_size

        window_start = start_ts
        step = self.slide_interval

        while window_start <= items[-1].get(timestamp_field, time.time()):
            window_items = [
                item for item in items
                if window_start <= item.get(timestamp_field, 0) < window_end
            ]

            if window_items:
                windows.append(Window(
                    window_id=self._window_id,
                    start_time=window_start,
                    end_time=window_end,
                    items=window_items,
                ))
                self._window_id += 1

            window_start += step
            window_end = window_start + self.window_size

        return windows

    def _process_session(
        self,
        items: List[Dict],
        timestamp_field: str,
    ) -> List[Window]:
        """Process with session windows."""
        windows: List[Window] = []

        if not items:
            return windows

        current_window = Window(
            window_id=self._window_id,
            start_time=items[0].get(timestamp_field, time.time()),
            end_time=0,
        )
        self._window_id += 1
        last_ts = current_window.start_time

        for item in items:
            ts = item.get(timestamp_field, time.time())

            if ts - last_ts > self.session_gap:
                if current_window.items:
                    current_window.end_time = last_ts
                    windows.append(current_window)

                current_window = Window(
                    window_id=self._window_id,
                    start_time=ts,
                    end_time=0,
                )
                self._window_id += 1

            current_window.items.append(item)
            last_ts = ts

        if current_window.items:
            current_window.end_time = last_ts
            windows.append(current_window)

        return windows

    def aggregate_windows(
        self,
        windows: List[Window],
        aggregator: Callable[[List], Any],
    ) -> List[Dict[str, Any]]:
        """Aggregate items within each window.

        Args:
            windows: List of windows
            aggregator: Function to aggregate items

        Returns:
            List of aggregated results
        """
        results = []

        for window in windows:
            if window.items:
                result = aggregator(window.items)
                results.append({
                    "window_id": window.window_id,
                    "start_time": window.start_time,
                    "end_time": window.end_time,
                    "item_count": len(window.items),
                    "result": result,
                })

        return results

    def window_stats(self, windows: List[Window]) -> Dict[str, Any]:
        """Get statistics about windows."""
        if not windows:
            return {"total_windows": 0}

        item_counts = [len(w.items) for w in windows]

        return {
            "total_windows": len(windows),
            "total_items": sum(item_counts),
            "avg_items_per_window": sum(item_counts) / len(windows),
            "min_items": min(item_counts),
            "max_items": max(item_counts),
        }

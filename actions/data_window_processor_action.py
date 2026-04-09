"""Data Window Processor Action Module.

Provides sliding and tumbling window processing for time series
and streaming data with configurable window sizes and aggregation.
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class WindowType(Enum):
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    COUNT_BASED = "count_based"


class WindowEmitType(Enum):
    WHEN_AVAILABLE = "when_available"
    WHEN_WINDOW_FULL = "when_window_full"
    ON_TRIGGER = "on_trigger"


@dataclass
class Window:
    window_id: str
    start_time: datetime
    end_time: datetime
    data: List[Any] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def size(self) -> int:
        return len(self.data)

    @property
    def duration(self) -> timedelta:
        return self.end_time - self.start_time

    def is_empty(self) -> bool:
        return len(self.data) == 0


@dataclass
class WindowConfig:
    window_type: WindowType = WindowType.TUMBLING
    size_seconds: float = 60.0
    slide_seconds: Optional[float] = None
    max_size: int = 10000
    emit_type: WindowEmitType = WindowEmitType.WHEN_WINDOW_FULL
    late_arrival_grace: float = 0.0


@dataclass
class WindowResult:
    window_id: str
    data: List[Any]
    start_time: datetime
    end_time: datetime
    duration_ms: float
    watermark: Optional[datetime] = None


class WindowProcessor:
    def __init__(
        self,
        config: Optional[WindowConfig] = None,
        aggregator: Optional[Callable[[List[Any]], Any]] = None,
    ):
        self.config = config or WindowConfig()
        self.aggregator = aggregator
        self._windows: Dict[str, Window] = {}
        self._pending_windows: deque = deque()
        self._current_time: Optional[datetime] = None
        self._results: List[WindowResult] = []

    def process(self, data: Any, timestamp: Optional[datetime] = None) -> List[WindowResult]:
        if timestamp is None:
            timestamp = datetime.now()

        self._current_time = timestamp
        results = []

        if self.config.window_type == WindowType.TUMBLING:
            results = self._process_tumbling(data, timestamp)
        elif self.config.window_type == WindowType.SLIDING:
            results = self._process_sliding(data, timestamp)
        elif self.config.window_type == WindowType.COUNT_BASED:
            results = self._process_count_based(data, timestamp)

        self._results.extend(results)
        return results

    def _process_tumbling(self, data: Any, timestamp: datetime) -> List[WindowResult]:
        results = []
        window_size = timedelta(seconds=self.config.size_seconds)
        window_id = self._get_tumbling_window_id(timestamp)

        if window_id not in self._windows:
            self._windows[window_id] = Window(
                window_id=window_id,
                start_time=timestamp - (timestamp - datetime.min.replace(tzinfo=timestamp.tzinfo)) % window_size,
                end_time=timestamp - (timestamp - datetime.min.replace(tzinfo=timestamp.tzinfo)) % window_size + window_size,
            )

        window = self._windows[window_id]
        window.data.append(data)

        if self._should_emit(window):
            result = self._emit_window(window)
            results.append(result)
            del self._windows[window_id]

        return results

    def _process_sliding(self, data: Any, timestamp: datetime) -> List[WindowResult]:
        results = []
        window_size = timedelta(seconds=self.config.size_seconds)
        slide_size = timedelta(seconds=self.config.slide_seconds or self.config.size_seconds)

        current_window_start = timestamp - window_size
        window_id = self._get_sliding_window_id(current_window_start)

        if window_id not in self._windows:
            self._windows[window_id] = Window(
                window_id=window_id,
                start_time=current_window_start,
                end_time=timestamp,
            )

        window = self._windows[window_id]
        window.data.append(data)
        window.end_time = timestamp

        expired_windows = []
        for wid, w in self._windows.items():
            if w.end_time < timestamp - window_size - timedelta(seconds=self.config.late_arrival_grace):
                expired_windows.append(wid)

        for wid in expired_windows:
            if self._should_emit(self._windows[wid]):
                result = self._emit_window(self._windows[wid])
                results.append(result)
            del self._windows[wid]

        return results

    def _process_count_based(self, data: Any, timestamp: datetime) -> List[WindowResult]:
        results = []
        window_id = f"count_window_{len(self._pending_windows) // self.config.max_size}"

        if not hasattr(self, '_count_window'):
            self._count_window = Window(
                window_id=window_id,
                start_time=timestamp,
                end_time=timestamp,
            )

        self._count_window.data.append(data)
        self._count_window.end_time = timestamp

        if len(self._count_window.data) >= self.config.max_size:
            result = self._emit_window(self._count_window)
            results.append(result)
            self._count_window = Window(
                window_id=f"count_window_{len(self._pending_windows) // self.config.max_size + 1}",
                start_time=timestamp,
                end_time=timestamp,
            )

        return results

    def _should_emit(self, window: Window) -> bool:
        if self.config.emit_type == WindowEmitType.WHEN_WINDOW_FULL:
            return window.size >= self.config.max_size
        elif self.config.emit_type == WindowEmitType.WHEN_AVAILABLE:
            return window.size > 0
        return False

    def _emit_window(self, window: Window) -> WindowResult:
        import time
        start = time.time()

        data = window.data
        if self.aggregator:
            data = self.aggregator(window.data)

        return WindowResult(
            window_id=window.window_id,
            data=data if isinstance(data, list) else [data],
            start_time=window.start_time,
            end_time=window.end_time,
            duration_ms=(time.time() - start) * 1000,
        )

    def _get_tumbling_window_id(self, timestamp: datetime) -> str:
        window_size = timedelta(seconds=self.config.size_seconds)
        start = timestamp - (timestamp - datetime.min.replace(tzinfo=timestamp.tzinfo)) % window_size
        return f"tumbling_{start.isoformat()}"

    def _get_sliding_window_id(self, start: datetime) -> str:
        return f"sliding_{start.isoformat()}"

    def close(self) -> List[WindowResult]:
        results = []
        for window in self._windows.values():
            if window.size > 0:
                result = self._emit_window(window)
                results.append(result)

        self._windows.clear()
        return results

    def get_results(self) -> List[WindowResult]:
        return self._results


def create_sliding_window(
    size_seconds: float,
    slide_seconds: Optional[float] = None,
) -> WindowProcessor:
    config = WindowConfig(
        window_type=WindowType.SLIDING,
        size_seconds=size_seconds,
        slide_seconds=slide_seconds,
    )
    return WindowProcessor(config)


def create_tumbling_window(
    size_seconds: float,
) -> WindowProcessor:
    config = WindowConfig(
        window_type=WindowType.TUMBLING,
        size_seconds=size_seconds,
    )
    return WindowProcessor(config)

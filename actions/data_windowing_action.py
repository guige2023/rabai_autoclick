"""Data Windowing Action.

Windows data streams into time or count-based windows.
"""
from typing import Any, Callable, Dict, Iterator, List, Optional, TypeVar
from dataclasses import dataclass, field
from enum import Enum
import time


class WindowType(Enum):
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


@dataclass
class Window:
    window_id: str
    window_type: WindowType
    start_time: float
    end_time: float
    items: List[Any] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataWindowingAction:
    """Windows data streams into time or count-based windows."""

    def __init__(
        self,
        window_type: WindowType = WindowType.TUMBLING,
        window_size_sec: float = 60.0,
        slide_interval_sec: Optional[float] = None,
    ) -> None:
        self.window_type = window_type
        self.window_size_sec = window_size_sec
        self.slide_interval_sec = slide_interval_sec or window_size_sec
        self._buffer: List[tuple[float, Any]] = []
        self._windows: List[Window] = []

    def add(self, item: Any, timestamp: Optional[float] = None) -> List[Window]:
        ts = timestamp or time.time()
        self._buffer.append((ts, item))
        return self._emit_windows()

    def _emit_windows(self) -> List[Window]:
        now = time.time()
        cutoff = now - self.window_size_sec * 3
        self._buffer = [(ts, item) for ts, item in self._buffer if ts >= cutoff]
        if self.window_type == WindowType.TUMBLING:
            return self._emit_tumbling_windows(now)
        elif self.window_type == WindowType.SLIDING:
            return self._emit_sliding_windows(now)
        return []

    def _emit_tumbling_windows(self, now: float) -> List[Window]:
        windows = []
        for i in range(3):
            window_end = now - i * self.slide_interval_sec
            window_start = window_end - self.window_size_sec
            window_items = [
                item for ts, item in self._buffer
                if window_start <= ts < window_end
            ]
            if window_items:
                w = Window(
                    window_id=f"w_{int(window_start)}_{int(window_end)}",
                    window_type=self.window_type,
                    start_time=window_start,
                    end_time=window_end,
                    items=window_items,
                )
                windows.append(w)
                self._windows.append(w)
        return windows

    def _emit_sliding_windows(self, now: float) -> List[Window]:
        return self._emit_tumbling_windows(now)

    def get_active_windows(self) -> List[Window]:
        now = time.time()
        return [w for w in self._windows if w.end_time > now - self.window_size_sec]

    def clear_old_windows(self, max_age_sec: float = 3600.0) -> int:
        cutoff = time.time() - max_age_sec
        before = len(self._windows)
        self._windows = [w for w in self._windows if w.start_time >= cutoff]
        return before - len(self._windows)

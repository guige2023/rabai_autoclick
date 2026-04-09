"""
Data Windowing Action Module.

Provides time-based and count-based windowing
with sliding and tumbling window support.
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar, Optional

T = TypeVar("T")


class WindowType(Enum):
    """Window types."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    COUNT = "count"


@dataclass
class WindowConfig:
    """Window configuration."""
    window_type: WindowType = WindowType.TUMBLING
    size: float = 60.0
    slide: float = 60.0
    max_size: int = 1000
    timeout: float = 30.0


@dataclass
class Window(Generic[T]):
    """Data window."""
    id: str
    start_time: float
    end_time: float
    data: list[T] = field(default_factory=list)
    is_closed: bool = False

    @property
    def size(self) -> int:
        return len(self.data)

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time


class TumblingWindowManager:
    """Tumbling window manager."""

    def __init__(self, size: float):
        self.size = size
        self._current_window: Optional[Window] = None
        self._closed_windows: list[Window] = []

    def add(self, item: T, timestamp: float) -> list[Window]:
        """Add item to window."""
        closed = []

        if self._current_window is None:
            self._current_window = Window(
                id=str(timestamp),
                start_time=timestamp,
                end_time=timestamp + self.size
            )

        if timestamp >= self._current_window.end_time:
            self._current_window.is_closed = True
            closed.append(self._current_window)
            self._closed_windows.append(self._current_window)

            self._current_window = Window(
                id=str(timestamp),
                start_time=timestamp,
                end_time=timestamp + self.size
            )

        self._current_window.data.append(item)
        return closed


class SlidingWindowManager:
    """Sliding window manager."""

    def __init__(self, size: float, slide: float):
        self.size = size
        self.slide = slide
        self._windows: list[Window] = []
        self._last_slide_time: float = 0

    def add(self, item: T, timestamp: float) -> list[Window]:
        """Add item to sliding window."""
        closed = []

        if not self._windows or timestamp >= self._windows[0].end_time:
            window = Window(
                id=str(timestamp),
                start_time=timestamp,
                end_time=timestamp + self.size
            )
            window.data.append(item)
            self._windows.append(window)
            self._last_slide_time = timestamp

            old_windows = [w for w in self._windows if w.end_time <= timestamp]
            for w in old_windows:
                w.is_closed = True
                closed.append(w)

            self._windows = [w for w in self._windows if not w.is_closed]

        else:
            self._windows[-1].data.append(item)

        return closed


class CountWindowManager:
    """Count-based window manager."""

    def __init__(self, size: int):
        self.size = size
        self._current_window: Optional[Window] = None
        self._closed_windows: list[Window] = []

    def add(self, item: T, timestamp: float) -> list[Window]:
        """Add item to window."""
        closed = []

        if self._current_window is None:
            self._current_window = Window(
                id=str(timestamp),
                start_time=timestamp,
                end_time=timestamp
            )

        self._current_window.data.append(item)

        if len(self._current_window.data) >= self.size:
            self._current_window.is_closed = True
            closed.append(self._current_window)
            self._closed_windows.append(self._current_window)
            self._current_window = None

        return closed


class DataWindowingAction:
    """
    Time and count-based windowing.

    Example:
        window = DataWindowingAction(
            window_type=WindowType.SLIDING,
            size=60.0,
            slide=10.0
        )

        window.add(data_point, timestamp)
        closed = window.get_closed_windows()
    """

    def __init__(
        self,
        window_type: WindowType = WindowType.TUMBLING,
        size: float = 60.0,
        slide: float = 60.0
    ):
        self.config = WindowConfig(
            window_type=window_type,
            size=size,
            slide=slide
        )

        if window_type == WindowType.TUMBLING:
            self._manager = TumblingWindowManager(size)
        elif window_type == WindowType.SLIDING:
            self._manager = SlidingWindowManager(size, slide)
        elif window_type == WindowType.COUNT:
            self._manager = CountWindowManager(int(size))
        else:
            self._manager = TumblingWindowManager(size)

    def add(self, item: T, timestamp: Optional[float] = None) -> list[Window[T]]:
        """Add item to window."""
        if timestamp is None:
            timestamp = time.time()
        return self._manager.add(item, timestamp)

    def get_current_window(self) -> Optional[Window[T]]:
        """Get current active window."""
        if hasattr(self._manager, '_current_window'):
            return self._manager._current_window
        if hasattr(self._manager, '_windows') and self._manager._windows:
            return self._manager._windows[-1]
        return None

    def get_closed_windows(self) -> list[Window[T]]:
        """Get closed windows."""
        if hasattr(self._manager, '_closed_windows'):
            return self._manager._closed_windows.copy()
        return []

    def get_all_windows(self) -> list[Window[T]]:
        """Get all windows."""
        if hasattr(self._manager, '_closed_windows'):
            closed = self._manager._closed_windows.copy()
            current = self._manager._current_window
            if current:
                return closed + [current]
            return closed
        return []

    def clear(self) -> None:
        """Clear all windows."""
        if hasattr(self._manager, '_closed_windows'):
            self._manager._closed_windows.clear()
        if hasattr(self._manager, '_current_window'):
            self._manager._current_window = None
        if hasattr(self._manager, '_windows'):
            self._manager._windows.clear()

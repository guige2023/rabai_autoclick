"""
Data Window Action Module.

Time-based and count-based windowing for streaming data.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Deque, Generic, List, Optional, TypeVar


T = TypeVar("T")


class WindowType(Enum):
    """Window types."""
    TUMBLING = auto()
    SLIDING = auto()
    SESSION = auto()


@dataclass
class Window(Generic[T]):
    """A data window."""
    window_id: str
    start_time: float
    end_time: Optional[float]
    items: List[T] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)


@dataclass
class WindowConfig:
    """Configuration for windowing."""
    window_type: WindowType = WindowType.TUMBLING
    size_seconds: float = 60.0
    slide_seconds: float = 60.0
    max_size: int = 1000
    session_gap_seconds: float = 30.0


class DataWindowAction(Generic[T]):
    """
    Windowing for streaming data.

    Supports tumbling, sliding, and session windows.
    """

    def __init__(
        self,
        config: Optional[WindowConfig] = None,
    ) -> None:
        self.config = config or WindowConfig()
        self._windows: Deque[Window[T]] = deque(maxlen=100)
        self._current_window: Optional[Window[T]] = None
        self._buffer: List[T] = []
        self._window_counter = 0

    def add(self, item: T) -> Optional[Window[T]]:
        """
        Add item to window.

        Args:
            item: Item to add

        Returns:
            Window if a window is complete, None otherwise
        """
        now = time.time()

        if self._current_window is None:
            self._current_window = self._create_window(now)

        self._buffer.append(item)

        if self.config.window_type == WindowType.TUMBLING:
            return self._check_tumbling_window(now)
        elif self.config.window_type == WindowType.SLIDING:
            return self._check_sliding_window(now)
        elif self.config.window_type == WindowType.SESSION:
            return self._check_session_window(now)

        return None

    def _create_window(self, start_time: float) -> Window[T]:
        """Create a new window."""
        self._window_counter += 1
        return Window[T](
            window_id=f"window_{self._window_counter}",
            start_time=start_time,
            end_time=None,
            items=[],
            metadata={},
        )

    def _check_tumbling_window(self, now: float) -> Optional[Window[T]]:
        """Check if tumbling window is complete."""
        if self._current_window is None:
            return None

        window_age = now - self._current_window.start_time

        if window_age >= self.config.size_seconds:
            self._current_window.items = list(self._buffer)
            self._current_window.end_time = now
            completed = self._current_window
            self._windows.append(completed)
            self._current_window = self._create_window(now)
            self._buffer.clear()
            return completed

        return None

    def _check_sliding_window(self, now: float) -> Optional[Window[T]]:
        """Check if sliding window is complete."""
        if self._current_window is None:
            return None

        window_age = now - self._current_window.start_time

        if window_age >= self.config.slide_seconds:
            self._current_window.items = list(self._buffer)
            self._current_window.end_time = now
            completed = self._current_window
            self._windows.append(completed)
            self._current_window = self._create_window(now)
            self._buffer.clear()
            return completed

        return None

    def _check_session_window(self, now: float) -> Optional[Window[T]]:
        """Check if session window should close."""
        if not self._buffer:
            return None

        if self._current_window is None:
            return None

        last_item_time = self._current_window.start_time
        gap = now - last_item_time

        if gap >= self.config.session_gap_seconds and len(self._buffer) > 0:
            self._current_window.items = list(self._buffer)
            self._current_window.end_time = now
            completed = self._current_window
            self._windows.append(completed)
            self._current_window = self._create_window(now)
            self._buffer.clear()
            return completed

        return None

    def get_current_window(self) -> Optional[Window[T]]:
        """Get current in-progress window."""
        if self._current_window and self._buffer:
            return Window[T](
                window_id=self._current_window.window_id,
                start_time=self._current_window.start_time,
                end_time=None,
                items=list(self._buffer),
                metadata=self._current_window.metadata.copy(),
            )
        return None

    def get_windows(self) -> List[Window[T]]:
        """Get all completed windows."""
        return list(self._windows)

    def get_window_by_id(self, window_id: str) -> Optional[Window[T]]:
        """Get a specific window by ID."""
        for window in self._windows:
            if window.window_id == window_id:
                return window
        return None

    def aggregate(
        self,
        window: Window[T],
        func: Callable[[List[T]], Any],
    ) -> Any:
        """
        Aggregate items in a window.

        Args:
            window: Window to aggregate
            func: Aggregation function

        Returns:
            Aggregation result
        """
        return func(window.items)

    def clear(self) -> None:
        """Clear all windows and buffers."""
        self._windows.clear()
        self._current_window = None
        self._buffer.clear()
        self._window_counter = 0

    def get_stats(self) -> dict:
        """Get window statistics."""
        return {
            "total_windows": len(self._windows),
            "current_window_items": len(self._buffer),
            "window_type": self.config.window_type.name,
            "size_seconds": self.config.size_seconds,
            "slide_seconds": self.config.slide_seconds,
        }

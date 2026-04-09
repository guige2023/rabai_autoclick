"""
Data Stream Action Module.

Stream processing utilities for automation including filtering,
transformation, windowing, and aggregation over data streams.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Deque, Dict, Generic, List, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")
T_out = TypeVar("T_out")


class WindowType(Enum):
    """Window types for stream processing."""
    TUMBLING = "tumbling"    # Fixed-size, non-overlapping
    SLIDING = "sliding"      # Fixed-size, overlapping
    SESSION = "session"      # Session-based windows


@dataclass
class Window:
    """A data window in a stream."""
    window_id: str
    window_type: WindowType
    start_time: float
    end_time: float
    items: List[Any] = field(default_factory=list)

    @property
    def size(self) -> int:
        """Number of items in window."""
        return len(self.items)

    @property
    def duration_seconds(self) -> float:
        """Duration of the window in seconds."""
        return self.end_time - self.start_time


@dataclass
class StreamConfig:
    """Configuration for stream processing."""
    name: str
    buffer_size: int = 1000
    window_size_seconds: float = 60.0
    window_type: WindowType = WindowType.TUMBLING
    slide_seconds: float = 10.0


class StreamWindowManager(Generic[T]):
    """Manages windows for stream data."""

    def __init__(
        self,
        window_size_seconds: float,
        window_type: WindowType = WindowType.TUMBLING,
        slide_seconds: float = 10.0,
    ) -> None:
        self.window_size_seconds = window_size_seconds
        self.window_type = window_type
        self.slide_seconds = slide_seconds
        self._windows: Deque[Window] = deque(maxlen=1000)
        self._current_window_start: Optional[float] = None

    def add_item(self, item: T, timestamp: float) -> List[Window]:
        """Add an item to the stream, creating/closing windows as needed."""
        new_windows: List[Window] = []

        # Initialize first window
        if self._current_window_start is None:
            self._current_window_start = timestamp

        if self.window_type == WindowType.TUMBLING:
            # Tumbling: close window when full
            window_end = self._current_window_start + self.window_size_seconds
            if timestamp >= window_end:
                # Close current window
                window = Window(
                    window_id=f"w-{int(self._current_window_start)}",
                    window_type=self.window_type,
                    start_time=self._current_window_start,
                    end_time=window_end,
                )
                self._windows.append(window)
                new_windows.append(window)
                self._current_window_start = timestamp

            # Start new window if needed
            if self._current_window_start == timestamp or not self._windows:
                if not new_windows:
                    self._current_window_start = timestamp

        elif self.window_type == WindowType.SLIDING:
            # Sliding: create windows at slide intervals
            while self._current_window_start + self.window_size_seconds <= timestamp:
                window = Window(
                    window_id=f"w-{int(self._current_window_start)}",
                    window_type=self.window_type,
                    start_time=self._current_window_start,
                    end_time=self._current_window_start + self.window_size_seconds,
                )
                self._windows.append(window)
                new_windows.append(window)
                self._current_window_start += self.slide_seconds

        return new_windows

    def get_active_window(self) -> Optional[Window]:
        """Get the most recent (active) window."""
        return self._windows[-1] if self._windows else None

    def get_windows(self) -> List[Window]:
        """Get all windows."""
        return list(self._windows)


class StreamProcessor(Generic[T]):
    """
    Processes data streams with filtering, transformation, and aggregation.

    Example:
        processor = StreamProcessor[str](name="events")

        processor.add_filter(lambda x: x.startswith("error"))
        processor.add_transform(lambda x: x.upper())
        processor.add_aggregate(lambda items: len(items), window_size_seconds=60)

        async for result in processor.stream(source):
            print(f"60s count: {result}")
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._filters: List[Callable[[T], bool]] = []
        self._transformers: List[Callable[[T], Any]] = []
        self._aggregates: Dict[str, Callable[[List[T]], Any]] = {}
        self._buffer: Deque[T] = deque(maxlen=10000)

    def add_filter(self, fn: Callable[[T], bool]) -> "StreamProcessor[T]":
        """Add a filter function to the pipeline."""
        self._filters.append(fn)
        return self

    def add_transform(
        self,
        fn: Callable[[T], T_out],
    ) -> "StreamProcessor[T_out]":
        """Add a transformation function to the pipeline."""
        # Return a new processor with the correct type
        new_proc = StreamProcessor[T_out](self.name)
        new_proc._filters = self._filters.copy()
        new_proc._transformers = self._transformers.copy() + [fn]
        new_proc._aggregates = self._aggregates.copy()
        return new_proc

    def add_aggregate(
        self,
        name: str,
        fn: Callable[[List[T]], Any],
    ) -> "StreamProcessor[T]":
        """Add an aggregation function."""
        self._aggregates[name] = fn
        return self

    def process(self, item: T) -> Optional[Any]:
        """Process a single item through the pipeline."""
        # Apply filters
        for f in self._filters:
            if not f(item):
                return None

        # Apply transformers
        result = item
        for t in self._transformers:
            result = t(result)

        self._buffer.append(result)
        return result

    def get_aggregate_results(self) -> Dict[str, Any]:
        """Get results of all aggregations."""
        items = list(self._buffer)
        return {name: fn(items) for name, fn in self._aggregates.items()}

    def clear(self) -> None:
        """Clear the buffer."""
        self._buffer.clear()


class DataStreamAction:
    """
    Unified stream processing action for automation.

    Provides stream creation, window management, and async iteration
    over data streams with full pipeline support.

    Example:
        stream = DataStreamAction(name="metrics")

        async for window in stream.tumbling_window(data_source, size_seconds=60):
            avg = sum(window.items) / len(window.items)
            print(f"60s average: {avg}")
    """

    def __init__(self, name: str = "stream") -> None:
        self.name = name
        self._processors: Dict[str, StreamProcessor] = {}

    def create_processor(
        self,
        name: str,
    ) -> StreamProcessor:
        """Create a named stream processor."""
        proc = StreamProcessor(name)
        self._processors[name] = proc
        return proc

    def get_processor(self, name: str) -> Optional[StreamProcessor]:
        """Get a named processor."""
        return self._processors.get(name)

    async def tumbling_window(
        self,
        source: Callable[[], Any],
        size_seconds: float = 60.0,
        max_items: int = 10000,
    ) -> Any:
        """
        Generate tumbling windows over a data source.

        A tumbling window has fixed size and does not overlap.
        """
        manager = StreamWindowManager(
            window_size_seconds=size_seconds,
            window_type=WindowType.TUMBLING,
        )
        buffer: Deque[Any] = deque(maxlen=max_items)
        import time

        while True:
            try:
                item = source()
                timestamp = time.time()
                manager.add_item(item, timestamp)
                buffer.append(item)

                active = manager.get_active_window()
                if active:
                    yield active

            except Exception as e:
                logger.error(f"Stream error: {e}")
                break

            await asyncio.sleep(0.01)

    async def sliding_window(
        self,
        data: List[T],
        size_seconds: float = 60.0,
        slide_seconds: float = 10.0,
    ) -> Any:
        """
        Generate sliding windows over a data list.
        """
        manager = StreamWindowManager(
            window_size_seconds=size_seconds,
            window_type=WindowType.SLIDING,
            slide_seconds=slide_seconds,
        )
        import time

        for item in data:
            manager.add_item(item, time.time())
            active = manager.get_active_window()
            if active:
                yield active

    async def windowed_aggregate(
        self,
        data: List[T],
        size_seconds: float,
        agg_fn: Callable[[List[T]], Any],
    ) -> Dict[str, Any]:
        """Aggregate data over time-based windows."""
        manager = StreamWindowManager(
            window_size_seconds=size_seconds,
            window_type=WindowType.TUMBLING,
        )
        results = {}
        import time

        for item in data:
            manager.add_item(item, time.time())

        for window in manager.get_windows():
            window_items = [i for i in data if window.start_time <= i]
            results[window.window_id] = agg_fn(window_items)

        return results

    def count_stream(
        self,
        data: List[T],
        key_fn: Callable[[T], str],
    ) -> Dict[str, int]:
        """Count occurrences by key in a stream."""
        counts: Dict[str, int] = {}
        for item in data:
            key = key_fn(item)
            counts[key] = counts.get(key, 0) + 1
        return counts

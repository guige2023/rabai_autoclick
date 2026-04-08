"""
Data Stream Action Module.

Stream processing for continuous data with
windowing, aggregation, and flow control.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Generic, TypeVar
from dataclasses import dataclass, field
import logging
import asyncio
import time
from collections import deque

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


@dataclass
class Window:
    """Time or count-based window."""
    name: str
    size: int
    slide: int = 0
    window_type: str = "tumbling"
    start_time: float = field(default_factory=time.time)


@dataclass
class StreamStats:
    """Stream processing statistics."""
    total_processed: int
    total_dropped: int
    current_window_size: int
    avg_processing_time_ms: float


class DataStreamAction(Generic[T]):
    """
    Stream processing with windowing support.

    Processes continuous data streams with
    tumbling/sliding windows and aggregations.

    Example:
        stream = DataStreamAction[int]()
        stream.window("1min", size=60, slide=60)
        stream.aggregate(sum, "1min")
        stream.process([1, 2, 3, 4, 5])
    """

    def __init__(
        self,
        max_buffer: int = 10000,
        backpressure_threshold: int = 5000,
    ) -> None:
        self.max_buffer = max_buffer
        self.backpressure_threshold = backpressure_threshold
        self._buffer: deque[T] = deque(maxlen=max_buffer)
        self._windows: dict[str, Window] = {}
        self._aggregations: dict[str, Callable[[list], Any]] = {}
        self._handlers: list[Callable[[Any], None]] = []
        self._stats = StreamStats(
            total_processed=0,
            total_dropped=0,
            current_window_size=0,
            avg_processing_time_ms=0.0,
        )
        self._total_time_ms = 0.0
        self._lock = asyncio.Lock()

    def window(
        self,
        name: str,
        size: int,
        slide: int = 0,
        window_type: str = "tumbling",
    ) -> "DataStreamAction":
        """Define a processing window."""
        self._windows[name] = Window(
            name=name,
            size=size,
            slide=slide or size,
            window_type=window_type,
        )
        return self

    def aggregate(
        self,
        func: Callable[[list], R],
        window_name: str,
    ) -> "DataStreamAction":
        """Register an aggregation function for a window."""
        self._aggregations[window_name] = func
        return self

    def on_data(self, handler: Callable[[Any], None]) -> "DataStreamAction":
        """Register a data handler."""
        self._handlers.append(handler)
        return self

    async def process(self, data: T) -> None:
        """Process a single data element."""
        async with self._lock:
            if len(self._buffer) >= self.backpressure_threshold:
                logger.warning("Backpressure: buffer full, dropping oldest")
                self._buffer.popleft()
                self._stats.total_dropped += 1

            self._buffer.append(data)
            self._stats.current_window_size = len(self._buffer)
            self._stats.total_processed += 1

            await self._trigger_handlers(data)

    def process_batch(self, data: list[T]) -> None:
        """Process a batch of data."""
        for item in data:
            if len(self._buffer) >= self.max_buffer:
                self._buffer.popleft()
                self._stats.total_dropped += 1

            self._buffer.append(item)
            self._stats.total_processed += 1

        self._stats.current_window_size = len(self._buffer)

    async def _trigger_handlers(self, data: Any) -> None:
        """Trigger registered handlers."""
        for handler in self._handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(data)
                else:
                    handler(data)
            except Exception as e:
                logger.error("Handler error: %s", e)

    def get_window_data(
        self,
        window_name: str,
    ) -> list[T]:
        """Get current window's data."""
        if window_name not in self._windows:
            return list(self._buffer)

        window = self._windows[window_name]

        if window.window_type == "tumbling":
            cutoff = time.time() - window.size
        else:
            cutoff = time.time() - window.slide

        items = list(self._buffer)

        return items

    def aggregate_window(
        self,
        window_name: str,
    ) -> Optional[Any]:
        """Aggregate current window data."""
        if window_name not in self._aggregations:
            return None

        data = self.get_window_data(window_name)
        if not data:
            return None

        agg_func = self._aggregations[window_name]
        return agg_func(data)

    def get_stats(self) -> StreamStats:
        """Get stream processing statistics."""
        self._stats.current_window_size = len(self._buffer)

        if self._stats.total_processed > 0:
            self._stats.avg_processing_time_ms = (
                self._total_time_ms / self._stats.total_processed
            )

        return self._stats

    def clear(self) -> None:
        """Clear the buffer."""
        self._buffer.clear()
        self._stats.current_window_size = 0

    @property
    def buffer_size(self) -> int:
        """Current buffer size."""
        return len(self._buffer)

    @property
    def is_full(self) -> bool:
        """Check if buffer is at capacity."""
        return len(self._buffer) >= self.max_buffer

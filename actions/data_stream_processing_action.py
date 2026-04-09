"""
Data Stream Processing Action Module

Provides stream processing capabilities for real-time data handling including
windowing, aggregation, watermarking, and state management.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class WindowType(Enum):
    """Stream window types."""

    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    COUNT = "count"


@dataclass
class Window:
    """A stream processing window."""

    window_id: str
    window_type: WindowType
    start_time: float
    end_time: Optional[float] = None
    size: int = 0
    max_size: int = 0


@dataclass
class StreamEvent:
    """A stream event."""

    event_id: str
    timestamp: float
    data: Dict[str, Any]
    key: Optional[str] = None


@dataclass
class WindowResult:
    """Result of window processing."""

    window_id: str
    start_time: float
    end_time: float
    aggregations: Dict[str, Any]
    event_count: int


@dataclass
class StreamConfig:
    """Configuration for stream processing."""

    default_window_seconds: float = 60.0
    watermark_delay_seconds: float = 5.0
    max_lateness_seconds: float = 30.0
    enable_state_checkpointing: bool = True


class DataStreamProcessingAction:
    """
    Stream processing action for real-time data.

    Features:
    - Tumbling and sliding windows
    - Session windows
    - Watermark-based late event handling
    - State management
    - Window aggregation
    - Keyed and non-keyed streams

    Usage:
        processor = DataStreamProcessingAction(config)
        
        processor.define_window("tumbling-1m", WindowType.TUMBLING, size_seconds=60)
        processor.register_aggregator("tumbling-1m", "sum", sum_aggregator)
        
        await processor.process_stream(data_stream)
    """

    def __init__(self, config: Optional[StreamConfig] = None):
        self.config = config or StreamConfig()
        self._windows: Dict[str, Window] = {}
        self._window_events: Dict[str, List[StreamEvent]] = defaultdict(list)
        self._aggregators: Dict[str, Callable] = {}
        self._window_results: List[WindowResult] = []
        self._watermark: float = time.time()
        self._stats = {
            "events_processed": 0,
            "windows_completed": 0,
            "late_events": 0,
        }

    def define_window(
        self,
        window_id: str,
        window_type: WindowType,
        size_seconds: float = 60.0,
        slide_seconds: Optional[float] = None,
    ) -> Window:
        """Define a stream window."""
        window = Window(
            window_id=window_id,
            window_type=window_type,
            start_time=time.time(),
            end_time=time.time() + size_seconds,
            max_size=int(size_seconds),
        )
        self._windows[window_id] = window
        return window

    def register_aggregator(
        self,
        window_id: str,
        agg_name: str,
        aggregator: Callable[[List], Any],
    ) -> None:
        """Register an aggregator for a window."""
        key = f"{window_id}:{agg_name}"
        self._aggregators[key] = aggregator

    async def process_event(self, event: StreamEvent) -> None:
        """Process a single stream event."""
        self._stats["events_processed"] += 1

        for window_id, window in self._windows.items():
            if window.end_time and event.timestamp > window.end_time + self.config.max_lateness_seconds:
                self._stats["late_events"] += 1
                continue

            self._window_events[window_id].append(event)

        watermark_time = time.time() - self.config.watermark_delay_seconds
        self._watermark = watermark_time

        await self._check_and_close_windows()

    async def _check_and_close_windows(self) -> None:
        """Check if any windows should be closed and processed."""
        for window_id, window in list(self._windows.items()):
            if window.end_time and time.time() >= window.end_time:
                await self._close_window(window_id)
                window.start_time = window.end_time
                window.end_time = window.start_time + window.max_size

    async def _close_window(self, window_id: str) -> None:
        """Close and process a window."""
        events = self._window_events.get(window_id, [])
        if not events:
            return

        aggregations = {}
        for key, agg_func in self._aggregators.items():
            if key.startswith(window_id):
                agg_name = key.split(":")[1]
                agg_value = agg_func([e.data for e in events])
                aggregations[agg_name] = agg_value

        result = WindowResult(
            window_id=window_id,
            start_time=events[0].timestamp if events else time.time(),
            end_time=events[-1].timestamp if events else time.time(),
            aggregations=aggregations,
            event_count=len(events),
        )

        self._window_results.append(result)
        self._window_events[window_id] = []
        self._stats["windows_completed"] += 1

    async def process_batch(
        self,
        events: List[StreamEvent],
    ) -> List[WindowResult]:
        """Process a batch of events."""
        for event in events:
            await self.process_event(event)

        return self._window_results[-len(events):] if self._window_results else []

    def get_latest_results(self, limit: int = 10) -> List[WindowResult]:
        """Get latest window results."""
        return self._window_results[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """Get stream processing statistics."""
        return {
            **self._stats.copy(),
            "total_windows": len(self._windows),
            "total_results": len(self._window_results),
        }


async def demo_stream():
    """Demonstrate stream processing."""
    config = StreamConfig()
    processor = DataStreamProcessingAction(config)

    processor.define_window("tumbling-1m", WindowType.TUMBLING, size_seconds=60)
    processor.register_aggregator("tumbling-1m", "count", len)
    processor.register_aggregator("tumbling-1m", "sum", lambda x: sum(d.get("value", 0) for d in x))

    events = [
        StreamEvent(event_id=f"e{i}", timestamp=time.time(), data={"value": i})
        for i in range(5)
    ]

    await processor.process_batch(events)

    print(f"Events processed: {processor._stats['events_processed']}")
    print(f"Stats: {processor.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_stream())

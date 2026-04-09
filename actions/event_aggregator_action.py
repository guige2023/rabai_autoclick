"""Event Aggregator Action Module.

Aggregate events from multiple sources with time-windowing.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class AggregationWindow(Enum):
    """Aggregation window types."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"


@dataclass
class AggregatedEvent(Generic[T]):
    """Aggregated event result."""
    window_id: str
    event_type: str
    count: int
    items: list[T]
    window_start: float
    window_end: float
    aggregates: dict[str, Any] = field(default_factory=dict)


class EventAggregator(Generic[T]):
    """Aggregate events within time windows."""

    def __init__(
        self,
        window_type: AggregationWindow = AggregationWindow.TUMBLING,
        window_size_seconds: float = 60.0,
        slide_size_seconds: float | None = None
    ) -> None:
        self.window_type = window_type
        self.window_size = window_size_seconds
        self.slide_size = slide_size_seconds or window_size_seconds
        self._windows: dict[str, list[T]] = defaultdict(list)
        self._window_times: dict[str, tuple[float, float]] = {}
        self._handlers: list[Callable[[AggregatedEvent], Any]] = []
        self._lock = asyncio.Lock()

    def on_aggregate(self, handler: Callable[[AggregatedEvent], Any]) -> None:
        """Register aggregation handler."""
        self._handlers.append(handler)

    async def add(self, event_type: str, item: T, timestamp: float | None = None) -> None:
        """Add an event to aggregation."""
        timestamp = timestamp or time.time()
        window_id = self._get_window_id(timestamp)
        async with self._lock:
            self._windows[window_id].append(item)
            if window_id not in self._window_times:
                window_start = self._get_window_start(timestamp)
                self._window_times[window_id] = (window_start, window_start + self.window_size)

    async def flush(self, window_id: str | None = None) -> list[AggregatedEvent]:
        """Flush completed windows and emit aggregated events."""
        async with self._lock:
            now = time.time()
            completed = []
            for wid, items in list(self._windows.items()):
                if window_id and wid != window_id:
                    continue
                start, end = self._window_times.get(wid, (0, 0))
                if now >= end or window_id:
                    aggregated = AggregatedEvent(
                        window_id=wid,
                        event_type="aggregated",
                        count=len(items),
                        items=list(items),
                        window_start=start,
                        window_end=end
                    )
                    aggregated.aggregates = self._compute_aggregates(items)
                    completed.append(aggregated)
                    del self._windows[wid]
                    self._window_times.pop(wid, None)
            for handler in self._handlers:
                for event in completed:
                    result = handler(event)
                    if asyncio.iscoroutine(result):
                        await result
            return completed

    def _get_window_id(self, timestamp: float) -> str:
        """Get window ID for timestamp."""
        if self.window_type == AggregationWindow.TUMBLING:
            start = self._get_window_start(timestamp)
            return f"{int(start)}"
        elif self.window_type == AggregationWindow.SLIDING:
            start = self._get_window_start(timestamp)
            return f"{int(start)}"
        return f"{int(timestamp)}"

    def _get_window_start(self, timestamp: float) -> float:
        """Calculate window start time."""
        return int(timestamp / self.slide_size) * self.slide_size

    def _compute_aggregates(self, items: list[T]) -> dict[str, Any]:
        """Compute aggregate metrics on items."""
        if not items:
            return {"count": 0}
        result = {"count": len(items)}
        if all(isinstance(i, (int, float)) for i in items):
            result["sum"] = sum(items)
            result["avg"] = result["sum"] / len(items)
            result["min"] = min(items)
            result["max"] = max(items)
        return result


class MultiSourceAggregator:
    """Aggregate events from multiple sources."""

    def __init__(self) -> None:
        self._aggregators: dict[str, EventAggregator] = {}
        self._lock = asyncio.Lock()

    async def add_aggregator(self, name: str, aggregator: EventAggregator) -> None:
        """Add a named aggregator."""
        async with self._lock:
            self._aggregators[name] = aggregator

    async def add(self, source: str, item: T, timestamp: float | None = None) -> None:
        """Add item to source aggregator."""
        async with self._lock:
            if source not in self._aggregators:
                self._aggregators[source] = EventAggregator()
            await self._aggregators[source].add(source, item, timestamp)

    async def flush_all(self) -> dict[str, list[AggregatedEvent]]:
        """Flush all aggregators."""
        results = {}
        for name, aggregator in list(self._aggregators.items()):
            results[name] = await aggregator.flush()
        return results

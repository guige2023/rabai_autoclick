"""
Data Aggregation Action Module.

Provides real-time data aggregation with rolling windows,
stream processing, and materialized view capabilities.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
import time
from collections import defaultdict, deque

logger = logging.getLogger(__name__)


class WindowType(Enum):
    """Window types for aggregation."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    COUNT = "count"


@dataclass
class Window:
    """Time window definition."""
    window_id: str
    window_type: WindowType
    start_time: datetime
    end_time: datetime
    size: timedelta
    slide: Optional[timedelta] = None


@dataclass
class AggregationResult:
    """Result of aggregation."""
    window_id: str
    timestamp: datetime
    measures: Dict[str, float]
    dimensions: Dict[str, Any]
    count: int


class RollingWindow:
    """Rolling window buffer."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._buffer: deque = deque(maxlen=max_size)

    def add(self, item: Any):
        """Add item to window."""
        self._buffer.append(item)

    def get_all(self) -> List[Any]:
        """Get all items."""
        return list(self._buffer)

    def get_range(self, start: datetime, end: datetime) -> List[Any]:
        """Get items in time range."""
        return [
            item for item in self._buffer
            if hasattr(item, "timestamp") and start <= item.timestamp <= end
        ]

    def size(self) -> int:
        """Get current size."""
        return len(self._buffer)


class AggregationFunction:
    """Built-in aggregation functions."""

    @staticmethod
    def sum(values: List[float]) -> float:
        return sum(values)

    @staticmethod
    def avg(values: List[float]) -> float:
        return sum(values) / len(values) if values else 0.0

    @staticmethod
    def min(values: List[float]) -> float:
        return min(values) if values else 0.0

    @staticmethod
    def max(values: List[float]) -> float:
        return max(values) if values else 0.0

    @staticmethod
    def count(values: List[Any]) -> int:
        return len(values)

    @staticmethod
    def distinct(values: List[Any]) -> int:
        return len(set(values))

    @staticmethod
    def stddev(values: List[float]) -> float:
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        return variance ** 0.5


class DataAggregator:
    """Main data aggregator."""

    def __init__(self):
        self.windows: Dict[str, RollingWindow] = {}
        self.aggregations: Dict[str, Dict[str, Callable]] = {}
        self._running = False

    def create_window(
        self,
        window_id: str,
        window_type: WindowType,
        size: timedelta,
        slide: Optional[timedelta] = None
    ) -> Window:
        """Create aggregation window."""
        window = Window(
            window_id=window_id,
            window_type=window_type,
            start_time=datetime.now(),
            end_time=datetime.now() + size,
            size=size,
            slide=slide
        )

        self.windows[window_id] = RollingWindow()
        self.aggregations[window_id] = {
            "sum": AggregationFunction.sum,
            "avg": AggregationFunction.avg,
            "min": AggregationFunction.min,
            "max": AggregationFunction.max,
            "count": AggregationFunction.count,
            "distinct": AggregationFunction.distinct,
            "stddev": AggregationFunction.stddev
        }

        return window

    def add_data(self, window_id: str, data: Any):
        """Add data to window."""
        if window_id in self.windows:
            self.windows[window_id].add(data)

    def aggregate(
        self,
        window_id: str,
        measure: str,
        value_extractor: Callable[[Any], float]
    ) -> Optional[float]:
        """Aggregate window data."""
        if window_id not in self.windows:
            return None

        window = self.windows[window_id]
        values = [value_extractor(item) for item in window.get_all()]

        agg_func = self.aggregations.get(window_id, {}).get(measure)
        if agg_func:
            return agg_func(values)

        return None

    def get_results(
        self,
        window_id: str,
        measures: List[str],
        value_extractors: Dict[str, Callable]
    ) -> Optional[AggregationResult]:
        """Get aggregation results."""
        if window_id not in self.windows:
            return None

        measures_result = {}
        for measure in measures:
            extractor = value_extractors.get(measure)
            if extractor:
                result = self.aggregate(window_id, measure, extractor)
                measures_result[measure] = result

        return AggregationResult(
            window_id=window_id,
            timestamp=datetime.now(),
            measures=measures_result,
            dimensions={},
            count=self.windows[window_id].size()
        )


class StreamProcessor:
    """Stream processing with windowed aggregation."""

    def __init__(self, aggregator: DataAggregator):
        self.aggregator = aggregator
        self._handlers: List[Callable] = []
        self._running = False

    def add_handler(self, handler: Callable):
        """Add result handler."""
        self._handlers.append(handler)

    async def process_stream(
        self,
        data_stream: List[Any],
        window_id: str
    ):
        """Process data stream."""
        for data in data_stream:
            self.aggregator.add_data(window_id, data)

            result = self.aggregator.get_results(
                window_id,
                ["sum", "avg", "count"],
                {"sum": lambda x: x, "avg": lambda x: x, "count": lambda x: 1}
            )

            if result:
                for handler in self._handlers:
                    try:
                        handler(result)
                    except Exception as e:
                        logger.error(f"Handler error: {e}")


class MaterializedView:
    """Materialized view for pre-computed aggregations."""

    def __init__(self, name: str):
        self.name = name
        self.data: List[Dict[str, Any]] = []
        self.last_refresh: Optional[datetime] = None
        self.refresh_interval: timedelta = timedelta(minutes=5)

    def refresh(self, data: List[Dict[str, Any]]):
        """Refresh materialized view."""
        self.data = data.copy()
        self.last_refresh = datetime.now()

    def query(
        self,
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Query materialized view."""
        result = self.data

        if filters:
            for key, value in filters.items():
                result = [r for r in result if r.get(key) == value]

        if order_by:
            reverse = order_by.startswith("-")
            field = order_by.lstrip("-")
            result = sorted(result, key=lambda x: x.get(field, 0), reverse=reverse)

        if limit:
            result = result[:limit]

        return result


async def main():
    """Demonstrate data aggregation."""
    aggregator = DataAggregator()

    window = aggregator.create_window(
        "metrics",
        WindowType.TUMBLING,
        size=timedelta(minutes=5)
    )

    for i in range(100):
        aggregator.add_data("metrics", float(i))

    result = aggregator.get_results(
        "metrics",
        ["sum", "avg", "min", "max", "count"],
        {"sum": lambda x: x, "avg": lambda x: x, "min": lambda x: x, "max": lambda x: x, "count": lambda x: 1}
    )

    if result:
        print(f"Sum: {result.measures.get('sum')}")
        print(f"Avg: {result.measures.get('avg')}")
        print(f"Count: {result.count}")


if __name__ == "__main__":
    asyncio.run(main())

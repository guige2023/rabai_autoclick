"""Data Rolling Window Action Module.

Provides rolling window data structures for time-series
aggregation with configurable window sizes and overlap.

Author: RabAi Team
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple, Union

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class WindowType(Enum):
    """Types of rolling windows."""
    TUMBLING = "tumbling"
    SLIDING = "sliding"
    SESSION = "session"
    EXPIRING = "expiring"


class AggregationType(Enum):
    """Types of aggregation functions."""
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    FIRST = "first"
    LAST = "last"
    STDDEV = "stddev"
    MEDIAN = "median"


@dataclass
class WindowConfig:
    """Configuration for rolling window."""
    window_size_ms: int = 60000
    window_type: WindowType = WindowType.SLIDING
    slide_interval_ms: int = 10000
    max_windows: int = 100
    aggregation: AggregationType = AggregationType.AVG


@dataclass
class WindowData:
    """Data in a single window."""
    window_id: int
    start_time: float
    end_time: float
    values: List[float]
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def count(self) -> int:
        return len(self.values)

    @property
    def is_empty(self) -> bool:
        return len(self.values) == 0


@dataclass
class AggregatedResult:
    """Result of window aggregation."""
    window_id: int
    start_time: float
    end_time: float
    value: float
    count: int
    aggregation_type: AggregationType


class RollingAggregator:
    """Aggregates data over rolling windows."""

    def __init__(self, config: WindowConfig):
        self.config = config
        self.windows: Deque[WindowData] = deque(maxlen=config.max_windows)
        self.pending_values: Deque[Tuple[float, float]] = deque()
        self._window_counter = 0
        self._current_window_start: Optional[float] = None

    def add(
        self,
        value: float,
        timestamp: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Optional[AggregatedResult]:
        """Add value to rolling window."""
        if timestamp is None:
            timestamp = time.time()

        result = None

        if self.config.window_type == WindowType.TUMBLING:
            result = self._add_tumbling(value, timestamp, metadata)
        elif self.config.window_type == WindowType.SLIDING:
            result = self._add_sliding(value, timestamp, metadata)
        elif self.config.window_type == WindowType.EXPIRING:
            result = self._add_expiring(value, timestamp, metadata)
        else:
            self._add_simple(value, timestamp, metadata)

        return result

    def _get_window_for_timestamp(self, timestamp: float) -> int:
        """Get window ID for timestamp."""
        window_size = self.config.window_size_ms / 1000.0
        return int(timestamp / window_size)

    def _create_window(
        self,
        window_id: int,
        start_time: float,
        metadata: Optional[Dict[str, Any]] = None
    ) -> WindowData:
        """Create new window."""
        return WindowData(
            window_id=window_id,
            start_time=start_time,
            end_time=start_time + (self.config.window_size_ms / 1000.0),
            values=[],
            metadata=metadata or {}
        )

    def _add_tumbling(
        self,
        value: float,
        timestamp: float,
        metadata: Optional[Dict[str, Any]]
    ) -> Optional[AggregatedResult]:
        """Add value to tumbling window."""
        window_id = self._get_window_for_timestamp(timestamp)
        window_size = self.config.window_size_ms / 1000.0
        window_start = window_id * window_size

        existing = None
        for w in self.windows:
            if w.window_id == window_id:
                existing = w
                break

        if existing:
            existing.values.append(value)
            result = self._aggregate_window(existing)
            return result
        else:
            new_window = self._create_window(window_id, window_start, metadata)
            new_window.values.append(value)
            self.windows.append(new_window)

            return self._aggregate_window(new_window)

    def _add_sliding(
        self,
        value: float,
        timestamp: float,
        metadata: Optional[Dict[str, Any]]
    ) -> Optional[AggregatedResult]:
        """Add value to sliding window."""
        self._evict_expired_windows(timestamp)

        window_id = self._get_window_for_timestamp(timestamp)

        for w in self.windows:
            if w.window_id == window_id:
                w.values.append(value)
                return self._aggregate_window(w)

        window_size = self.config.window_size_ms / 1000.0
        window_start = window_id * window_size
        new_window = self._create_window(window_id, window_start, metadata)
        new_window.values.append(value)
        self.windows.append(new_window)

        return self._aggregate_window(new_window)

    def _add_expiring(
        self,
        value: float,
        timestamp: float,
        metadata: Optional[Dict[str, Any]]
    ) -> Optional[AggregatedResult]:
        """Add value to expiring window."""
        self.pending_values.append((timestamp, value))

        self._evict_expired_windows(timestamp)

        return None

    def _add_simple(
        self,
        value: float,
        timestamp: float,
        metadata: Optional[Dict[str, Any]]
    ) -> None:
        """Add value without window management."""
        self.pending_values.append((timestamp, value))

    def _evict_expired_windows(self, current_time: float) -> None:
        """Remove expired windows."""
        cutoff = current_time - (self.config.window_size_ms / 1000.0)

        while self.windows and self.windows[0].end_time < cutoff:
            self.windows.popleft()

        while self.pending_values and self.pending_values[0][0] < cutoff:
            self.pending_values.popleft()

    def _aggregate_window(self, window: WindowData) -> AggregatedResult:
        """Aggregate values in a window."""
        if not window.values:
            return AggregatedResult(
                window_id=window.window_id,
                start_time=window.start_time,
                end_time=window.end_time,
                value=0.0,
                count=0,
                aggregation_type=self.config.aggregation
            )

        values = window.values

        if self.config.aggregation == AggregationType.SUM:
            agg_value = sum(values)
        elif self.config.aggregation == AggregationType.AVG:
            agg_value = sum(values) / len(values)
        elif self.config.aggregation == AggregationType.MIN:
            agg_value = min(values)
        elif self.config.aggregation == AggregationType.MAX:
            agg_value = max(values)
        elif self.config.aggregation == AggregationType.COUNT:
            agg_value = float(len(values))
        elif self.config.aggregation == AggregationType.FIRST:
            agg_value = values[0]
        elif self.config.aggregation == AggregationType.LAST:
            agg_value = values[-1]
        elif self.config.aggregation == AggregationType.STDDEV:
            mean = sum(values) / len(values)
            variance = sum((x - mean) ** 2 for x in values) / len(values)
            agg_value = variance ** 0.5
        elif self.config.aggregation == AggregationType.MEDIAN:
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            if n % 2 == 0:
                agg_value = (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
            else:
                agg_value = sorted_vals[n // 2]
        else:
            agg_value = sum(values) / len(values)

        return AggregatedResult(
            window_id=window.window_id,
            start_time=window.start_time,
            end_time=window.end_time,
            value=agg_value,
            count=len(values),
            aggregation_type=self.config.aggregation
        )

    def get_all_aggregated(self) -> List[AggregatedResult]:
        """Get aggregated results for all windows."""
        return [self._aggregate_window(w) for w in self.windows if w.values]

    def get_current_window(self) -> Optional[AggregatedResult]:
        """Get current window aggregation."""
        if not self.windows:
            return None

        return self._aggregate_window(self.windows[-1])

    def get_statistics(self) -> Dict[str, Any]:
        """Get rolling window statistics."""
        total_values = sum(w.count for w in self.windows)

        return {
            "window_count": len(self.windows),
            "pending_values": len(self.pending_values),
            "total_values": total_values,
            "window_size_ms": self.config.window_size_ms,
            "window_type": self.config.window_type.value,
            "slide_interval_ms": self.config.slide_interval_ms,
            "aggregation": self.config.aggregation.value,
            "current_window": self.get_current_window().__dict__ if self.windows else None
        }


class DataRollingWindowAction(BaseAction):
    """Action for rolling window data operations."""

    def __init__(self):
        super().__init__("data_rolling_window")
        self._windows: Dict[str, RollingAggregator] = {}

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute rolling window action."""
        try:
            operation = params.get("operation", "add")

            if operation == "create":
                return self._create(params)
            elif operation == "add":
                return self._add(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "get_all":
                return self._get_all(params)
            elif operation == "stats":
                return self._get_stats(params)
            elif operation == "clear":
                return self._clear(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create new rolling window."""
        name = params.get("name", "default")

        config = WindowConfig(
            window_size_ms=params.get("window_size_ms", 60000),
            window_type=WindowType(params.get("window_type", "sliding")),
            slide_interval_ms=params.get("slide_interval_ms", 10000),
            max_windows=params.get("max_windows", 100),
            aggregation=AggregationType(params.get("aggregation", "avg"))
        )

        self._windows[name] = RollingAggregator(config)

        return ActionResult(
            success=True,
            message=f"Rolling window created: {name}"
        )

    def _add(self, params: Dict[str, Any]) -> ActionResult:
        """Add value to window."""
        name = params.get("name", "default")

        if name not in self._windows:
            self._windows[name] = RollingAggregator(WindowConfig())

        value = params.get("value", 0.0)
        timestamp = params.get("timestamp")
        metadata = params.get("metadata")

        if timestamp:
            timestamp = float(timestamp)

        result = self._windows[name].add(value, timestamp, metadata)

        if result:
            return ActionResult(
                success=True,
                data={
                    "window_id": result.window_id,
                    "aggregated_value": result.value,
                    "count": result.count
                }
            )

        return ActionResult(success=True)

    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get current window aggregation."""
        name = params.get("name", "default")

        if name not in self._windows:
            return ActionResult(success=False, message=f"Window not found: {name}")

        result = self._windows[name].get_current_window()

        if not result:
            return ActionResult(success=True, data={"window_id": None})

        return ActionResult(
            success=True,
            data={
                "window_id": result.window_id,
                "start_time": result.start_time,
                "end_time": result.end_time,
                "value": result.value,
                "count": result.count,
                "aggregation_type": result.aggregation_type.value
            }
        )

    def _get_all(self, params: Dict[str, Any]) -> ActionResult:
        """Get all window aggregations."""
        name = params.get("name", "default")

        if name not in self._windows:
            return ActionResult(success=False, message=f"Window not found: {name}")

        results = self._windows[name].get_all_aggregated()

        return ActionResult(
            success=True,
            data={
                "windows": [
                    {
                        "window_id": r.window_id,
                        "start_time": r.start_time,
                        "end_time": r.end_time,
                        "value": r.value,
                        "count": r.count,
                        "aggregation_type": r.aggregation_type.value
                    }
                    for r in results
                ]
            }
        )

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get window statistics."""
        name = params.get("name", "default")

        if name not in self._windows:
            return ActionResult(success=False, message=f"Window not found: {name}")

        stats = self._windows[name].get_statistics()
        return ActionResult(success=True, data=stats)

    def _clear(self, params: Dict[str, Any]) -> ActionResult:
        """Clear window data."""
        name = params.get("name", "default")

        if name not in self._windows:
            return ActionResult(success=False, message=f"Window not found: {name}")

        self._windows[name].windows.clear()
        self._windows[name].pending_values.clear()

        return ActionResult(success=True, message=f"Window cleared: {name}")


from enum import Enum

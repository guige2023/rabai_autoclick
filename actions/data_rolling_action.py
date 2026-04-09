"""Data Rolling Window Action Module.

Provides rolling window computations over data streams including
aggregations, statistics, and trend detection.
"""

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional, Union

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class AggregationType(Enum):
    """Rolling aggregation types."""
    SUM = "sum"
    MEAN = "mean"
    MEDIAN = "median"
    MIN = "min"
    MAX = "max"
    STD = "std"
    VARIANCE = "variance"
    COUNT = "count"
    FIRST = "first"
    LAST = "last"
    DELTA = "delta"
    RATE = "rate"


class TrendDirection(Enum):
    """Trend direction."""
    UP = "up"
    DOWN = "down"
    STABLE = "stable"


@dataclass
class RollingWindow:
    """A rolling window buffer."""
    values: Deque[float] = field(default_factory=deque)
    timestamps: Deque[float] = field(default_factory=deque)
    max_size: int = 100

    def push(self, value: float, timestamp: Optional[float] = None) -> None:
        """Add a value to the window."""
        ts = timestamp if timestamp is not None else time.time()
        self.values.append(value)
        self.timestamps.append(ts)
        while len(self.values) > self.max_size:
            self.values.popleft()
            self.timestamps.popleft()

    def get_values(self) -> List[float]:
        """Get all values as a list."""
        return list(self.values)

    def size(self) -> int:
        return len(self.values)

    def is_full(self) -> bool:
        return len(self.values) >= self.max_size


class DataRollingAction(BaseAction):
    """Rolling window data processing action.

    Maintains rolling windows over numeric data and computes
    various aggregations, statistics, and trend indicators.

    Args:
        context: Execution context.
        params: Dict with keys:
            - operation: Operation (push, get, aggregate, trend, clear)
            - window_id: Identifier for the window
            - value: Numeric value to push
            - window_size: Max size of window
            - aggregation: Aggregation type
            - data: List of values for batch operations
    """
    action_type = "data_rolling"
    display_name = "数据滚动窗口"
    description = "滚动窗口聚合与趋势分析"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "window_id": "default",
            "value": None,
            "values": None,
            "window_size": 100,
            "aggregation": "mean",
            "trend_threshold": 0.05,
            "data": None,
        }

    def __init__(self) -> None:
        super().__init__()
        self._windows: Dict[str, RollingWindow] = {}
        self._custom_aggregators: Dict[str, Callable] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute rolling window operation."""
        start_time = time.time()

        operation = params.get("operation", "get")
        window_id = params.get("window_id", "default")
        value = params.get("value")
        values = params.get("values", [])
        window_size = params.get("window_size", 100)
        aggregation = params.get("aggregation", "mean")
        trend_threshold = params.get("trend_threshold", 0.05)
        data = params.get("data")

        # Ensure window exists
        if window_id not in self._windows:
            self._windows[window_id] = RollingWindow(max_size=window_size)

        window = self._windows[window_id]

        if operation == "push":
            return self._push_value(window, value, values, data, window_id, start_time)
        elif operation == "get":
            return self._get_window(window, window_id, start_time)
        elif operation == "aggregate":
            return self._compute_aggregation(window, aggregation, window_id, start_time)
        elif operation == "trend":
            return self._detect_trend(window, trend_threshold, window_id, start_time)
        elif operation == "clear":
            return self._clear_window(window, window_id, start_time)
        elif operation == "stats":
            return self._compute_stats(window, window_id, start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _push_value(
        self,
        window: RollingWindow,
        value: Optional[float],
        values: List[float],
        data: Any,
        window_id: str,
        start_time: float
    ) -> ActionResult:
        """Push values into the rolling window."""
        pushed = 0
        if value is not None:
            try:
                window.push(float(value))
                pushed += 1
            except (ValueError, TypeError):
                return ActionResult(
                    success=False,
                    message=f"Cannot convert value to float: {value}",
                    duration=time.time() - start_time
                )
        if values:
            for v in values:
                try:
                    window.push(float(v))
                    pushed += 1
                except (ValueError, TypeError):
                    pass
        if data is not None:
            if isinstance(data, list):
                for v in data:
                    try:
                        window.push(float(v))
                        pushed += 1
                    except (ValueError, TypeError):
                        pass
            else:
                try:
                    window.push(float(data))
                    pushed += 1
                except (ValueError, TypeError):
                    pass

        return ActionResult(
            success=True,
            message=f"Pushed {pushed} values to window '{window_id}'",
            data={"window_id": window_id, "pushed": pushed, "window_size": window.size()},
            duration=time.time() - start_time
        )

    def _get_window(self, window: RollingWindow, window_id: str, start_time: float) -> ActionResult:
        """Get current window values."""
        return ActionResult(
            success=True,
            message=f"Retrieved window '{window_id}'",
            data={
                "window_id": window_id,
                "size": window.size(),
                "max_size": window.max_size,
                "is_full": window.is_full(),
                "values": window.get_values(),
            },
            duration=time.time() - start_time
        )

    def _compute_aggregation(
        self,
        window: RollingWindow,
        aggregation: str,
        window_id: str,
        start_time: float
    ) -> ActionResult:
        """Compute aggregation over the window."""
        values = window.get_values()
        if not values:
            return ActionResult(
                success=True,
                message=f"Window '{window_id}' is empty",
                data={"window_id": window_id, "aggregation": aggregation, "result": None},
                duration=time.time() - start_time
            )

        try:
            import math
            result = None
            if aggregation == "sum":
                result = sum(values)
            elif aggregation == "mean":
                result = sum(values) / len(values)
            elif aggregation == "median":
                sorted_vals = sorted(values)
                n = len(sorted_vals)
                mid = n // 2
                result = sorted_vals[mid] if n % 2 == 1 else (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
            elif aggregation == "min":
                result = min(values)
            elif aggregation == "max":
                result = max(values)
            elif aggregation == "count":
                result = len(values)
            elif aggregation == "first":
                result = values[0]
            elif aggregation == "last":
                result = values[-1]
            elif aggregation == "std":
                mean = sum(values) / len(values)
                variance = sum((x - mean) ** 2 for x in values) / len(values)
                result = math.sqrt(variance)
            elif aggregation == "variance":
                mean = sum(values) / len(values)
                result = sum((x - mean) ** 2 for x in values) / len(values)
            elif aggregation == "delta":
                result = values[-1] - values[0] if len(values) >= 2 else 0.0
            elif aggregation == "rate":
                if len(values) >= 2:
                    dt = window.timestamps[-1] - window.timestamps[0]
                    result = (values[-1] - values[0]) / dt if dt > 0 else 0.0
                else:
                    result = 0.0
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown aggregation: {aggregation}",
                    duration=time.time() - start_time
                )

            return ActionResult(
                success=True,
                message=f"Aggregation '{aggregation}' on window '{window_id}': {result}",
                data={"window_id": window_id, "aggregation": aggregation, "result": result, "sample_count": len(values)},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Aggregation failed: {str(e)}",
                duration=time.time() - start_time
            )

    def _detect_trend(
        self,
        window: RollingWindow,
        threshold: float,
        window_id: str,
        start_time: float
    ) -> ActionResult:
        """Detect trend direction in the window."""
        import math
        values = window.get_values()
        if len(values) < 2:
            return ActionResult(
                success=True,
                message=f"Not enough data for trend in window '{window_id}'",
                data={"window_id": window_id, "trend": "stable", "values_count": len(values)},
                duration=time.time() - start_time
            )

        n = len(values)
        half = n // 2
        first_half_avg = sum(values[:half]) / half
        second_half_avg = sum(values[half:]) / (n - half)

        mean_val = sum(values) / n
        change_ratio = (second_half_avg - first_half_avg) / (abs(mean_val) + 1e-9)

        if change_ratio > threshold:
            direction = "up"
        elif change_ratio < -threshold:
            direction = "down"
        else:
            direction = "stable"

        return ActionResult(
            success=True,
            message=f"Trend detected: {direction} ({change_ratio:+.2%})",
            data={
                "window_id": window_id,
                "trend": direction,
                "change_ratio": change_ratio,
                "threshold": threshold,
                "first_half_avg": first_half_avg,
                "second_half_avg": second_half_avg,
            },
            duration=time.time() - start_time
        )

    def _compute_stats(self, window: RollingWindow, window_id: str, start_time: float) -> ActionResult:
        """Compute comprehensive statistics."""
        import math
        values = window.get_values()
        if not values:
            return ActionResult(
                success=True,
                message=f"Window '{window_id}' is empty",
                data={"window_id": window_id, "stats": None},
                duration=time.time() - start_time
            )

        n = len(values)
        mean = sum(values) / n
        sorted_vals = sorted(values)
        variance = sum((x - mean) ** 2 for x in values) / n
        std = math.sqrt(variance)
        median_idx = n // 2
        median = sorted_vals[median_idx] if n % 2 == 1 else (sorted_vals[median_idx - 1] + sorted_vals[median_idx]) / 2

        return ActionResult(
            success=True,
            message=f"Stats computed for window '{window_id}'",
            data={
                "window_id": window_id,
                "stats": {
                    "count": n,
                    "mean": mean,
                    "median": median,
                    "min": min(values),
                    "max": max(values),
                    "std": std,
                    "variance": variance,
                    "sum": sum(values),
                }
            },
            duration=time.time() - start_time
        )

    def _clear_window(self, window: RollingWindow, window_id: str, start_time: float) -> ActionResult:
        """Clear the rolling window."""
        window.values.clear()
        window.timestamps.clear()
        return ActionResult(
            success=True,
            message=f"Window '{window_id}' cleared",
            data={"window_id": window_id, "size": 0},
            duration=time.time() - start_time
        )


from enum import Enum

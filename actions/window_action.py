"""Window action module for RabAI AutoClick.

Provides window function utilities:
- WindowFunctions: Moving window aggregations
- SlidingWindow: Sliding window
- TumblingWindow: Fixed-size windows
- SessionWindow: Session-based windows
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class WindowResult:
    """Result from a window."""
    window_id: str
    start_index: int
    end_index: int
    values: List[Any]
    aggregate: Any = None


class SlidingWindow:
    """Sliding window over data."""

    def __init__(self, size: int, step: int = 1):
        self.size = size
        self.step = step

    def apply(self, items: List[Any], aggregator: Optional[Callable[[List[Any]], Any]] = None) -> List[WindowResult]:
        """Apply sliding window."""
        results = []
        window_id = 0

        for i in range(0, len(items) - self.size + 1, self.step):
            window_values = items[i:i + self.size]
            aggregate = aggregator(window_values) if aggregator else None

            results.append(WindowResult(
                window_id=f"w_{window_id}",
                start_index=i,
                end_index=i + self.size - 1,
                values=window_values,
                aggregate=aggregate,
            ))
            window_id += 1

        return results


class TumblingWindow:
    """Tumbling (non-overlapping) window."""

    def __init__(self, size: int):
        self.size = size

    def apply(self, items: List[Any], aggregator: Optional[Callable[[List[Any]], Any]] = None) -> List[WindowResult]:
        """Apply tumbling window."""
        results = []
        window_id = 0

        for i in range(0, len(items), self.size):
            window_values = items[i:i + self.size]
            aggregate = aggregator(window_values) if aggregator else None

            results.append(WindowResult(
                window_id=f"t_{window_id}",
                start_index=i,
                end_index=min(i + self.size - 1, len(items) - 1),
                values=window_values,
                aggregate=aggregate,
            ))
            window_id += 1

        return results


class WindowFunctions:
    """Window function aggregations."""

    @staticmethod
    def sum_(values: List[float]) -> float:
        """Sum aggregation."""
        return sum(values)

    @staticmethod
    def avg(values: List[float]) -> float:
        """Average aggregation."""
        return sum(values) / len(values) if values else 0.0

    @staticmethod
    def min_(values: List[float]) -> float:
        """Min aggregation."""
        return min(values) if values else 0.0

    @staticmethod
    def max_(values: List[float]) -> float:
        """Max aggregation."""
        return max(values) if values else 0.0

    @staticmethod
    def count(values: List[Any]) -> int:
        """Count aggregation."""
        return len(values)

    @staticmethod
    def first(values: List[Any]) -> Any:
        """First value."""
        return values[0] if values else None

    @staticmethod
    def last(values: List[Any]) -> Any:
        """Last value."""
        return values[-1] if values else None

    @staticmethod
    def stddev(values: List[float]) -> float:
        """Standard deviation."""
        if not values:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5


class WindowAction(BaseAction):
    """Window function action."""
    action_type = "window"
    display_name = "窗口函数"
    description = "窗口聚合函数"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "sliding")

            if operation == "sliding":
                return self._sliding(params)
            elif operation == "tumbling":
                return self._tumbling(params)
            elif operation == "aggregate":
                return self._aggregate(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Window error: {str(e)}")

    def _sliding(self, params: Dict[str, Any]) -> ActionResult:
        """Apply sliding window."""
        items = params.get("items", [])
        size = params.get("size", 3)
        step = params.get("step", 1)
        agg_type = params.get("aggregation")

        if not items:
            return ActionResult(success=False, message="items is required")

        window = SlidingWindow(size=size, step=step)

        aggregator = None
        if agg_type:
            agg_map = {
                "sum": WindowFunctions.sum_,
                "avg": WindowFunctions.avg,
                "average": WindowFunctions.avg,
                "min": WindowFunctions.min_,
                "max": WindowFunctions.max_,
                "count": WindowFunctions.count,
                "first": WindowFunctions.first,
                "last": WindowFunctions.last,
                "stddev": WindowFunctions.stddev,
            }
            aggregator = agg_map.get(agg_type)

        results = window.apply(items, aggregator)

        return ActionResult(
            success=True,
            message=f"{len(results)} windows",
            data={
                "windows": [
                    {
                        "window_id": r.window_id,
                        "start": r.start_index,
                        "end": r.end_index,
                        "aggregate": r.aggregate,
                    }
                    for r in results
                ],
            },
        )

    def _tumbling(self, params: Dict[str, Any]) -> ActionResult:
        """Apply tumbling window."""
        items = params.get("items", [])
        size = params.get("size", 3)
        agg_type = params.get("aggregation")

        if not items:
            return ActionResult(success=False, message="items is required")

        window = TumblingWindow(size=size)

        aggregator = None
        if agg_type:
            agg_map = {
                "sum": WindowFunctions.sum_,
                "avg": WindowFunctions.avg,
                "average": WindowFunctions.avg,
                "min": WindowFunctions.min_,
                "max": WindowFunctions.max_,
                "count": WindowFunctions.count,
            }
            aggregator = agg_map.get(agg_type)

        results = window.apply(items, aggregator)

        return ActionResult(
            success=True,
            message=f"{len(results)} windows",
            data={
                "windows": [
                    {
                        "window_id": r.window_id,
                        "start": r.start_index,
                        "end": r.end_index,
                        "aggregate": r.aggregate,
                    }
                    for r in results
                ],
            },
        )

    def _aggregate(self, params: Dict[str, Any]) -> ActionResult:
        """Apply aggregation to values."""
        values = params.get("values", [])
        agg_type = params.get("aggregation", "sum")

        if not values:
            return ActionResult(success=False, message="values is required")

        agg_map = {
            "sum": WindowFunctions.sum_,
            "avg": WindowFunctions.avg,
            "average": WindowFunctions.avg,
            "min": WindowFunctions.min_,
            "max": WindowFunctions.max_,
            "count": WindowFunctions.count,
            "first": WindowFunctions.first,
            "last": WindowFunctions.last,
            "stddev": WindowFunctions.stddev,
        }

        aggregator = agg_map.get(agg_type)
        if not aggregator:
            return ActionResult(success=False, message=f"Unknown aggregation: {agg_type}")

        try:
            result = aggregator(values)
            return ActionResult(success=True, message=f"{agg_type} = {result}", data={"aggregation": agg_type, "result": result})
        except Exception as e:
            return ActionResult(success=False, message=f"Aggregation failed: {e}")

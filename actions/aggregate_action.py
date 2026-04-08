"""Aggregate action module for RabAI AutoClick.

Provides data aggregation utilities:
- Aggregator: Aggregate data
- GroupBy: Group and aggregate
- WindowFunctions: Window functions
"""

from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass
import threading
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class AggregationResult:
    """Result of aggregation."""
    name: str
    value: Any
    count: int = 0


class Aggregator:
    """Data aggregator."""

    def __init__(self):
        self._operations: Dict[str, Callable] = {
            "sum": lambda values: sum(values),
            "avg": lambda values: sum(values) / len(values) if values else 0,
            "min": lambda values: min(values) if values else None,
            "max": lambda values: max(values) if values else None,
            "count": lambda values: len(values),
            "first": lambda values: values[0] if values else None,
            "last": lambda values: values[-1] if values else None,
        }

    def aggregate(self, data: List[Any], operation: str, key: Optional[str] = None) -> AggregationResult:
        """Aggregate data."""
        if key:
            values = [item[key] for item in data if key in item]
        else:
            values = data

        if operation not in self._operations:
            return AggregationResult(name=operation, value=None, count=0)

        value = self._operations[operation](values)

        return AggregationResult(name=operation, value=value, count=len(values))

    def multi_aggregate(self, data: List[Any], operations: List[str], key: Optional[str] = None) -> Dict[str, Any]:
        """Apply multiple aggregations."""
        results = {}
        for op in operations:
            result = self.aggregate(data, op, key)
            results[op] = result.value
        return results


class GroupByAggregator:
    """Group by aggregator."""

    def aggregate(self, data: List[Dict[str, Any]], group_key: str, agg_key: str, operation: str) -> Dict[Any, Any]:
        """Group by and aggregate."""
        groups: Dict[Any, List[Any]] = {}

        for item in data:
            if group_key not in item:
                continue
            group_value = item[group_key]
            if group_value not in groups:
                groups[group_value] = []
            if agg_key in item:
                groups[group_value].append(item[agg_key])

        aggregator = Aggregator()
        results = {}
        for group_value, values in groups.items():
            result = aggregator.aggregate(values, operation)
            results[group_value] = result.value

        return results


class WindowFunctions:
    """Window functions for time series data."""

    @staticmethod
    def moving_average(data: List[float], window: int) -> List[float]:
        """Calculate moving average."""
        if len(data) < window:
            return []
        result = []
        for i in range(len(data) - window + 1):
            window_data = data[i : i + window]
            result.append(sum(window_data) / window)
        return result

    @staticmethod
    def cumulative_sum(data: List[float]) -> List[float]:
        """Calculate cumulative sum."""
        result = []
        total = 0.0
        for value in data:
            total += value
            result.append(total)
        return result

    @staticmethod
    def rank(data: List[float]) -> List[int]:
        """Rank values."""
        sorted_data = sorted(enumerate(data), key=lambda x: x[1])
        ranks = [0] * len(data)
        for rank, (index, _) in enumerate(sorted_data, 1):
            ranks[index] = rank
        return ranks


class AggregateAction(BaseAction):
    """Aggregation action."""
    action_type = "aggregate"
    display_name = "数据聚合"
    description = "聚合计算"

    def __init__(self):
        super().__init__()
        self._aggregator = Aggregator()
        self._group_by = GroupByAggregator()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "aggregate")

            if operation == "aggregate":
                return self._aggregate(params)
            elif operation == "group_by":
                return self._group_by_agg(params)
            elif operation == "moving_avg":
                return self._moving_average(params)
            elif operation == "cumsum":
                return self._cumsum(params)
            elif operation == "rank":
                return self._rank(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Aggregate error: {str(e)}")

    def _aggregate(self, params: Dict[str, Any]) -> ActionResult:
        """Aggregate data."""
        data = params.get("data", [])
        agg_operation = params.get("operation", "sum")
        key = params.get("key")

        result = self._aggregator.aggregate(data, agg_operation, key)

        return ActionResult(success=True, message=f"{agg_operation}: {result.value}", data={"operation": result.name, "value": result.value, "count": result.count})

    def _group_by_agg(self, params: Dict[str, Any]) -> ActionResult:
        """Group by and aggregate."""
        data = params.get("data", [])
        group_key = params.get("group_key")
        agg_key = params.get("agg_key")
        agg_operation = params.get("operation", "sum")

        if not group_key or not agg_key:
            return ActionResult(success=False, message="group_key and agg_key are required")

        results = self._group_by.aggregate(data, group_key, agg_key, agg_operation)

        return ActionResult(success=True, message=f"Grouped: {len(results)} groups", data={"results": results})

    def _moving_average(self, params: Dict[str, Any]) -> ActionResult:
        """Calculate moving average."""
        data = params.get("data", [])
        window = params.get("window", 3)

        result = WindowFunctions.moving_average(data, window)

        return ActionResult(success=True, message=f"Moving average: {len(result)} points", data={"result": result})

    def _cumsum(self, params: Dict[str, Any]) -> ActionResult:
        """Calculate cumulative sum."""
        data = params.get("data", [])

        result = WindowFunctions.cumulative_sum(data)

        return ActionResult(success=True, message=f"Cumulative sum: {len(result)} points", data={"result": result})

    def _rank(self, params: Dict[str, Any]) -> ActionResult:
        """Rank values."""
        data = params.get("data", [])

        result = WindowFunctions.rank(data)

        return ActionResult(success=True, message=f"Ranks: {len(result)} items", data={"result": result})

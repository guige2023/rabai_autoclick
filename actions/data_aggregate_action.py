"""Data Aggregate Action Module.

Provides data aggregation with grouping, pivoting, statistical
functions, and multi-dimensional analysis capabilities.
"""

from __future__ import annotations

import logging
import math
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class AggregateFunction(Enum):
    """Supported aggregation functions."""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    MEDIAN = "median"
    STDDEV = "stddev"
    VARIANCE = "variance"
    DISTINCT_COUNT = "distinct_count"
    PERCENTILE = "percentile"
    CONCAT = "concat"
    ARRAY_AGG = "array_agg"


@dataclass
class AggregationSpec:
    """Specification for an aggregation operation."""
    field: str
    function: AggregateFunction
    alias: Optional[str] = None
    percentile_value: Optional[float] = None

    @property
    def output_name(self) -> str:
        """Get the output field name."""
        if self.alias:
            return self.alias
        return f"{self.function.value}_{self.field}"


@dataclass
class GroupSpec:
    """Specification for grouping."""
    fields: List[str]
    having: Optional[Dict[str, Any]] = None


@dataclass
class AggregateResult:
    """Result of an aggregation operation."""
    groups: List[Dict[str, Any]]
    total_groups: int
    total_records: int
    aggregation_time_ms: float


class DataAggregator:
    """Performs data aggregation operations."""

    @staticmethod
    def _get_nested_value(item: Dict[str, Any], field: str) -> Any:
        """Get nested field value using dot notation."""
        parts = field.split(".")
        value = item
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            elif isinstance(value, (list, tuple)):
                try:
                    value = value[int(part)]
                except (ValueError, IndexError):
                    return None
            else:
                return None
            if value is None:
                return None
        return value

    @staticmethod
    def _group_items(
        items: List[Dict[str, Any]], group_fields: List[str]
    ) -> Dict[Tuple, List[Dict[str, Any]]]:
        """Group items by specified fields."""
        groups: Dict[Tuple, List[Dict[str, Any]]] = defaultdict(list)
        for item in items:
            if not isinstance(item, dict):
                continue
            key_parts = []
            for field in group_fields:
                value = DataAggregator._get_nested_value(item, field)
                key_parts.append(value if value is not None else "__null__")
            key = tuple(key_parts)
            groups[key].append(item)
        return groups

    @staticmethod
    def _calculate_aggregation(
        values: List[Any], func: AggregateFunction, percentile: Optional[float] = None
    ) -> Any:
        """Calculate aggregation on a list of values."""
        # Filter out None values
        values = [v for v in values if v is not None]
        if not values:
            return None

        try:
            if func == AggregateFunction.SUM:
                return sum(float(v) for v in values)
            elif func == AggregateFunction.COUNT:
                return len(values)
            elif func == AggregateFunction.AVG:
                return sum(float(v) for v in values) / len(values)
            elif func == AggregateFunction.MIN:
                return min(values)
            elif func == AggregateFunction.MAX:
                return max(values)
            elif func == AggregateFunction.FIRST:
                return values[0]
            elif func == AggregateFunction.LAST:
                return values[-1]
            elif func == AggregateFunction.MEDIAN:
                sorted_vals = sorted(values)
                n = len(sorted_vals)
                if n % 2 == 0:
                    return (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
                return sorted_vals[n // 2]
            elif func == AggregateFunction.STDDEV:
                if len(values) < 2:
                    return 0
                mean = sum(values) / len(values)
                variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
                return math.sqrt(variance)
            elif func == AggregateFunction.VARIANCE:
                if len(values) < 2:
                    return 0
                mean = sum(values) / len(values)
                return sum((v - mean) ** 2 for v in values) / (len(values) - 1)
            elif func == AggregateFunction.DISTINCT_COUNT:
                return len(set(values))
            elif func == AggregateFunction.PERCENTILE:
                if percentile is None:
                    percentile = 50
                sorted_vals = sorted(values)
                idx = (percentile / 100) * (len(sorted_vals) - 1)
                lower = int(idx)
                upper = min(lower + 1, len(sorted_vals) - 1)
                weight = idx - lower
                return sorted_vals[lower] * (1 - weight) + sorted_vals[upper] * weight
            elif func == AggregateFunction.CONCAT:
                return ",".join(str(v) for v in values)
            elif func == AggregateFunction.ARRAY_AGG:
                return values
            else:
                return None
        except (TypeError, ValueError):
            return None

    @classmethod
    def aggregate(
        cls,
        items: List[Dict[str, Any]],
        group_spec: Optional[GroupSpec],
        aggregations: List[AggregationSpec],
    ) -> Tuple[List[Dict[str, Any]], int]:
        """Perform aggregation operation."""
        if not items:
            return [], 0

        if group_spec is None or not group_spec.fields:
            # No grouping - aggregate all data
            result: Dict[str, Any] = {}
            for agg in aggregations:
                values = [cls._get_nested_value(item, agg.field) for item in items]
                result[agg.output_name] = cls._calculate_aggregation(
                    values, agg.function, agg.percentile_value
                )
            return [result], len(items)

        # Group and aggregate
        groups = cls._group_items(items, group_spec.fields)
        results = []

        for key, group_items in groups.items():
            row: Dict[str, Any] = {}

            # Add group key fields
            for i, field_name in enumerate(group_spec.fields):
                row[field_name] = key[i]

            # Calculate aggregations
            for agg in aggregations:
                values = [
                    cls._get_nested_value(item, agg.field) for item in group_items
                ]
                row[agg.output_name] = cls._calculate_aggregation(
                    values, agg.function, agg.percentile_value
                )

            results.append(row)

        # Sort results by group fields
        results.sort(key=lambda r: [r.get(f) for f in group_spec.fields])

        return results, len(items)


class DataAggregateAction(BaseAction):
    """Data Aggregate Action for grouping and aggregation.

    Supports multiple grouping fields, various aggregation functions,
    filtering, sorting, and pivot operations.

    Examples:
        >>> action = DataAggregateAction()
        >>> result = action.execute(ctx, {
        ...     "data": [
        ...         {"dept": "A", "sales": 100, "region": "North"},
        ...         {"dept": "A", "sales": 150, "region": "South"},
        ...     ],
        ...     "group_by": ["dept"],
        ...     "aggregations": [
        ...         {"field": "sales", "function": "sum"},
        ...         {"field": "sales", "function": "avg"},
        ...     ]
        ... })
    """

    action_type = "data_aggregate"
    display_name = "数据聚合"
    description = "分组聚合、统计计算、多维分析"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data aggregation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - data: List of dicts to aggregate
                - group_by: List of fields to group by
                - aggregations: List of aggregation specs
                - having: Filter groups by aggregated values (optional)
                - order_by: Fields to order results by (optional)
                - order_desc: Sort descending (default: False)
                - limit: Max groups to return (optional)

        Returns:
            ActionResult with aggregated data and stats.
        """
        import time
        start_time = time.time()

        data = params.get("data", [])
        group_by = params.get("group_by", [])
        aggregations_config = params.get("aggregations", [])
        having = params.get("having")
        order_by = params.get("order_by")
        order_desc = params.get("order_desc", False)
        limit = params.get("limit")

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message="'data' parameter must be a list"
            )

        # Parse aggregation specs
        aggregations = []
        for agg_cfg in aggregations_config:
            if isinstance(agg_cfg, AggregationSpec):
                aggregations.append(agg_cfg)
            else:
                agg = AggregationSpec(
                    field=agg_cfg["field"],
                    function=AggregateFunction(agg_cfg.get("function", "count")),
                    alias=agg_cfg.get("alias"),
                    percentile_value=agg_cfg.get("percentile_value"),
                )
                aggregations.append(agg)

        # Create group spec
        group_spec = GroupSpec(fields=group_by, having=having) if group_by else None

        # Perform aggregation
        results, total_records = DataAggregator.aggregate(
            data, group_spec, aggregations
        )

        # Apply having clause
        if having:
            results = self._apply_having(results, having)

        # Sort results
        if order_by:
            results = self._sort_results(results, order_by, order_desc)

        # Apply limit
        total_groups = len(results)
        if limit:
            results = results[:limit]

        duration_ms = (time.time() - start_time) * 1000

        return ActionResult(
            success=True,
            message=f"Aggregated {total_records} records into {total_groups} groups",
            data={
                "aggregated_data": results,
                "total_groups": total_groups,
                "total_records": total_records,
                "aggregation_time_ms": duration_ms,
            }
        )

    def _apply_having(
        self, groups: List[Dict[str, Any]], having: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Apply having clause to filter groups."""
        filtered = []
        for group in groups:
            for field_name, condition in having.items():
                if isinstance(condition, dict):
                    op = condition.get("op", "gt")
                    value = condition.get("value")
                    group_value = group.get(field_name)
                    if group_value is None:
                        continue
                    if op == "gt" and not (group_value > value):
                        break
                    elif op == "ge" and not (group_value >= value):
                        break
                    elif op == "lt" and not (group_value < value):
                        break
                    elif op == "le" and not (group_value <= value):
                        break
                    elif op == "eq" and not (group_value == value):
                        break
                    elif op == "ne" and not (group_value != value):
                        break
                else:
                    if group.get(field_name) != condition:
                        break
            else:
                filtered.append(group)
        return filtered

    def _sort_results(
        self, groups: List[Dict[str, Any]], order_by: List[str], descending: bool
    ) -> List[Dict[str, Any]]:
        """Sort results by specified fields."""
        def get_sort_key(item: Dict[str, Any]) -> Tuple:
            return tuple(item.get(f) for f in order_by)
        return sorted(groups, key=get_sort_key, reverse=descending)

    def pivot(
        self,
        data: List[Dict[str, Any]],
        index: List[str],
        columns: str,
        values: str,
        aggfunc: AggregateFunction = AggregateFunction.SUM,
    ) -> List[Dict[str, Any]]:
        """Create a pivot table from data."""
        pivot_data: Dict[Tuple, Dict[str, float]] = defaultdict(dict)
        column_values: Set[Any] = set()

        for item in data:
            if not isinstance(item, dict):
                continue
            idx_key = tuple(item.get(f) for f in index)
            col_val = item.get(columns)
            val = item.get(values)
            if col_val is not None:
                column_values.add(col_val)
                pivot_data[idx_key][col_val] = val

        results = []
        for idx_key, col_vals in pivot_data.items():
            row = dict(zip(index, idx_key))
            for col in column_values:
                row[f"{col}"] = col_vals.get(col)
            results.append(row)

        return results

    def get_required_params(self) -> List[str]:
        return ["data", "aggregations"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "group_by": [],
            "having": None,
            "order_by": None,
            "order_desc": False,
            "limit": None,
        }

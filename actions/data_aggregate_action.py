"""Data aggregation action module.

Provides group-by aggregation, pivot tables, and rollup operations
for lists of dicts with multiple aggregation functions.
"""

from __future__ import annotations

import logging
from typing import Optional, Dict, Any, List, Callable
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class AggFunction(Enum):
    """Aggregation functions."""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    LIST = "list"
    SET = "set"
    STDDEV = "stddev"
    MEDIAN = "median"


@dataclass
class AggregationSpec:
    """Specification for an aggregation operation."""
    field: str
    function: AggFunction
    output_name: Optional[str] = None


class DataAggregateAction:
    """Data aggregation engine.

    Provides group-by, pivot, and rollup aggregation operations.

    Example:
        data = [{"dept": "A", "salary": 100}, {"dept": "A", "salary": 200}, {"dept": "B", "salary": 150}]
        result = DataAggregateAction().group_by(data, "dept", [("salary", "sum")])
    """

    def group_by(
        self,
        data: List[Dict[str, Any]],
        group_key: str,
        aggregations: List[tuple],
        having: Optional[Callable[[Dict[str, Any]], bool]] = None,
    ) -> List[Dict[str, Any]]:
        """Group data and apply aggregations.

        Args:
            data: List of dicts to aggregate.
            group_key: Field to group by.
            aggregations: List of (field, function_name) tuples.
            having: Optional filter on aggregated results.

        Returns:
            List of aggregated dicts.
        """
        groups: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)

        for item in data:
            key_val = item.get(group_key)
            groups[key_val].append(item)

        result = []
        for key_val, items in groups.items():
            agg_result = {group_key: key_val}

            for field_name, func_name in aggregations:
                values = [item.get(field_name) for item in items if item.get(field_name) is not None]
                output_field = f"{func_name}_{field_name}"
                agg_result[output_field] = self._apply_func(values, func_name)

            if having is None or having(agg_result):
                result.append(agg_result)

        return result

    def group_by_multiple(
        self,
        data: List[Dict[str, Any]],
        group_keys: List[str],
        aggregations: List[tuple],
        having: Optional[Callable[[Dict[str, Any]], bool]] = None,
    ) -> List[Dict[str, Any]]:
        """Group by multiple keys.

        Args:
            data: List of dicts to aggregate.
            group_keys: Fields to group by.
            aggregations: List of (field, function_name) tuples.
            having: Optional filter on aggregated results.

        Returns:
            List of aggregated dicts.
        """
        groups: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)

        for item in data:
            key_tuple = tuple(item.get(k) for k in group_keys)
            groups[key_tuple].append(item)

        result = []
        for key_tuple, items in groups.items():
            agg_result = dict(zip(group_keys, key_tuple))

            for field_name, func_name in aggregations:
                values = [item.get(field_name) for item in items if item.get(field_name) is not None]
                output_field = f"{func_name}_{field_name}"
                agg_result[output_field] = self._apply_func(values, func_name)

            if having is None or having(agg_result):
                result.append(agg_result)

        return result

    def pivot(
        self,
        data: List[Dict[str, Any]],
        index: str,
        columns: str,
        values: str,
        aggfunc: str = "sum",
    ) -> List[Dict[str, Any]]:
        """Create a pivot table.

        Args:
            data: List of dicts.
            index: Field to use as row index.
            columns: Field to use as column headers.
            values: Field to aggregate.
            aggfunc: Aggregation function name.

        Returns:
            Pivot table as list of dicts.
        """
        pivot: Dict[Any, Dict[Any, Any]] = defaultdict(dict)

        for item in data:
            row_key = item.get(index)
            col_key = item.get(columns)
            val = item.get(values)

            if row_key is not None and col_key is not None:
                pivot[row_key][col_key] = val

        result = []
        all_cols = set()
        for row in pivot.values():
            all_cols.update(row.keys())

        for row_key, cols in pivot.items():
            row_result = {index: row_key}
            for col in all_cols:
                row_result[str(col)] = cols.get(col)
            result.append(row_result)

        return result

    def rollup(
        self,
        data: List[Dict[str, Any]],
        group_keys: List[str],
        aggregations: List[tuple],
    ) -> List[Dict[str, Any]]:
        """Compute hierarchical rollup (including subtotals).

        Args:
            data: List of dicts.
            group_keys: Keys for hierarchical grouping (e.g., ["region", "dept"]).
            aggregations: List of (field, function_name) tuples.

        Returns:
            List with rollup rows including subtotals.
        """
        result = []

        result.append(self._compute_totals(data, group_keys[0] if group_keys else "", aggregations))

        for i in range(len(group_keys)):
            groups: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
            for item in data:
                key_val = item.get(group_keys[i])
                groups[key_val].append(item)

            for key_val, items in groups.items():
                row = {group_keys[i]: key_val}
                for j in range(i + 1, len(group_keys)):
                    row[group_keys[j]] = "(all)"
                for field_name, func_name in aggregations:
                    values = [item.get(field_name) for item in items if item.get(field_name) is not None]
                    row[f"{func_name}_{field_name}"] = self._apply_func(values, func_name)
                result.append(row)

        return result

    def window(
        self,
        data: List[Dict[str, Any]],
        partition_by: Optional[str] = None,
        order_by: Optional[str] = None,
        window_size: int = 0,
        aggregations: List[tuple],
    ) -> List[Dict[str, Any]]:
        """Compute window functions over data.

        Args:
            data: List of dicts.
            partition_by: Field to partition by.
            order_by: Field to order by within partition.
            window_size: Window size (0 = all rows).
            aggregations: List of (field, function_name) tuples.

        Returns:
            Data with added window aggregate columns.
        """
        if order_by:
            data = sorted(data, key=lambda x: x.get(order_by, ""))

        if partition_by:
            partitions: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
            for item in data:
                partitions[item.get(partition_by)].append(item)
        else:
            partitions = {"__all__": data}

        result = []
        for partition in partitions.values():
            for i, item in enumerate(partition):
                new_item = dict(item)
                start = max(0, i - window_size)
                end = min(len(partition), i + window_size + 1)
                window_data = partition[start:end]

                for field_name, func_name in aggregations:
                    values = [d.get(field_name) for d in window_data if d.get(field_name) is not None]
                    output_field = f"{func_name}_{field_name}_window"
                    new_item[output_field] = self._apply_func(values, func_name)

                result.append(new_item)

        return result

    def _compute_totals(
        self,
        data: List[Dict[str, Any]],
        label: str,
        aggregations: List[tuple],
    ) -> Dict[str, Any]:
        """Compute total row for rollup."""
        result = {label: "(total)"}
        for field_name, func_name in aggregations:
            values = [item.get(field_name) for item in data if item.get(field_name) is not None]
            result[f"{func_name}_{field_name}"] = self._apply_func(values, func_name)
        return result

    def _apply_func(self, values: List, func_name: str) -> Any:
        """Apply an aggregation function to a list of values."""
        if not values:
            return None

        try:
            if func_name == "sum":
                return sum(values)
            elif func_name == "count":
                return len(values)
            elif func_name == "avg":
                return sum(values) / len(values)
            elif func_name == "min":
                return min(values)
            elif func_name == "max":
                return max(values)
            elif func_name == "first":
                return values[0]
            elif func_name == "last":
                return values[-1]
            elif func_name == "list":
                return values
            elif func_name == "set":
                return list(set(values))
            elif func_name == "stddev":
                import statistics
                return statistics.stdev(values) if len(values) > 1 else 0
            elif func_name == "median":
                import statistics
                return statistics.median(values)
            return values
        except (ValueError, TypeError, StatisticsError):
            return None

"""
Data Aggregation Action - Aggregates data with grouping and statistics.

This module provides data aggregation capabilities including group-by,
rollup, cube operations, and statistical aggregations.
"""

from __future__ import annotations

import statistics
import math
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar
from enum import Enum
from collections import defaultdict


T = TypeVar("T")


class AggregationFunction(Enum):
    """Aggregation functions."""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    MEDIAN = "median"
    STD_DEV = "std_dev"
    VARIANCE = "variance"
    DISTINCT = "distinct"


@dataclass
class GroupBySpec:
    """Specification for group-by operation."""
    fields: list[str]
    aggregations: list[tuple[str, AggregationFunction, str]]
    

@dataclass
class AggregationResult:
    """Result of aggregation operation."""
    groups: list[dict[str, Any]]
    group_keys: list[dict[str, Any]]
    total_records: int
    group_count: int


class DataAggregator:
    """
    Aggregates data with various grouping strategies.
    
    Example:
        aggregator = DataAggregator()
        result = aggregator.aggregate(
            records,
            ["category"],
            [("amount", AggregationFunction.SUM, "total_amount")]
        )
    """
    
    def __init__(self) -> None:
        self._functions = {
            AggregationFunction.SUM: self._sum,
            AggregationFunction.COUNT: self._count,
            AggregationFunction.AVG: self._avg,
            AggregationFunction.MIN: self._min,
            AggregationFunction.MAX: self._max,
            AggregationFunction.FIRST: self._first,
            AggregationFunction.LAST: self._last,
            AggregationFunction.MEDIAN: self._median,
            AggregationFunction.STD_DEV: self._std_dev,
            AggregationFunction.VARIANCE: self._variance,
            AggregationFunction.DISTINCT: self._distinct,
        }
    
    def aggregate(
        self,
        data: list[dict[str, Any]],
        group_fields: list[str],
        aggregations: list[tuple[str, AggregationFunction, str]],
    ) -> AggregationResult:
        """
        Aggregate data by grouping fields.
        
        Args:
            data: List of records
            group_fields: Fields to group by
            aggregations: List of (source_field, function, output_field)
            
        Returns:
            AggregationResult with grouped data
        """
        groups: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
        
        for record in data:
            key = tuple(self._get_nested(record, f) for f in group_fields)
            groups[key].append(record)
        
        results: list[dict[str, Any]] = []
        group_keys: list[dict[str, Any]] = []
        
        for key, group_data in groups.items():
            result: dict[str, Any] = {}
            
            for field_name, key_part in zip(group_fields, key):
                result[field_name] = key_part
            
            for source_field, agg_func, output_field in aggregations:
                values = [self._get_nested(r, source_field) for r in group_data]
                values = [v for v in values if v is not None]
                
                func = self._functions.get(agg_func, self._count)
                result[output_field] = func(values)
            
            results.append(result)
            group_keys.append(dict(zip(group_fields, key)))
        
        return AggregationResult(
            groups=results,
            group_keys=group_keys,
            total_records=len(data),
            group_count=len(results),
        )
    
    def _sum(self, values: list[Any]) -> float:
        """Sum of values."""
        return sum(v for v in values if isinstance(v, (int, float)))
    
    def _count(self, values: list[Any]) -> int:
        """Count of values."""
        return len(values)
    
    def _avg(self, values: list[Any]) -> float | None:
        """Average of values."""
        numeric = [v for v in values if isinstance(v, (int, float))]
        return sum(numeric) / len(numeric) if numeric else None
    
    def _min(self, values: list[Any]) -> Any:
        """Minimum value."""
        filtered = [v for v in values if v is not None]
        return min(filtered) if filtered else None
    
    def _max(self, values: list[Any]) -> Any:
        """Maximum value."""
        filtered = [v for v in values if v is not None]
        return max(filtered) if filtered else None
    
    def _first(self, values: list[Any]) -> Any:
        """First value."""
        return values[0] if values else None
    
    def _last(self, values: list[Any]) -> Any:
        """Last value."""
        return values[-1] if values else None
    
    def _median(self, values: list[Any]) -> float | None:
        """Median of values."""
        numeric = [v for v in values if isinstance(v, (int, float))]
        if not numeric:
            return None
        sorted_vals = sorted(numeric)
        n = len(sorted_vals)
        if n % 2 == 0:
            return (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
        return sorted_vals[n // 2]
    
    def _std_dev(self, values: list[Any]) -> float | None:
        """Standard deviation."""
        numeric = [v for v in values if isinstance(v, (int, float))]
        if len(numeric) < 2:
            return None
        return statistics.stdev(numeric)
    
    def _variance(self, values: list[Any]) -> float | None:
        """Variance."""
        numeric = [v for v in values if isinstance(v, (int, float))]
        if len(numeric) < 2:
            return None
        return statistics.variance(numeric)
    
    def _distinct(self, values: list[Any]) -> int:
        """Count of distinct values."""
        return len(set(values))
    
    def _get_nested(self, data: dict[str, Any], path: str) -> Any:
        """Get nested value using dot notation."""
        keys = path.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
        return current


class RollupAggregator:
    """Performs rollup aggregations (hierarchical grouping)."""
    
    def __init__(self) -> None:
        self._aggregator = DataAggregator()
    
    def rollup(
        self,
        data: list[dict[str, Any]],
        hierarchy_fields: list[str],
        aggregations: list[tuple[str, AggregationFunction, str]],
    ) -> list[dict[str, Any]]:
        """
        Perform rollup aggregation across hierarchy levels.
        
        Example: Rollup on [year, quarter, month] produces:
        - year totals
        - year/quarter totals
        - year/quarter/month totals
        """
        results = []
        
        for level in range(len(hierarchy_fields)):
            group_fields = hierarchy_fields[: len(hierarchy_fields) - level]
            
            if not group_fields:
                continue
            
            result = self._aggregator.aggregate(data, group_fields, aggregations)
            
            for group_result in result.groups:
                group_result["_rollup_level"] = len(group_fields)
                results.append(group_result)
        
        return results


class DataAggregationAction:
    """
    Data aggregation action for automation workflows.
    
    Example:
        action = DataAggregationAction()
        result = await action.aggregate(
            sales_records,
            group_by=["region", "product"],
            aggregations=[
                ("amount", AggregationFunction.SUM, "total_sales"),
                ("quantity", AggregationFunction.AVG, "avg_quantity"),
            ]
        )
    """
    
    def __init__(self) -> None:
        self.aggregator = DataAggregator()
        self.rollup_aggregator = RollupAggregator()
    
    async def aggregate(
        self,
        data: list[dict[str, Any]],
        group_by: list[str],
        aggregations: list[tuple[str, str, str]],
    ) -> AggregationResult:
        """Aggregate data with grouping."""
        agg_specs = [
            (field, AggregationFunction(agg.upper()), output)
            for field, agg, output in aggregations
        ]
        return self.aggregator.aggregate(data, group_by, agg_specs)
    
    async def rollup(
        self,
        data: list[dict[str, Any]],
        hierarchy: list[str],
        aggregations: list[tuple[str, str, str]],
    ) -> list[dict[str, Any]]:
        """Perform rollup aggregation."""
        agg_specs = [
            (field, AggregationFunction(agg.upper()), output)
            for field, agg, output in aggregations
        ]
        return self.rollup_aggregator.rollup(data, hierarchy, agg_specs)


# Export public API
__all__ = [
    "AggregationFunction",
    "GroupBySpec",
    "AggregationResult",
    "DataAggregator",
    "RollupAggregator",
    "DataAggregationAction",
]

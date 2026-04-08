# Copyright (c) 2024. coded by claude
"""Data Aggregator Action Module.

Provides data aggregation utilities for API responses including
sum, average, count, min, max, and custom aggregation functions.
"""
from typing import Optional, Dict, Any, List, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
import statistics
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class AggregationFunction(Enum):
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    MEDIAN = "median"
    STDDEV = "stddev"
    CUSTOM = "custom"


@dataclass
class AggregationConfig:
    group_by: Optional[List[str]] = None
    metrics: Dict[str, AggregationFunction] = field(default_factory=dict)
    having: Optional[Callable[[Dict[str, Any]], bool]] = None


@dataclass
class AggregationResult:
    groups: List[Dict[str, Any]]
    total_records: int
    groups_count: int


class DataAggregator:
    def __init__(self, config: Optional[AggregationConfig] = None):
        self.config = config or AggregationConfig()

    def aggregate(self, data: List[Dict[str, Any]]) -> AggregationResult:
        if not data:
            return AggregationResult(groups=[], total_records=0, groups_count=0)

        if not self.config.group_by:
            return self._aggregate_all(data)

        return self._aggregate_groups(data)

    def _aggregate_all(self, data: List[Dict[str, Any]]) -> AggregationResult:
        result: Dict[str, Any] = {"_group": "all"}
        for field_name, func in self.config.metrics.items():
            values = [item.get(field_name, 0) for item in data if field_name in item]
            result[field_name] = self._apply_function(values, func)
        return AggregationResult(
            groups=[result],
            total_records=len(data),
            groups_count=1,
        )

    def _aggregate_groups(self, data: List[Dict[str, Any]]) -> AggregationResult:
        groups: Dict[Tuple, List[Dict[str, Any]]] = {}
        for item in data:
            key = tuple(item.get(k) for k in self.config.group_by)
            if key not in groups:
                groups[key] = []
            groups[key].append(item)

        results: List[Dict[str, Any]] = []
        for key, items in groups.items():
            group_result: Dict[str, Any] = dict(zip(self.config.group_by, key))
            for field_name, func in self.config.metrics.items():
                values = [item.get(field_name, 0) for item in items if field_name in item]
                group_result[field_name] = self._apply_function(values, func)
            if self.config.having is None or self.config.having(group_result):
                results.append(group_result)

        return AggregationResult(
            groups=results,
            total_records=len(data),
            groups_count=len(results),
        )

    def _apply_function(self, values: List[Any], func: AggregationFunction) -> Any:
        if not values:
            return None
        numeric_values = [v for v in values if isinstance(v, (int, float))]
        if not numeric_values:
            return values[0] if values else None
        try:
            if func == AggregationFunction.SUM:
                return sum(numeric_values)
            elif func == AggregationFunction.AVG:
                return statistics.mean(numeric_values)
            elif func == AggregationFunction.COUNT:
                return len(values)
            elif func == AggregationFunction.MIN:
                return min(numeric_values)
            elif func == AggregationFunction.MAX:
                return max(numeric_values)
            elif func == AggregationFunction.MEDIAN:
                return statistics.median(numeric_values)
            elif func == AggregationFunction.STDDEV:
                return statistics.stdev(numeric_values) if len(numeric_values) > 1 else 0
            return numeric_values[0]
        except Exception:
            return numeric_values[0] if numeric_values else None

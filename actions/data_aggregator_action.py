"""Data Aggregator Action Module.

Provides data aggregation with support for grouping, filtering,
sorting, and computing statistical measures.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class AggregationType(Enum):
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    DISTINCT = "distinct"
    CUSTOM = "custom"


@dataclass
class AggregationConfig:
    group_by: List[str] = field(default_factory=list)
    aggregations: Dict[str, List[AggregationType]] = field(default_factory=dict)
    filters: Optional[Callable[[Dict[str, Any]], bool]] = None
    sort_by: Optional[str] = None
    sort_reverse: bool = False
    limit: Optional[int] = None
    having: Optional[Callable[[Dict[str, Any]], bool]] = None


@dataclass
class AggregationResult:
    groups: List[Dict[str, Any]] = field(default_factory=list)
    total_count: int = 0
    aggregation_config: AggregationConfig = field(default_factory=dict)
    computed_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataAggregator:
    def __init__(self, config: Optional[AggregationConfig] = None):
        self.config = config or AggregationConfig()

    def aggregate(self, data: List[Dict[str, Any]]) -> AggregationResult:
        if not data:
            return AggregationResult(groups=[], total_count=0)

        filtered_data = data
        if self.config.filters:
            filtered_data = [d for d in filtered_data if self.config.filters(d)]

        if not self.config.group_by:
            result = self._aggregate_single(filtered_data)
            return AggregationResult(
                groups=[result],
                total_count=len(filtered_data),
                aggregation_config=self.config,
            )

        grouped = defaultdict(list)
        for item in filtered_data:
            key = tuple(item.get(field, None) for field in self.config.group_by)
            grouped[key].append(item)

        results = []
        for key, items in grouped.items():
            group_result = dict(zip(self.config.group_by, key))

            for field_name, agg_types in self.config.aggregations.items():
                values = [item.get(field_name) for item in items if field_name in item]
                for agg_type in agg_types:
                    result_key = f"{field_name}_{agg_type.value}"
                    group_result[result_key] = self._compute_aggregation(values, agg_type)

            results.append(group_result)

        if self.config.having:
            results = [r for r in results if self.config.having(r)]

        if self.config.sort_by:
            results.sort(key=lambda r: r.get(self.config.sort_by, 0), reverse=self.config.sort_reverse)

        if self.config.limit:
            results = results[:self.config.limit]

        return AggregationResult(
            groups=results,
            total_count=len(results),
            aggregation_config=self.config,
        )

    def _aggregate_single(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        result = {}
        for field_name, agg_types in self.config.aggregations.items():
            values = [item.get(field_name) for item in data if field_name in item]
            for agg_type in agg_types:
                result_key = f"{field_name}_{agg_type.value}"
                result[result_key] = self._compute_aggregation(values, agg_type)
        return result

    def _compute_aggregation(self, values: List[Any], agg_type: AggregationType) -> Any:
        if not values:
            return None

        numeric_values = [v for v in values if isinstance(v, (int, float))]
        non_none_values = [v for v in values if v is not None]

        if agg_type == AggregationType.SUM:
            return sum(numeric_values) if numeric_values else 0

        elif agg_type == AggregationType.COUNT:
            return len(values)

        elif agg_type == AggregationType.AVG:
            return sum(numeric_values) / len(numeric_values) if numeric_values else 0

        elif agg_type == AggregationType.MIN:
            return min(non_none_values) if non_none_values else None

        elif agg_type == AggregationType.MAX:
            return max(non_none_values) if non_none_values else None

        elif agg_type == AggregationType.FIRST:
            return values[0]

        elif agg_type == AggregationType.LAST:
            return values[-1]

        elif agg_type == AggregationType.DISTINCT:
            return len(set(non_none_values))

        return None


def group_by_field(data: List[Dict[str, Any]], field: str) -> Dict[Any, List[Dict[str, Any]]]:
    grouped = defaultdict(list)
    for item in data:
        key = item.get(field)
        grouped[key].append(item)
    return dict(grouped)


def compute_stats(values: List[float]) -> Dict[str, float]:
    if not values:
        return {"count": 0, "sum": 0, "avg": 0, "min": 0, "max": 0}

    sorted_values = sorted(values)
    n = len(values)

    return {
        "count": n,
        "sum": sum(values),
        "avg": sum(values) / n,
        "min": min(values),
        "max": max(values),
        "median": sorted_values[n // 2] if n % 2 else (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2,
        "p25": sorted_values[n // 4],
        "p75": sorted_values[3 * n // 4],
    }

"""
Data Aggregator Action Module.

Collects and aggregates data from multiple sources with configurable
 grouping, filtering, and transformation pipelines.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, TypeVar, Union
from dataclasses import dataclass, field
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")
U = TypeVar("U")


class AggregationFunc(Enum):
    """Built-in aggregation functions."""
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    FIRST = "first"
    LAST = "last"
    LIST = "list"
    DICT = "dict"
    CUSTOM = "custom"


@dataclass
class AggregationConfig:
    """Configuration for a single aggregation."""
    group_by: Optional[str] = None
    field: Optional[str] = None
    agg_func: AggregationFunc = AggregationFunc.COUNT
    custom_func: Optional[Callable[[list], Any]] = None
    alias: Optional[str] = None


@dataclass
class AggregatedResult:
    """Result of an aggregation operation."""
    groups: dict[Any, dict[str, Any]]
    total_count: int
    fields_aggregated: list[str]


class DataAggregatorAction:
    """
    Multi-source data aggregation with flexible pipelines.

    Collects data from multiple sources, groups by keys, applies
    aggregations, and returns consolidated results.

    Example:
        aggregator = DataAggregatorAction()
        aggregator.add_source(users_list, name="users")
        aggregator.add_source(orders_list, name="orders")
        result = aggregator.aggregate([
            AggregationConfig(group_by="region", field="revenue", agg_func=AggregationFunc.SUM),
        ])
    """

    def __init__(self) -> None:
        self._sources: dict[str, list[dict[str, Any]]] = {}
        self._source_names: list[str] = []
        self._filters: list[Callable[[dict[str, Any]], bool]] = []

    def add_source(
        self,
        data: list[dict[str, Any]],
        name: str,
        key_field: Optional[str] = None,
    ) -> "DataAggregatorAction":
        """Add a data source to aggregate."""
        self._sources[name] = data
        self._source_names.append(name)
        return self

    def add_filter(
        self,
        filter_func: Callable[[dict[str, Any]], bool],
    ) -> "DataAggregatorAction":
        """Add a filter function applied to each record."""
        self._filters.append(filter_func)
        return self

    def filter_by_field(
        self,
        field: str,
        value: Any,
        op: str = "eq",
    ) -> "DataAggregatorAction":
        """Add a simple field-based filter."""
        def make_filter(f: str, v: Any, op: str) -> Callable[[dict[str, Any]], bool]:
            ops = {
                "eq": lambda r: r.get(f) == v,
                "ne": lambda r: r.get(f) != v,
                "gt": lambda r: r.get(f, 0) > v,
                "ge": lambda r: r.get(f, 0) >= v,
                "lt": lambda r: r.get(f, 0) < v,
                "le": lambda r: r.get(f, 0) <= v,
                "in": lambda r: r.get(f) in v,
                "contains": lambda r: v in str(r.get(f, "")),
            }
            return ops.get(op, ops["eq"])

        self._filters.append(make_filter(field, value, op))
        return self

    def aggregate(
        self,
        configs: list[AggregationConfig],
    ) -> AggregatedResult:
        """Execute aggregation on all sources."""
        all_records = self._collect_records()
        filtered_records = self._apply_filters(all_records)

        groups: dict[Any, list[dict[str, Any]]] = defaultdict(list)

        for record in filtered_records:
            if configs:
                key = self._extract_group_key(record, configs[0].group_by)
            else:
                key = "_global"
            groups[key].append(record)

        results: dict[Any, dict[str, Any]] = {}
        for group_key, group_records in groups.items():
            results[group_key] = self._aggregate_group(group_records, configs)

        return AggregatedResult(
            groups=results,
            total_count=len(filtered_records),
            fields_aggregated=[c.alias or c.field or str(c.agg_func.value) for c in configs],
        )

    def _collect_records(self) -> list[dict[str, Any]]:
        """Collect and merge records from all sources."""
        records = []
        for source_name in self._source_names:
            for record in self._sources.get(source_name, []):
                record["_source"] = source_name
                records.append(record)
        return records

    def _apply_filters(self, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Apply all filters to records."""
        filtered = records
        for f in self._filters:
            filtered = [r for r in filtered if f(r)]
        return filtered

    def _extract_group_key(self, record: dict[str, Any], group_by: Optional[str]) -> Any:
        """Extract grouping key from record."""
        if not group_by:
            return "_global"
        return record.get(group_by, "_none")

    def _aggregate_group(
        self,
        records: list[dict[str, Any]],
        configs: list[AggregationConfig],
    ) -> dict[str, Any]:
        """Aggregate a group of records."""
        result: dict[str, Any] = {"_count": len(records)}

        for config in configs:
            alias = config.alias or config.field or config.agg_func.value
            if config.field:
                values = [r.get(config.field) for r in records if config.field in r]
            else:
                values = list(records)

            result[alias] = self._apply_aggregation(
                values, config.agg_func, config.custom_func
            )

        return result

    def _apply_aggregation(
        self,
        values: list[Any],
        func: AggregationFunc,
        custom_func: Optional[Callable[[list], Any]] = None,
    ) -> Any:
        """Apply an aggregation function to values."""
        if not values:
            return None

        numeric_values = [v for v in values if isinstance(v, (int, float))]

        if func == AggregationFunc.SUM:
            return sum(numeric_values) if numeric_values else None
        elif func == AggregationFunc.AVG:
            return sum(numeric_values) / len(numeric_values) if numeric_values else None
        elif func == AggregationFunc.MIN:
            return min(numeric_values) if numeric_values else None
        elif func == AggregationFunc.MAX:
            return max(numeric_values) if numeric_values else None
        elif func == AggregationFunc.COUNT:
            return len(values)
        elif func == AggregationFunc.FIRST:
            return values[0]
        elif func == AggregationFunc.LAST:
            return values[-1]
        elif func == AggregationFunc.LIST:
            return values
        elif func == AggregationFunc.DICT:
            return {str(i): v for i, v in enumerate(values)}
        elif func == AggregationFunc.CUSTOM and custom_func:
            return custom_func(values)

        return None

    def clear_sources(self) -> None:
        """Clear all data sources."""
        self._sources.clear()
        self._source_names.clear()

    def get_source(self, name: str) -> Optional[list[dict[str, Any]]]:
        """Get records from a specific source."""
        return self._sources.get(name)

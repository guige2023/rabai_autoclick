"""
Data Aggregation Action Module.

Provides data aggregation capabilities including
sum, average, count, min, max, and custom aggregations.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class AggregationType(Enum):
    """Aggregation types."""
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    DISTINCT = "distinct"
    CUSTOM = "custom"


@dataclass
class AggregationConfig:
    """Aggregation configuration."""
    field: str
    aggregation_type: AggregationType
    alias: Optional[str] = None
    filter_func: Optional[Callable] = None


@dataclass
class AggregationResult:
    """Result of aggregation."""
    field: str
    aggregation_type: AggregationType
    value: Any
    record_count: int = 0


class DataAggregator:
    """Aggregates data based on configuration."""

    def __init__(self):
        self.aggregations: List[AggregationConfig] = []

    def add_aggregation(
        self,
        field: str,
        aggregation_type: AggregationType,
        alias: Optional[str] = None
    ):
        """Add an aggregation."""
        config = AggregationConfig(
            field=field,
            aggregation_type=aggregation_type,
            alias=alias
        )
        self.aggregations.append(config)

    def _sum(self, values: List[Any]) -> float:
        """Calculate sum."""
        numeric = [v for v in values if isinstance(v, (int, float))]
        return sum(numeric) if numeric else 0

    def _avg(self, values: List[Any]) -> float:
        """Calculate average."""
        numeric = [v for v in values if isinstance(v, (int, float))]
        return sum(numeric) / len(numeric) if numeric else 0

    def _count(self, values: List[Any]) -> int:
        """Count non-null values."""
        return sum(1 for v in values if v is not None)

    def _min(self, values: List[Any]) -> Any:
        """Find minimum."""
        non_null = [v for v in values if v is not None]
        return min(non_null) if non_null else None

    def _max(self, values: List[Any]) -> Any:
        """Find maximum."""
        non_null = [v for v in values if v is not None]
        return max(non_null) if non_null else None

    def _first(self, values: List[Any]) -> Any:
        """Get first value."""
        return values[0] if values else None

    def _last(self, values: List[Any]) -> Any:
        """Get last value."""
        return values[-1] if values else None

    def _distinct(self, values: List[Any]) -> Set[Any]:
        """Get distinct values."""
        return set(values)

    def _aggregate_single(
        self,
        records: List[Dict[str, Any]],
        config: AggregationConfig
    ) -> AggregationResult:
        """Aggregate a single field."""
        values = []

        for record in records:
            if config.filter_func and not config.filter_func(record):
                continue

            value = record.get(config.field)
            values.append(value)

        agg_type = config.aggregation_type

        if agg_type == AggregationType.SUM:
            result_value = self._sum(values)
        elif agg_type == AggregationType.AVG:
            result_value = self._avg(values)
        elif agg_type == AggregationType.COUNT:
            result_value = self._count(values)
        elif agg_type == AggregationType.MIN:
            result_value = self._min(values)
        elif agg_type == AggregationType.MAX:
            result_value = self._max(values)
        elif agg_type == AggregationType.FIRST:
            result_value = self._first(values)
        elif agg_type == AggregationType.LAST:
            result_value = self._last(values)
        elif agg_type == AggregationType.DISTINCT:
            result_value = len(self._distinct(values))
        else:
            result_value = None

        return AggregationResult(
            field=config.field,
            aggregation_type=agg_type,
            value=result_value,
            record_count=len(values)
        )

    def aggregate(self, records: List[Dict[str, Any]]) -> List[AggregationResult]:
        """Aggregate all configured fields."""
        results = []
        for config in self.aggregations:
            result = self._aggregate_single(records, config)
            results.append(result)
        return results

    def group_and_aggregate(
        self,
        records: List[Dict[str, Any]],
        group_by: List[str]
    ) -> Dict[Tuple, List[AggregationResult]]:
        """Group records and aggregate within each group."""
        groups: Dict[Tuple, List[Dict[str, Any]]] = {}

        for record in records:
            key = tuple(record.get(field) for field in group_by)
            if key not in groups:
                groups[key] = []
            groups[key].append(record)

        results = {}
        for key, group_records in groups.items():
            results[key] = self.aggregate(group_records)

        return results


class StreamingAggregator:
    """Aggregates data in streaming fashion."""

    def __init__(self):
        self.accumulators: Dict[str, List[Any]] = {}

    def add(self, field: str, value: Any):
        """Add a value to accumulator."""
        if field not in self.accumulators:
            self.accumulators[field] = []
        self.accumulators[field].append(value)

    def get_result(self, field: str, agg_type: AggregationType) -> Any:
        """Get aggregation result."""
        values = self.accumulators.get(field, [])
        aggregator = DataAggregator()

        if agg_type == AggregationType.SUM:
            return aggregator._sum(values)
        elif agg_type == AggregationType.AVG:
            return aggregator._avg(values)
        elif agg_type == AggregationType.COUNT:
            return aggregator._count(values)
        elif agg_type == AggregationType.MIN:
            return aggregator._min(values)
        elif agg_type == AggregationType.MAX:
            return aggregator._max(values)
        elif agg_type == AggregationType.FIRST:
            return aggregator._first(values)
        elif agg_type == AggregationType.LAST:
            return aggregator._last(values)
        elif agg_type == AggregationType.DISTINCT:
            return len(aggregator._distinct(values))

        return None


def main():
    """Demonstrate data aggregation."""
    aggregator = DataAggregator()
    aggregator.add_aggregation("price", AggregationType.SUM)
    aggregator.add_aggregation("price", AggregationType.AVG)
    aggregator.add_aggregation("price", AggregationType.MIN)
    aggregator.add_aggregation("price", AggregationType.MAX)
    aggregator.add_aggregation("category", AggregationType.DISTINCT)

    records = [
        {"product": "A", "price": 100, "category": "electronics"},
        {"product": "B", "price": 200, "category": "electronics"},
        {"product": "C", "price": 50, "category": "books"},
    ]

    results = aggregator.aggregate(records)
    for r in results:
        print(f"{r.field} {r.aggregation_type.value}: {r.value}")


if __name__ == "__main__":
    main()

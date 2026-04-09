"""
Data Aggregation Action Module.

Real-time data aggregation with grouping, filtering,
windowing, and multiple aggregation functions.
"""

from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar, Optional
from enum import Enum
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)
T = TypeVar("T")


class AggregationType(Enum):
    """Aggregation function types."""
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
    """
    Configuration for a single aggregation.

    Attributes:
        name: Output field name.
        agg_type: Type of aggregation.
        field_name: Source field to aggregate.
        filter_func: Optional filter condition.
        custom_func: Custom aggregation function.
    """
    name: str
    agg_type: AggregationType
    field_name: Optional[str] = None
    filter_func: Optional[Callable[[Any], bool]] = None
    custom_func: Optional[Callable[[list], Any]] = None


@dataclass
class AggregationResult:
    """Result of an aggregation operation."""
    group_key: Any
    values: dict[str, Any]
    record_count: int


class DataAggregationAction(Generic[T]):
    """
    Data aggregation with support for grouping and multiple aggregation types.

    Example:
        aggregator = DataAggregationAction[dict]()
        aggregator.group_by("category")
        aggregator.add_aggregation("total", AggregationType.SUM, "amount")
        aggregator.add_aggregation("count", AggregationType.COUNT)
        result = aggregator.compute(data_records)
    """

    def __init__(self):
        """Initialize data aggregation action."""
        self.group_by_fields: list[str] = []
        self.aggregations: list[AggregationConfig] = []
        self._groups: dict = {}

    def group_by(self, *fields: str) -> "DataAggregationAction":
        """
        Set grouping fields.

        Args:
            *fields: Field names to group by.

        Returns:
            Self for method chaining.
        """
        self.group_by_fields = list(fields)
        return self

    def add_aggregation(
        self,
        name: str,
        agg_type: AggregationType,
        field_name: Optional[str] = None,
        filter_func: Optional[Callable] = None,
        custom_func: Optional[Callable] = None
    ) -> "DataAggregationAction":
        """
        Add an aggregation configuration.

        Args:
            name: Output field name.
            agg_type: Aggregation type.
            field_name: Source field for aggregation.
            filter_func: Optional filter.
            custom_func: Custom aggregation function.

        Returns:
            Self for method chaining.
        """
        config = AggregationConfig(
            name=name,
            agg_type=agg_type,
            field_name=field_name,
            filter_func=filter_func,
            custom_func=custom_func
        )
        self.aggregations.append(config)
        return self

    def sum(self, field_name: str, output_name: Optional[str] = None) -> "DataAggregationAction":
        """Add SUM aggregation."""
        name = output_name or f"{field_name}_sum"
        self.add_aggregation(name, AggregationType.SUM, field_name)
        return self

    def avg(self, field_name: str, output_name: Optional[str] = None) -> "DataAggregationAction":
        """Add AVG aggregation."""
        name = output_name or f"{field_name}_avg"
        self.add_aggregation(name, AggregationType.AVG, field_name)
        return self

    def min(self, field_name: str, output_name: Optional[str] = None) -> "DataAggregationAction":
        """Add MIN aggregation."""
        name = output_name or f"{field_name}_min"
        self.add_aggregation(name, AggregationType.MIN, field_name)
        return self

    def max(self, field_name: str, output_name: Optional[str] = None) -> "DataAggregationAction":
        """Add MAX aggregation."""
        name = output_name or f"{field_name}_max"
        self.add_aggregation(name, AggregationType.MAX, field_name)
        return self

    def count(self, output_name: str = "count") -> "DataAggregationAction":
        """Add COUNT aggregation."""
        self.add_aggregation(output_name, AggregationType.COUNT)
        return self

    def _get_group_key(self, record: T) -> Any:
        """Extract group key from record."""
        if not self.group_by_fields:
            return "__all__"

        if isinstance(record, dict):
            return tuple(record.get(f) for f in self.group_by_fields)
        return tuple(getattr(record, f, None) for f in self.group_by_fields)

    def _get_field_value(self, record: T, field_name: str) -> Any:
        """Extract field value from record."""
        if isinstance(record, dict):
            return record.get(field_name)
        return getattr(record, field_name, None)

    def _aggregate_values(self, values: list) -> Any:
        """Perform aggregation on a list of values."""
        return values

    def compute(self, data: list[T]) -> list[AggregationResult]:
        """
        Compute aggregations on data.

        Args:
            data: List of records to aggregate.

        Returns:
            List of AggregationResult, one per group.
        """
        self._groups = defaultdict(lambda: {"__records__": []})

        for record in data:
            key = self._get_group_key(record)
            self._groups[key]["__records__"].append(record)

        results = []

        for group_key, group_data in self._groups.items():
            records = group_data["__records__"]
            values = {}

            for agg in self.aggregations:
                field_values = []

                for record in records:
                    if agg.filter_func and not agg.filter_func(record):
                        continue

                    if agg.field_name:
                        val = self._get_field_value(record, agg.field_name)
                        field_values.append(val)
                    else:
                        field_values.append(record)

                result = self._compute_single_aggregation(agg, field_values)
                values[agg.name] = result

            results.append(AggregationResult(
                group_key=group_key,
                values=values,
                record_count=len(records)
            ))

        return results

    def _compute_single_aggregation(self, agg: AggregationConfig, values: list) -> Any:
        """Compute a single aggregation."""
        if not values:
            return None

        if agg.agg_type == AggregationType.SUM:
            return sum(v for v in values if v is not None)

        elif agg.agg_type == AggregationType.AVG:
            valid = [v for v in values if v is not None]
            return sum(valid) / len(valid) if valid else None

        elif agg.agg_type == AggregationType.MIN:
            return min(v for v in values if v is not None) if values else None

        elif agg.agg_type == AggregationType.MAX:
            return max(v for v in values if v is not None) if values else None

        elif agg.agg_type == AggregationType.COUNT:
            return len(values)

        elif agg.agg_type == AggregationType.FIRST:
            return values[0] if values else None

        elif agg.agg_type == AggregationType.LAST:
            return values[-1] if values else None

        elif agg.agg_type == AggregationType.LIST:
            return values

        elif agg.agg_type == AggregationType.DICT:
            return {i: v for i, v in enumerate(values)}

        elif agg.agg_type == AggregationType.CUSTOM and agg.custom_func:
            return agg.custom_func(values)

        return values

    def compute_totals(self, data: list[T]) -> dict[str, Any]:
        """
        Compute aggregations across entire dataset (no grouping).

        Args:
            data: List of records.

        Returns:
            Dictionary of aggregated values.
        """
        self._groups = {"__all__": {"__records__": data}}

        results = self.compute(data)
        return results[0].values if results else {}

    def clear(self) -> None:
        """Clear all aggregations and groupings."""
        self.group_by_fields.clear()
        self.aggregations.clear()
        self._groups.clear()

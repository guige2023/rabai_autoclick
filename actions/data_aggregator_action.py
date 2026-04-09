"""
Data Aggregator Action Module.

Provides data aggregation with grouping, windowing,
and multiple aggregation functions.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar, Optional
from collections import defaultdict
import hashlib
import json

T = TypeVar("T")
R = TypeVar("R")


class AggregationType(Enum):
    """Aggregation function types."""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    DISTINCT = "distinct"
    LIST = "list"
    DICT = "dict"
    CUSTOM = "custom"


@dataclass
class AggregationConfig:
    """Configuration for aggregation."""
    group_by: list[str]
    aggregations: list[tuple[str, AggregationType, Any]] = field(default_factory=list)
    having: Optional[Callable[[dict], bool]] = None
    order_by: Optional[list[tuple[str, bool]]] = None
    limit: Optional[int] = None


@dataclass
class WindowConfig:
    """Configuration for windowing."""
    window_size: int = 100
    slide_size: int = 100
    window_type: str = "tumbling"
    time_field: Optional[str] = None


class AggregationFunction:
    """Base aggregation function."""

    def __init__(self, field_name: str, output_name: Optional[str] = None):
        self.field_name = field_name
        self.output_name = output_name or f"{field_name}_{self._type()}"

    def _type(self) -> str:
        return "base"

    def accumulate(self, values: list) -> Any:
        raise NotImplementedError

    def merge(self, partial1: Any, partial2: Any) -> Any:
        raise NotImplementedError

    def finalize(self, value: Any) -> Any:
        return value


class SumAgg(AggregationFunction):
    """Sum aggregation."""

    def _type(self) -> str:
        return "sum"

    def accumulate(self, values: list) -> float:
        return sum(v for v in values if v is not None)

    def merge(self, partial1: float, partial2: float) -> float:
        return partial1 + partial2


class CountAgg(AggregationFunction):
    """Count aggregation."""

    def _type(self) -> str:
        return "count"

    def accumulate(self, values: list) -> int:
        return len(values)

    def merge(self, partial1: int, partial2: int) -> int:
        return partial1 + partial2


class AvgAgg(AggregationFunction):
    """Average aggregation."""

    def __init__(self, field_name: str, output_name: Optional[str] = None):
        super().__init__(field_name, output_name)

    def _type(self) -> str:
        return "avg"

    def accumulate(self, values: list) -> tuple[float, int]:
        valid = [v for v in values if v is not None]
        return (sum(valid), len(valid))

    def merge(self, partial1: tuple[float, int], partial2: tuple[float, int]) -> tuple[float, int]:
        return (partial1[0] + partial2[0], partial1[1] + partial2[1])

    def finalize(self, value: tuple[float, int]) -> float:
        return value[0] / value[1] if value[1] > 0 else 0.0


class MinAgg(AggregationFunction):
    """Minimum aggregation."""

    def _type(self) -> str:
        return "min"

    def accumulate(self, values: list) -> Any:
        valid = [v for v in values if v is not None]
        return min(valid) if valid else None

    def merge(self, partial1: Any, partial2: Any) -> Any:
        if partial1 is None:
            return partial2
        if partial2 is None:
            return partial1
        return min(partial1, partial2)


class MaxAgg(AggregationFunction):
    """Maximum aggregation."""

    def _type(self) -> str:
        return "max"

    def accumulate(self, values: list) -> Any:
        valid = [v for v in values if v is not None]
        return max(valid) if valid else None

    def merge(self, partial1: Any, partial2: Any) -> Any:
        if partial1 is None:
            return partial2
        if partial2 is None:
            return partial1
        return max(partial1, partial2)


class DistinctAgg(AggregationFunction):
    """Distinct values aggregation."""

    def _type(self) -> str:
        return "distinct"

    def accumulate(self, values: list) -> set:
        return set(v for v in values if v is not None)

    def merge(self, partial1: set, partial2: set) -> set:
        return partial1 | partial2

    def finalize(self, value: set) -> list:
        return list(value)


class ListAgg(AggregationFunction):
    """List aggregation."""

    def _type(self) -> str:
        return "list"

    def accumulate(self, values: list) -> list:
        return [v for v in values if v is not None]

    def merge(self, partial1: list, partial2: list) -> list:
        return partial1 + partial2


class DataAggregator:
    """Data aggregator."""

    AGG_MAP = {
        AggregationType.SUM: SumAgg,
        AggregationType.COUNT: CountAgg,
        AggregationType.AVG: AvgAgg,
        AggregationType.MIN: MinAgg,
        AggregationType.MAX: MaxAgg,
        AggregationType.DISTINCT: DistinctAgg,
        AggregationType.LIST: ListAgg,
    }

    def __init__(self, config: Optional[AggregationConfig] = None):
        self.config = config or AggregationConfig(group_by=[], aggregations=[])
        self._aggregations: list[AggregationFunction] = []

        for field_name, agg_type, output_name in self.config.aggregations:
            agg_class = self.AGG_MAP.get(agg_type)
            if agg_class:
                self._aggregations.append(agg_class(field_name, output_name))

    def _get_group_key(self, record: dict, group_fields: list[str]) -> str:
        """Generate group key from record."""
        key_parts = [str(record.get(f, "")) for f in group_fields]
        return "|".join(key_parts)

    def _get_nested_value(self, record: dict, path: str) -> Any:
        """Get nested value from record."""
        parts = path.split(".")
        value = record
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value

    async def aggregate(self, data: list[dict]) -> list[dict]:
        """Perform aggregation."""
        groups: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))

        for record in data:
            key = self._get_group_key(record, self.config.group_by)

            for agg in self._aggregations:
                values = []
                for field_name in self.config.group_by:
                    values.append(self._get_nested_value(record, field_name))
                    break

                agg_values = []
                for record_item in data:
                    val = self._get_nested_value(record_item, agg.field_name)
                    if self._get_group_key(record_item, self.config.group_by) == key:
                        agg_values.append(val)

                groups[key][agg.output_name].append(self._get_nested_value(record, agg.field_name))

        results = []
        for key, agg_results in groups.items():
            result = {}

            if self.config.group_by:
                key_parts = key.split("|")
                for i, field_name in enumerate(self.config.group_by):
                    result[field_name] = key_parts[i] if i < len(key_parts) else ""

            for agg in self._aggregations:
                agg_values = agg_results.get(agg.output_name, [])
                result[agg.output_name] = agg.finalize(agg.accumulate(agg_values))

            if self.config.having and not self.config.having(result):
                continue

            results.append(result)

        if self.config.order_by:
            def sort_key(r: dict) -> tuple:
                parts = []
                for field_name, ascending in self.config.order_by:
                    val = r.get(field_name, "")
                    parts.append((val if ascending else -val) if val is not None else 0)
                return tuple(parts)

            results.sort(key=sort_key)

        if self.config.limit:
            results = results[:self.config.limit]

        return results


class DataAggregatorAction:
    """
    Data aggregation with grouping and windowing.

    Example:
        agg = DataAggregatorAction(
            group_by=["category", "region"],
            aggregations=[
                ("amount", AggregationType.SUM, "total_amount"),
                ("amount", AggregationType.AVG, "avg_amount"),
                ("id", AggregationType.COUNT, "count")
            ]
        )

        results = await agg.aggregate(sales_data)
    """

    def __init__(
        self,
        group_by: Optional[list[str]] = None,
        aggregations: Optional[list[tuple[str, AggregationType, Any]]] = None
    ):
        config = AggregationConfig(
            group_by=group_by or [],
            aggregations=aggregations or []
        )
        self._aggregator = DataAggregator(config)

    async def aggregate(self, data: list[dict]) -> list[dict]:
        """Aggregate data."""
        return await self._aggregator.aggregate(data)

    async def aggregate_async(
        self,
        data: list[dict],
        parallel: bool = True
    ) -> list[dict]:
        """Aggregate with optional parallel processing."""
        if parallel and len(data) > 1000:
            chunk_size = len(data) // 4
            chunks = [
                data[i:i + chunk_size]
                for i in range(0, len(data), chunk_size)
            ]
            tasks = [self._aggregator.aggregate(chunk) for chunk in chunks]
            chunk_results = await asyncio.gather(*tasks)

            merged = []
            for results in chunk_results:
                merged.extend(results)

            return merged
        else:
            return await self._aggregator.aggregate(data)

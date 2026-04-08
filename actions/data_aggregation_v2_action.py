"""Data Aggregation v2 Action.

Advanced aggregation with group-by, having, and multi-output.
"""
from typing import Any, Callable, Dict, List, Optional, TypeVar
from dataclasses import dataclass, field
import statistics


T = TypeVar("T")


@dataclass
class Aggregator:
    name: str
    fn: Callable[[List], Any]
    alias: Optional[str] = None


@dataclass
class AggregationResult:
    groups: Dict[str, Dict[str, Any]]
    group_count: int
    total_items: int


class DataAggregationV2Action:
    """Advanced data aggregation with group-by and multiple aggregators."""

    def __init__(self) -> None:
        self.aggregators: List[Aggregator] = []

    def add_aggregator(
        self,
        name: str,
        fn: Callable[[List], Any],
        alias: Optional[str] = None,
    ) -> "DataAggregationV2Action":
        self.aggregators.append(Aggregator(name=name, fn=fn, alias=alias))
        return self

    def avg(self, alias: Optional[str] = None) -> "DataAggregationV2Action":
        return self.add_aggregator("avg", lambda x: statistics.mean(x) if x else None, alias)

    def sum(self, alias: Optional[str] = None) -> "DataAggregationV2Action":
        return self.add_aggregator("sum", lambda x: sum(x) if x else None, alias)

    def count(self, alias: Optional[str] = None) -> "DataAggregationV2Action":
        return self.add_aggregator("count", len, alias)

    def min(self, alias: Optional[str] = None) -> "DataAggregationV2Action":
        return self.add_aggregator("min", lambda x: min(x) if x else None, alias)

    def max(self, alias: Optional[str] = None) -> "DataAggregationV2Action":
        return self.add_aggregator("max", lambda x: max(x) if x else None, alias)

    def stddev(self, alias: Optional[str] = None) -> "DataAggregationV2Action":
        return self.add_aggregator("stddev", lambda x: statistics.stdev(x) if len(x) > 1 else 0, alias)

    def group_by(
        self,
        items: List[T],
        key_fn: Callable[[T], str],
        value_fn: Callable[[T], Any],
    ) -> AggregationResult:
        groups: Dict[str, List[Any]] = {}
        for item in items:
            key = key_fn(item)
            groups.setdefault(key, []).append(value_fn(item))
        results: Dict[str, Dict[str, Any]] = {}
        for group_key, values in groups.items():
            result: Dict[str, Any] = {"_count": len(values)}
            for agg in self.aggregators:
                result[agg.alias or agg.name] = agg.fn(values)
            results[group_key] = result
        return AggregationResult(
            groups=results,
            group_count=len(results),
            total_items=len(items),
        )

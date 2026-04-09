"""
Data Aggregator Action Module.

Provides data aggregation capabilities including grouping,
pivoting, rollup, and multi-dimensional aggregation.
"""

import math
from typing import Optional, List, Dict, Any, Callable, Union, TypeVar
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict


class AggregationType(Enum):
    """Aggregation function types."""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    MEDIAN = "median"
    STD = "std"
    VAR = "var"
    PRODUCT = "product"
    CONCAT = "concat"


@dataclass
class AggregationConfig:
    """Configuration for aggregation."""
    group_by: List[str] = field(default_factory=list)
    aggregations: Dict[str, List[AggregationType]] = field(default_factory=dict)
    having: Optional[Callable[[Dict[str, Any]], bool]] = None
    sort_by: Optional[List[str]] = None
    sort_order: str = "asc"  # asc or desc
    limit: Optional[int] = None
    fill_value: Any = None


@dataclass
class AggregationResult:
    """Result of an aggregation operation."""
    groups: List[Dict[str, Any]]
    total_groups: int
    total_records: int
    aggregation_time_ms: float


class DataAggregatorAction:
    """
    Data aggregation action with multiple aggregation types.

    Supports grouping, multi-level aggregation, filtering with having,
    sorting, and result limiting.
    """

    def __init__(self, config: Optional[AggregationConfig] = None):
        self.config = config or AggregationConfig()

    def _get_aggregation_func(self, agg_type: AggregationType) -> Callable:
        """Get aggregation function for type."""
        if agg_type == AggregationType.SUM:
            return sum
        elif agg_type == AggregationType.COUNT:
            return len
        elif agg_type == AggregationType.AVG:
            return lambda x: sum(x) / len(x) if x else 0
        elif agg_type == AggregationType.MIN:
            return min
        elif agg_type == AggregationType.MAX:
            return max
        elif agg_type == AggregationType.FIRST:
            return lambda x: x[0] if x else None
        elif agg_type == AggregationType.LAST:
            return lambda x: x[-1] if x else None
        elif agg_type == AggregationType.MEDIAN:
            def median(x):
                if not x:
                    return None
                sorted_x = sorted(x)
                n = len(sorted_x)
                if n % 2 == 0:
                    return (sorted_x[n // 2 - 1] + sorted_x[n // 2]) / 2
                return sorted_x[n // 2]
            return median
        elif agg_type == AggregationType.STD:
            def std(x):
                if not x:
                    return 0
                n = len(x)
                mean = sum(x) / n
                variance = sum((xi - mean) ** 2 for xi in x) / n
                return math.sqrt(variance)
            return std
        elif agg_type == AggregationType.VAR:
            def var(x):
                if not x:
                    return 0
                n = len(x)
                mean = sum(x) / n
                return sum((xi - mean) ** 2 for xi in x) / n
            return var
        elif agg_type == AggregationType.PRODUCT:
            def product(x):
                result = 1
                for xi in x:
                    result *= xi
                return result
            return product
        elif agg_type == AggregationType.CONCAT:
            return lambda x: ",".join(str(xi) for xi in x)
        return lambda x: x

    def aggregate(
        self,
        data: List[Dict[str, Any]],
    ) -> AggregationResult:
        """
        Perform aggregation on data.

        Args:
            data: List of records to aggregate

        Returns:
            AggregationResult with grouped and aggregated data
        """
        import time
        start_time = time.time()

        if not data:
            return AggregationResult(
                groups=[],
                total_groups=0,
                total_records=0,
                aggregation_time_ms=0,
            )

        # Group data
        groups: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)

        for record in data:
            if self.config.group_by:
                key = tuple(record.get(g) for g in self.config.group_by)
            else:
                key = ()
            groups[key].append(record)

        # Apply aggregations
        results = []
        for key, group_data in groups.items():
            result = {}

            # Add group by keys
            if self.config.group_by:
                for i, g in enumerate(self.config.group_by):
                    result[g] = key[i]

            # Apply aggregations
            for field_name, agg_types in self.config.aggregations.items():
                values = [record.get(field_name) for record in group_data if field_name in record]
                numeric_values = [v for v in values if isinstance(v, (int, float))]

                agg_results = {}
                for agg_type in agg_types:
                    if agg_type in (AggregationType.SUM, AggregationType.AVG, AggregationType.MIN,
                                   AggregationType.MAX, AggregationType.PRODUCT) and numeric_values:
                        agg_results[agg_type.value] = self._get_aggregation_func(agg_type)(numeric_values)
                    elif agg_type == AggregationType.COUNT:
                        agg_results[agg_type.value] = len(values)
                    elif agg_type == AggregationType.CONCAT:
                        agg_results[agg_type.value] = self._get_aggregation_func(agg_type)(values)
                    elif agg_type == AggregationType.MEDIAN and numeric_values:
                        agg_results[agg_type.value] = self._get_aggregation_func(agg_type)(numeric_values)
                    elif agg_type == AggregationType.STD and numeric_values:
                        agg_results[agg_type.value] = self._get_aggregation_func(agg_type)(numeric_values)
                    elif agg_type == AggregationType.VAR and numeric_values:
                        agg_results[agg_type.value] = self._get_aggregation_func(agg_type)(numeric_values)
                    elif agg_type in (AggregationType.FIRST, AggregationType.LAST):
                        agg_results[agg_type.value] = self._get_aggregation_func(agg_type)(values)

                # Single aggregation shorthand
                if len(agg_types) == 1:
                    result[f"{field_name}_{agg_types[0].value}"] = list(agg_results.values())[0]
                else:
                    for agg_name, agg_value in agg_results.items():
                        result[f"{field_name}_{agg_name}"] = agg_value

            results.append(result)

        # Apply having filter
        if self.config.having:
            results = [r for r in results if self.config.having(r)]

        # Apply sorting
        if self.config.sort_by:
            reverse = self.config.sort_order == "desc"
            results = sorted(
                results,
                key=lambda x: tuple(x.get(s) for s in self.config.sort_by),
                reverse=reverse,
            )

        # Apply limit
        total_groups = len(results)
        if self.config.limit:
            results = results[:self.config.limit]

        aggregation_time_ms = (time.time() - start_time) * 1000

        return AggregationResult(
            groups=results,
            total_groups=total_groups,
            total_records=len(data),
            aggregation_time_ms=aggregation_time_ms,
        )

    def pivot(
        self,
        data: List[Dict[str, Any]],
        index: str,
        columns: str,
        values: str,
        agg_func: AggregationType = AggregationType.SUM,
    ) -> Dict[str, Dict[str, float]]:
        """
        Pivot data from long to wide format.

        Args:
            data: Input data
            index: Column to use as index
            columns: Column to use as columns
            values: Column with values to aggregate
            agg_func: Aggregation function

        Returns:
            Pivot table as nested dict
        """
        pivot_dict: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))

        for record in data:
            idx = record.get(index)
            col = record.get(columns)
            val = record.get(values)

            if idx is not None and col is not None and val is not None:
                if isinstance(val, (int, float)):
                    pivot_dict[idx][col].append(val)

        # Apply aggregation
        result = {}
        agg_fn = self._get_aggregation_func(agg_func)
        for idx, col_dict in pivot_dict.items():
            result[idx] = {}
            for col, values in col_dict.items():
                result[idx][col] = agg_fn(values) if values else self.config.fill_value or 0

        return result

    def rollup(
        self,
        data: List[Dict[str, Any]],
        group_by: List[str],
        value_field: str,
        agg_func: AggregationType = AggregationType.SUM,
    ) -> List[Dict[str, Any]]:
        """
        Create hierarchical rollup with subtotals and grand total.

        Args:
            data: Input data
            group_by: Fields to group by
            value_field: Field to aggregate
            agg_func: Aggregation function

        Returns:
            List with subtotals at each level
        """
        results = []
        agg_fn = self._get_aggregation_func(agg_func)

        # Generate all combinations of group by fields
        def generate_rollup(level: int, current_key: tuple, data_subset: List[Dict]):
            if level > len(group_by):
                # Compute aggregate
                values = [r.get(value_field) for r in data_subset if isinstance(r.get(value_field), (int, float))]
                if values:
                    result = {}
                    for i, g in enumerate(group_by):
                        if i < len(current_key):
                            result[g] = current_key[i]
                        else:
                            result[g] = "Total"
                    result[f"{value_field}_{agg_func.value}"] = agg_fn(values)
                    results.append(result)
                return

            # Group by current level
            groups: Dict[Any, List[Dict]] = defaultdict(list)
            for record in data_subset:
                key_val = record.get(group_by[level])
                groups[key_val].append(record)

            for key_val, group_data in groups.items():
                new_key = current_key + (key_val,)
                generate_rollup(level + 1, new_key, group_data)

        generate_rollup(0, (), data)
        return results


class GroupedAggregation:
    """Helper for incremental grouped aggregation."""

    def __init__(self):
        self._groups: Dict[tuple, Dict[str, List[Any]]] = defaultdict(lambda: defaultdict(list))
        self._group_keys: List[tuple] = []

    def add(self, record: Dict[str, Any], group_by: List[str]) -> None:
        """Add a record to the aggregation."""
        if not group_by:
            key = ()
        else:
            key = tuple(record.get(g) for g in group_by)

        if key not in self._group_keys:
            self._group_keys.append(key)

        for field_name, value in record.items():
            self._groups[key][field_name].append(value)

    def get_aggregation(
        self,
        aggregations: Dict[str, List[AggregationType]],
    ) -> List[Dict[str, Any]]:
        """Get aggregated results."""
        results = []
        for key in self._group_keys:
            result = {}
            group_data = self._groups[key]

            for i, g in enumerate(group_by):
                if i < len(key):
                    result[g] = key[i]

            for field_name, agg_types in aggregations.items():
                values = group_data.get(field_name, [])
                numeric_values = [v for v in values if isinstance(v, (int, float))]

                for agg_type in agg_types:
                    if agg_type in (AggregationType.SUM, AggregationType.AVG, AggregationType.MIN,
                                   AggregationType.MAX, AggregationType.PRODUCT) and numeric_values:
                        result[f"{field_name}_{agg_type.value}"] = self._get_aggregation_func(agg_type)(numeric_values)
                    elif agg_type == AggregationType.COUNT:
                        result[f"{field_name}_{agg_type.value}"] = len(values)

            results.append(result)

        return results

    @staticmethod
    def _get_aggregation_func(agg_type: AggregationType) -> Callable:
        if agg_type == AggregationType.SUM:
            return sum
        elif agg_type == AggregationType.AVG:
            return lambda x: sum(x) / len(x) if x else 0
        elif agg_type == AggregationType.MIN:
            return min
        elif agg_type == AggregationType.MAX:
            return max
        elif agg_type == AggregationType.COUNT:
            return len
        elif agg_type == AggregationType.PRODUCT:
            def product(x):
                result = 1
                for xi in x:
                    result *= xi
                return result
            return product
        return lambda x: x

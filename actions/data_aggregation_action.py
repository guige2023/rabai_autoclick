"""
Data Aggregation Action Module

Provides data aggregation, rollup, and grouping operations.
"""
from typing import Any, Optional, Callable, TypeVar
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict
import asyncio


T = TypeVar('T')


@dataclass
class AggregationConfig:
    """Configuration for aggregation."""
    group_by: list[str]
    aggregations: list[dict[str, str]]  # [{field: "amount", func: "sum", alias: "total"}]
    having: Optional[Callable[[dict], bool]] = None
    order_by: Optional[list[tuple[str, str]]] = None  # [(field, "asc"|"desc")]
    limit: Optional[int] = None


@dataclass
class AggregationResult:
    """Result of aggregation."""
    groups: list[dict[str, Any]]
    total_groups: int
    total_records: int
    duration_ms: float


class DataAggregationAction:
    """Main data aggregation action handler."""
    
    def __init__(self):
        self._custom_aggregators: dict[str, Callable] = {}
        self._stats: dict[str, Any] = defaultdict(int)
    
    def register_aggregator(
        self,
        name: str,
        func: Callable[[list], Any]
    ) -> "DataAggregationAction":
        """Register a custom aggregator function."""
        self._custom_aggregators[name] = func
        return self
    
    async def aggregate(
        self,
        data: list[dict[str, Any]],
        config: AggregationConfig
    ) -> AggregationResult:
        """
        Aggregate data according to configuration.
        
        Args:
            data: List of records to aggregate
            config: Aggregation configuration
            
        Returns:
            AggregationResult with aggregated groups
        """
        start_time = datetime.now()
        
        if not data:
            return AggregationResult(
                groups=[],
                total_groups=0,
                total_records=0,
                duration_ms=0
            )
        
        # Group data
        groups: dict[tuple, list[dict]] = defaultdict(list)
        
        for record in data:
            key = tuple(record.get(field) for field in config.group_by)
            groups[key].append(record)
        
        # Apply aggregations to each group
        results = []
        for key, group_data in groups.items():
            result = {}
            
            # Add group key fields
            for i, field_name in enumerate(config.group_by):
                result[field_name] = key[i]
            
            # Apply aggregation functions
            for agg_spec in config.aggregations:
                field_name = agg_spec["field"]
                func_name = agg_spec["func"]
                alias = agg_spec.get("alias", f"{func_name}_{field_name}")
                
                values = [r.get(field_name) for r in group_data if field_name in r]
                result[alias] = await self._apply_aggregation(func_name, values)
            
            # Add group metadata
            result["_count"] = len(group_data)
            result["_first"] = group_data[0] if group_data else {}
            result["_last"] = group_data[-1] if group_data else {}
            
            results.append(result)
        
        # Apply having clause
        if config.having:
            results = [r for r in results if config.having(r)]
        
        # Apply ordering
        if config.order_by:
            for field_name, direction in reversed(config.order_by):
                results.sort(
                    key=lambda r: r.get(field_name, 0),
                    reverse=(direction.lower() == "desc")
                )
        
        # Apply limit
        if config.limit:
            results = results[:config.limit]
        
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        self._stats["aggregations"] += 1
        self._stats["records_processed"] += len(data)
        
        return AggregationResult(
            groups=results,
            total_groups=len(results),
            total_records=len(data),
            duration_ms=duration_ms
        )
    
    async def _apply_aggregation(
        self,
        func_name: str,
        values: list
    ) -> Any:
        """Apply aggregation function to values."""
        # Filter out None values for numeric operations
        numeric_values = [v for v in values if v is not None and isinstance(v, (int, float))]
        
        if func_name == "count":
            return len(values)
        
        elif func_name == "count_distinct":
            return len(set(values))
        
        elif func_name == "sum":
            return sum(numeric_values) if numeric_values else 0
        
        elif func_name == "avg":
            return sum(numeric_values) / len(numeric_values) if numeric_values else 0
        
        elif func_name == "min":
            return min(values) if values else None
        
        elif func_name == "max":
            return max(values) if values else None
        
        elif func_name == "first":
            return values[0] if values else None
        
        elif func_name == "last":
            return values[-1] if values else None
        
        elif func_name == "array_agg":
            return values
        
        elif func_name == "string_agg":
            return ",".join(str(v) for v in values)
        
        elif func_name == "stddev":
            if len(numeric_values) < 2:
                return 0
            import statistics
            return statistics.stdev(numeric_values)
        
        elif func_name == "variance":
            if len(numeric_values) < 2:
                return 0
            import statistics
            return statistics.variance(numeric_values)
        
        elif func_name == "median":
            if not values:
                return None
            sorted_values = sorted(values)
            n = len(sorted_values)
            if n % 2 == 0:
                return (sorted_values[n//2-1] + sorted_values[n//2]) / 2
            return sorted_values[n//2]
        
        elif func_name in self._custom_aggregators:
            return self._custom_aggregators[func_name](values)
        
        return None
    
    async def rollup(
        self,
        data: list[dict[str, Any]],
        group_by_fields: list[str]
    ) -> list[dict[str, Any]]:
        """
        Generate rollup aggregations (hierarchical grouping).
        
        Creates aggregations at each level of the grouping hierarchy.
        """
        results = []
        
        # Generate all combinations of group by fields
        for i in range(len(group_by_fields)):
            level_fields = group_by_fields[i:]
            
            config = AggregationConfig(
                group_by=level_fields,
                aggregations=[
                    {"field": group_by_fields[0], "func": "count", "alias": "total"}
                ]
            )
            
            result = await self.aggregate(data, config)
            results.extend(result.groups)
        
        # Add grand total
        grand_total = {"_is_grand_total": True}
        for field in group_by_fields:
            grand_total[field] = "_total"
        grand_total["total"] = len(data)
        results.append(grand_total)
        
        return results
    
    async def pivot(
        self,
        data: list[dict[str, Any]],
        index: list[str],
        columns: str,
        values: str,
        aggfunc: str = "sum"
    ) -> list[dict[str, Any]]:
        """
        Pivot data (transform rows to columns).
        
        Args:
            data: List of records
            index: Fields to use as row index
            columns: Field to use as column names
            values: Field to use as values
            aggfunc: Aggregation function to use
            
        Returns:
            Pivoted data
        """
        # Group by index
        groups: dict[tuple, dict[str, list]] = defaultdict(lambda: defaultdict(list))
        
        for record in data:
            key = tuple(record.get(f) for f in index)
            col_value = record.get(columns)
            val = record.get(values)
            
            groups[key][col_value].append(val)
        
        # Build pivot table
        results = []
        all_columns = set()
        
        for key, col_values in groups.items():
            result = dict(zip(index, key))
            
            for col, values_list in col_values.items():
                all_columns.add(col)
                result[col] = await self._apply_aggregation(aggfunc, values_list)
            
            results.append(result)
        
        # Add column totals
        for result in results:
            result["_total"] = await self._apply_aggregation(
                aggfunc,
                [result.get(c, 0) for c in all_columns]
            )
        
        return results
    
    async def window_aggregate(
        self,
        data: list[dict[str, Any]],
        partition_by: list[str],
        order_by: list[str],
        window_funcs: list[dict[str, str]]
    ) -> list[dict[str, Any]]:
        """
        Apply window functions to data.
        
        Window functions operate on a window of rows relative to current row.
        """
        # Sort data
        sorted_data = sorted(
            data,
            key=lambda r: tuple(r.get(f, "") for f in order_by)
        )
        
        results = []
        
        for i, record in enumerate(sorted_data):
            result = dict(record)
            partition_key = tuple(record.get(f) for f in partition_by)
            
            # Find window bounds for current partition
            window_start = i
            window_end = i
            
            for j in range(i - 1, -1, -1):
                other_key = tuple(sorted_data[j].get(f) for f in partition_by)
                if other_key == partition_key:
                    window_start = j
                else:
                    break
            
            for j in range(i + 1, len(sorted_data)):
                other_key = tuple(sorted_data[j].get(f) for f in partition_by)
                if other_key == partition_key:
                    window_end = j
                else:
                    break
            
            # Apply window functions
            window_data = sorted_data[window_start:window_end + 1]
            
            for func_spec in window_funcs:
                field_name = func_spec["field"]
                func_name = func_spec["func"]
                alias = func_spec.get("alias", f"{func_name}_{field_name}")
                
                values = [r.get(field_name) for r in window_data if field_name in r]
                result[alias] = await self._apply_aggregation(func_name, values)
            
            results.append(result)
        
        return results
    
    def get_stats(self) -> dict[str, Any]:
        """Get aggregation statistics."""
        return dict(self._stats)

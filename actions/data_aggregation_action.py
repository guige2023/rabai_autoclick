"""
Data Aggregation Action Module.

Provides data aggregation capabilities including grouping, filtering,
statistical computations, and multi-dimensional rollups for data pipelines.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import threading
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import groupby, combinations
from functools import reduce


class AggregationType(Enum):
    """Types of aggregation operations."""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    MEDIAN = "median"
    STD_DEV = "std_dev"
    PERCENTILE = "percentile"
    DISTINCT = "distinct"
    LIST = "list"
    DICT = "dict"


@dataclass
class AggregationConfig:
    """Configuration for an aggregation operation."""
    field: str
    agg_type: AggregationType
    alias: Optional[str] = None
    filter_fn: Optional[Callable[[Any], bool]] = None


@dataclass
class GroupedResult:
    """Result of a grouping operation."""
    groups: Dict[str, List[Any]]
    aggregates: Dict[str, Dict[str, Any]]
    group_keys: List[str]


class DataAggregator:
    """
    Configurable data aggregator with multiple operations.
    
    Example:
        aggregator = DataAggregator()
        aggregator.add_aggregation("price", AggregationType.SUM, "total_price")
        aggregator.add_aggregation("category", AggregationType.COUNT)
        
        result = aggregator.aggregate(data_records)
    """
    
    def __init__(self):
        self.aggregations: List[AggregationConfig] = []
        self.group_by_fields: List[str] = []
        self._lock = threading.Lock()
    
    def add_aggregation(
        self,
        field: str,
        agg_type: AggregationType,
        alias: Optional[str] = None,
        filter_fn: Optional[Callable] = None
    ) -> "DataAggregator":
        """Add an aggregation configuration."""
        self.aggregations.append(AggregationConfig(
            field=field,
            agg_type=agg_type,
            alias=alias,
            filter_fn=filter_fn
        ))
        return self
    
    def group_by(self, *fields: str) -> "DataAggregator":
        """Set grouping fields."""
        self.group_by_fields.extend(fields)
        return self
    
    def aggregate(self, data: List[Dict]) -> Dict[str, Any]:
        """Perform aggregation on data."""
        if not self.group_by_fields:
            return self._aggregate_all(data)
        
        return self._aggregate_grouped(data)
    
    def _aggregate_all(self, data: List[Dict]) -> Dict[str, Any]:
        """Aggregate all data without grouping."""
        result = {}
        
        for agg in self.aggregations:
            values = self._get_field_values(data, agg)
            if values is not None:
                result[agg.alias or f"{agg.field}_{agg.agg_type.value}"] = self._compute(
                    values, agg.agg_type
                )
        
        return result
    
    def _aggregate_grouped(self, data: List[Dict]) -> GroupedResult:
        """Aggregate data with grouping."""
        # Sort data by group fields
        sorted_data = sorted(data, key=lambda x: tuple(x.get(f) for f in self.group_by_fields))
        
        groups: Dict[str, List[Any]] = {}
        aggregates: Dict[str, Dict[str, Any]] = {}
        
        for key, group in groupby(sorted_data, key=lambda x: tuple(x.get(f) for f in self.group_by_fields)):
            key_str = str(key)
            groups[key_str] = list(group)
            
            group_data = groups[key_str]
            group_agg = {}
            
            for agg in self.aggregations:
                values = self._get_field_values(group_data, agg)
                if values is not None:
                    group_agg[agg.alias or f"{agg.field}_{agg.agg_type.value}"] = self._compute(
                        values, agg.agg_type
                    )
            
            aggregates[key_str] = group_agg
        
        return GroupedResult(
            groups=groups,
            aggregates=aggregates,
            group_keys=self.group_by_fields
        )
    
    def _get_field_values(self, data: List[Dict], agg: AggregationConfig) -> List[Any]:
        """Extract field values from data."""
        values = []
        
        for item in data:
            if agg.filter_fn and not agg.filter_fn(item):
                continue
            
            value = item.get(agg.field)
            if value is not None:
                values.append(value)
        
        return values
    
    def _compute(self, values: List[Any], agg_type: AggregationType) -> Any:
        """Compute aggregation on values."""
        if not values:
            return None
        
        numeric_values = [v for v in values if isinstance(v, (int, float))]
        
        if agg_type == AggregationType.SUM:
            return sum(numeric_values) if numeric_values else sum(values)
        
        elif agg_type == AggregationType.COUNT:
            return len(values)
        
        elif agg_type == AggregationType.AVG:
            if numeric_values:
                return sum(numeric_values) / len(numeric_values)
            return sum(values) / len(values) if values else 0
        
        elif agg_type == AggregationType.MIN:
            return min(values)
        
        elif agg_type == AggregationType.MAX:
            return max(values)
        
        elif agg_type == AggregationType.FIRST:
            return values[0]
        
        elif agg_type == AggregationType.LAST:
            return values[-1]
        
        elif agg_type == AggregationType.MEDIAN:
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            if n % 2 == 0:
                return (sorted_vals[n//2-1] + sorted_vals[n//2]) / 2
            return sorted_vals[n//2]
        
        elif agg_type == AggregationType.STD_DEV:
            if len(values) < 2:
                return 0
            mean = sum(values) / len(values)
            variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
            return variance ** 0.5
        
        elif agg_type == AggregationType.DISTINCT:
            return len(set(values))
        
        elif agg_type == AggregationType.LIST:
            return values
        
        elif agg_type == AggregationType.DICT:
            counts = defaultdict(int)
            for v in values:
                counts[v] += 1
            return dict(counts)
        
        return values


class MultiDimensionalAggregator(DataAggregator):
    """
    Multi-dimensional aggregation with rollup and cube.
    
    Example:
        mda = MultiDimensionalAggregator()
        mda.add_dimension("region")
        mda.add_dimension("category")
        mda.add_aggregation("sales", AggregationType.SUM)
        
        result = mda.rollup(data)
    """
    
    def __init__(self):
        super().__init__()
        self.dimensions: List[str] = []
    
    def add_dimension(self, field: str) -> "MultiDimensionalAggregator":
        """Add a dimension for multi-dimensional analysis."""
        self.dimensions.append(field)
        return self
    
    def rollup(self, data: List[Dict]) -> List[Dict[str, Any]]:
        """Compute rollup aggregation (hierarchical)."""
        results = []
        
        # Start with all dimensions
        for depth in range(len(self.dimensions) + 1):
            dims = self.dimensions[:depth] if depth > 0 else []
            
            if dims:
                grouped = self._group_by_dims(data, dims)
                for key, group_data in grouped.items():
                    row = self._build_rollup_row(key, dims, group_data)
                    results.append(row)
            else:
                # Total row
                row = self._build_rollup_row("TOTAL", [], data)
                results.append(row)
        
        return results
    
    def cube(self, data: List[Dict]) -> List[Dict[str, Any]]:
        """Compute full cube aggregation (all combinations)."""
        results = []
        
        # All possible dimension combinations
        for r in range(len(self.dimensions) + 1):
            for dims in combinations(self.dimensions, r):
                if dims:
                    grouped = self._group_by_dims(data, list(dims))
                    for key, group_data in grouped.items():
                        row = self._build_rollup_row(key, list(dims), group_data)
                        results.append(row)
                else:
                    row = self._build_rollup_row("ALL", [], data)
                    results.append(row)
        
        return results
    
    def _group_by_dims(self, data: List[Dict], dims: List[str]) -> Dict[str, List[Dict]]:
        """Group data by dimensions."""
        grouped = defaultdict(list)
        for item in data:
            key = tuple(item.get(d) for d in dims)
            grouped[key].append(item)
        return grouped
    
    def _build_rollup_row(
        self,
        key: Tuple,
        dims: List[str],
        group_data: List[Dict]
    ) -> Dict[str, Any]:
        """Build a rollup row with aggregates."""
        row = {}
        
        # Add dimension values
        if key != "TOTAL" and key != "ALL":
            for i, dim in enumerate(dims):
                row[dim] = key[i]
        
        # Add aggregates
        for agg in self.aggregations:
            values = self._get_field_values(group_data, agg)
            if values is not None:
                row[agg.alias or f"{agg.field}_{agg.agg_type.value}"] = self._compute(
                    values, agg.agg_type
                )
        
        return row


class TimeSeriesAggregator:
    """
    Time-series data aggregator with windowing.
    
    Example:
        tsa = TimeSeriesAggregator(window=timedelta(hours=1))
        tsa.add_aggregation("value", AggregationType.AVG, "hourly_avg")
        
        result = tsa.aggregate(time_series_data)
    """
    
    def __init__(
        self,
        window: timedelta,
        timestamp_field: str = "timestamp"
    ):
        self.window = window
        self.timestamp_field = timestamp_field
        self.aggregations: List[AggregationConfig] = []
    
    def add_aggregation(
        self,
        field: str,
        agg_type: AggregationType,
        alias: Optional[str] = None
    ) -> "TimeSeriesAggregator":
        """Add an aggregation."""
        self.aggregations.append(AggregationConfig(
            field=field,
            agg_type=agg_type,
            alias=alias
        ))
        return self
    
    def aggregate(self, data: List[Dict]) -> List[Dict[str, Any]]:
        """Aggregate time-series data into windows."""
        if not data:
            return []
        
        # Sort by timestamp
        sorted_data = sorted(
            data,
            key=lambda x: x.get(self.timestamp_field, datetime.min)
        )
        
        windows: Dict[str, List[Dict]] = defaultdict(list)
        
        for item in sorted_data:
            timestamp = item.get(self.timestamp_field)
            if isinstance(timestamp, str):
                timestamp = datetime.fromisoformat(timestamp)
            
            if timestamp is None:
                continue
            
            # Calculate window key
            window_start = timestamp.replace(
                minute=0, second=0, microsecond=0
            )
            
            # For larger windows
            if self.window >= timedelta(days=1):
                window_start = window_start.replace(hour=0)
            
            window_key = window_start.isoformat()
            windows[window_key].append(item)
        
        results = []
        for window_start, window_data in sorted(windows.items()):
            row = {"window_start": window_start}
            
            for agg in self.aggregations:
                values = [item.get(agg.field) for item in window_data]
                values = [v for v in values if v is not None]
                
                if values:
                    row[agg.alias or f"{agg.field}_{agg.agg_type.value}"] = self._compute(
                        values, agg.agg_type
                    )
            
            results.append(row)
        
        return results
    
    def _compute(self, values: List[Any], agg_type: AggregationType) -> Any:
        """Compute aggregation."""
        if not values:
            return None
        
        if agg_type == AggregationType.SUM:
            return sum(values)
        elif agg_type == AggregationType.COUNT:
            return len(values)
        elif agg_type == AggregationType.AVG:
            return sum(values) / len(values)
        elif agg_type == AggregationType.MIN:
            return min(values)
        elif agg_type == AggregationType.MAX:
            return max(values)
        elif agg_type == AggregationType.FIRST:
            return values[0]
        elif agg_type == AggregationType.LAST:
            return values[-1]
        
        return values


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class DataAggregationAction(BaseAction):
    """
    Data aggregation action for data pipelines.
    
    Parameters:
        data: List of records to aggregate
        aggregations: List of aggregation specs
        group_by: Fields to group by
        dimensions: Dimensions for multi-dimensional analysis
    
    Example:
        action = DataAggregationAction()
        result = action.execute({}, {
            "data": [{"category": "A", "value": 10}, {"category": "B", "value": 20}],
            "aggregations": [{"field": "value", "type": "sum", "alias": "total"}],
            "group_by": ["category"]
        })
    """
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute data aggregation."""
        data = params.get("data", [])
        aggregation_specs = params.get("aggregations", [])
        group_by = params.get("group_by", [])
        mode = params.get("mode", "standard")  # standard, rollup, cube, timeseries
        timestamp_field = params.get("timestamp_field", "timestamp")
        window_size = params.get("window_size")  # in seconds for time series
        
        # Parse aggregation specs
        agg_type_map = {
            "sum": AggregationType.SUM,
            "count": AggregationType.COUNT,
            "avg": AggregationType.AVG,
            "min": AggregationType.MIN,
            "max": AggregationType.MAX,
            "first": AggregationType.FIRST,
            "last": AggregationType.LAST,
            "median": AggregationType.MEDIAN,
            "distinct": AggregationType.DISTINCT,
            "list": AggregationType.LIST
        }
        
        if mode == "timeseries":
            aggregator = TimeSeriesAggregator(
                window=timedelta(seconds=window_size or 3600),
                timestamp_field=timestamp_field
            )
        else:
            aggregator = MultiDimensionalAggregator() if mode in ["rollup", "cube"] else DataAggregator()
        
        # Add aggregations
        for spec in aggregation_specs:
            agg_type = agg_type_map.get(spec.get("type", "sum"), AggregationType.SUM)
            aggregator.add_aggregation(
                field=spec["field"],
                agg_type=agg_type,
                alias=spec.get("alias")
            )
        
        # Set group by
        if group_by:
            if hasattr(aggregator, 'group_by'):
                for field in group_by:
                    aggregator.group_by(field)
            if hasattr(aggregator, 'add_dimension'):
                for field in group_by:
                    aggregator.add_dimension(field)
        
        # Execute aggregation
        if mode == "rollup":
            results = aggregator.rollup(data)
        elif mode == "cube":
            results = aggregator.cube(data)
        else:
            results = aggregator.aggregate(data)
        
        return {
            "success": True,
            "mode": mode,
            "input_count": len(data),
            "output_count": len(results) if isinstance(results, list) else 1,
            "results": results,
            "aggregated_at": datetime.now().isoformat()
        }

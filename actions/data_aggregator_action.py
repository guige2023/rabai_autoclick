"""
Data Aggregator Action Module

Multi-dimensional data aggregation with grouping,
windowing, and rollup support.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class AggregationFunc(Enum):
    """Aggregation functions."""
    
    SUM = "sum"
    AVG = "avg"
    COUNT = "count"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    DISTINCT = "distinct"
    STDDEV = "stddev"
    MEDIAN = "median"


class TimeWindow(Enum):
    """Time window types."""
    
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"


@dataclass
class AggregationConfig:
    """Configuration for aggregation."""
    
    group_by: List[str] = field(default_factory=list)
    aggregations: Dict[str, List[AggregationFunc]] = field(default_factory=dict)
    time_field: Optional[str] = None
    time_window: Optional[TimeWindow] = None
    having: Optional[Callable] = None
    order_by: List[Tuple[str, str]] = field(default_factory=list)


@dataclass
class AggregationResult:
    """Result of an aggregation operation."""
    
    groups: List[Dict]
    total_groups: int
    records_processed: int
    execution_time_ms: float


class GroupedAggregator:
    """Handles grouped aggregations."""
    
    def __init__(self, config: AggregationConfig):
        self.config = config
    
    def aggregate(self, records: List[Dict]) -> List[Dict]:
        """Perform grouped aggregation."""
        groups: Dict[Tuple, List[Dict]] = defaultdict(list)
        
        for record in records:
            key = tuple(record.get(field, None) for field in self.config.group_by)
            groups[key].append(record)
        
        results = []
        for key, group_records in groups.items():
            result = self._aggregate_group(key, group_records)
            if result:
                if self.config.having is None or self.config.having(result):
                    results.append(result)
        
        if self.config.order_by:
            results = self._sort_results(results)
        
        return results
    
    def _aggregate_group(self, key: Tuple, records: List[Dict]) -> Optional[Dict]:
        """Aggregate a single group."""
        result = {}
        
        for i, field_name in enumerate(self.config.group_by):
            result[field_name] = key[i]
        
        for field_name, funcs in self.config.aggregations.items():
            for func in funcs:
                result[f"{field_name}_{func.value}"] = self._apply_function(
                    field_name, records, func
                )
        
        return result
    
    def _apply_function(
        self,
        field_name: str,
        records: List[Dict],
        func: AggregationFunc
    ) -> Any:
        """Apply aggregation function to field values."""
        values = [
            record.get(field_name)
            for record in records
            if field_name in record and record.get(field_name) is not None
        ]
        
        if not values:
            return None
        
        if func == AggregationFunc.COUNT:
            return len(values)
        
        if func == AggregationFunc.SUM:
            return sum(v for v in values if isinstance(v, (int, float)))
        
        if func == AggregationFunc.AVG:
            numeric = [v for v in values if isinstance(v, (int, float))]
            return sum(numeric) / len(numeric) if numeric else None
        
        if func == AggregationFunc.MIN:
            return min(values)
        
        if func == AggregationFunc.MAX:
            return max(values)
        
        if func == AggregationFunc.FIRST:
            return values[0]
        
        if func == AggregationFunc.LAST:
            return values[-1]
        
        if func == AggregationFunc.DISTINCT:
            return len(set(values))
        
        if func == AggregationFunc.STDDEV:
            import statistics
            numeric = [v for v in values if isinstance(v, (int, float))]
            return statistics.stdev(numeric) if len(numeric) > 1 else 0
        
        if func == AggregationFunc.MEDIAN:
            import statistics
            return statistics.median(values)
        
        return values
    
    def _sort_results(self, results: List[Dict]) -> List[Dict]:
        """Sort results by order_by fields."""
        def sort_key(item):
            keys = []
            for field_name, direction in self.config.order_by:
                val = item.get(field_name, 0)
                if direction.upper() == "DESC":
                    val = -val if isinstance(val, (int, float)) else val
                keys.append(val)
            return tuple(keys)
        
        return sorted(results, key=sort_key)


class TimeSeriesAggregator:
    """Handles time-series aggregations."""
    
    def __init__(self, config: AggregationConfig):
        self.config = config
        self.time_field = config.time_field or "timestamp"
    
    def aggregate(self, records: List[Dict]) -> List[Dict]:
        """Aggregate records into time buckets."""
        if not self.config.time_window:
            return records
        
        buckets: Dict[str, List[Dict]] = defaultdict(list)
        
        for record in records:
            timestamp = record.get(self.time_field)
            if not timestamp:
                continue
            
            if isinstance(timestamp, str):
                try:
                    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                    timestamp = dt.timestamp()
                except Exception:
                    continue
            elif isinstance(timestamp, datetime):
                timestamp = timestamp.timestamp()
            
            bucket_key = self._get_bucket_key(timestamp)
            buckets[bucket_key].append(record)
        
        results = []
        for bucket_key in sorted(buckets.keys()):
            bucket_records = buckets[bucket_key]
            result = self._aggregate_bucket(bucket_key, bucket_records)
            results.append(result)
        
        return results
    
    def _get_bucket_key(self, timestamp: float) -> str:
        """Get bucket key for timestamp."""
        dt = datetime.fromtimestamp(timestamp)
        
        if self.config.time_window == TimeWindow.MINUTE:
            return dt.strftime("%Y-%m-%d %H:%M")
        elif self.config.time_window == TimeWindow.HOUR:
            return dt.strftime("%Y-%m-%d %H:00")
        elif self.config.time_window == TimeWindow.DAY:
            return dt.strftime("%Y-%m-%d")
        elif self.config.time_window == TimeWindow.WEEK:
            return dt.strftime("%Y-W%W")
        elif self.config.time_window == TimeWindow.MONTH:
            return dt.strftime("%Y-%m")
        
        return dt.isoformat()
    
    def _aggregate_bucket(self, bucket_key: str, records: List[Dict]) -> Dict:
        """Aggregate a single time bucket."""
        result = {"_bucket": bucket_key, "_count": len(records)}
        
        for field_name, funcs in self.config.aggregations.items():
            for func in funcs:
                values = [
                    record.get(field_name)
                    for record in records
                    if field_name in record and record.get(field_name) is not None
                ]
                
                if not values:
                    result[f"{field_name}_{func.value}"] = None
                    continue
                
                if func == AggregationFunc.SUM:
                    result[f"{field_name}_{func.value}"] = sum(
                        v for v in values if isinstance(v, (int, float))
                    )
                elif func == AggregationFunc.AVG:
                    numeric = [v for v in values if isinstance(v, (int, float))]
                    result[f"{field_name}_{func.value}"] = (
                        sum(numeric) / len(numeric) if numeric else None
                    )
                elif func == AggregationFunc.MIN:
                    result[f"{field_name}_{func.value}"] = min(values)
                elif func == AggregationFunc.MAX:
                    result[f"{field_name}_{func.value}"] = max(values)
                elif func == AggregationFunc.COUNT:
                    result[f"{field_name}_{func.value}"] = len(values)
        
        return result


class DataAggregatorAction:
    """
    Main data aggregator action handler.
    
    Provides multi-dimensional aggregation with grouping,
    time-windowing, and rollup support.
    """
    
    def __init__(self, config: Optional[AggregationConfig] = None):
        self.config = config or AggregationConfig()
        self._grouped = GroupedAggregator(self.config)
        self._time_series = TimeSeriesAggregator(self.config)
    
    def aggregate(self, records: List[Dict]) -> AggregationResult:
        """Perform aggregation on records."""
        import time
        start = time.time()
        
        if self.config.time_field and self.config.time_window:
            results = self._time_series.aggregate(records)
        else:
            results = self._grouped.aggregate(records)
        
        execution_time = (time.time() - start) * 1000
        
        return AggregationResult(
            groups=results,
            total_groups=len(results),
            records_processed=len(records),
            execution_time_ms=execution_time
        )
    
    def configure(
        self,
        group_by: Optional[List[str]] = None,
        aggregations: Optional[Dict[str, List[AggregationFunc]]] = None,
        time_field: Optional[str] = None,
        time_window: Optional[TimeWindow] = None
    ) -> None:
        """Update aggregation configuration."""
        if group_by is not None:
            self.config.group_by = group_by
        
        if aggregations is not None:
            self.config.aggregations = aggregations
        
        if time_field is not None:
            self.config.time_field = time_field
        
        if time_window is not None:
            self.config.time_window = time_window
        
        self._grouped = GroupedAggregator(self.config)
        self._time_series = TimeSeriesAggregator(self.config)
    
    def rollup(
        self,
        records: List[Dict],
        levels: List[List[str]]
    ) -> List[Dict]:
        """Perform rollup aggregation at multiple levels."""
        all_results = []
        
        for level in levels:
            self.configure(group_by=level)
            result = self.aggregate(records)
            all_results.extend(result.groups)
        
        return all_results
    
    def drilldown(
        self,
        records: List[Dict],
        dimensions: List[str]
    ) -> Dict[str, List[Dict]]:
        """Perform drilldown aggregation across dimensions."""
        results = {}
        
        for dim in dimensions:
            self.configure(group_by=[dim])
            result = self.aggregate(records)
            results[dim] = result.groups
        
        return results

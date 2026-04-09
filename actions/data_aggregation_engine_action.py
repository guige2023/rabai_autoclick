"""
Data Aggregation Engine Module.

Provides powerful aggregation capabilities for data processing
including grouping, windowing, rolling calculations, and
multi-dimensional data analysis.
"""

from typing import (
    Dict, List, Optional, Any, Callable, Tuple,
    Set, TypeVar, Generic, Union, Iterator
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime, timedelta
import logging
from collections import defaultdict
from itertools import groupby, chain

logger = logging.getLogger(__name__)

T = TypeVar("T")
K = TypeVar("K")
V = TypeVar("V")


class AggregationType(Enum):
    """Types of aggregation operations."""
    SUM = auto()
    AVG = auto()
    COUNT = auto()
    MIN = auto()
    MAX = auto()
    FIRST = auto()
    LAST = auto()
    COUNT_DISTINCT = auto()
    STDDEV = auto()
    MEDIAN = auto()
    PERCENTILE = auto()
    RATE = auto()
    CUSTOM = auto()


@dataclass
class AggregationConfig:
    """Configuration for an aggregation operation."""
    field: str
    agg_type: AggregationType
    alias: Optional[str] = None
    custom_func: Optional[Callable[[List[Any]], Any]] = None
    percentile_value: Optional[float] = None


@dataclass
class AggregationResult:
    """Result of aggregation operation."""
    dimensions: Dict[str, Any]
    metrics: Dict[str, Any]
    record_count: int
    metadata: Dict[str, Any] = field(default_factory=dict)


class WindowFunction:
    """Window function for rolling calculations."""
    
    def __init__(
        self,
        window_size: int,
        min_periods: Optional[int] = None,
        center: bool = False
    ) -> None:
        self.window_size = window_size
        self.min_periods = min_periods or 1
        self.center = center
    
    def calculate(
        self,
        values: List[float],
        func: Callable[[List[float]], float]
    ) -> List[Optional[float]]:
        """Calculate windowed aggregation."""
        n = len(values)
        result = [None] * n
        
        for i in range(n):
            if self.center:
                start = max(0, i - self.window_size // 2)
                end = min(n, i + self.window_size // 2 + 1)
            else:
                start = max(0, i - self.window_size + 1)
                end = i + 1
            
            window = values[start:end]
            
            if len(window) >= self.min_periods:
                result[i] = func(window)
        
        return result


class DataAggregator:
    """
    Comprehensive data aggregation engine.
    
    Supports grouping, multiple aggregation functions,
    windowing, and custom aggregation logic.
    """
    
    def __init__(self) -> None:
        self._group_by_fields: List[str] = []
        self._aggregations: List[AggregationConfig] = []
        self._filters: List[Callable[[Dict], bool]] = []
    
    def group_by(self, *fields: str) -> "DataAggregator":
        """Set group-by fields."""
        self._group_by_fields = list(fields)
        return self
    
    def aggregate(
        self,
        field: str,
        agg_type: AggregationType,
        alias: Optional[str] = None,
        **kwargs
    ) -> "DataAggregator":
        """Add an aggregation operation."""
        config = AggregationConfig(
            field=field,
            agg_type=agg_type,
            alias=alias,
            **kwargs
        )
        self._aggregations.append(config)
        return self
    
    def filter(self, filter_func: Callable[[Dict], bool]) -> "DataAggregator":
        """Add a filter function."""
        self._filters.append(filter_func)
        return self
    
    def execute(self, data: List[Dict[str, Any]]) -> List[AggregationResult]:
        """
        Execute aggregation on data.
        
        Args:
            data: Input data list
            
        Returns:
            List of aggregation results
        """
        if not data:
            return []
        
        # Apply filters
        filtered_data = data
        for f in self._filters:
            filtered_data = [row for row in filtered_data if f(row)]
        
        if not self._group_by_fields:
            # No grouping - single result
            return [self._aggregate_all(filtered_data)]
        
        # Group data
        groups = self._group_data(filtered_data)
        
        # Calculate aggregations for each group
        results = []
        for dimensions, group_data in groups.items():
            metrics = self._calculate_metrics(group_data)
            results.append(AggregationResult(
                dimensions=dimensions,
                metrics=metrics,
                record_count=len(group_data)
            ))
        
        return results
    
    def _group_data(
        self,
        data: List[Dict[str, Any]]
    ) -> Dict[Tuple, List[Dict[str, Any]]]:
        """Group data by configured fields."""
        groups: Dict[Tuple, List[Dict[str, Any]]] = defaultdict(list)
        
        for row in data:
            key = tuple(row.get(field) for field in self._group_by_fields)
            groups[key].append(row)
        
        return groups
    
    def _calculate_metrics(
        self,
        group_data: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Calculate metrics for a group."""
        metrics = {}
        
        for config in self._aggregations:
            values = [row.get(config.field) for row in group_data if config.field in row]
            result = self._apply_aggregation(values, config)
            
            alias = config.alias or f"{config.field}_{config.agg_type.name.lower()}"
            metrics[alias] = result
        
        return metrics
    
    def _apply_aggregation(
        self,
        values: List[Any],
        config: AggregationConfig
    ) -> Any:
        """Apply aggregation function to values."""
        # Filter out None values for numeric operations
        numeric_values = [v for v in values if v is not None]
        
        try:
            numeric_values = [float(v) for v in numeric_values]
        except (ValueError, TypeError):
            numeric_values = []
        
        if config.agg_type == AggregationType.SUM:
            return sum(numeric_values) if numeric_values else None
        
        elif config.agg_type == AggregationType.AVG:
            return sum(numeric_values) / len(numeric_values) if numeric_values else None
        
        elif config.agg_type == AggregationType.COUNT:
            return len(values)
        
        elif config.agg_type == AggregationType.MIN:
            return min(numeric_values) if numeric_values else None
        
        elif config.agg_type == AggregationType.MAX:
            return max(numeric_values) if numeric_values else None
        
        elif config.agg_type == AggregationType.FIRST:
            return values[0] if values else None
        
        elif config.agg_type == AggregationType.LAST:
            return values[-1] if values else None
        
        elif config.agg_type == AggregationType.COUNT_DISTINCT:
            return len(set(values))
        
        elif config.agg_type == AggregationType.STDDEV:
            return self._calculate_stddev(numeric_values)
        
        elif config.agg_type == AggregationType.MEDIAN:
            return self._calculate_median(numeric_values)
        
        elif config.agg_type == AggregationType.PERCENTILE:
            return self._calculate_percentile(
                numeric_values,
                config.percentile_value or 50
            )
        
        elif config.agg_type == AggregationType.CUSTOM and config.custom_func:
            return config.custom_func(values)
        
        return None
    
    def _calculate_stddev(self, values: List[float]) -> Optional[float]:
        """Calculate standard deviation."""
        if len(values) < 2:
            return None
        
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
        return variance ** 0.5
    
    def _calculate_median(self, values: List[float]) -> Optional[float]:
        """Calculate median."""
        if not values:
            return None
        
        sorted_values = sorted(values)
        n = len(sorted_values)
        
        if n % 2 == 0:
            return (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
        else:
            return sorted_values[n // 2]
    
    def _calculate_percentile(
        self,
        values: List[float],
        percentile: float
    ) -> Optional[float]:
        """Calculate percentile value."""
        if not values:
            return None
        
        sorted_values = sorted(values)
        index = (len(sorted_values) - 1) * percentile / 100
        lower = int(index)
        upper = lower + 1
        
        if upper >= len(sorted_values):
            return sorted_values[-1]
        
        weight = index - lower
        return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight
    
    def _aggregate_all(self, data: List[Dict[str, Any]]) -> AggregationResult:
        """Aggregate without grouping."""
        metrics = self._calculate_metrics(data)
        
        return AggregationResult(
            dimensions={},
            metrics=metrics,
            record_count=len(data)
        )


class TimeWindowAggregator:
    """Time-based window aggregation."""
    
    def __init__(
        self,
        time_field: str,
        window_seconds: int,
        window_type: str = "tumbling"  # tumbling, sliding, session
    ) -> None:
        self.time_field = time_field
        self.window_seconds = window_seconds
        self.window_type = window_type
    
    def aggregate(
        self,
        data: List[Dict[str, Any]],
        aggregations: List[AggregationConfig]
    ) -> List[AggregationResult]:
        """Aggregate data into time windows."""
        if not data:
            return []
        
        # Sort by time
        sorted_data = sorted(
            data,
            key=lambda x: x.get(self.time_field, datetime.min)
        )
        
        results = []
        window_start = None
        window_data = []
        
        for row in sorted_data:
            timestamp = row.get(self.time_field)
            
            if window_start is None:
                window_start = timestamp
            
            # Check if we should start a new window
            if (timestamp - window_start).total_seconds() >= self.window_seconds:
                if window_data:
                    results.append(self._aggregate_window(window_data, window_start, aggregations))
                window_start = timestamp
                window_data = []
            
            window_data.append(row)
        
        # Process final window
        if window_data:
            results.append(self._aggregate_window(window_data, window_start, aggregations))
        
        return results
    
    def _aggregate_window(
        self,
        window_data: List[Dict[str, Any]],
        window_start: datetime,
        aggregations: List[AggregationConfig]
    ) -> AggregationResult:
        """Aggregate a single time window."""
        engine = DataAggregator()
        
        metrics = {}
        for config in aggregations:
            values = [row.get(config.field) for row in window_data]
            result = engine._apply_aggregation(values, config)
            alias = config.alias or config.field
            metrics[alias] = result
        
        return AggregationResult(
            dimensions={"window_start": window_start},
            metrics=metrics,
            record_count=len(window_data)
        )


class RollingCalculator:
    """Rolling window calculations."""
    
    def __init__(self, window_size: int) -> None:
        self.window_size = window_size
    
    def rolling_sum(self, values: List[float]) -> List[Optional[float]]:
        """Calculate rolling sum."""
        return self._rolling_calc(values, sum)
    
    def rolling_avg(self, values: List[float]) -> List[Optional[float]]:
        """Calculate rolling average."""
        def avg(vals):
            return sum(vals) / len(vals)
        return self._rolling_calc(values, avg)
    
    def rolling_max(self, values: List[float]) -> List[Optional[float]]:
        """Calculate rolling max."""
        return self._rolling_calc(values, max)
    
    def rolling_min(self, values: List[float]) -> List[Optional[float]]:
        """Calculate rolling min."""
        return self._rolling_calc(values, min)
    
    def _rolling_calc(
        self,
        values: List[float],
        func: Callable[[List[float]], float]
    ) -> List[Optional[float]]:
        """Generic rolling calculation."""
        result = []
        
        for i in range(len(values)):
            start_idx = max(0, i - self.window_size + 1)
            window = values[start_idx:i + 1]
            result.append(func(window) if window else None)
        
        return result


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Sample sales data
    sales_data = [
        {"region": "North", "product": "A", "sales": 100, "date": "2024-01-01"},
        {"region": "North", "product": "A", "sales": 150, "date": "2024-01-02"},
        {"region": "North", "product": "B", "sales": 200, "date": "2024-01-01"},
        {"region": "South", "product": "A", "sales": 80, "date": "2024-01-01"},
        {"region": "South", "product": "B", "sales": 120, "date": "2024-01-02"},
        {"region": "South", "product": "A", "sales": 90, "date": "2024-01-03"},
        {"region": "East", "product": "A", "sales": 110, "date": "2024-01-01"},
        {"region": "East", "product": "B", "sales": 130, "date": "2024-01-01"},
    ]
    
    print("=== Aggregation Demo ===")
    
    # Simple aggregation
    result = (
        DataAggregator()
        .aggregate("sales", AggregationType.SUM, alias="total_sales")
        .aggregate("sales", AggregationType.AVG, alias="avg_sales")
        .aggregate("sales", AggregationType.MAX, alias="max_sale")
        .execute(sales_data)
    )
    
    print("\nSimple aggregation:")
    print(f"  Total: {result[0].metrics}")
    
    # Group by aggregation
    result = (
        DataAggregator()
        .group_by("region")
        .aggregate("sales", AggregationType.SUM, alias="total_sales")
        .aggregate("sales", AggregationType.AVG, alias="avg_sales")
        .execute(sales_data)
    )
    
    print("\nBy region:")
    for r in result:
        print(f"  {r.dimensions}: {r.metrics}")
    
    # Multi-dimensional aggregation
    result = (
        DataAggregator()
        .group_by("region", "product")
        .aggregate("sales", AggregationType.SUM, alias="total_sales")
        .aggregate("sales", AggregationType.COUNT, alias="num_sales")
        .execute(sales_data)
    )
    
    print("\nBy region and product:")
    for r in result:
        print(f"  {r.dimensions}: {r.metrics}")
    
    # Rolling calculation
    print("\n=== Rolling Calculations ===")
    
    values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    
    rolling = RollingCalculator(window_size=3)
    
    print(f"Rolling sum: {rolling.rolling_sum(values)}")
    print(f"Rolling avg: {[round(v, 1) if v else None for v in rolling.rolling_avg(values)]}")

"""Data Aggregate Action Module.

Provides data aggregation capabilities for summarizing and analyzing
datasets with support for various aggregation functions and grouping.

Example:
    >>> from actions.data.data_aggregate_action import DataAggregateAction
    >>> action = DataAggregateAction()
    >>> result = action.aggregate(data, group_by="category", agg="sum", field="amount")
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import math


class AggregationFunction(Enum):
    """Aggregation function types."""
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    MEDIAN = "median"
    STDDEV = "stddev"
    VARIANCE = "variance"
    DISTINCT = "distinct"
    LIST = "list"
    DICT = "dict"


@dataclass
class AggregationConfig:
    """Configuration for aggregation operations.
    
    Attributes:
        group_by: Field(s) to group by
        agg_field: Field to aggregate
        agg_func: Aggregation function
        having: Having clause for filtering groups
        order_by: Field to order results by
        order_desc: Whether to sort in descending order
        limit: Maximum number of results
    """
    group_by: Optional[Union[str, List[str]]] = None
    agg_field: Optional[str] = None
    agg_func: AggregationFunction = AggregationFunction.SUM
    having: Optional[Callable[[Any], bool]] = None
    order_by: Optional[str] = None
    order_desc: bool = False
    limit: Optional[int] = None


@dataclass
class AggregationResult:
    """Result of an aggregation operation.
    
    Attributes:
        success: Whether the operation succeeded
        data: Aggregated data
        groups: Number of groups created
        total: Total number of input records
        errors: List of errors
        metadata: Additional metadata
    """
    success: bool
    data: Any = None
    groups: int = 0
    total: int = 0
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataAggregateAction:
    """Handles data aggregation operations.
    
    Provides various aggregation functions with support for
    grouping, filtering, and ordering.
    
    Example:
        >>> action = DataAggregateAction()
        >>> result = action.aggregate(data, group_by="category", agg="sum", field="amount")
    """
    
    def __init__(self):
        """Initialize the data aggregate action."""
        self._custom_aggregators: Dict[str, Callable[[List], Any]] = {}
        self._lock = threading.RLock()
    
    def register_aggregator(
        self,
        name: str,
        aggregator_fn: Callable[[List], Any]
    ) -> "DataAggregateAction":
        """Register a custom aggregator function.
        
        Args:
            name: Aggregator name
            aggregator_fn: Function that takes a list and returns aggregated value
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._custom_aggregators[name] = aggregator_fn
            return self
    
    def aggregate(
        self,
        data: List[Dict[str, Any]],
        group_by: Optional[Union[str, List[str]]] = None,
        agg: Union[str, AggregationFunction] = AggregationFunction.SUM,
        field: Optional[str] = None,
        config: Optional[AggregationConfig] = None
    ) -> AggregationResult:
        """Aggregate data.
        
        Args:
            data: List of dictionaries to aggregate
            group_by: Field(s) to group by
            agg: Aggregation function name
            field: Field to aggregate
            config: Optional AggregationConfig
        
        Returns:
            AggregationResult with aggregated data
        """
        errors: List[str] = []
        
        if not data:
            return AggregationResult(
                success=True,
                data=[],
                groups=0,
                total=0
            )
        
        # Normalize aggregation function
        if isinstance(agg, str):
            try:
                agg_func = AggregationFunction(agg.lower())
            except ValueError:
                return AggregationResult(
                    success=False,
                    errors=[f"Invalid aggregation function: {agg}"]
                )
        else:
            agg_func = agg
        
        total = len(data)
        
        # No grouping - return single aggregate
        if not group_by:
            try:
                values = [self._get_field_value(row, field) for row in data if field is None or self._get_field_value(row, field) is not None]
                result = self._apply_aggregation(values, agg_func)
                
                return AggregationResult(
                    success=True,
                    data=result,
                    groups=1,
                    total=total,
                    metadata={"aggregation": agg_func.value, "field": field}
                )
            except Exception as e:
                return AggregationResult(
                    success=False,
                    errors=[f"Aggregation error: {str(e)}"]
                )
        
        # Group by
        group_fields = [group_by] if isinstance(group_by, str) else group_by
        
        try:
            groups = self._group_data(data, group_fields)
            
            results = []
            for group_key, group_data in groups.items():
                if field:
                    values = [self._get_field_value(row, field) for row in group_data]
                    agg_value = self._apply_aggregation(values, agg_func)
                else:
                    agg_value = len(group_data)
                
                # Build result row
                if isinstance(group_key, tuple):
                    row = dict(zip(group_fields, group_key))
                else:
                    row = {group_fields[0]: group_key}
                
                row["_aggregate"] = agg_value
                
                # Apply having clause
                if config and config.having and not config.having(agg_value):
                    continue
                
                results.append(row)
            
            # Apply ordering
            if config:
                if config.order_by:
                    results = self._order_results(results, config.order_by, config.order_desc)
                if config.limit:
                    results = results[:config.limit]
            elif len(group_fields) > 0:
                # Default ordering by group key
                results = self._order_results(results, group_fields[0], False)
            
            return AggregationResult(
                success=True,
                data=results,
                groups=len(results),
                total=total,
                metadata={
                    "aggregation": agg_func.value,
                    "field": field,
                    "group_by": group_fields
                }
            )
            
        except Exception as e:
            return AggregationResult(
                success=False,
                errors=[f"Grouping error: {str(e)}"]
            )
    
    def aggregate_multiple(
        self,
        data: List[Dict[str, Any]],
        group_by: Optional[str] = None,
        aggregations: Dict[str, Union[str, AggregationFunction]]
    ) -> AggregationResult:
        """Perform multiple aggregations at once.
        
        Args:
            data: Data to aggregate
            group_by: Field to group by
            aggregations: Dict of {output_field: (source_field, agg_func)}
        
        Returns:
            AggregationResult with multiple aggregates
        """
        errors: List[str] = []
        
        if not data:
            return AggregationResult(success=True, data=[], groups=0, total=0)
        
        total = len(data)
        
        try:
            if not group_by:
                # Single row result
                result_row = {}
                
                for out_field, (src_field, agg_func) in aggregations.items():
                    if isinstance(agg_func, str):
                        agg_func = AggregationFunction(agg_func.lower())
                    
                    values = [self._get_field_value(row, src_field) for row in data]
                    result_row[out_field] = self._apply_aggregation(values, agg_func)
                
                return AggregationResult(
                    success=True,
                    data=result_row,
                    groups=1,
                    total=total
                )
            
            # Grouped result
            groups = self._group_data(data, [group_by])
            results = []
            
            for group_key, group_data in groups.items():
                result_row = {group_by: group_key}
                
                for out_field, (src_field, agg_func) in aggregations.items():
                    if isinstance(agg_func, str):
                        agg_func = AggregationFunction(agg_func.lower())
                    
                    values = [self._get_field_value(row, src_field) for row in group_data]
                    result_row[out_field] = self._apply_aggregation(values, agg_func)
                
                results.append(result_row)
            
            return AggregationResult(
                success=True,
                data=results,
                groups=len(results),
                total=total
            )
            
        except Exception as e:
            return AggregationResult(success=False, errors=[str(e)])
    
    def pivot(
        self,
        data: List[Dict[str, Any]],
        index: str,
        columns: str,
        values: str,
        agg_func: Union[str, AggregationFunction] = AggregationFunction.SUM
    ) -> AggregationResult:
        """Create a pivot table.
        
        Args:
            data: Data to pivot
            index: Field to use as index
            columns: Field to use as columns
            values: Field to aggregate
            agg_func: Aggregation function
        
        Returns:
            AggregationResult with pivoted data
        """
        if isinstance(agg_func, str):
            agg_func = AggregationFunction(agg_func.lower())
        
        try:
            # Get unique column values
            column_values = set(self._get_field_value(row, columns) for row in data)
            
            # Group by index
            groups = self._group_data(data, [index])
            
            results = []
            for idx_val, group_data in groups.items():
                row = {index: idx_val}
                
                # Initialize all columns to None
                for col_val in column_values:
                    row[str(col_val)] = None
                
                # Aggregate by column value
                col_groups = self._group_data(group_data, [columns])
                for col_val, col_data in col_groups.items():
                    vals = [self._get_field_value(row, values) for row in col_data]
                    row[str(col_val)] = self._apply_aggregation(vals, agg_func)
                
                results.append(row)
            
            return AggregationResult(
                success=True,
                data=results,
                groups=len(results),
                total=len(data)
            )
            
        except Exception as e:
            return AggregationResult(success=False, errors=[str(e)])
    
    def _group_data(
        self,
        data: List[Dict[str, Any]],
        fields: List[str]
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """Group data by fields.
        
        Args:
            data: Data to group
            fields: Fields to group by
        
        Returns:
            Dict mapping group keys to data rows
        """
        groups: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        
        for row in data:
            key_values = [self._get_field_value(row, f) for f in fields]
            key = tuple(key_values) if len(key_values) > 1 else key_values[0]
            groups[key].append(row)
        
        return dict(groups)
    
    def _apply_aggregation(
        self,
        values: List[Any],
        func: AggregationFunction
    ) -> Any:
        """Apply an aggregation function to values.
        
        Args:
            values: List of values to aggregate
            field: Field name for custom aggregators
        
        Returns:
            Aggregated value
        """
        # Filter out None values
        values = [v for v in values if v is not None]
        
        if not values:
            return None
        
        if func == AggregationFunction.SUM:
            return sum(values)
        
        elif func == AggregationFunction.COUNT:
            return len(values)
        
        elif func == AggregationFunction.AVG:
            return sum(values) / len(values)
        
        elif func == AggregationFunction.MIN:
            return min(values)
        
        elif func == AggregationFunction.MAX:
            return max(values)
        
        elif func == AggregationFunction.FIRST:
            return values[0]
        
        elif func == AggregationFunction.LAST:
            return values[-1]
        
        elif func == AggregationFunction.MEDIAN:
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            mid = n // 2
            if n % 2 == 0:
                return (sorted_vals[mid - 1] + sorted_vals[mid]) / 2
            return sorted_vals[mid]
        
        elif func == AggregationFunction.STDDEV:
            if len(values) < 2:
                return 0
            avg = sum(values) / len(values)
            variance = sum((v - avg) ** 2 for v in values) / (len(values) - 1)
            return math.sqrt(variance)
        
        elif func == AggregationFunction.VARIANCE:
            if len(values) < 2:
                return 0
            avg = sum(values) / len(values)
            return sum((v - avg) ** 2 for v in values) / (len(values) - 1)
        
        elif func == AggregationFunction.DISTINCT:
            return len(set(values))
        
        elif func == AggregationFunction.LIST:
            return values
        
        elif func == AggregationFunction.DICT:
            counts = defaultdict(int)
            for v in values:
                counts[v] += 1
            return dict(counts)
        
        else:
            # Custom aggregator
            if func.value in self._custom_aggregators:
                return self._custom_aggregators[func.value](values)
            
            return sum(values) / len(values)
    
    def _get_field_value(self, row: Dict[str, Any], field_path: str) -> Any:
        """Get a field value using dot notation.
        
        Args:
            row: Data row
            field_path: Dot-separated field path
        
        Returns:
            Field value or None
        """
        if not field_path:
            return None
        
        keys = field_path.split(".")
        current = row
        
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        
        return current
    
    def _order_results(
        self,
        results: List[Dict[str, Any]],
        order_by: str,
        descending: bool
    ) -> List[Dict[str, Any]]:
        """Order results by a field.
        
        Args:
            results: Results to order
            order_by: Field to order by
            descending: Sort descending
        
        Returns:
            Ordered results
        """
        return sorted(
            results,
            key=lambda r: r.get(order_by, 0),
            reverse=descending
        )

"""Aggregate V2 action module for RabAI AutoClick.

Provides advanced aggregation operations with grouping,
sorting, filtering, and multiple aggregation functions.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass
from collections import defaultdict, Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class AggregationResult:
    """Result of an aggregation operation.
    
    Attributes:
        groups: Grouped results.
        totals: Totals across all groups.
        group_count: Number of groups created.
        record_count: Total number of records processed.
        duration: Time taken for aggregation.
    """
    groups: Dict[str, Any]
    totals: Dict[str, Any]
    group_count: int
    record_count: int
    duration: float


class GroupAggregator:
    """Performs grouping and aggregation on collections of data."""
    
    AGG_FUNCTIONS = {
        'sum': lambda values: sum(v for v in values if v is not None),
        'avg': lambda values: sum(v for v in values if v is not None) / len([v for v in values if v is not None]) if any(v is not None for v in values) else None,
        'min': lambda values: min((v for v in values if v is not None), default=None),
        'max': lambda values: max((v for v in values if v is not None), default=None),
        'count': lambda values: len([v for v in values if v is not None]),
        'first': lambda values: next((v for v in values if v is not None), None),
        'last': lambda values: next((v for v in reversed(values) if v is not None), None),
        'list': lambda values: [v for v in values if v is not None],
        'set': lambda values: list(set(v for v in values if v is not None)),
        'count_distinct': lambda values: len(set(v for v in values if v is not None)),
    }
    
    def __init__(self):
        pass
    
    def aggregate(
        self,
        data: List[Dict[str, Any]],
        group_by: Union[str, List[str]],
        agg_config: Dict[str, Union[str, List[str]]]
    ) -> AggregationResult:
        """Perform grouped aggregation.
        
        Args:
            data: List of records to aggregate.
            group_by: Field name(s) to group by.
            agg_config: Dict mapping output field names to aggregation
                       function names or lists of function names.
        
        Returns:
            AggregationResult with groups and totals.
        """
        start_time = time.time()
        
        if isinstance(group_by, str):
            group_by = [group_by]
        
        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        for record in data:
            key = tuple(record.get(field) for field in group_by)
            groups[key].append(record)
        
        results: Dict[str, Any] = {}
        
        for key, records in groups.items():
            key_str = '_'.join(str(k) for k in key) if len(key) > 1 else str(key[0])
            group_result: Dict[str, Any] = {}
            
            for field, agg_funcs in agg_config.items():
                if isinstance(agg_funcs, str):
                    agg_funcs = [agg_funcs]
                
                for func_name in agg_funcs:
                    output_field = f"{field}_{func_name}" if len(agg_funcs) > 1 else field
                    values = [rec.get(field) for rec in records]
                    
                    if func_name in self.AGG_FUNCTIONS:
                        group_result[output_field] = self.AGG_FUNCTIONS[func_name](values)
                    else:
                        group_result[output_field] = None
            
            for field in group_by:
                group_result[field] = records[0].get(field)
            
            results[key_str] = group_result
        
        all_totals: Dict[str, Any] = {}
        for field, agg_funcs in agg_config.items():
            if isinstance(agg_funcs, str):
                agg_funcs = [agg_funcs]
            
            all_values = [rec.get(field) for rec in data for _ in range(1)]
            all_values = [rec.get(field) for rec in data]
            
            for func_name in agg_funcs:
                output_field = f"total_{field}_{func_name}" if len(agg_funcs) > 1 else f"total_{field}"
                
                if func_name in self.AGG_FUNCTIONS:
                    all_totals[output_field] = self.AGG_FUNCTIONS[func_name](all_values)
        
        return AggregationResult(
            groups=results,
            totals=all_totals,
            group_count=len(groups),
            record_count=len(data),
            duration=time.time() - start_time
        )
    
    def sort_results(
        self,
        results: Dict[str, Any],
        sort_by: str,
        reverse: bool = False
    ) -> List[Dict[str, Any]]:
        """Sort aggregation results by a field.
        
        Args:
            results: Aggregation results from aggregate().
            sort_by: Field name to sort by.
            reverse: Sort in descending order.
        
        Returns:
            Sorted list of group results.
        """
        items = list(results.values())
        
        def get_sort_key(item: Dict[str, Any]) -> Any:
            return item.get(sort_by, 0)
        
        return sorted(items, key=get_sort_key, reverse=reverse)
    
    def filter_results(
        self,
        results: Dict[str, Any],
        filter_expr: Callable[[Dict[str, Any]], bool]
    ) -> Dict[str, Any]:
        """Filter aggregation results by a predicate.
        
        Args:
            results: Aggregation results from aggregate().
            filter_expr: Callable that returns True to keep a group.
        
        Returns:
            Filtered aggregation results.
        """
        return {k: v for k, v in results.items() if filter_expr(v)}


class AggregateV2Action(BaseAction):
    """Advanced aggregation with grouping and multiple functions."""
    action_type = "aggregate_v2"
    display_name = "高级聚合"
    description = "分组聚合多种统计函数"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Perform grouped aggregation.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, group_by, aggregations,
                   sort_by, sort_reverse, filter_expr.
        
        Returns:
            ActionResult with aggregated results.
        """
        data = params.get('data', [])
        group_by = params.get('group_by', [])
        aggregations = params.get('aggregations', {})
        sort_by = params.get('sort_by', None)
        sort_reverse = params.get('sort_reverse', False)
        
        if not data:
            return ActionResult(success=False, message="Data is required")
        
        if not group_by:
            return ActionResult(success=False, message="group_by is required")
        
        if not aggregations:
            return ActionResult(success=False, message="aggregations config is required")
        
        try:
            aggregator = GroupAggregator()
            result = aggregator.aggregate(data, group_by, aggregations)
            
            groups_list = list(result.groups.values())
            
            if sort_by:
                groups_list = aggregator.sort_results(result.groups, sort_by, sort_reverse)
            
            return ActionResult(
                success=True,
                message=f"Aggregated {result.record_count} records into {result.group_count} groups",
                data={
                    "groups": groups_list,
                    "totals": result.totals,
                    "group_count": result.group_count,
                    "record_count": result.record_count,
                    "duration_ms": round(result.duration * 1000, 2)
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Aggregation failed: {str(e)}")


class PivotTableAction(BaseAction):
    """Create a pivot table from data."""
    action_type = "pivot_table"
    display_name = "数据透视"
    description = "创建数据透视表"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create a pivot table.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, rows, columns, values, agg_func.
        
        Returns:
            ActionResult with pivot table data.
        """
        data = params.get('data', [])
        rows = params.get('rows', [])
        columns = params.get('columns', [])
        values = params.get('values', [])
        agg_func = params.get('agg_func', 'sum')
        
        if not data:
            return ActionResult(success=False, message="Data is required")
        
        if not rows or not values:
            return ActionResult(success=False, message="rows and values are required")
        
        try:
            pivot: Dict[str, Dict[str, float]] = defaultdict(dict)
            row_keys: set = set()
            col_keys: set = set()
            
            for record in data:
                row_key = tuple(record.get(r) for r in rows)
                row_key_str = '_'.join(str(k) for k in row_key)
                col_key = tuple(record.get(c) for c in columns) if columns else ('_total',)
                col_key_str = '_'.join(str(k) for k in col_key)
                
                row_keys.add(row_key_str)
                col_keys.add(col_key_str)
                
                if col_key_str not in pivot[row_key_str]:
                    pivot[row_key_str][col_key_str] = []
                
                for val_field in values:
                    pivot[row_key_str][col_key_str].append(record.get(val_field))
            
            AGG_FUNCTIONS = GroupAggregator.AGG_FUNCTIONS
            result_data = {}
            
            for row_key in row_keys:
                result_data[row_key] = {}
                for col_key in col_keys:
                    vals = pivot.get(row_key, {}).get(col_key, [])
                    if agg_func in AGG_FUNCTIONS:
                        result_data[row_key][col_key] = AGG_FUNCTIONS[agg_func](vals)
                    else:
                        result_data[row_key][col_key] = vals
            
            return ActionResult(
                success=True,
                message=f"Pivot table created with {len(row_keys)} rows",
                data={
                    "data": result_data,
                    "row_keys": list(row_keys),
                    "col_keys": list(col_keys),
                    "rows": rows,
                    "columns": columns,
                    "values": values,
                    "agg_func": agg_func
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pivot table failed: {str(e)}")


class StatisticsAction(BaseAction):
    """Calculate statistical measures on a numeric field."""
    action_type = "statistics"
    display_name = "统计分析"
    description = "计算数值字段统计量"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Calculate statistics.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field.
        
        Returns:
            ActionResult with statistical measures.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        
        if not data:
            return ActionResult(success=False, message="Data is required")
        
        if not field:
            return ActionResult(success=False, message="field is required")
        
        try:
            values = [record.get(field) for record in data if record.get(field) is not None]
            
            if not values:
                return ActionResult(success=False, message=f"No non-null values for field '{field}'")
            
            sorted_values = sorted(values)
            n = len(sorted_values)
            total = sum(sorted_values)
            mean = total / n
            
            median = sorted_values[n // 2] if n % 2 == 1 else (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
            
            variance = sum((x - mean) ** 2 for x in sorted_values) / n
            std_dev = variance ** 0.5
            
            q1_idx = n // 4
            q3_idx = 3 * n // 4
            q1 = sorted_values[q1_idx]
            q3 = sorted_values[q3_idx]
            iqr = q3 - q1
            
            return ActionResult(
                success=True,
                message=f"Statistics for '{field}'",
                data={
                    "field": field,
                    "count": n,
                    "sum": total,
                    "mean": mean,
                    "median": median,
                    "min": sorted_values[0],
                    "max": sorted_values[-1],
                    "std_dev": std_dev,
                    "variance": variance,
                    "q1": q1,
                    "q3": q3,
                    "iqr": iqr
                }
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Statistics failed: {str(e)}")

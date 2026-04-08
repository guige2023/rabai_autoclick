"""Data aggregator action module for RabAI AutoClick.

Provides data aggregation with grouping, pivoting, and
multi-dimensional analysis capabilities.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Callable, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from itertools import groupby

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class Aggregation:
    """An aggregation definition."""
    field: str
    function: str  # sum, count, avg, min, max, first, last, distinct
    alias: str = ""


class DataAggregatorAction(BaseAction):
    """Aggregate data with group-by and multiple aggregation functions.
    
    Supports multi-level grouping, computed aggregations, and
    pivot table generation.
    """
    action_type = "data_aggregator_v2"
    display_name = "数据聚合器"
    description = "分组聚合和多维分析"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data aggregation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data (list of dicts)
                - group_by: Field or list of fields to group by
                - aggregations: List of Aggregation dicts
                - having: Having clause for filtered aggregation
                - order_by: Field to order results by
        
        Returns:
            ActionResult with aggregated results.
        """
        data = params.get('data', [])
        group_by = params.get('group_by')
        aggregations = params.get('aggregations', [])
        having = params.get('having')
        order_by = params.get('order_by')
        
        if not data:
            return ActionResult(success=False, message="data is required")
        
        if not group_by:
            return ActionResult(success=False, message="group_by is required")
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        # Normalize group_by to list
        if isinstance(group_by, str):
            group_by = [group_by]
        
        # Parse aggregations
        aggs = []
        for a in aggregations:
            if isinstance(a, dict):
                aggs.append(Aggregation(
                    field=a['field'],
                    function=a['function'],
                    alias=a.get('alias', '')
                ))
            else:
                aggs.append(a)
        
        # Group data
        groups = self._group_data(data, group_by)
        
        # Compute aggregations for each group
        results = []
        for group_key, group_items in groups.items():
            row = self._build_group_row(group_key, group_by, group_items, aggs)
            results.append(row)
        
        # Apply having clause
        if having:
            results = self._apply_having(results, having)
        
        # Apply ordering
        if order_by:
            reverse = order_by.startswith('-')
            field = order_by.lstrip('-')
            results = sorted(
                results,
                key=lambda r: r.get(field, 0),
                reverse=reverse
            )
        
        return ActionResult(
            success=True,
            message=f"Aggregated into {len(results)} groups",
            data={
                'results': results,
                'group_count': len(results)
            }
        )
    
    def _group_data(
        self,
        data: List[Dict],
        group_by: List[str]
    ) -> Dict[Tuple, List[Dict]]:
        """Group data by fields."""
        groups = defaultdict(list)
        
        for item in data:
            key = tuple(item.get(field) for field in group_by)
            groups[key].append(item)
        
        return dict(groups)
    
    def _build_group_row(
        self,
        group_key: Tuple,
        group_by: List[str],
        items: List[Dict],
        aggregations: List[Aggregation]
    ) -> Dict[str, Any]:
        """Build result row for a group."""
        row = {}
        
        # Add group key fields
        for i, field in enumerate(group_by):
            row[field] = group_key[i]
        
        # Compute aggregations
        for agg in aggregations:
            value = self._compute_agg(items, agg)
            alias = agg.alias or f"{agg.field}_{agg.function}"
            row[alias] = value
        
        # Add count
        row['_count'] = len(items)
        
        return row
    
    def _compute_agg(self, items: List[Dict], agg: Aggregation) -> Any:
        """Compute aggregation for items."""
        values = []
        for item in items:
            val = item.get(agg.field)
            if val is not None:
                values.append(val)
        
        if not values:
            return None
        
        func = agg.function.lower()
        
        if func == 'sum':
            return sum(values)
        elif func == 'count':
            return len(values)
        elif func == 'avg':
            return sum(values) / len(values)
        elif func == 'min':
            return min(values)
        elif func == 'max':
            return max(values)
        elif func == 'first':
            return values[0]
        elif func == 'last':
            return values[-1]
        elif func == 'distinct':
            return len(set(values))
        
        return None
    
    def _apply_having(
        self,
        results: List[Dict],
        having: Dict[str, Any]
    ) -> List[Dict]:
        """Apply having clause filter."""
        filtered = []
        for row in results:
            for field, condition in having.items():
                if isinstance(condition, dict):
                    op = condition.get('op')
                    value = condition.get('value')
                    row_value = row.get(field, 0)
                    
                    if op == 'gt' and not (row_value > value):
                        break
                    elif op == 'gte' and not (row_value >= value):
                        break
                    elif op == 'lt' and not (row_value < value):
                        break
                    elif op == 'lte' and not (row_value <= value):
                        break
                    elif op == 'eq' and not (row_value == value):
                        break
                else:
                    if row.get(field) != condition:
                        break
            else:
                filtered.append(row)
        
        return filtered


class PivotTableAction(BaseAction):
    """Generate pivot tables from data."""
    action_type = "pivot_table"
    display_name = "透视表"
    description = "数据透视表生成"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate pivot table.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Input data
                - index: Field for row index
                - columns: Field for column headers
                - values: Field to aggregate
                - aggfunc: Aggregation function
        
        Returns:
            ActionResult with pivot table.
        """
        data = params.get('data', [])
        index = params.get('index')
        columns = params.get('columns')
        values = params.get('values')
        aggfunc = params.get('aggfunc', 'sum')
        
        if not data or not index or not columns or not values:
            return ActionResult(
                success=False,
                message="data, index, columns, and values are required"
            )
        
        # Build pivot structure
        pivot = defaultdict(lambda: defaultdict(list))
        column_values = set()
        
        for item in data:
            row_key = item.get(index)
            col_key = item.get(columns)
            val = item.get(values)
            
            if row_key is not None and col_key is not None:
                pivot[row_key][col_key].append(val)
                column_values.add(col_key)
        
        # Compute aggregations
        results = []
        for row_key in sorted(pivot.keys()):
            row = {index: row_key}
            for col_key in sorted(column_values):
                vals = pivot[row_key].get(col_key, [])
                if vals:
                    if aggfunc == 'sum':
                        row[str(col_key)] = sum(vals)
                    elif aggfunc == 'count':
                        row[str(col_key)] = len(vals)
                    elif aggfunc == 'avg':
                        row[str(col_key)] = sum(vals) / len(vals)
                    elif aggfunc == 'min':
                        row[str(col_key)] = min(vals)
                    elif aggfunc == 'max':
                        row[str(col_key)] = max(vals)
                else:
                    row[str(col_key)] = None
            results.append(row)
        
        return ActionResult(
            success=True,
            message=f"Generated pivot table with {len(results)} rows",
            data={
                'results': results,
                'columns': list(sorted(column_values))
            }
        )

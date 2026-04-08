"""Aggregator action module for RabAI AutoClick.

Provides data aggregation operations (group, sum, count, average, etc.).
"""

import sys
import os
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AggregatorAction(BaseAction):
    """Data aggregation operations.
    
    Supports group-by aggregation with sum, count, avg, min, max,
    std, median, and custom aggregation functions.
    """
    action_type = "aggregator"
    display_name = "数据聚合"
    description = "分组聚合：求和、计数、平均、最值"
    
    def __init__(self) -> None:
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute aggregation operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'group_by', 'total', 'pivot'
                - data: List of dicts to aggregate
                - group_by: Field name(s) to group by (str or list)
                - aggregations: Dict of field -> agg function(s) {'field': ['sum','avg']}
                - having: Filter groups by condition (e.g., {'count': {'>': 10}})
        
        Returns:
            ActionResult with aggregated results.
        """
        command = params.get('command', 'group_by')
        data = params.get('data', [])
        group_by = params.get('group_by')
        aggregations = params.get('aggregations', {})
        having = params.get('having')
        
        if not isinstance(data, list):
            return ActionResult(success=False, message="data must be a list")
        
        if command == 'group_by':
            return self._group_by(data, group_by, aggregations, having)
        if command == 'total':
            return self._total(data, aggregations)
        if command == 'pivot':
            return self._pivot(data, params.get('index'), params.get('columns'), params.get('values'))
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _group_by(self, data: List[Dict], group_by: Any, aggregations: Dict, having: Optional[Dict]) -> ActionResult:
        """Group data and apply aggregations."""
        if not group_by:
            return ActionResult(success=False, message="group_by field is required")
        
        group_fields = [group_by] if isinstance(group_by, str) else group_by
        
        def get_group_key(row):
            if len(group_fields) == 1:
                return str(row.get(group_fields[0], ''))
            return tuple(str(row.get(f, '')) for f in group_fields)
        
        groups: Dict[Any, List[Dict]] = defaultdict(list)
        for row in data:
            groups[get_group_key(row)].append(row)
        
        results = []
        for group_key, group_rows in groups.items():
            result: Dict[str, Any] = {}
            if len(group_fields) == 1:
                result[group_fields[0]] = group_key
            else:
                for i, f in enumerate(group_fields):
                    result[f] = group_key[i]
            
            result['_count'] = len(group_rows)
            
            for field, funcs in aggregations.items():
                func_list = [funcs] if isinstance(funcs, str) else funcs
                for func in func_list:
                    values = [r.get(field) for r in group_rows if r.get(field) is not None]
                    values = [v for v in values if isinstance(v, (int, float))]
                    if not values:
                        result[f'{field}_{func}'] = None
                    elif func == 'sum':
                        result[f'{field}_{func}'] = sum(values)
                    elif func == 'count':
                        result[f'{field}_{func}'] = len(values)
                    elif func == 'avg':
                        result[f'{field}_{func}'] = sum(values) / len(values)
                    elif func == 'min':
                        result[f'{field}_{func}'] = min(values)
                    elif func == 'max':
                        result[f'{field}_{func}'] = max(values)
                    elif func == 'std':
                        import statistics
                        result[f'{field}_{func}'] = statistics.stdev(values) if len(values) > 1 else 0
                    elif func == 'median':
                        import statistics
                        result[f'{field}_{func}'] = statistics.median(values)
            
            results.append(result)
        
        if having:
            results = self._apply_having(results, having)
        
        return ActionResult(
            success=True,
            message=f"Aggregated into {len(results)} groups",
            data={'results': results, 'group_count': len(results)}
        )
    
    def _apply_having(self, results: List[Dict], having: Dict) -> List[Dict]:
        """Filter groups by having condition."""
        filtered = []
        for row in results:
            passes = True
            for field, condition in having.items():
                if isinstance(condition, dict):
                    val = row.get(field, 0)
                    for op, threshold in condition.items():
                        if op == '>':
                            if not val > threshold:
                                passes = False
                        elif op == '>=':
                            if not val >= threshold:
                                passes = False
                        elif op == '<':
                            if not val < threshold:
                                passes = False
                        elif op == '<=':
                            if not val <= threshold:
                                passes = False
                        elif op == '==':
                            if not val == threshold:
                                passes = False
                        elif op == '!=':
                            if not val != threshold:
                                passes = False
                if not passes:
                    break
            if passes:
                filtered.append(row)
        return filtered
    
    def _total(self, data: List[Dict], aggregations: Dict) -> ActionResult:
        """Calculate totals across all data."""
        result: Dict[str, Any] = {'_count': len(data)}
        
        for field, funcs in aggregations.items():
            values = [r.get(field) for r in data if r.get(field) is not None]
            values = [v for v in values if isinstance(v, (int, float))]
            func_list = [funcs] if isinstance(funcs, str) else funcs
            for func in func_list:
                if not values:
                    result[f'{field}_{func}'] = None
                elif func == 'sum':
                    result[f'{field}_{func}'] = sum(values)
                elif func == 'avg':
                    result[f'{field}_{func}'] = sum(values) / len(values)
                elif func == 'min':
                    result[f'{field}_{func}'] = min(values)
                elif func == 'max':
                    result[f'{field}_{func}'] = max(values)
        
        return ActionResult(
            success=True,
            message=f"Total for {len(data)} rows",
            data={'totals': result}
        )
    
    def _pivot(self, data: List[Dict], index: Optional[str], columns: Optional[str], values: Optional[str]) -> ActionResult:
        """Create pivot table."""
        if not index or not columns or not values:
            return ActionResult(success=False, message="index, columns, values are required for pivot")
        
        pivot: Dict[Any, Dict[Any, Any]] = defaultdict(dict)
        col_values = set()
        
        for row in data:
            idx = row.get(index)
            col = row.get(columns)
            val = row.get(values)
            if idx is not None and col is not None:
                pivot[idx][col] = val
                col_values.add(col)
        
        results = []
        for idx, col_dict in pivot.items():
            result_row = {index: idx}
            for col in sorted(col_values):
                result_row[col] = col_dict.get(col)
            results.append(result_row)
        
        return ActionResult(
            success=True,
            message=f"Pivot table: {len(results)} rows x {len(col_values)} columns",
            data={'results': results, 'columns': sorted(col_values)}
        )

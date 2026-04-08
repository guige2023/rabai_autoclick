"""Data grouper action module for RabAI AutoClick.

Provides data grouping capabilities with aggregation functions
for grouping data by one or more fields.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataGrouperAction(BaseAction):
    """Data grouper action for grouping and aggregating data.
    
    Groups data by specified fields and applies aggregation
    functions to produce grouped results.
    """
    action_type = "data_grouper"
    display_name = "数据分组器"
    description = "数据分组聚合"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute grouping operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                data: Data to group
                group_by: Field(s) to group by
                aggregations: Dict of field -> aggregation function
                having: Filter groups by condition
                sort_by: Field to sort results by
                sort_order: 'asc' or 'desc'.
        
        Returns:
            ActionResult with grouped data.
        """
        data = params.get('data', [])
        group_by = params.get('group_by')
        aggregations = params.get('aggregations', {})
        having = params.get('having')
        sort_by = params.get('sort_by')
        sort_order = params.get('sort_order', 'asc')
        
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        if not group_by:
            return ActionResult(success=False, message="group_by field required")
        
        group_fields = group_by.split(',') if isinstance(group_by, str) else group_by
        
        groups: Dict[Tuple, List[Dict]] = defaultdict(list)
        
        for item in data:
            if not isinstance(item, dict):
                continue
            
            key = tuple(item.get(f.strip()) for f in group_fields)
            groups[key].append(item)
        
        results = []
        
        for key, items in groups.items():
            result = {}
            
            for i, field in enumerate(group_fields):
                result[field.strip()] = key[i]
            
            for field, func in aggregations.items():
                values = [item.get(field) for item in items if item.get(field) is not None]
                result[f"{field}_{func}"] = self._aggregate(values, func)
            
            result['_count'] = len(items)
            results.append(result)
        
        if having:
            results = self._apply_having(results, having)
        
        if sort_by:
            reverse = sort_order == 'desc'
            results = sorted(results, key=lambda x: x.get(sort_by, ''), reverse=reverse)
        
        return ActionResult(
            success=True,
            message=f"Grouped into {len(results)} groups",
            data={
                'groups': results,
                'group_count': len(results),
                'total_items': len(data)
            }
        )
    
    def _aggregate(self, values: List[Any], func: str) -> Any:
        """Apply aggregation function to values."""
        if not values:
            return None
        
        numeric_values = [v for v in values if isinstance(v, (int, float))]
        
        if func == 'count':
            return len(values)
        elif func == 'count_distinct':
            return len(set(values))
        elif func == 'sum':
            return sum(numeric_values) if numeric_values else None
        elif func == 'avg' or func == 'mean':
            return sum(numeric_values) / len(numeric_values) if numeric_values else None
        elif func == 'min':
            return min(values)
        elif func == 'max':
            return max(values)
        elif func == 'first':
            return values[0]
        elif func == 'last':
            return values[-1]
        elif func == 'array':
            return values
        elif func == 'stddev':
            if len(numeric_values) > 1:
                import statistics
                return statistics.stdev(numeric_values)
            return 0
        elif func == 'median':
            import statistics
            return statistics.median(values)
        
        return values[0] if values else None
    
    def _apply_having(self, results: List[Dict], having: Dict) -> List[Dict]:
        """Filter groups using having clause."""
        filtered = []
        
        for result in results:
            passes = True
            
            for field, condition in having.items():
                if isinstance(condition, dict):
                    op = condition.get('op', '==')
                    value = condition.get('value')
                    
                    actual = result.get(field)
                    
                    if op == '>' and not actual > value:
                        passes = False
                    elif op == '>=' and not actual >= value:
                        passes = False
                    elif op == '<' and not actual < value:
                        passes = False
                    elif op == '<=' and not actual <= value:
                        passes = False
                    elif op == '!=' and actual == value:
                        passes = False
                    elif op == '==' and actual != value:
                        passes = False
                else:
                    if result.get(field) != condition:
                        passes = False
                
                if not passes:
                    break
            
            if passes:
                filtered.append(result)
        
        return filtered

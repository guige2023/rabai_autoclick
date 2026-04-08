"""Data Group Action.

Groups data by key fields with aggregation functions, nested grouping,
and configurable output formats.
"""

import sys
import os
from typing import Any, Dict, List, Optional
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataGroupAction(BaseAction):
    """Group data by key fields with aggregations.
    
    Groups records by single or multiple keys with support for
    sum, count, avg, min, max, and custom aggregations.
    """
    action_type = "data_group"
    display_name = "数据分组"
    description = "按字段分组数据，支持聚合函数和嵌套分组"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Group data by key fields.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: List of records to group.
                - group_by: Field(s) to group by.
                - aggregations: Dict of field -> aggregation function.
                - having: Filter groups by condition.
                - flatten: Flatten output (default: True).
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with grouped data.
        """
        try:
            data = params.get('data')
            group_by = params.get('group_by')
            aggregations = params.get('aggregations', {})
            having = params.get('having')
            flatten = params.get('flatten', True)
            save_to_var = params.get('save_to_var', 'grouped_data')

            if data is None:
                data = context.get_variable(params.get('use_var', 'input_data'))

            if not data:
                return ActionResult(success=False, message="No data provided")

            if not isinstance(data, list):
                return ActionResult(success=False, message="Data must be a list")

            if not group_by:
                return ActionResult(success=False, message="group_by is required")

            if isinstance(group_by, str):
                group_by = [group_by]

            # Group data
            groups = defaultdict(list)
            for item in data:
                if isinstance(item, dict):
                    key = tuple(item.get(f) for f in group_by)
                    groups[key].append(item)

            # Apply aggregations
            results = []
            for key, items in groups.items():
                group_result = {f: k for f, k in zip(group_by, key)}
                
                for field, agg_func in aggregations.items():
                    values = [item.get(field) for item in items if field in item]
                    group_result[field] = self._apply_aggregation(values, agg_func)
                
                # Count
                group_result['_count'] = len(items)
                
                results.append(group_result)

            # Apply having filter
            if having:
                results = self._apply_having(results, having)

            if flatten and group_by:
                key_fields = group_by
                results = [{k: v for k, v in r.items() if k not in ['_items']} for r in results]

            summary = {
                'original_count': len(data),
                'group_count': len(results),
                'groups': results
            }

            context.set_variable(save_to_var, results)
            return ActionResult(success=True, data=summary,
                             message=f"Grouped into {len(results)} groups")

        except Exception as e:
            return ActionResult(success=False, message=f"Group error: {e}")

    def _apply_aggregation(self, values: List, func: str) -> Any:
        """Apply aggregation function to values."""
        if not values:
            return None

        numeric_values = [v for v in values if isinstance(v, (int, float))]

        if func == 'sum':
            return sum(numeric_values) if numeric_values else None
        elif func == 'count':
            return len(values)
        elif func == 'avg' or func == 'mean':
            return sum(numeric_values) / len(numeric_values) if numeric_values else None
        elif func == 'min':
            return min(values) if values else None
        elif func == 'max':
            return max(values) if values else None
        elif func == 'first':
            return values[0]
        elif func == 'last':
            return values[-1]
        elif func == 'list' or func == 'array':
            return values
        elif func == 'set' or func == 'distinct':
            return list(set(values))
        elif func == 'concat':
            return ','.join(str(v) for v in values)
        elif func == 'count_distinct':
            return len(set(values))
        elif func == 'stddev':
            if len(numeric_values) > 1:
                import statistics
                return statistics.stdev(numeric_values)
            return 0
        elif func == 'median':
            import statistics
            return statistics.median(values)
        return values

    def _apply_having(self, groups: List[Dict], having: Dict) -> List:
        """Apply having filter to groups."""
        field = having.get('field')
        op = having.get('op', 'gt')
        value = having.get('value')

        filtered = []
        for group in groups:
            group_val = group.get(field)
            if group_val is None:
                continue
            
            if op == 'gt' and group_val > value:
                filtered.append(group)
            elif op == 'ge' and group_val >= value:
                filtered.append(group)
            elif op == 'lt' and group_val < value:
                filtered.append(group)
            elif op == 'le' and group_val <= value:
                filtered.append(group)
            elif op == 'eq' and group_val == value:
                filtered.append(group)
            elif op == 'ne' and group_val != value:
                filtered.append(group)
        
        return filtered

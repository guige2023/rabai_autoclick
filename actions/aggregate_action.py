"""Aggregate action module for RabAI AutoClick.

Provides data aggregation actions including grouping,
counting, summing, and statistical operations.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class GroupByAction(BaseAction):
    """Group data by specified fields.
    
    Creates grouped collections from flat data.
    """
    action_type = "group_by"
    display_name = "分组"
    description = "数据分组聚合"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Group data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, group_by, having,
                   order_by.
        
        Returns:
            ActionResult with grouped data.
        """
        data = params.get('data', [])
        group_by = params.get('group_by', [])
        having = params.get('having', None)
        order_by = params.get('order_by', None)

        if not data:
            return ActionResult(success=False, message="data is required")
        if not group_by:
            return ActionResult(success=False, message="group_by fields required")

        try:
            groups = defaultdict(list)

            for item in data:
                if not isinstance(item, dict):
                    continue

                key_values = tuple(item.get(field) for field in group_by)
                groups[key_values].append(item)

            result = []
            for key_values, items in groups.items():
                group_data = {
                    '_key': dict(zip(group_by, key_values)),
                    '_items': items,
                    '_count': len(items)
                }
                
                if having:
                    if self._evaluate_having(group_data, having):
                        result.append(group_data)
                else:
                    result.append(group_data)

            if order_by:
                reverse = order_by.get('desc', False)
                key = order_by.get('field', '_count')
                result.sort(key=lambda x: x.get(key, 0), reverse=reverse)

            return ActionResult(
                success=True,
                message=f"Grouped into {len(result)} groups",
                data={
                    'groups': result,
                    'group_count': len(result),
                    'group_by': group_by
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"GroupBy failed: {str(e)}")

    def _evaluate_having(self, group_data: Dict, having: Dict) -> bool:
        """Evaluate HAVING clause."""
        field = having.get('field', '_count')
        operator = having.get('operator', '>')
        value = having.get('value', 0)

        field_value = group_data.get(field, 0)

        if operator == '>':
            return field_value > value
        elif operator == '>=':
            return field_value >= value
        elif operator == '<':
            return field_value < value
        elif operator == '<=':
            return field_value <= value
        elif operator == '==':
            return field_value == value
        elif operator == '!=':
            return field_value != value

        return True


class SumAction(BaseAction):
    """Sum numeric values in data.
    
    Calculates sum of specified fields.
    """
    action_type = "sum"
    display_name = "求和"
    description = "数值字段求和"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Sum values.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, group_by.
        
        Returns:
            ActionResult with sum result.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        group_by = params.get('group_by', [])

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            if group_by:
                groups = defaultdict(float)
                
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    key = tuple(item.get(g) for g in group_by)
                    value = item.get(field, 0)
                    if isinstance(value, (int, float)):
                        groups[key] += value

                result = [
                    dict(zip(group_by, key), **{'sum': value, '_key': dict(zip(group_by, key))})
                    for key, value in groups.items()
                ]

                return ActionResult(
                    success=True,
                    message=f"Summed by {len(result)} groups",
                    data={'results': result, 'group_by': group_by, 'field': field}
                )

            else:
                total = 0
                for item in data:
                    if isinstance(item, (int, float)):
                        total += item
                    elif isinstance(item, dict):
                        value = item.get(field, 0)
                        if isinstance(value, (int, float)):
                            total += value

                return ActionResult(
                    success=True,
                    message=f"Sum: {total}",
                    data={'sum': total, 'field': field, 'count': len(data)}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Sum failed: {str(e)}")


class CountAction(BaseAction):
    """Count items in data.
    
    Supports count all, count by field, and distinct count.
    """
    action_type = "count"
    display_name = "计数"
    description = "数据计数统计"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Count items.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, distinct,
                   group_by.
        
        Returns:
            ActionResult with count result.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        distinct = params.get('distinct', False)
        group_by = params.get('group_by', [])

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            if group_by:
                groups = defaultdict(set if distinct else list)
                
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    key = tuple(item.get(g) for g in group_by)
                    value = item.get(field) if field else item
                    
                    if distinct:
                        groups[key].add(value)
                    else:
                        groups[key].append(value)

                result = [
                    dict(zip(group_by, key), **{
                        'count': len(values),
                        '_key': dict(zip(group_by, key))
                    })
                    for key, values in groups.items()
                ]

                return ActionResult(
                    success=True,
                    message=f"Counted {len(result)} groups",
                    data={'results': result, 'group_by': group_by, 'distinct': distinct}
                )

            else:
                if distinct:
                    if field:
                        values = set(item.get(field) for item in data if isinstance(item, dict) and field in item)
                    else:
                        values = set(str(item) for item in data)
                    count = len(values)
                else:
                    count = len(data)

                return ActionResult(
                    success=True,
                    message=f"Count: {count}",
                    data={'count': count, 'distinct': distinct, 'field': field}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Count failed: {str(e)}")


class AverageAction(BaseAction):
    """Calculate average of numeric values.
    
    Supports mean, median, and weighted average.
    """
    action_type = "average"
    display_name = "平均值"
    description = "数值平均计算"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Calculate average.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, method,
                   group_by.
        
        Returns:
            ActionResult with average result.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        method = params.get('method', 'mean')
        group_by = params.get('group_by', [])

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            def get_values(items):
                if field:
                    return [item.get(field) for item in items if isinstance(item, dict) and isinstance(item.get(field), (int, float))]
                return [item for item in items if isinstance(item, (int, float))]

            if group_by:
                groups = defaultdict(list)
                
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    key = tuple(item.get(g) for g in group_by)
                    value = item.get(field) if field else item
                    if isinstance(value, (int, float)):
                        groups[key].append(value)

                result = []
                for key, values in groups.items():
                    avg = self._calculate_avg(values, method)
                    result.append(dict(zip(group_by, key), **{
                        'average': avg,
                        '_key': dict(zip(group_by, key))
                    }))

                return ActionResult(
                    success=True,
                    message=f"Calculated average for {len(result)} groups",
                    data={'results': result, 'method': method}
                )

            else:
                values = get_values(data)
                if not values:
                    return ActionResult(success=False, message="No numeric values found")

                avg = self._calculate_avg(values, method)

                return ActionResult(
                    success=True,
                    message=f"Average: {avg}",
                    data={'average': avg, 'method': method, 'count': len(values)}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Average failed: {str(e)}")

    def _calculate_avg(self, values: List, method: str) -> float:
        """Calculate average using specified method."""
        if not values:
            return 0

        if method == 'mean':
            return sum(values) / len(values)
        
        elif method == 'median':
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            if n % 2 == 0:
                return (sorted_vals[n // 2 - 1] + sorted_vals[n // 2]) / 2
            return sorted_vals[n // 2]
        
        elif method == 'mode':
            from collections import Counter
            counter = Counter(values)
            return counter.most_common(1)[0][0]

        return sum(values) / len(values)


class MinMaxAction(BaseAction):
    """Find minimum and maximum values.
    
    Supports finding min/max in data and by groups.
    """
    action_type = "min_max"
    display_name = "最大最小"
    description = "查找最大最小值"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Find min/max.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, field, group_by,
                   return_item.
        
        Returns:
            ActionResult with min/max result.
        """
        data = params.get('data', [])
        field = params.get('field', '')
        group_by = params.get('group_by', [])
        return_item = params.get('return_item', False)

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            if group_by:
                groups = defaultdict(list)
                
                for item in data:
                    if not isinstance(item, dict):
                        continue
                    key = tuple(item.get(g) for g in group_by)
                    value = item.get(field) if field else item
                    if isinstance(value, (int, float)):
                        groups[key].append((value, item))

                result = []
                for key, values in groups.items():
                    if not values:
                        continue
                    min_val = min(values, key=lambda x: x[0])
                    max_val = max(values, key=lambda x: x[0])
                    
                    item = {
                        '_key': dict(zip(group_by, key)),
                        'min': min_val[0],
                        'max': max_val[0],
                        **dict(zip(group_by, key))
                    }
                    
                    if return_item:
                        item['min_item'] = min_val[1]
                        item['max_item'] = max_val[1]
                    
                    result.append(item)

                return ActionResult(
                    success=True,
                    message=f"Found min/max for {len(result)} groups",
                    data={'results': result, 'group_by': group_by}
                )

            else:
                if field:
                    items_with_values = [(item.get(field), item) for item in data if isinstance(item, dict) and isinstance(item.get(field), (int, float))]
                else:
                    items_with_values = [(item, item) for item in data if isinstance(item, (int, float))]

                if not items_with_values:
                    return ActionResult(success=False, message="No numeric values found")

                min_val = min(items_with_values, key=lambda x: x[0])
                max_val = max(items_with_values, key=lambda x: x[0])

                result = {
                    'min': min_val[0],
                    'max': max_val[0],
                    'min_item': min_val[1] if return_item else None,
                    'max_item': max_val[1] if return_item else None
                }

                return ActionResult(
                    success=True,
                    message=f"Min: {min_val[0]}, Max: {max_val[0]}",
                    data=result
                )

        except Exception as e:
            return ActionResult(success=False, message=f"MinMax failed: {str(e)}")

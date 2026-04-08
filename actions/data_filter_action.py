"""Data Filter Action.

Filters data based on conditions with support for complex expressions,
nested field access, and multiple filter strategies.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataFilterAction(BaseAction):
    """Filter data based on conditions.
    
    Supports field-based filtering, regex patterns, lambda expressions,
    and complex nested field access.
    """
    action_type = "data_filter"
    display_name = "数据过滤"
    description = "根据条件过滤数据，支持表达式、正则和嵌套字段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Filter data based on conditions.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: List of records to filter.
                - filters: List of filter conditions.
                - filter_expr: Lambda expression for filtering.
                - mode: 'all' (AND) or 'any' (OR) for multiple filters.
                - negate: Invert filter results.
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with filtered data.
        """
        try:
            data = params.get('data')
            filters = params.get('filters', [])
            filter_expr = params.get('filter_expr')
            mode = params.get('mode', 'all').lower()
            negate = params.get('negate', False)
            save_to_var = params.get('save_to_var', 'filtered_data')

            if data is None:
                data = context.get_variable(params.get('use_var', 'input_data'))

            if not data:
                return ActionResult(success=False, message="No data provided")

            if not isinstance(data, list):
                return ActionResult(success=False, message="Data must be a list")

            if filter_expr:
                filtered = self._filter_by_expr(data, filter_expr)
            elif filters:
                filtered = self._filter_by_conditions(data, filters, mode)
            else:
                return ActionResult(success=False, message="filters or filter_expr is required")

            if negate:
                filtered = self._negate_filter(data, filtered)

            result = {
                'original_count': len(data),
                'filtered_count': len(filtered),
                'removed_count': len(data) - len(filtered),
                'filtered': filtered
            }

            context.set_variable(save_to_var, filtered)
            return ActionResult(success=True, data=result,
                             message=f"Filtered: {len(filtered)}/{len(data)} kept")

        except Exception as e:
            return ActionResult(success=False, message=f"Filter error: {e}")

    def _filter_by_conditions(self, data: List, conditions: List[Dict], 
                             mode: str) -> List:
        """Filter by list of conditions."""
        def matches_all(item):
            for cond in conditions:
                if not self._evaluate_condition(item, cond):
                    return False
            return True

        def matches_any(item):
            for cond in conditions:
                if self._evaluate_condition(item, cond):
                    return True
            return False

        matcher = matches_all if mode == 'all' else matches_any
        return [item for item in data if matcher(item)]

    def _filter_by_expr(self, data: List, expr: str) -> List:
        """Filter by lambda expression."""
        try:
            filter_fn = eval(expr)
            return [item for item in data if filter_fn(item)]
        except Exception as e:
            raise ValueError(f"Invalid filter expression: {e}")

    def _negate_filter(self, data: List, filtered: List) -> List:
        """Return items not in filtered result."""
        filtered_set = set(id(item) for item in filtered)
        return [item for item in data if id(item) not in filtered_set]

    def _evaluate_condition(self, item: Dict, cond: Dict) -> bool:
        """Evaluate a single condition against an item."""
        field = cond.get('field')
        op = cond.get('op', 'eq')
        value = cond.get('value')
        nested = cond.get('nested', False)

        # Get field value
        if nested:
            item_value = self._get_nested_value(item, field)
        else:
            item_value = item.get(field) if isinstance(item, dict) else None

        # Evaluate operator
        if op == 'eq' or op == '==':
            return item_value == value
        elif op == 'ne' or op == '!=':
            return item_value != value
        elif op == 'gt' or op == '>':
            return item_value > value if item_value is not None else False
        elif op == 'ge' or op == '>=':
            return item_value >= value if item_value is not None else False
        elif op == 'lt' or op == '<':
            return item_value < value if item_value is not None else False
        elif op == 'le' or op == '<=':
            return item_value <= value if item_value is not None else False
        elif op == 'contains':
            return value in item_value if item_value is not None else False
        elif op == 'not_contains':
            return value not in item_value if item_value is not None else True
        elif op == 'startswith':
            return str(item_value).startswith(str(value)) if item_value is not None else False
        elif op == 'endswith':
            return str(item_value).endswith(str(value)) if item_value is not None else False
        elif op == 'regex':
            try:
                return bool(re.match(str(value), str(item_value)))
            except Exception:
                return False
        elif op == 'in':
            return item_value in value if item_value is not None else False
        elif op == 'not_in':
            return item_value not in value if item_value is not None else True
        elif op == 'is_none':
            return item_value is None
        elif op == 'is_not_none':
            return item_value is not None
        elif op == 'is_empty':
            return not item_value if item_value is not None else True
        elif op == 'is_not_empty':
            return bool(item_value) if item_value is not None else False
        elif op == 'type_is':
            return type(item_value).__name__ == value
        elif op == 'has_key':
            return field in item if isinstance(item, dict) else False
        elif op == 'exists':
            return field in item if isinstance(item, dict) else False
        else:
            return item_value == value

    def _get_nested_value(self, data: Any, path: str) -> Any:
        """Get nested value using dot notation."""
        keys = path.split('.')
        current = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            elif isinstance(current, list):
                try:
                    idx = int(key)
                    current = current[idx] if 0 <= idx < len(current) else None
                except (ValueError, TypeError):
                    return None
            else:
                return None
            if current is None:
                return None
        return current

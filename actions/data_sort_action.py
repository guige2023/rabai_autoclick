"""Data Sort Action.

Sorts data by single or multiple fields with ascending/descending order,
case sensitivity options, and null handling.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataSortAction(BaseAction):
    """Sort data by field(s).
    
    Supports multi-field sorting with ascending/descending order,
    case sensitivity, and null value positioning.
    """
    action_type = "data_sort"
    display_name = "数据排序"
    description = "数据排序，支持多字段升序/降序排列"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Sort data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Data to sort.
                - sort_by: Field(s) to sort by.
                - order: 'asc' or 'desc' (default: asc).
                - nulls_first: Place null values first (default: False).
                - case_sensitive: Case sensitive for strings (default: False).
                - custom_sort_fn: Custom comparison function.
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with sorted data.
        """
        try:
            data = params.get('data')
            sort_by = params.get('sort_by')
            order = params.get('order', 'asc').lower()
            nulls_first = params.get('nulls_first', False)
            case_sensitive = params.get('case_sensitive', False)
            custom_sort_fn = params.get('custom_sort_fn')
            save_to_var = params.get('save_to_var', 'sorted_data')

            if data is None:
                data = context.get_variable(params.get('use_var', 'input_data'))

            if not data:
                return ActionResult(success=False, message="No data provided")

            if not sort_by:
                return ActionResult(success=False, message="sort_by is required")

            if isinstance(sort_by, str):
                sort_by = [sort_by]

            reverse = order == 'desc'

            if custom_sort_fn:
                result = sorted(data, key=lambda x: eval(custom_sort_fn)(x), reverse=reverse)
            else:
                result = self._sort_data(data, sort_by, reverse, nulls_first, case_sensitive)

            context.set_variable(save_to_var, result)
            return ActionResult(success=True, data={'count': len(result)},
                             message=f"Sorted {len(result)} items by {sort_by}")

        except Exception as e:
            return ActionResult(success=False, message=f"Sort error: {e}")

    def _sort_data(self, data: List, sort_by: List[str], reverse: bool,
                  nulls_first: bool, case_sensitive: bool) -> List:
        """Sort data by multiple fields."""
        def get_sort_key(item):
            keys = []
            for field in sort_by:
                value = item.get(field) if isinstance(item, dict) else None
                
                # Handle None/null values
                if value is None:
                    keys.append((0 if nulls_first else 2, ''))
                elif isinstance(value, str):
                    if case_sensitive:
                        keys.append((1, value))
                    else:
                        keys.append((1, value.lower()))
                else:
                    keys.append((1, value))
            
            return tuple(keys)

        return sorted(data, key=get_sort_key, reverse=reverse)

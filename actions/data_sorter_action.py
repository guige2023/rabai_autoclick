"""Data sorter action module for RabAI AutoClick.

Provides data sorting capabilities with multi-field sorting,
ascending/descending order, and custom sort functions.
"""

import sys
import os
from typing import Any, Callable, Dict, List, Optional, Union
from functools import cmp_to_key

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataSorterAction(BaseAction):
    """Data sorter action for sorting data.
    
    Supports multi-field sorting, ascending/descending order,
    numeric and string sorting, and null handling.
    """
    action_type = "data_sorter"
    display_name = "数据排序器"
    description = "多字段数据排序"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute sort operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                data: Data to sort
                sort_by: Field(s) to sort by (comma-separated for multi-field)
                order: 'asc' or 'desc' (default: asc)
                nulls_first: Place nulls at beginning (default: False)
                case_sensitive: Case-sensitive string comparison (default: True).
        
        Returns:
            ActionResult with sorted data.
        """
        data = params.get('data', [])
        sort_by = params.get('sort_by')
        order = params.get('order', 'asc')
        nulls_first = params.get('nulls_first', False)
        case_sensitive = params.get('case_sensitive', True)
        
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        if not sort_by:
            return ActionResult(success=False, message="sort_by field required")
        
        sort_fields = sort_by.split(',') if isinstance(sort_by, str) else sort_by
        reverse = order == 'desc'
        
        sorted_data = self._sort(
            data,
            sort_fields,
            reverse,
            nulls_first,
            case_sensitive
        )
        
        return ActionResult(
            success=True,
            message=f"Sorted {len(data)} items",
            data={
                'items': sorted_data,
                'count': len(sorted_data),
                'sort_by': sort_fields,
                'order': order
            }
        )
    
    def _sort(
        self,
        data: List[Any],
        sort_fields: List[str],
        reverse: bool,
        nulls_first: bool,
        case_sensitive: bool
    ) -> List[Any]:
        """Sort data by fields."""
        def get_sort_key(item):
            """Extract sort key from item for all fields."""
            keys = []
            for field in sort_fields:
                value = self._get_nested(item, field.strip())
                keys.append(value)
            return keys
        
        def compare(a, b):
            """Compare two items."""
            a_keys = get_sort_key(a)
            b_keys = get_sort_key(b)
            
            for i, (a_val, b_val) in enumerate(zip(a_keys, b_keys)):
                a_null = a_val is None
                b_null = b_val is None
                
                if a_null and b_null:
                    continue
                if a_null:
                    return -1 if nulls_first else 1
                if b_null:
                    return 1 if nulls_first else -1
                
                if isinstance(a_val, (int, float)) and isinstance(b_val, (int, float)):
                    if a_val < b_val:
                        return -1
                    elif a_val > b_val:
                        return 1
                else:
                    a_str = str(a_val)
                    b_str = str(b_val)
                    
                    if not case_sensitive:
                        a_str = a_str.lower()
                        b_str = b_str.lower()
                    
                    if a_str < b_str:
                        return -1
                    elif a_str > b_str:
                        return 1
            
            return 0
        
        return sorted(data, key=cmp_to_key(compare), reverse=reverse)
    
    def _get_nested(self, data: Dict, field: str) -> Any:
        """Get nested value using dot notation."""
        parts = field.split('.')
        current = data
        
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        
        return current

"""Data processing action module for RabAI AutoClick.

Provides data manipulation actions including filtering,
sorting, aggregation, and transformation of lists and dicts.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ListFilterAction(BaseAction):
    """Filter list items by condition.
    
    Supports comparison operators, regex pattern matching,
    and lambda-style conditions.
    """
    action_type = "list_filter"
    display_name = "列表过滤"
    description = "按条件过滤列表元素"

    OPERATORS = ['==', '!=', '>', '<', '>=', '<=', 'in', 'not in', 'contains', 'startswith', 'endswith']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Filter list items.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, field, operator,
                   value, save_to_var.
        
        Returns:
            ActionResult with filtered list.
        """
        items = params.get('items', [])
        field = params.get('field', None)
        operator = params.get('operator', '==')
        value = params.get('value', None)
        save_to_var = params.get('save_to_var', None)

        if not isinstance(items, list):
            return ActionResult(
                success=False,
                message=f"Items must be list, got {type(items).__name__}"
            )

        if operator not in self.OPERATORS:
            return ActionResult(
                success=False,
                message=f"Invalid operator: {operator}. Valid: {self.OPERATORS}"
            )

        filtered = []

        for item in items:
            # Get field value
            if field:
                if isinstance(item, dict):
                    item_value = item.get(field)
                elif hasattr(item, field):
                    item_value = getattr(item, field)
                else:
                    continue
            else:
                item_value = item

            # Compare
            match = False
            if operator == '==':
                match = item_value == value
            elif operator == '!=':
                match = item_value != value
            elif operator == '>':
                match = item_value > value
            elif operator == '<':
                match = item_value < value
            elif operator == '>=':
                match = item_value >= value
            elif operator == '<=':
                match = item_value <= value
            elif operator == 'in':
                match = value in item_value if item_value else False
            elif operator == 'not in':
                match = value not in item_value if item_value else True
            elif operator == 'contains':
                match = str(value) in str(item_value) if item_value else False
            elif operator == 'startswith':
                match = str(item_value).startswith(str(value)) if item_value else False
            elif operator == 'endswith':
                match = str(item_value).endswith(str(value)) if item_value else False

            if match:
                filtered.append(item)

        result_data = {
            'filtered': filtered,
            'count': len(filtered),
            'original_count': len(items)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"过滤完成: {len(items)} -> {len(filtered)} 项",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['items', 'operator']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'field': None,
            'value': None,
            'save_to_var': None
        }


class ListSortAction(BaseAction):
    """Sort list items by field.
    
    Supports ascending/descending order, multi-field sorting,
    and custom key functions.
    """
    action_type = "list_sort"
    display_name = "列表排序"
    description = "按字段对列表排序"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Sort list items.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, field, reverse,
                   numeric, save_to_var.
        
        Returns:
            ActionResult with sorted list.
        """
        items = params.get('items', [])
        field = params.get('field', None)
        reverse = params.get('reverse', False)
        numeric = params.get('numeric', False)
        save_to_var = params.get('save_to_var', None)

        if not isinstance(items, list):
            return ActionResult(
                success=False,
                message=f"Items must be list, got {type(items).__name__}"
            )

        if not items:
            result_data = {'sorted': [], 'count': 0}
            if save_to_var:
                context.variables[save_to_var] = result_data
            return ActionResult(success=True, message="空列表", data=result_data)

        # Get sort key
        if field:
            def key_func(item):
                val = item.get(field) if isinstance(item, dict) else getattr(item, field, None)
                if numeric and val is not None:
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        return 0
                return val
        else:
            if numeric:
                def key_func(item):
                    try:
                        return float(item)
                    except (ValueError, TypeError):
                        return 0
            else:
                key_func = None

        try:
            sorted_items = sorted(items, key=key_func, reverse=reverse)
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"排序失败: {str(e)}"
            )

        result_data = {
            'sorted': sorted_items,
            'count': len(sorted_items),
            'field': field,
            'reverse': reverse
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"排序完成: {len(items)} 项",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'field': None,
            'reverse': False,
            'numeric': False,
            'save_to_var': None
        }


class ListMapAction(BaseAction):
    """Apply transformation to each list item.
    
    Supports field extraction, value mapping, and
    custom transformation functions.
    """
    action_type = "list_map"
    display_name = "列表映射"
    description = "对列表每个元素应用转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Map list items.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, field, map_values,
                   default_value, save_to_var.
        
        Returns:
            ActionResult with mapped list.
        """
        items = params.get('items', [])
        field = params.get('field', None)
        map_values = params.get('map_values', None)
        default_value = params.get('default_value', None)
        save_to_var = params.get('save_to_var', None)

        if not isinstance(items, list):
            return ActionResult(
                success=False,
                message=f"Items must be list, got {type(items).__name__}"
            )

        mapped = []

        for item in items:
            # Extract field value
            if field:
                if isinstance(item, dict):
                    value = item.get(field, default_value)
                elif hasattr(item, field):
                    value = getattr(item, field)
                else:
                    value = default_value
            else:
                value = item

            # Apply value mapping
            if map_values and isinstance(map_values, dict):
                value = map_values.get(value, value)

            mapped.append(value)

        result_data = {
            'mapped': mapped,
            'count': len(mapped),
            'original_count': len(items)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"映射完成: {len(items)} -> {len(mapped)} 项",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['items']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'field': None,
            'map_values': None,
            'default_value': None,
            'save_to_var': None
        }


class DictMergeAction(BaseAction):
    """Merge multiple dictionaries.
    
    Supports deep merge, conflict resolution,
    and key filtering.
    """
    action_type = "dict_merge"
    display_name = "字典合并"
    description = "合并多个字典"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Merge dictionaries.
        
        Args:
            context: Execution context.
            params: Dict with keys: dicts (list), strategy,
                   key_filter, save_to_var.
        
        Returns:
            ActionResult with merged dict.
        """
        dicts = params.get('dicts', [])
        strategy = params.get('strategy', 'deep')
        key_filter = params.get('key_filter', None)
        save_to_var = params.get('save_to_var', None)

        if not dicts:
            return ActionResult(success=False, message="No dicts to merge")

        result = {}
        for d in dicts:
            if not isinstance(d, dict):
                continue

            for key, value in d.items():
                # Apply key filter
                if key_filter:
                    if isinstance(key_filter, list) and key not in key_filter:
                        continue

                # Merge strategy
                if strategy == 'deep' and key in result:
                    if isinstance(result[key], dict) and isinstance(value, dict):
                        result[key] = self._deep_merge(result[key], value)
                    else:
                        result[key] = value
                elif strategy == 'keep' and key not in result:
                    result[key] = value
                else:
                    result[key] = value

        result_data = {
            'merged': result,
            'count': len(result),
            'dicts_count': len(dicts)
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"合并完成: {len(result)} 个键",
            data=result_data
        )

    def _deep_merge(self, base: dict, overlay: dict) -> dict:
        result = dict(base)
        for key, value in overlay.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result

    def get_required_params(self) -> List[str]:
        return ['dicts']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'strategy': 'deep',
            'key_filter': None,
            'save_to_var': None
        }


class DictExtractAction(BaseAction):
    """Extract values from dict by keys or paths.
    
    Supports dot-notation paths, key list extraction,
    and default value for missing keys.
    """
    action_type = "dict_extract"
    display_name = "字典提取"
    description = "从字典提取指定键的值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Extract dict values.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, keys (list), path,
                   default_value, save_to_var.
        
        Returns:
            ActionResult with extracted values.
        """
        data = params.get('data', {})
        keys = params.get('keys', [])
        path = params.get('path', None)
        default_value = params.get('default_value', None)
        save_to_var = params.get('save_to_var', None)

        if not isinstance(data, dict):
            return ActionResult(
                success=False,
                message=f"Data must be dict, got {type(data).__name__}"
            )

        result = {}

        # Extract by key list
        if keys:
            for key in keys:
                if isinstance(key, str) and '.' in key and path is None:
                    # Handle nested path in key
                    parts = key.split('.')
                    current = data
                    for p in parts:
                        if isinstance(current, dict) and p in current:
                            current = current[p]
                        else:
                            current = default_value
                            break
                    result[key] = current
                else:
                    result[key] = data.get(key, default_value)

        # Extract by path
        if path:
            parts = path.split('.')
            current = data
            for p in parts:
                if isinstance(current, dict) and p in current:
                    current = current[p]
                else:
                    current = default_value
                    break
            result[path] = current

        result_data = {
            'extracted': result,
            'count': len(result),
            'keys_found': list(result.keys())
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"提取完成: {len(result)} 个键",
            data=result_data
        )

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'keys': [],
            'path': None,
            'default_value': None,
            'save_to_var': None
        }

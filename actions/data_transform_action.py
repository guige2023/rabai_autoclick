"""Data transformation action module for RabAI AutoClick.

Provides data manipulation actions including filtering, mapping, aggregation, and conversion.
"""

import copy
import re
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FilterAction(BaseAction):
    """Filter a list based on a predicate function.
    
    Keeps only items that match the filter condition.
    """
    action_type = "filter"
    display_name = "列表过滤"
    description = "根据条件过滤列表元素"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Filter list items.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: items, condition, field, operator, value.
        
        Returns:
            ActionResult with filtered list.
        """
        items = params.get('items', [])
        condition = params.get('condition', None)
        field = params.get('field', None)
        operator_type = params.get('operator', '==')
        value = params.get('value', None)
        
        if not isinstance(items, list):
            return ActionResult(
                success=False,
                message=f"items must be a list, got {type(items).__name__}"
            )
        
        if len(items) == 0:
            return ActionResult(
                success=True,
                message="Empty list",
                data={'items': [], 'count': 0}
            )
        
        def matches(item: Any) -> bool:
            """Check if item matches filter condition."""
            # If condition is a callable, use it directly
            if callable(condition):
                return bool(condition(item))
            
            # If field is specified, get the field value
            if field:
                try:
                    item_value = item[field] if isinstance(item, dict) else getattr(item, field)
                except (KeyError, AttributeError):
                    return False
            else:
                item_value = item
            
            # Compare with value
            if operator_type == '==':
                return item_value == value
            elif operator_type == '!=':
                return item_value != value
            elif operator_type == '>':
                return item_value > value
            elif operator_type == '>=':
                return item_value >= value
            elif operator_type == '<':
                return item_value < value
            elif operator_type == '<=':
                return item_value <= value
            elif operator_type == 'contains':
                return value in item_value if item_value else False
            elif operator_type == 'starts_with':
                return str(item_value).startswith(str(value))
            elif operator_type == 'matches':
                return bool(re.match(str(value), str(item_value)))
            else:
                return False
        
        try:
            filtered = [item for item in items if matches(item)]
            
            return ActionResult(
                success=True,
                message=f"Filtered {len(items)} items to {len(filtered)}",
                data={'items': filtered, 'count': len(filtered), 'original_count': len(items)}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Filter error: {e}",
                data={'error': str(e)}
            )


class MapAction(BaseAction):
    """Transform each item in a list using a mapping function.
    
    Applies a transformation to every element.
    """
    action_type = "map"
    display_name = "列表映射"
    description = "对列表每个元素进行转换"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Map list items.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: items, transform, field, format_string.
        
        Returns:
            ActionResult with mapped list.
        """
        items = params.get('items', [])
        transform = params.get('transform', None)
        field = params.get('field', None)
        format_string = params.get('format_string', None)
        
        if not isinstance(items, list):
            return ActionResult(
                success=False,
                message=f"items must be a list, got {type(items).__name__}"
            )
        
        def transform_item(item: Any) -> Any:
            """Transform a single item."""
            if callable(transform):
                return transform(item)
            
            if format_string:
                try:
                    return format_string.format(item=item)
                except Exception:
                    return item
            
            if field:
                try:
                    return item[field] if isinstance(item, dict) else getattr(item, field)
                except (KeyError, AttributeError):
                    return None
            
            return item
        
        try:
            mapped = [transform_item(item) for item in items]
            
            return ActionResult(
                success=True,
                message=f"Mapped {len(items)} items",
                data={'items': mapped, 'count': len(mapped)}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Map error: {e}",
                data={'error': str(e)}
            )


class ReduceAction(BaseAction):
    """Reduce a list to a single value using an aggregator function.
    
    Computes aggregate values like sum, min, max, or custom functions.
    """
    action_type = "reduce"
    display_name = "列表聚合"
    description = "将列表聚合为单个值"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Reduce list to single value.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: items, operation, field, initial_value.
        
        Returns:
            ActionResult with aggregated value.
        """
        items = params.get('items', [])
        operation = params.get('operation', 'sum')
        field = params.get('field', None)
        initial_value = params.get('initial_value', None)
        
        if not isinstance(items, list):
            return ActionResult(
                success=False,
                message=f"items must be a list, got {type(items).__name__}"
            )
        
        if len(items) == 0:
            return ActionResult(
                success=True,
                message="Empty list",
                data={'result': initial_value, 'operation': operation}
            )
        
        # Extract field values if specified
        if field:
            try:
                values = [item[field] if isinstance(item, dict) else getattr(item, field) for item in items]
            except (KeyError, AttributeError) as e:
                return ActionResult(
                    success=False,
                    message=f"Field not found: {e}"
                )
        else:
            values = items
        
        try:
            if operation == 'sum':
                result = sum(values)
            elif operation == 'min':
                result = min(values)
            elif operation == 'max':
                result = max(values)
            elif operation == 'avg' or operation == 'mean':
                result = sum(values) / len(values)
            elif operation == 'count':
                result = len(values)
            elif operation == 'first':
                result = values[0]
            elif operation == 'last':
                result = values[-1]
            elif operation == 'join':
                separator = params.get('separator', ', ')
                result = separator.join(str(v) for v in values)
            elif operation == 'concat':
                result = ''.join(str(v) for v in values)
            elif operation == 'product':
                result = 1
                for v in values:
                    result *= v
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )
            
            return ActionResult(
                success=True,
                message=f"{operation} = {result}",
                data={'result': result, 'operation': operation, 'count': len(values)}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Reduce error: {e}",
                data={'error': str(e)}
            )


class GroupByAction(BaseAction):
    """Group list items by a key field.
    
    Creates a dictionary of lists grouped by the specified field.
    """
    action_type = "group_by"
    display_name = "列表分组"
    description = "按字段对列表分组"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Group list items.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: items, key, preserve_order.
        
        Returns:
            ActionResult with grouped dictionary.
        """
        items = params.get('items', [])
        key = params.get('key', '')
        preserve_order = params.get('preserve_order', False)
        
        if not isinstance(items, list):
            return ActionResult(
                success=False,
                message=f"items must be a list, got {type(items).__name__}"
            )
        
        if not key:
            return ActionResult(
                success=False,
                message="key is required"
            )
        
        try:
            grouped = {}
            
            for item in items:
                try:
                    group_key = item[key] if isinstance(item, dict) else getattr(item, key)
                except (KeyError, AttributeError):
                    group_key = None
                
                if group_key not in grouped:
                    grouped[group_key] = []
                grouped[group_key].append(item)
            
            # Optionally convert to ordered dict
            if preserve_order:
                from collections import OrderedDict
                grouped = OrderedDict(grouped)
            
            return ActionResult(
                success=True,
                message=f"Grouped into {len(grouped)} groups",
                data={
                    'groups': grouped,
                    'group_count': len(grouped),
                    'total_items': len(items)
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"GroupBy error: {e}",
                data={'error': str(e)}
            )


class FlattenAction(BaseAction):
    """Flatten nested lists into a single flat list.
    
    Handles arbitrary nesting depth.
    """
    action_type = "flatten"
    display_name = "列表扁平化"
    description = "将嵌套列表展平为单层列表"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Flatten nested list.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: items, max_depth, preserve_types.
        
        Returns:
            ActionResult with flattened list.
        """
        items = params.get('items', [])
        max_depth = params.get('max_depth', -1)
        preserve_types = params.get('preserve_types', False)
        
        if not isinstance(items, list):
            return ActionResult(
                success=False,
                message=f"items must be a list, got {type(items).__name__}"
            )
        
        def flatten_helper(lst: List, depth: int = -1) -> List:
            """Recursively flatten list."""
            result = []
            for item in lst:
                if isinstance(item, list) and depth != 0:
                    result.extend(flatten_helper(item, depth - 1))
                elif preserve_types and isinstance(item, (tuple, set)):
                    result.append(list(item))
                else:
                    result.append(item)
            return result
        
        try:
            flattened = flatten_helper(items, max_depth if max_depth > 0 else -1)
            
            return ActionResult(
                success=True,
                message=f"Flattened to {len(flattened)} items",
                data={'items': flattened, 'count': len(flattened)}
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Flatten error: {e}",
                data={'error': str(e)}
            )

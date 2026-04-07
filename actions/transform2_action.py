"""Data transformation action module for RabAI AutoClick.

Provides transformation operations:
- TransformMapAction: Map values through a function
- TransformFilterAction: Filter values
- TransformFlattenAction: Flatten nested structure
- TransformGroupAction: Group by key
- TransformSortAction: Sort by key
- TransformUniqueAction: Get unique values
"""

from __future__ import annotations

import sys
from typing import Any, Dict, List, Optional, Callable

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TransformMapAction(BaseAction):
    """Map values through transformation."""
    action_type = "transform_map"
    display_name = "数据映射"
    description = "映射转换数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute map transformation."""
        data = params.get('data', [])
        field = params.get('field', None)
        transform = params.get('transform', None)  # upper, lower, int, float, str, abs, len
        output_var = params.get('output_var', 'mapped_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            import copy

            resolved_data = context.resolve_value(data) if context else data
            resolved_field = context.resolve_value(field) if context else field
            resolved_transform = context.resolve_value(transform) if context else transform

            result = []
            for item in resolved_data:
                if isinstance(item, dict) and resolved_field:
                    val = item.get(resolved_field)
                else:
                    val = item

                if resolved_transform:
                    t = resolved_transform.lower()
                    if t == 'upper':
                        val = str(val).upper()
                    elif t == 'lower':
                        val = str(val).lower()
                    elif t == 'int':
                        val = int(val)
                    elif t == 'float':
                        val = float(val)
                    elif t == 'str':
                        val = str(val)
                    elif t == 'abs':
                        val = abs(float(val))
                    elif t == 'len':
                        val = len(val)

                if isinstance(item, dict) and resolved_field:
                    item = copy.copy(item)
                    item[resolved_field] = val
                    result.append(item)
                else:
                    result.append(val)

            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Mapped {len(result)} items", data={'result': result})
        except Exception as e:
            return ActionResult(success=False, message=f"Map error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'field': None, 'transform': None, 'output_var': 'mapped_data'}


class TransformFlattenAction(BaseAction):
    """Flatten nested structure."""
    action_type = "transform_flatten"
    display_name = "数据扁平化"
    description = "扁平化嵌套结构"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute flatten."""
        data = params.get('data', [])
        max_depth = params.get('max_depth', 10)
        output_var = params.get('output_var', 'flattened_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_depth = context.resolve_value(max_depth) if context else max_depth

            def _flatten(obj, depth=0):
                if depth >= resolved_depth:
                    return [obj]
                if isinstance(obj, list):
                    result = []
                    for item in obj:
                        result.extend(_flatten(item, depth + 1))
                    return result
                elif isinstance(obj, dict):
                    result = []
                    for value in obj.values():
                        result.extend(_flatten(value, depth + 1))
                    return result
                else:
                    return [obj]

            flattened = _flatten(resolved_data)
            if context:
                context.set(output_var, flattened)
            return ActionResult(success=True, message=f"Flattened to {len(flattened)} items", data={'result': flattened})
        except Exception as e:
            return ActionResult(success=False, message=f"Flatten error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'max_depth': 10, 'output_var': 'flattened_data'}


class TransformUniqueAction(BaseAction):
    """Get unique values."""
    action_type = "transform_unique"
    display_name = "数据去重"
    description = "获取唯一值"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute unique."""
        data = params.get('data', [])
        key = params.get('key', None)  # for dict items
        output_var = params.get('output_var', 'unique_data')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_key = context.resolve_value(key) if context else key

            if resolved_key:
                seen = set()
                unique = []
                for item in resolved_data:
                    if isinstance(item, dict):
                        k = item.get(resolved_key)
                    else:
                        k = item
                    if k not in seen:
                        seen.add(k)
                        unique.append(item)
            else:
                unique = list(dict.fromkeys(resolved_data))

            if context:
                context.set(output_var, unique)
            return ActionResult(success=True, message=f"Unique: {len(unique)} items", data={'result': unique, 'count': len(unique)})
        except Exception as e:
            return ActionResult(success=False, message=f"Unique error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'key': None, 'output_var': 'unique_data'}


class TransformGroupAction(BaseAction):
    """Group data by key."""
    action_type = "transform_group"
    display_name = "数据分组"
    description = "按键分组"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute group."""
        data = params.get('data', [])
        key = params.get('key', '')
        output_var = params.get('output_var', 'grouped_data')

        if not data or not key:
            return ActionResult(success=False, message="data and key are required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_key = context.resolve_value(key) if context else key

            groups = {}
            for item in resolved_data:
                if isinstance(item, dict):
                    group_val = item.get(resolved_key)
                else:
                    group_val = str(item)
                if group_val not in groups:
                    groups[group_val] = []
                groups[group_val].append(item)

            if context:
                context.set(output_var, groups)
            return ActionResult(success=True, message=f"Grouped into {len(groups)} groups", data={'groups': {k: len(v) for k, v in groups.items()}, 'count': len(groups)})
        except Exception as e:
            return ActionResult(success=False, message=f"Group error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data', 'key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'grouped_data'}


class TransformPivotAction(BaseAction):
    """Pivot data (transpose rows/columns)."""
    action_type = "transform_pivot"
    display_name = "数据透视"
    description = "数据透视转换"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pivot."""
        data = params.get('data', [])
        row_key = params.get('row_key', '')
        col_key = params.get('col_key', '')
        value_key = params.get('value_key', '')
        output_var = params.get('output_var', 'pivoted_data')

        if not data or not row_key or not value_key:
            return ActionResult(success=False, message="data, row_key, and value_key are required")

        try:
            resolved_data = context.resolve_value(data) if context else data
            resolved_row = context.resolve_value(row_key) if context else row_key
            resolved_col = context.resolve_value(col_key) if context else col_key
            resolved_val = context.resolve_value(value_key) if context else value_key

            pivot = {}
            for item in resolved_data:
                if isinstance(item, dict):
                    row = item.get(resolved_row)
                    if resolved_col:
                        col = item.get(resolved_col)
                        if row not in pivot:
                            pivot[row] = {}
                        pivot[row][col] = item.get(resolved_val)
                    else:
                        if row not in pivot:
                            pivot[row] = []
                        pivot[row].append(item.get(resolved_val))

            if context:
                context.set(output_var, pivot)
            return ActionResult(success=True, message=f"Pivoted data: {len(pivot)} rows", data={'result': pivot})
        except Exception as e:
            return ActionResult(success=False, message=f"Pivot error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['data', 'row_key', 'value_key']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'col_key': '', 'output_var': 'pivoted_data'}

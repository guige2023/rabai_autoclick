"""Flatten action module for RabAI AutoClick.

Provides actions for flattening nested data structures,
converting hierarchical data to flat tabular format.
"""

import copy
import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class FlattenAction(BaseAction):
    """Flatten nested dictionaries and lists.
    
    Converts nested structures to flat key-value format.
    """
    action_type = "flatten"
    display_name = "扁平化"
    description = "嵌套结构扁平化"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Flatten nested structure.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, separator, max_depth,
                   preserve_types.
        
        Returns:
            ActionResult with flattened data.
        """
        data = params.get('data', {})
        separator = params.get('separator', '.')
        max_depth = params.get('max_depth', 10)
        preserve_types = params.get('preserve_types', False)

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            flattened = {}
            depth = 0

            def flatten_recursive(obj: Any, prefix: str, current_depth: int):
                if current_depth > max_depth:
                    flattened[prefix] = str(obj)
                    return

                if isinstance(obj, dict):
                    for key, value in obj.items():
                        new_key = f"{prefix}{separator}{key}" if prefix else key
                        flatten_recursive(value, new_key, current_depth + 1)
                
                elif isinstance(obj, list):
                    for i, item in enumerate(obj):
                        new_key = f"{prefix}[{i}]"
                        flatten_recursive(item, new_key, current_depth + 1)
                
                else:
                    if preserve_types:
                        flattened[prefix] = obj
                    else:
                        flattened[prefix] = str(obj) if not isinstance(obj, (str, int, float, bool, type(None))) else obj

            flatten_recursive(data, '', depth)

            return ActionResult(
                success=True,
                message=f"Flattened to {len(flattened)} keys",
                data={
                    'flattened': flattened,
                    'key_count': len(flattened),
                    'original_type': type(data).__name__
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Flatten failed: {str(e)}")


class UnflattenAction(BaseAction):
    """Restore flattened data to nested structure.
    
    Converts flat key-value back to hierarchical format.
    """
    action_type = "unflatten"
    display_name = "反扁平化"
    description = "扁平数据恢复嵌套"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Unflatten to nested structure.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, separator, delimiter.
        
        Returns:
            ActionResult with nested data.
        """
        data = params.get('data', {})
        separator = params.get('separator', '.')
        delimiter = params.get('delimiter', '[]')

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            nested = {}

            for flat_key, value in data.items():
                keys = self._parse_key(flat_key, separator, delimiter)
                self._set_nested(nested, keys, value)

            return ActionResult(
                success=True,
                message=f"Unflattened to nested structure",
                data={'nested': nested, 'original_keys': len(data)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Unflatten failed: {str(e)}")

    def _parse_key(self, key: str, separator: str, delimiter: str) -> List[Union[str, int]]:
        """Parse flat key to path components."""
        result = []
        parts = key.split(separator)
        
        for part in parts:
            if delimiter in part and part.endswith(']'):
                base, idx = part.rstrip(']').split('[')
                if base:
                    result.append(base)
                result.append(int(idx))
            else:
                result.append(part)
        
        return result

    def _set_nested(self, obj: Dict, keys: List, value: Any):
        """Set value at nested path in object."""
        current = obj
        for i, key in enumerate(keys[:-1]):
            if isinstance(key, int):
                while len(current) <= key:
                    current.append({})
                current = current[key]
            else:
                if key not in current:
                    next_key = keys[i + 1] if i + 1 < len(keys) else None
                    current[key] = [] if isinstance(next_key, int) else {}
                current = current[key]
        
        final_key = keys[-1]
        if isinstance(final_key, int):
            while len(current) <= final_key:
                current.append(None)
            current[final_key] = value
        else:
            current[final_key] = value


class FlattenToRowsAction(BaseAction):
    """Flatten nested data to rows format.
    
    Converts hierarchical data to list of flat rows.
    """
    action_type = "flatten_to_rows"
    display_name = "扁平化为行"
    description = "嵌套数据转行列表"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Convert to row format.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, key_field, value_field,
                   array_fields.
        
        Returns:
            ActionResult with rows.
        """
        data = params.get('data', {})
        key_field = params.get('key_field', 'key')
        value_field = params.get('value_field', 'value')
        array_fields = params.get('array_fields', [])

        if not data:
            return ActionResult(success=False, message="data is required")

        try:
            rows = []

            def flatten_to_row(obj: Any, prefix: str, current_row: Dict):
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        new_key = f"{prefix}.{key}" if prefix else key
                        flatten_to_row(value, new_key, current_row)
                
                elif isinstance(obj, list):
                    for item in obj:
                        flatten_to_row(item, prefix, copy.copy(current_row))
                
                else:
                    current_row[key_field] = prefix
                    current_row[value_field] = obj
                    rows.append(current_row)

            flatten_to_row(data, '', {})

            return ActionResult(
                success=True,
                message=f"Generated {len(rows)} rows",
                data={
                    'rows': rows,
                    'row_count': len(rows)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Flatten to rows failed: {str(e)}")


class PivotAction(BaseAction):
    """Pivot data from rows to columns.
    
    Reshapes data in pivot table format.
    """
    action_type = "pivot"
    display_name = "数据透视"
    description = "数据透视表转换"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Pivot data.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, index, columns, values,
                   aggfunc.
        
        Returns:
            ActionResult with pivoted data.
        """
        data = params.get('data', [])
        index = params.get('index', '')
        columns = params.get('columns', '')
        values = params.get('values', '')
        aggfunc = params.get('aggfunc', 'first')

        if not data:
            return ActionResult(success=False, message="data is required")
        if not index:
            return ActionResult(success=False, message="index field required")

        try:
            pivot = {}
            
            for row in data:
                if not isinstance(row, dict):
                    continue
                
                row_index = row.get(index)
                if not row_index:
                    continue

                if row_index not in pivot:
                    pivot[row_index] = {}

                if columns:
                    col_key = row.get(columns, '')
                    if col_key:
                        if col_key not in pivot[row_index]:
                            pivot[row_index][col_key] = []
                        pivot[row_index][col_key].append(row.get(values))
                else:
                    pivot[row_index][values] = row.get(values)

            result = {}
            for row_index, cols in pivot.items():
                result[row_index] = {}
                for col_key, vals in cols.items():
                    if isinstance(vals, list):
                        if aggfunc == 'first':
                            result[row_index][col_key] = vals[0] if vals else None
                        elif aggfunc == 'sum':
                            result[row_index][col_key] = sum(vals)
                        elif aggfunc == 'count':
                            result[row_index][col_key] = len(vals)
                        elif aggfunc == 'avg':
                            result[row_index][col_key] = sum(vals) / len(vals) if vals else 0
                        else:
                            result[row_index][col_key] = vals
                    else:
                        result[row_index][col_key] = vals

            return ActionResult(
                success=True,
                message=f"Pivoted to {len(result)} rows",
                data={
                    'pivoted': result,
                    'row_count': len(result)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Pivot failed: {str(e)}")

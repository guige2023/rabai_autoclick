"""Data Unpack Action.

Unpacks nested data structures (JSON, nested dicts) into flat format
with configurable flattening strategies and array expansion.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataUnpackAction(BaseAction):
    """Unpack nested data into flat format.
    
    Flattens nested dictionaries and expands arrays
    with configurable separator and strategies.
    """
    action_type = "data_unpack"
    display_name = "数据展开"
    description = "展开嵌套数据为扁平格式，支持数组扩展"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Unpack nested data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Data to unpack.
                - separator: Key separator for flattening (default: '.').
                - max_depth: Maximum nesting depth (default: 10).
                - expand_arrays: Expand arrays into separate records (default: False).
                - array_field: Field name for array expansion index.
                - preserve_root: Keep root key prefix (default: False).
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with unpacked data.
        """
        try:
            data = params.get('data')
            separator = params.get('separator', '.')
            max_depth = params.get('max_depth', 10)
            expand_arrays = params.get('expand_arrays', False)
            array_field = params.get('array_field', '_index')
            preserve_root = params.get('preserve_root', False)
            save_to_var = params.get('save_to_var', 'unpacked_data')

            if data is None:
                data = context.get_variable(params.get('use_var', 'input_data'))

            if not data:
                return ActionResult(success=False, message="No data provided")

            if isinstance(data, list):
                result = []
                for i, item in enumerate(data):
                    if isinstance(item, dict):
                        unpacked = self._flatten_dict(item, separator, max_depth, expand_arrays, array_field, preserve_root, f'[{i}]')
                        result.append(unpacked)
                    else:
                        result.append({array_field: i, '_value': item})
            elif isinstance(data, dict):
                result = self._flatten_dict(data, separator, max_depth, expand_arrays, array_field, preserve_root, '')
            else:
                result = {'_value': data}

            context.set_variable(save_to_var, result)
            return ActionResult(success=True, data={'count': len(result) if isinstance(result, list) else 1},
                             message=f"Unpacked nested data")

        except Exception as e:
            return ActionResult(success=False, message=f"Unpack error: {e}")

    def _flatten_dict(self, data: Dict, sep: str, max_depth: int,
                     expand_arrays: bool, array_field: str,
                     preserve_root: bool, root_prefix: str) -> Dict:
        """Flatten a dictionary recursively."""
        result = {}

        def _flatten(obj: Any, prefix: str, depth: int):
            if depth > max_depth:
                result[prefix] = obj
                return

            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_key = f"{prefix}{sep}{key}" if prefix and preserve_root else (f"{prefix}{sep}{key}" if prefix else key)
                    _flatten(value, new_key, depth + 1)
            elif isinstance(obj, list):
                if expand_arrays:
                    for i, item in enumerate(obj):
                        new_key = f"{prefix}{sep}{i}" if prefix else str(i)
                        _flatten(item, new_key, depth + 1)
                else:
                    result[prefix] = json.dumps(obj)
            else:
                result[prefix] = obj

        _flatten(data, root_prefix, 0)
        return result

    def _expand_arrays(self, data: Dict, array_field: str) -> List[Dict]:
        """Expand arrays into separate records."""
        # Find array fields
        array_paths = []
        
        def find_arrays(obj: Any, path: str = ''):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    find_arrays(value, f"{path}{sep}{key}" if path else key)
            elif isinstance(obj, list):
                array_paths.append((path, obj))
        
        find_arrays(data)
        
        if not array_paths:
            return [data]
        
        # Expand first array found
        path, arr = array_paths[0]
        results = []
        for i, item in enumerate(arr):
            new_record = data.copy()
            new_record[array_field] = i
            # Replace array with single item
            keys = path.split('.')
            temp = new_record
            for k in keys[:-1]:
                temp = temp[k]
            temp[keys[-1]] = item
            results.append(new_record)
        
        return results

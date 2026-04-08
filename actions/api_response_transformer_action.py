"""API Response Transformer Action.

Transforms API responses into standardized formats with field mapping,
type coercion, filtering, and nested data extraction.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiResponseTransformerAction(BaseAction):
    """Transform API responses into standardized output formats.
    
    Supports field mapping, type coercion, nested extraction via dot notation,
    filtering predicates, default values, and array flattening.
    """
    action_type = "api_response_transformer"
    display_name = "API响应转换"
    description = "将API响应转换为标准格式，支持字段映射、类型转换、嵌套提取"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Transform API response data.
        
        Args:
            context: Execution context with 'response_data'.
            params: Dict with keys:
                - response_data: Raw API response (dict/list)
                - field_mapping: Dict mapping output -> input fields
                - extract_fields: List of dot-notation field paths to extract
                - coerce_types: Dict field -> target type
                - filter_predicate: Lambda expression for filtering
                - defaults: Dict of default values for missing fields
                - flatten_arrays: Whether to flatten nested arrays
                - save_to_var: Variable name to store result.
        
        Returns:
            ActionResult with transformed data.
        """
        try:
            response_data = params.get('response_data')
            field_mapping = params.get('field_mapping', {})
            extract_fields = params.get('extract_fields', [])
            coerce_types = params.get('coerce_types', {})
            filter_predicate = params.get('filter_predicate', None)
            defaults = params.get('defaults', {})
            flatten_arrays = params.get('flatten_arrays', False)
            save_to_var = params.get('save_to_var', 'transformed_response')

            if response_data is None:
                return ActionResult(success=False, message="response_data is required")

            # Handle single item vs list
            if isinstance(response_data, list):
                result = [self._transform_item(item, field_mapping, extract_fields, coerce_types, defaults, flatten_arrays) 
                         for item in response_data]
            else:
                result = self._transform_item(response_data, field_mapping, extract_fields, coerce_types, defaults, flatten_arrays)

            # Apply filter if predicate provided
            if filter_predicate and isinstance(result, list):
                try:
                    filter_fn = eval(filter_predicate)
                    result = [item for item in result if filter_fn(item)]
                except Exception as e:
                    return ActionResult(success=False, message=f"Filter predicate error: {e}")

            context.set_variable(save_to_var, result)
            return ActionResult(success=True, data=result, message=f"Transformed {len(result) if isinstance(result, list) else 1} items")

        except Exception as e:
            return ActionResult(success=False, message=f"Transform error: {e}")

    def _transform_item(self, item: Dict, field_mapping: Dict, extract_fields: List, 
                       coerce_types: Dict, defaults: Dict, flatten_arrays: bool) -> Dict:
        """Transform a single item."""
        if not isinstance(item, dict):
            return item

        result = {}

        # Apply field mapping
        for out_key, in_key in field_mapping.items():
            if isinstance(in_key, str):
                result[out_key] = self._get_nested(item, in_key, defaults.get(out_key))
            else:
                result[out_key] = in_key

        # Extract specified fields
        for field in extract_fields:
            value = self._get_nested(item, field)
            if value is not None:
                result[field] = value

        # Add defaults for missing fields
        for key, default_val in defaults.items():
            if key not in result:
                result[key] = default_val

        # Apply type coercion
        for field, type_name in coerce_types.items():
            if field in result:
                result[field] = self._coerce_type(result[field], type_name)

        return result

    def _get_nested(self, data: Any, path: str, default: Any = None) -> Any:
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
                    return default
            else:
                return default
            if current is None:
                return default
        return current

    def _coerce_type(self, value: Any, type_name: str) -> Any:
        """Coerce value to target type."""
        try:
            if type_name == 'int':
                return int(value)
            elif type_name == 'float':
                return float(value)
            elif type_name == 'str':
                return str(value)
            elif type_name == 'bool':
                if isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes')
                return bool(value)
            elif type_name == 'list':
                if isinstance(value, str):
                    return [value]
                return list(value) if value is not None else []
            elif type_name == 'dict':
                if isinstance(value, str):
                    return json.loads(value)
                return dict(value) if value is not None else {}
            return value
        except (ValueError, TypeError, json.JSONDecodeError):
            return value

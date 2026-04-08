"""Data Transform Action.

Transforms data using expressions, field computations, and
custom transformation functions with caching support.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataTransformAction(BaseAction):
    """Transform data using expressions and functions.
    
    Applies field transformations, computed fields, and custom
    functions to transform data records.
    """
    action_type = "data_transform"
    display_name = "数据变换"
    description = "数据变换，支持表达式计算和自定义函数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Transform data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Data to transform.
                - transforms: Dict of field -> expression or transform dict.
                - computed_fields: List of new field definitions.
                - drop_fields: Fields to remove.
                - rename_fields: Dict of old_name -> new_name.
                - cache_transforms: Cache computed transforms (default: True).
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with transformed data.
        """
        try:
            data = params.get('data')
            transforms = params.get('transforms', {})
            computed_fields = params.get('computed_fields', [])
            drop_fields = params.get('drop_fields', [])
            rename_fields = params.get('rename_fields', {})
            save_to_var = params.get('save_to_var', 'transformed_data')

            if data is None:
                data = context.get_variable(params.get('use_var', 'input_data'))

            if not data:
                return ActionResult(success=False, message="No data provided")

            result = []
            for item in data:
                if not isinstance(item, dict):
                    result.append(item)
                    continue

                transformed = item.copy()

                # Apply transforms
                for field, transform in transforms.items():
                    if field in transformed:
                        transformed[field] = self._apply_transform(
                            transformed[field], transform, transformed
                        )

                # Add computed fields
                for cf in computed_fields:
                    name = cf.get('name')
                    expr = cf.get('expression')
                    fn = cf.get('fn')
                    
                    if name:
                        if expr:
                            transformed[name] = self._evaluate_expr(expr, transformed)
                        elif fn:
                            try:
                                transformed[name] = eval(fn)(transformed)
                            except Exception:
                                pass

                # Drop fields
                for f in drop_fields:
                    transformed.pop(f, None)

                # Rename fields
                for old_name, new_name in rename_fields.items():
                    if old_name in transformed:
                        transformed[new_name] = transformed.pop(old_name)

                result.append(transformed)

            context.set_variable(save_to_var, result)
            return ActionResult(success=True, data={'count': len(result)},
                             message=f"Transformed {len(result)} items")

        except Exception as e:
            return ActionResult(success=False, message=f"Transform error: {e}")

    def _apply_transform(self, value: Any, transform: Any, context: Dict) -> Any:
        """Apply a single transform to a value."""
        if isinstance(transform, str):
            # Expression string
            if '${' in transform:
                # Interpolate context
                for k, v in context.items():
                    transform = transform.replace(f'${{{k}}}', str(v))
            return self._evaluate_expr(transform, context)
        elif isinstance(transform, dict):
            op = transform.get('op')
            if op == 'upper':
                return str(value).upper() if value is not None else None
            elif op == 'lower':
                return str(value).lower() if value is not None else None
            elif op == 'trim':
                return str(value).strip() if value is not None else None
            elif op == 'round':
                decimals = transform.get('decimals', 0)
                return round(value, decimals) if isinstance(value, (int, float)) else value
            elif op == 'abs':
                return abs(value) if isinstance(value, (int, float)) else value
            elif op == 'negate':
                return -value if isinstance(value, (int, float)) else value
            elif op == 'len':
                return len(value) if value is not None else 0
            elif op == 'type':
                return type(value).__name__
            elif op == 'str':
                return str(value)
            elif op == 'int':
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return 0
            elif op == 'float':
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return 0.0
            elif op == 'bool':
                return bool(value)
            elif op == 'upper':
                return str(value).upper() if value is not None else None
            elif op == 'contains':
                return transform.get('substring', '') in str(value) if value is not None else False
            elif op == 'replace':
                return str(value).replace(
                    transform.get('old', ''), 
                    transform.get('new', '')
                ) if value is not None else None
            elif op == 'regex_replace':
                pattern = transform.get('pattern', '')
                replacement = transform.get('replacement', '')
                return re.sub(pattern, replacement, str(value)) if value is not None else None
            elif op == 'regex_extract':
                pattern = transform.get('pattern', '')
                match = re.search(pattern, str(value))
                return match.group(0) if match else None
        return value

    def _evaluate_expr(self, expr: str, context: Dict) -> Any:
        """Evaluate an expression against context."""
        try:
            # Replace field references
            for key, value in context.items():
                if isinstance(value, (int, float, str, bool)):
                    expr = expr.replace(f'${key}', repr(value))
            
            return eval(expr)
        except Exception:
            return None

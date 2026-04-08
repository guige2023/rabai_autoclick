"""API Payload Builder Action Module.

Provides utilities for building and transforming API payloads with
support for nested structures, field mapping, and data validation.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
from copy import deepcopy

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PayloadBuilderAction(BaseAction):
    """Build structured API payloads from input data.
    
    Supports field mapping, nested structures, default values,
    computed fields, and payload validation.
    """
    action_type = "payload_builder"
    display_name = "载荷构建"
    description = "从输入数据构建结构化API载荷，支持字段映射和嵌套结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Build a structured payload from input data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Source data to build payload from.
                - mapping: Field name mapping dict.
                - defaults: Default values for missing fields.
                - computed: Dict of computed field functions.
                - required: List of required field names.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with built payload or error.
        """
        data = params.get('data', {})
        mapping = params.get('mapping', {})
        defaults = params.get('defaults', {})
        computed = params.get('computed', {})
        required = params.get('required', [])
        output_var = params.get('output_var', 'payload')
        nested = params.get('nested', {})

        if not isinstance(data, dict):
            return ActionResult(
                success=False,
                message=f"Expected dict for 'data', got {type(data).__name__}"
            )

        try:
            # Apply field mapping
            payload = {}
            for src_field, dst_field in mapping.items():
                if src_field in data:
                    payload[dst_field] = data[src_field]

            # Add unmapped fields (if keep_unmapped=True)
            if params.get('keep_unmapped', False):
                mapped_keys = set(mapping.values())
                for key, value in data.items():
                    if key not in mapping and key not in payload:
                        payload[key] = value

            # Apply defaults
            for field, default_value in defaults.items():
                if field not in payload:
                    payload[field] = default_value

            # Apply nested structure
            for nested_path, nested_data in nested.items():
                parts = nested_path.split('.')
                current = payload
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[parts[-1]] = nested_data

            # Apply computed fields
            for field, func_ref in computed.items():
                if isinstance(func_ref, str):
                    func = self._resolve_function(func_ref)
                    if func:
                        try:
                            payload[field] = func(data, payload)
                        except Exception as e:
                            payload[field] = None
                else:
                    payload[field] = func_ref

            # Validate required fields
            missing = [f for f in required if f not in payload]
            if missing:
                return ActionResult(
                    success=False,
                    message=f"Missing required fields: {missing}"
                )

            # Validate payload
            if params.get('validate', False):
                schema = params.get('schema', {})
                errors = self._validate_payload(payload, schema)
                if errors:
                    return ActionResult(
                        success=False,
                        message=f"Payload validation failed: {errors}"
                    )

            # Store in context
            context.variables[output_var] = payload
            return ActionResult(
                success=True,
                data=payload,
                message=f"Payload built with {len(payload)} fields"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Payload building failed: {str(e)}"
            )

    def _resolve_function(self, func_ref: str) -> Optional[Callable]:
        """Resolve a function reference string to a callable."""
        parts = func_ref.split('.')
        if len(parts) >= 2:
            module_name = '.'.join(parts[:-1])
            func_name = parts[-1]
            try:
                import importlib
                module = importlib.import_module(module_name)
                return getattr(module, func_name, None)
            except ImportError:
                return None
        return None

    def _validate_payload(
        self, payload: Dict, schema: Dict
    ) -> List[str]:
        """Validate payload against a simple schema."""
        errors = []
        for field, field_schema in schema.items():
            expected_type = field_schema.get('type')
            if expected_type and not isinstance(payload.get(field), eval(expected_type)):
                errors.append(f"Field '{field}' expected {expected_type}")
        return errors


class PayloadTransformAction(BaseAction):
    """Transform existing payloads with field operations.
    
    Supports renaming, removing, type casting, and value mapping.
    """
    action_type = "payload_transform"
    display_name = "载荷转换"
    description = "转换和转换现有载荷，支持重命名、删除、类型转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Transform a payload with field operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - payload: Input payload to transform.
                - rename: Dict of {old_name: new_name}.
                - remove: List of field names to remove.
                - type_cast: Dict of {field: target_type}.
                - value_map: Dict of {field: {old_val: new_val}}.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with transformed payload or error.
        """
        payload_var = params.get('payload_var', 'payload')
        payload = params.get('payload', context.variables.get(payload_var, {}))
        rename = params.get('rename', {})
        remove = params.get('remove', [])
        type_cast = params.get('type_cast', {})
        value_map = params.get('value_map', {})
        output_var = params.get('output_var', 'payload')

        if not isinstance(payload, dict):
            return ActionResult(
                success=False,
                message=f"Expected dict for payload, got {type(payload).__name__}"
            )

        try:
            result = deepcopy(payload)

            # Rename fields
            for old_name, new_name in rename.items():
                if old_name in result:
                    result[new_name] = result.pop(old_name)

            # Remove fields
            for field in remove:
                result.pop(field, None)

            # Type cast fields
            for field, target_type in type_cast.items():
                if field in result:
                    result[field] = self._cast_value(result[field], target_type)

            # Value mapping
            for field, mapping in value_map.items():
                if field in result and result[field] in mapping:
                    result[field] = mapping[result[field]]

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Payload transformed: {len(payload)} -> {len(result)} fields"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Payload transform failed: {str(e)}"
            )

    def _cast_value(self, value: Any, target_type: str) -> Any:
        """Cast a value to target type."""
        type_map = {
            'str': str,
            'int': int,
            'float': float,
            'bool': bool,
            'list': list,
            'dict': dict,
        }
        if target_type in type_map:
            try:
                return type_map[target_type](value)
            except (ValueError, TypeError):
                return value
        return value


class PayloadMergeAction(BaseAction):
    """Merge multiple payloads into a single payload.
    
    Supports deep merging, conflict resolution, and array concatenation.
    """
    action_type = "payload_merge"
    display_name = "载荷合并"
    description = "合并多个载荷，支持深度合并和冲突解决"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Merge multiple payloads.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - payloads: List of payload dicts to merge.
                - strategy: Merge strategy - 'deep', 'shallow', 'override'.
                - conflict_resolver: How to resolve conflicts.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with merged payload or error.
        """
        payloads = params.get('payloads', [])
        strategy = params.get('strategy', 'deep')
        conflict_resolver = params.get('conflict_resolver', 'last')
        output_var = params.get('output_var', 'payload')

        if not payloads:
            return ActionResult(
                success=False,
                message="No payloads provided to merge"
            )

        try:
            result = {}

            for payload in payloads:
                if not isinstance(payload, dict):
                    continue

                if strategy == 'deep':
                    result = self._deep_merge(result, payload, conflict_resolver)
                elif strategy == 'shallow':
                    result = {**result, **payload}
                elif strategy == 'override':
                    result = payload.copy()

            context.variables[output_var] = result
            return ActionResult(
                success=True,
                data=result,
                message=f"Merged {len(payloads)} payloads into {len(result)} fields"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Payload merge failed: {str(e)}"
            )

    def _deep_merge(
        self, base: Dict, override: Dict, resolver: str
    ) -> Dict:
        """Deep merge two dictionaries."""
        result = deepcopy(base)
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value, resolver)
            elif key in result and isinstance(result[key], list) and isinstance(value, list):
                if resolver == 'concat':
                    result[key] = result[key] + value
                elif resolver == 'unique':
                    result[key] = list(set(result[key] + value))
                else:
                    result[key] = value
            else:
                result[key] = value
        return result


class PayloadExtractAction(BaseAction):
    """Extract specific fields from a payload.
    
    Supports dot-notation paths, array indexing, and JMESPath-like queries.
    """
    action_type = "payload_extract"
    display_name = "载荷提取"
    description = "从载荷中提取特定字段，支持点号路径和数组索引"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Extract fields from a payload.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - payload: Source payload.
                - fields: List of field paths to extract.
                - flatten: Whether to flatten nested results.
                - output_var: Variable name to store result.
        
        Returns:
            ActionResult with extracted data or error.
        """
        payload = params.get('payload', {})
        fields = params.get('fields', [])
        flatten = params.get('flatten', False)
        output_var = params.get('output_var', 'extracted')

        if not isinstance(payload, dict):
            return ActionResult(
                success=False,
                message=f"Expected dict for payload, got {type(payload).__name__}"
            )

        try:
            extracted = {}
            for field_path in fields:
                value = self._get_nested(payload, field_path)
                if value is not None:
                    extracted[field_path] = value

            if flatten:
                flattened = self._flatten_dict(extracted)
                extracted = flattened

            context.variables[output_var] = extracted
            return ActionResult(
                success=True,
                data=extracted,
                message=f"Extracted {len(extracted)} fields"
            )

        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Payload extract failed: {str(e)}"
            )

    def _get_nested(self, data: Any, path: str) -> Any:
        """Get nested value using dot notation."""
        parts = path.split('.')
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list):
                try:
                    index = int(part)
                    current = current[index] if 0 <= index < len(current) else None
                except ValueError:
                    current = None
            else:
                return None
            if current is None:
                return None
        return current

    def _flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '.') -> Dict:
        """Flatten a nested dictionary."""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

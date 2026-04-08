"""Schema validation action module for RabAI AutoClick.

Provides JSON Schema and custom schema validation
for data records with detailed error reporting.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class SchemaValidateAction(BaseAction):
    """Validate data against a JSON Schema.
    
    Supports type checking, required fields, patterns,
    min/max, enums, and custom validators.
    """
    action_type = "schema_validate"
    display_name = "Schema验证"
    description = "使用JSON Schema验证数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Validate data against schema.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: item or list to validate
                - schema: dict (JSON Schema)
                - stop_on_first_error: bool
                - save_to_var: str
        
        Returns:
            ActionResult with validation result.
        """
        data = params.get('data')
        schema = params.get('schema', {})
        stop_on_first = params.get('stop_on_first_error', False)
        save_to_var = params.get('save_to_var', 'validation_result')

        if data is None:
            return ActionResult(success=False, message="No data provided")

        # Normalize to list
        items = data if isinstance(data, list) else [data]

        errors = []
        valid_count = 0

        for i, item in enumerate(items):
            item_errors = self._validate_item(item, schema, f"item[{i}]")
            if item_errors:
                errors.extend(item_errors)
                if stop_on_first:
                    break
            else:
                valid_count += 1

        is_valid = len(errors) == 0

        result = {
            'valid': is_valid,
            'total': len(items),
            'valid_count': valid_count,
            'error_count': len(errors),
            'errors': errors,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=is_valid,
            data=result,
            message=f"Validated {len(items)} items: {valid_count} valid, {len(errors)} errors"
        )

    def _validate_item(self, item: Any, schema: Dict, path: str) -> List[Dict]:
        """Recursively validate an item against schema."""
        errors = []

        if item is None:
            if schema.get('type') not in ['null', 'any', None]:
                errors.append({'path': path, 'error': 'null value not allowed'})
            return errors

        item_type = schema.get('type', 'any')

        # Type checking
        type_map = {
            'string': str, 'number': (int, float),
            'integer': int, 'boolean': bool, 'array': list, 'object': dict,
        }

        if item_type != 'any' and item_type in type_map:
            expected = type_map[item_type]
            if not isinstance(item, expected):
                errors.append({
                    'path': path,
                    'error': f'expected {item_type}, got {type(item).__name__}'
                })
                return errors

        # Required fields
        required = schema.get('required', [])
        if isinstance(item, dict):
            for field in required:
                if field not in item:
                    errors.append({'path': f'{path}.{field}', 'error': 'required field missing'})

        # Enum
        enum = schema.get('enum')
        if enum is not None and item not in enum:
            errors.append({'path': path, 'error': f'value not in enum: {enum}'})

        # Pattern
        pattern = schema.get('pattern')
        if pattern and isinstance(item, str):
            if not re.match(pattern, item):
                errors.append({'path': path, 'error': f'pattern mismatch: {pattern}'})

        # Min/max for numbers
        if isinstance(item, (int, float)):
            minimum = schema.get('minimum')
            maximum = schema.get('maximum')
            if minimum is not None and item < minimum:
                errors.append({'path': path, 'error': f'below minimum: {minimum}'})
            if maximum is not None and item > maximum:
                errors.append({'path': path, 'error': f'above maximum: {maximum}'})

        # Min/max length
        min_length = schema.get('minLength')
        max_length = schema.get('maxLength')
        if isinstance(item, (str, list)):
            if min_length is not None and len(item) < min_length:
                errors.append({'path': path, 'error': f'too short: min {minLength}'})
            if max_length is not None and len(item) > max_length:
                errors.append({'path': path, 'error': f'too long: max {maxLength}'})

        # Properties validation
        properties = schema.get('properties', {})
        for prop_name, prop_schema in properties.items():
            if prop_name in item:
                prop_errors = self._validate_item(item[prop_name], prop_schema, f'{path}.{prop_name}')
                errors.extend(prop_errors)

        # Items validation for arrays
        items_schema = schema.get('items')
        if items_schema and isinstance(item, list):
            for idx, array_item in enumerate(item):
                item_errors = self._validate_item(array_item, items_schema, f'{path}[{idx}]')
                errors.extend(item_errors)

        return errors


class TypeCheckAction(BaseAction):
    """Check and cast data types.
    
    Validate that values match expected types
    and optionally cast to target types.
    """
    action_type = "type_check"
    display_name = "类型检查"
    description = "检查并转换数据类型"

    TYPES = ['string', 'number', 'integer', 'boolean', 'array', 'object', 'null']

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Check and optionally cast types.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: item to check
                - expected_type: str
                - cast: bool (attempt cast if mismatch)
                - save_to_var: str
        
        Returns:
            ActionResult with type check result.
        """
        data = params.get('data')
        expected_type = params.get('expected_type', 'string')
        do_cast = params.get('cast', False)
        save_to_var = params.get('save_to_var', 'type_result')

        if data is None:
            return ActionResult(success=False, message="No data provided")

        current_type = type(data).__name__
        is_match = self._is_type(data, expected_type)

        result_value = data
        if not is_match and do_cast:
            result_value = self._cast(data, expected_type)
            is_match = result_value is not None

        result = {
            'original_type': current_type,
            'expected_type': expected_type,
            'is_match': is_match,
            'cast_success': do_cast and is_match,
            'value': result_value,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=is_match,
            data=result,
            message=f"Type check: {current_type} {'==' if is_match else '!='} {expected_type}"
        )

    def _is_type(self, value: Any, type_name: str) -> bool:
        """Check if value matches type."""
        if type_name == 'string':
            return isinstance(value, str)
        elif type_name == 'number':
            return isinstance(value, (int, float)) and not isinstance(value, bool)
        elif type_name == 'integer':
            return isinstance(value, int) and not isinstance(value, bool)
        elif type_name == 'boolean':
            return isinstance(value, bool)
        elif type_name == 'array':
            return isinstance(value, list)
        elif type_name == 'object':
            return isinstance(value, dict)
        elif type_name == 'null':
            return value is None
        return False

    def _cast(self, value: Any, target_type: str) -> Any:
        """Cast value to target type."""
        try:
            if target_type == 'string':
                return str(value)
            elif target_type == 'number':
                return float(value)
            elif target_type == 'integer':
                return int(float(value))
            elif target_type == 'boolean':
                if isinstance(value, str):
                    return value.lower() in ('true', '1', 'yes', 'on')
                return bool(value)
            elif target_type == 'array' and isinstance(value, (str, dict)):
                return [value]
            elif target_type == 'object' and isinstance(value, str):
                import json
                return json.loads(value)
        except (ValueError, TypeError):
            pass
        return None


class RangeCheckAction(BaseAction):
    """Validate numeric values are within range.
    
    Check min, max, and optional step constraints.
    """
    action_type = "range_check"
    display_name = "范围检查"
    description = "检查数值是否在指定范围内"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Validate numeric range.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - value: number to check
                - minimum: float (optional)
                - maximum: float (optional)
                - exclusive_min: float (optional)
                - exclusive_max: float (optional)
                - save_to_var: str
        
        Returns:
            ActionResult with range check result.
        """
        value = params.get('value')
        minimum = params.get('minimum')
        maximum = params.get('maximum')
        exclusive_min = params.get('exclusive_min')
        exclusive_max = params.get('exclusive_max')
        save_to_var = params.get('save_to_var', 'range_result')

        try:
            num = float(value)
        except (ValueError, TypeError):
            return ActionResult(success=False, message=f"Cannot convert {value} to number")

        violations = []

        if minimum is not None and num < minimum:
            violations.append(f'value {num} < minimum {minimum}')
        if maximum is not None and num > maximum:
            violations.append(f'value {num} > maximum {maximum}')
        if exclusive_min is not None and num <= exclusive_min:
            violations.append(f'value {num} <= exclusive_min {exclusive_min}')
        if exclusive_max is not None and num >= exclusive_max:
            violations.append(f'value {num} >= exclusive_max {exclusive_max}')

        is_valid = len(violations) == 0

        result = {
            'value': num,
            'valid': is_valid,
            'violations': violations,
            'minimum': minimum,
            'maximum': maximum,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=is_valid,
            data=result,
            message=f"Range check: {'PASS' if is_valid else 'FAIL'} - {', '.join(violations) if violations else 'within range'}"
        )


class PatternMatchAction(BaseAction):
    """Validate strings against regex patterns.
    
    Check if strings match, contain, start/end with
    patterns, with optional capture groups.
    """
    action_type = "pattern_match"
    display_name = "正则匹配"
    description = "使用正则表达式验证字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Validate string pattern.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - value: str to check
                - pattern: regex pattern
                - match_type: str (match/contains/startswith/endswith)
                - case_sensitive: bool
                - capture: bool (return capture groups)
                - save_to_var: str
        
        Returns:
            ActionResult with pattern match result.
        """
        value = params.get('value', '')
        pattern = params.get('pattern', '')
        match_type = params.get('match_type', 'match')
        case_sensitive = params.get('case_sensitive', True)
        capture = params.get('capture', False)
        save_to_var = params.get('save_to_var', 'pattern_result')

        if not isinstance(value, str):
            value = str(value)

        flags = 0 if case_sensitive else re.IGNORECASE

        try:
            regex = re.compile(pattern, flags)
        except re.error as e:
            return ActionResult(success=False, message=f"Invalid regex: {e}")

        match = None
        is_match = False
        groups = None

        if match_type == 'match':
            match = regex.match(value)
            is_match = match is not None
        elif match_type == 'contains':
            is_match = regex.search(value) is not None
            match = regex.search(value)
        elif match_type == 'startswith':
            is_match = bool(regex.match(value))
            match = regex.match(value)
        elif match_type == 'endswith':
            pattern_full = f'.*{pattern}$'
            match = re.search(pattern_full, value, flags)
            is_match = match is not None
        elif match_type == 'fullmatch':
            match = regex.fullmatch(value)
            is_match = match is not None

        if capture and match:
            groups = match.groups()

        result = {
            'value': value,
            'pattern': pattern,
            'match_type': match_type,
            'is_match': is_match,
            'groups': groups,
        }

        if context and save_to_var:
            context.variables[save_to_var] = result

        return ActionResult(
            success=is_match,
            data=result,
            message=f"Pattern {'MATCH' if is_match else 'NO MATCH'}: {pattern}"
        )

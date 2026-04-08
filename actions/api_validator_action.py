"""API Validator Action.

Validates API requests and responses with schema validation,
status code checking, and custom validation rules.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ApiValidatorAction(BaseAction):
    """Validate API requests and responses.
    
    Validates request/response structure, status codes,
    required fields, and custom validation rules.
    """
    action_type = "api_validator"
    display_name = "API验证器"
    description = "验证API请求和响应，支持schema和自定义规则"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Validate API data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - action: 'validate_request', 'validate_response', 'validate_schema'.
                - data: Data to validate.
                - required_fields: List of required field names.
                - field_types: Dict of field -> expected type.
                - status_code: Expected status code (for response validation).
                - expected_status_codes: List of acceptable status codes.
                - custom_rules: List of custom validation lambda functions.
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with validation result.
        """
        try:
            action = params.get('action', 'validate_response')
            data = params.get('data')
            required_fields = params.get('required_fields', [])
            field_types = params.get('field_types', {})
            status_code = params.get('status_code')
            expected_status_codes = params.get('expected_status_codes', [200, 201, 204])
            custom_rules = params.get('custom_rules', [])
            save_to_var = params.get('save_to_var', 'validation_result')

            if data is None:
                data = context.get_variable(params.get('use_var', 'input_data'))

            errors = []
            warnings = []

            # Validate based on action
            if action == 'validate_request':
                errors.extend(self._validate_request(data, required_fields, field_types, custom_rules))
            elif action == 'validate_response':
                errors.extend(self._validate_response(data, status_code, expected_status_codes, 
                                                     required_fields, field_types, custom_rules))
            elif action == 'validate_schema':
                errors.extend(self._validate_schema(data, required_fields, field_types))

            is_valid = len(errors) == 0
            result = {
                'valid': is_valid,
                'errors': errors,
                'warnings': warnings,
                'validated_fields': len(required_fields) + len(field_types)
            }

            context.set_variable(save_to_var, result)
            return ActionResult(success=is_valid, data=result,
                             message=f"{'VALID' if is_valid else 'INVALID'}: {len(errors)} errors")

        except Exception as e:
            return ActionResult(success=False, message=f"Validator error: {e}")

    def _validate_request(self, data: Any, required_fields: List[str],
                        field_types: Dict[str, str], custom_rules: List[str]) -> List[str]:
        """Validate API request."""
        errors = []

        if not isinstance(data, dict):
            errors.append("Request data must be a dictionary")
            return errors

        # Check required fields
        for field in required_fields:
            if field not in data or data[field] is None:
                errors.append(f"Missing required field: {field}")

        # Check field types
        for field, expected_type in field_types.items():
            if field in data and data[field] is not None:
                actual_type = type(data[field]).__name__
                if not self._is_valid_type(data[field], expected_type):
                    errors.append(f"Field '{field}' has type {actual_type}, expected {expected_type}")

        # Apply custom rules
        for rule in custom_rules:
            try:
                if not eval(rule)(data):
                    errors.append(f"Custom rule failed: {rule}")
            except Exception as e:
                errors.append(f"Custom rule error: {e}")

        return errors

    def _validate_response(self, data: Any, status_code: Optional[int],
                          expected_codes: List[int], required_fields: List[str],
                          field_types: Dict[str, str], custom_rules: List[str]) -> List[str]:
        """Validate API response."""
        errors = []

        # Check status code
        if status_code:
            if status_code not in expected_codes:
                errors.append(f"Unexpected status code: {status_code}, expected one of {expected_codes}")
        elif isinstance(data, dict) and 'status_code' in data:
            status_code = data['status_code']
            if status_code not in expected_codes:
                errors.append(f"Unexpected status code: {status_code}, expected one of {expected_codes}")

        # Check for error responses
        if status_code and status_code >= 400:
            errors.append(f"Error response: status code {status_code}")

        # Validate body
        if isinstance(data, dict):
            body = data.get('body') or data.get('data') or data
            if isinstance(body, dict):
                errors.extend(self._validate_request(body, required_fields, field_types, custom_rules))

        return errors

    def _validate_schema(self, data: Any, required_fields: List[str],
                       field_types: Dict[str, str]) -> List[str]:
        """Validate data against schema."""
        errors = []

        if not isinstance(data, dict):
            errors.append("Data must be a dictionary for schema validation")
            return errors

        for field in required_fields:
            if field not in data:
                errors.append(f"Missing required field: {field}")

        for field, expected_type in field_types.items():
            if field in data and data[field] is not None:
                if not self._is_valid_type(data[field], expected_type):
                    errors.append(f"Field '{field}' type mismatch")

        return errors

    def _is_valid_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected type."""
        type_map = {
            'str': str, 'string': str,
            'int': int, 'integer': int,
            'float': float, 'double': float,
            'bool': bool, 'boolean': bool,
            'list': list, 'array': list,
            'dict': dict, 'object': dict,
            'number': (int, float)
        }

        expected = type_map.get(expected_type.lower(), object)
        return isinstance(value, expected)

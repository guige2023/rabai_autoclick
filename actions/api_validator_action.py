"""API validator action module for RabAI AutoClick.

Provides request and response validation for API operations including
JSON schema validation, field validation, type checking, and
custom validation rules.
"""

import re
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class JsonSchemaValidatorAction(BaseAction):
    """Validate JSON data against a JSON Schema.
    
    Supports draft-07 and later JSON Schema specifications
    with comprehensive type and constraint validation.
    """
    action_type = "json_schema_validator"
    display_name = "JSON Schema验证"
    description = "使用JSON Schema验证请求和响应数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Validate data against JSON Schema.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, schema, strict, error_mode.
        
        Returns:
            ActionResult with validation result.
        """
        data = params.get('data')
        schema = params.get('schema', {})
        strict = params.get('strict', False)
        error_mode = params.get('error_mode', 'first')
        start_time = time.time()

        if data is None:
            return ActionResult(
                success=False,
                message="data is required"
            )

        errors = self._validate(data, schema, error_mode)
        if errors:
            return ActionResult(
                success=False,
                message=f"Validation failed with {len(errors)} error(s)",
                data={
                    'valid': False,
                    'errors': errors,
                    'error_count': len(errors)
                },
                duration=time.time() - start_time
            )

        return ActionResult(
            success=True,
            message="JSON Schema validation passed",
            data={'valid': True, 'validated_data': data},
            duration=time.time() - start_time
        )

    def _validate(
        self,
        data: Any,
        schema: Dict[str, Any],
        error_mode: str
    ) -> List[Dict[str, str]]:
        """Validate data against schema recursively."""
        errors = []
        schema_type = schema.get('type')
        properties = schema.get('properties', {})
        required = schema.get('required', [])
        additional_props = schema.get('additionalProperties', True)

        if isinstance(data, dict):
            for req_field in required:
                if req_field not in data:
                    errors.append({
                        'path': f"$.{req_field}",
                        'message': f"Required field '{req_field}' is missing",
                        'type': 'required'
                    })

            for key, value in data.items():
                if key in properties:
                    field_schema = properties[key]
                    field_errors = self._validate_field(key, value, field_schema, error_mode)
                    errors.extend(field_errors)
                elif not additional_props and schema_type == 'object':
                    errors.append({
                        'path': f"$.{key}",
                        'message': f"Additional property '{key}' is not allowed",
                        'type': 'additionalProperties'
                    })

        elif isinstance(data, list) and schema_type == 'array':
            if 'minItems' in schema and len(data) < schema['minItems']:
                errors.append({
                    'path': '$',
                    'message': f"Array has {len(data)} items, minimum is {schema['minItems']}",
                    'type': 'minItems'
                })
            if 'maxItems' in schema and len(data) > schema['maxItems']:
                errors.append({
                    'path': '$',
                    'message': f"Array has {len(data)} items, maximum is {schema['maxItems']}",
                    'type': 'maxItems'
                })
            if 'items' in schema:
                for i, item in enumerate(data):
                    item_errors = self._validate(item, schema['items'], error_mode)
                    for err in item_errors:
                        err['path'] = f"$.[{i}]{err['path'][1:]}"
                    errors.extend(item_errors)
                    if error_mode == 'first' and errors:
                        break

        elif schema_type == 'string' and not isinstance(data, str):
            errors.append({
                'path': '$',
                'message': f"Expected string, got {type(data).__name__}",
                'type': 'type'
            })
        elif schema_type == 'number' and not isinstance(data, (int, float)):
            errors.append({
                'path': '$',
                'message': f"Expected number, got {type(data).__name__}",
                'type': 'type'
            })
        elif schema_type == 'integer' and not isinstance(data, int):
            errors.append({
                'path': '$',
                'message': f"Expected integer, got {type(data).__name__}",
                'type': 'type'
            })
        elif schema_type == 'boolean' and not isinstance(data, bool):
            errors.append({
                'path': '$',
                'message': f"Expected boolean, got {type(data).__name__}",
                'type': 'type'
            })

        if schema_type == 'string' and isinstance(data, str):
            if 'minLength' in schema and len(data) < schema['minLength']:
                errors.append({
                    'path': '$',
                    'message': f"String length {len(data)} is less than minLength {schema['minLength']}",
                    'type': 'minLength'
                })
            if 'maxLength' in schema and len(data) > schema['maxLength']:
                errors.append({
                    'path': '$',
                    'message': f"String length {len(data)} exceeds maxLength {schema['maxLength']}",
                    'type': 'maxLength'
                })
            if 'pattern' in schema:
                if not re.match(schema['pattern'], data):
                    errors.append({
                        'path': '$',
                        'message': f"String does not match pattern: {schema['pattern']}",
                        'type': 'pattern'
                    })
            if 'format' in schema:
                fmt_errors = self._validate_format(data, schema['format'])
                errors.extend(fmt_errors)

        return errors

    def _validate_field(
        self,
        key: str,
        value: Any,
        field_schema: Dict[str, Any],
        error_mode: str
    ) -> List[Dict[str, str]]:
        """Validate a single field."""
        return self._validate(value, field_schema, error_mode)

    def _validate_format(self, value: str, fmt: str) -> List[Dict[str, str]]:
        """Validate string format."""
        errors = []
        format_validators = {
            'email': r'^[\w\.-]+@[\w\.-]+\.\w+$',
            'uri': r'^https?://',
            'uuid': r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
            'ipv4': r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$',
            'date': r'^\d{4}-\d{2}-\d{2}$',
            'date-time': r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',
        }
        if fmt in format_validators:
            if not re.match(format_validators[fmt], value):
                errors.append({
                    'path': '$',
                    'message': f"String does not match format: {fmt}",
                    'type': 'format'
                })
        return errors


class RequestFieldValidatorAction(BaseAction):
    """Validate specific fields in API requests.
    
    Supports required fields, type checking, range validation,
    pattern matching, and custom validator functions.
    """
    action_type = "request_field_validator"
    display_name = "请求字段验证"
    description = "验证API请求中的特定字段"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Validate request fields.
        
        Args:
            context: Execution context.
            params: Dict with keys: request, field_specs.
                   field_specs is list of {name, type, required,
                   min, max, pattern, enum}.
        
        Returns:
            ActionResult with validation result.
        """
        request = params.get('request', {})
        field_specs = params.get('field_specs', [])
        start_time = time.time()

        errors = []
        validated = {}

        for spec in field_specs:
            field_name = spec.get('name')
            if not field_name:
                continue

            value = request.get(field_name)
            required = spec.get('required', False)

            if value is None or value == '':
                if required:
                    errors.append({
                        'field': field_name,
                        'message': f"Required field '{field_name}' is missing or empty",
                        'type': 'required'
                    })
                continue

            field_errors = self._validate_field(field_name, value, spec)
            errors.extend(field_errors)
            if not field_errors:
                validated[field_name] = value

        if errors:
            return ActionResult(
                success=False,
                message=f"Field validation failed: {len(errors)} error(s)",
                data={'valid': False, 'errors': errors},
                duration=time.time() - start_time
            )

        return ActionResult(
            success=True,
            message="All field validations passed",
            data={'valid': True, 'validated_fields': validated},
            duration=time.time() - start_time
        )

    def _validate_field(
        self,
        name: str,
        value: Any,
        spec: Dict[str, Any]
    ) -> List[Dict[str, str]]:
        """Validate a single field against its spec."""
        errors = []
        field_type = spec.get('type')

        type_map = {
            'string': str,
            'integer': int,
            'number': (int, float),
            'boolean': bool,
            'array': list,
            'object': dict,
        }
        if field_type and field_type in type_map:
            if not isinstance(value, type_map[field_type]):
                errors.append({
                    'field': name,
                    'message': f"Field '{name}' must be of type {field_type}",
                    'type': 'type'
                })
                return errors

        if isinstance(value, (int, float)) and field_type in ('integer', 'number'):
            min_val = spec.get('min')
            max_val = spec.get('max')
            if min_val is not None and value < min_val:
                errors.append({
                    'field': name,
                    'message': f"Field '{name}' value {value} is less than minimum {min_val}",
                    'type': 'range'
                })
            if max_val is not None and value > max_val:
                errors.append({
                    'field': name,
                    'message': f"Field '{name}' value {value} exceeds maximum {max_val}",
                    'type': 'range'
                })

        if isinstance(value, str):
            pattern = spec.get('pattern')
            if pattern and not re.match(pattern, value):
                errors.append({
                    'field': name,
                    'message': f"Field '{name}' does not match pattern: {pattern}",
                    'type': 'pattern'
                })
            enum = spec.get('enum')
            if enum and value not in enum:
                errors.append({
                    'field': name,
                    'message': f"Field '{name}' value '{value}' not in allowed values: {enum}",
                    'type': 'enum'
                })

        return errors


class ResponseValidatorAction(BaseAction):
    """Validate API response structure and status codes.
    
    Checks response status code, headers, body structure,
    and optionally verifies expected response patterns.
    """
    action_type = "response_validator"
    display_name = "响应验证"
    description = "验证API响应的状态码和结构"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Validate API response.
        
        Args:
            context: Execution context.
            params: Dict with keys: response, expected_status,
                   allowed_statuses, required_headers,
                   body_schema.
        
        Returns:
            ActionResult with validation result.
        """
        response = params.get('response', {})
        expected_status = params.get('expected_status')
        allowed_statuses = params.get('allowed_statuses', [200, 201, 204])
        required_headers = params.get('required_headers', [])
        body_schema = params.get('body_schema')
        start_time = time.time()

        errors = []
        status_code = response.get('status_code', 0)
        headers = response.get('headers', {})
        body = response.get('body')

        if allowed_statuses and status_code not in allowed_statuses:
            errors.append({
                'type': 'status_code',
                'message': f"Unexpected status code {status_code}, expected one of {allowed_statuses}",
                'actual': status_code,
                'expected': allowed_statuses
            })

        for hdr in required_headers:
            if hdr not in headers:
                errors.append({
                    'type': 'header',
                    'message': f"Required header '{hdr}' is missing",
                    'missing_header': hdr
                })

        if body_schema and body is not None:
            schema_validator = JsonSchemaValidatorAction()
            schema_result = schema_validator.execute(context, {
                'data': body,
                'schema': body_schema,
                'strict': True,
                'error_mode': 'all'
            })
            if not schema_result.success:
                errors.extend([{
                    'type': 'body_schema',
                    **err
                } for err in schema_result.data.get('errors', [])])

        if errors:
            return ActionResult(
                success=False,
                message=f"Response validation failed: {len(errors)} error(s)",
                data={
                    'valid': False,
                    'errors': errors,
                    'status_code': status_code
                },
                duration=time.time() - start_time
            )

        return ActionResult(
            success=True,
            message=f"Response validation passed (status {status_code})",
            data={
                'valid': True,
                'status_code': status_code,
                'body': body
            },
            duration=time.time() - start_time
        )


class CustomValidatorAction(BaseAction):
    """Apply custom validation logic to API data.
    
    Accepts validator functions defined in params to perform
    custom business logic validation.
    """
    action_type = "custom_validator"
    display_name = "自定义验证"
    description = "使用自定义验证逻辑验证数据"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Apply custom validation.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, rules.
                   rules is list of {field, rule, error_message}.
        
        Returns:
            ActionResult with validation result.
        """
        data = params.get('data', {})
        rules = params.get('rules', [])
        start_time = time.time()

        errors = []
        for rule in rules:
            field = rule.get('field', '')
            rule_type = rule.get('rule', '')
            error_msg = rule.get('error_message', f"Validation failed for field '{field}'")
            rule_params = rule.get('params', {})

            value = self._get_nested(data, field)
            passed = self._apply_rule(value, rule_type, rule_params)

            if not passed:
                errors.append({
                    'field': field,
                    'rule': rule_type,
                    'message': error_msg,
                    'value': value
                })

        if errors:
            return ActionResult(
                success=False,
                message=f"Custom validation failed: {len(errors)} error(s)",
                data={'valid': False, 'errors': errors},
                duration=time.time() - start_time
            )

        return ActionResult(
            success=True,
            message="Custom validation passed",
            data={'valid': True, 'data': data},
            duration=time.time() - start_time
        )

    def _get_nested(self, data: Dict[str, Any], path: str) -> Any:
        """Get nested field value using dot notation."""
        keys = path.split('.')
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value

    def _apply_rule(self, value: Any, rule_type: str, params: Dict[str, Any]) -> bool:
        """Apply a validation rule to a value."""
        if rule_type == 'not_empty':
            return value not in (None, '', [], {})
        elif rule_type == 'equals':
            return value == params.get('expected')
        elif rule_type == 'not_equals':
            return value != params.get('unexpected')
        elif rule_type == 'contains':
            return params.get('substring', '') in str(value)
        elif rule_type == 'length_min':
            return len(str(value)) >= params.get('min', 0)
        elif rule_type == 'length_max':
            return len(str(value)) <= params.get('max', float('inf'))
        elif rule_type == 'range':
            try:
                num = float(value)
                return params.get('min', float('-inf')) <= num <= params.get('max', float('inf'))
            except (TypeError, ValueError):
                return False
        elif rule_type == 'in_list':
            return value in params.get('values', [])
        elif rule_type == 'matches':
            return bool(re.match(params.get('pattern', ''), str(value)))
        return True

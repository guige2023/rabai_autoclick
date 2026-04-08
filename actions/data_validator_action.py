"""Data validator action module for RabAI AutoClick.

Provides comprehensive data validation with schema support, type checking,
and custom validation rules for API requests and data processing.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional, Callable, Union, Type
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ValidationType(Enum):
    """Type of validation to perform."""
    REQUIRED = "required"
    TYPE = "type"
    RANGE = "range"
    PATTERN = "pattern"
    ENUM = "enum"
    CUSTOM = "custom"
    LENGTH = "length"


@dataclass
class ValidationRule:
    """A single validation rule."""
    name: str
    rule_type: ValidationType
    message: str
    params: Dict[str, Any] = field(default_factory=dict)
    validator: Optional[Callable[[Any], bool]] = None


@dataclass
class ValidationError:
    """A validation error."""
    field: str
    message: str
    rule: str
    value: Any = None


@dataclass 
class ValidationResult:
    """Result of validation."""
    valid: bool
    errors: List[ValidationError]
    data: Optional[Dict[str, Any]] = None


class DataValidatorAction(BaseAction):
    """Validate data against schemas and custom rules.
    
    Supports type checking, range validation, pattern matching,
    enum validation, and custom validators.
    """
    action_type = "data_validator"
    display_name = "数据验证"
    description = "Schema验证和自定义规则验证"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data validation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Data to validate
                - schema: Dict of field -> ValidationRule
                - strict: Fail on unknown fields (default False)
                - partial: Only validate provided fields (default False)
        
        Returns:
            ActionResult with validation result.
        """
        data = params.get('data')
        schema = params.get('schema', {})
        strict = params.get('strict', False)
        partial = params.get('partial', False)
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        
        if not isinstance(data, dict):
            return ActionResult(success=False, message="data must be a dict")
        
        if not isinstance(schema, dict):
            return ActionResult(success=False, message="schema must be a dict")
        
        errors = []
        
        # Check for unknown fields
        if strict:
            unknown = set(data.keys()) - set(schema.keys())
            if unknown:
                for field_name in unknown:
                    errors.append(ValidationError(
                        field=field_name,
                        message=f"Unknown field: {field_name}",
                        rule="strict"
                    ))
        
        # Validate each field in schema
        for field_name, rules in schema.items():
            value = data.get(field_name)
            
            # Check required
            if field_name not in data or value is None:
                required_rule = self._find_rule(rules, ValidationType.REQUIRED)
                if required_rule:
                    errors.append(ValidationError(
                        field=field_name,
                        message=required_rule.message or f"{field_name} is required",
                        rule="required",
                        value=value
                    ))
                elif partial:
                    continue
                continue
            
            # Validate each rule
            if isinstance(rules, list):
                rule_list = rules
            else:
                rule_list = [rules]
            
            for rule in rule_list:
                if isinstance(rule, dict):
                    rule = self._dict_to_rule(field_name, rule)
                
                error = self._validate_field(field_name, value, rule)
                if error:
                    errors.append(error)
        
        valid = len(errors) == 0
        
        return ActionResult(
            success=valid,
            message=f"{'Valid' if valid else f'Validation failed: {len(errors)} errors'}",
            data={
                'valid': valid,
                'errors': [
                    {'field': e.field, 'message': e.message, 'rule': e.rule}
                    for e in errors
                ]
            }
        )
    
    def _find_rule(self, rules: List, rule_type: ValidationType):
        """Find first rule of given type."""
        for rule in rules:
            if isinstance(rule, ValidationRule) and rule.rule_type == rule_type:
                return rule
            elif isinstance(rule, dict) and rule.get('type') == rule_type.value:
                return rule
        return None
    
    def _dict_to_rule(self, field_name: str, d: Dict) -> ValidationRule:
        """Convert dict to ValidationRule."""
        return ValidationRule(
            name=d.get('name', field_name),
            rule_type=ValidationType(d.get('type', 'custom')),
            message=d.get('message', ''),
            params=d.get('params', {}),
            validator=d.get('validator')
        )
    
    def _validate_field(
        self,
        field_name: str,
        value: Any,
        rule: Union[ValidationRule, Dict]
    ) -> Optional[ValidationError]:
        """Validate a single field with a rule."""
        if isinstance(rule, dict):
            rule = self._dict_to_rule(field_name, rule)
        
        rule_type = rule.rule_type
        
        if rule_type == ValidationType.TYPE:
            expected_type = rule.params.get('expected')
            if not self._check_type(value, expected_type):
                return ValidationError(
                    field=field_name,
                    message=rule.message or f"{field_name} must be {expected_type}",
                    rule="type",
                    value=value
                )
        
        elif rule_type == ValidationType.RANGE:
            min_val = rule.params.get('min')
            max_val = rule.params.get('max')
            if not self._check_range(value, min_val, max_val):
                return ValidationError(
                    field=field_name,
                    message=rule.message or f"{field_name} out of range",
                    rule="range",
                    value=value
                )
        
        elif rule_type == ValidationType.PATTERN:
            pattern = rule.params.get('pattern')
            if pattern and not re.match(pattern, str(value)):
                return ValidationError(
                    field=field_name,
                    message=rule.message or f"{field_name} does not match pattern",
                    rule="pattern",
                    value=value
                )
        
        elif rule_type == ValidationType.ENUM:
            allowed = rule.params.get('values', [])
            if value not in allowed:
                return ValidationError(
                    field=field_name,
                    message=rule.message or f"{field_name} must be one of {allowed}",
                    rule="enum",
                    value=value
                )
        
        elif rule_type == ValidationType.LENGTH:
            min_len = rule.params.get('min')
            max_len = rule.params.get('max')
            length = len(value)
            if (min_len and length < min_len) or (max_len and length > max_len):
                return ValidationError(
                    field=field_name,
                    message=rule.message or f"{field_name} length out of range",
                    rule="length",
                    value=value
                )
        
        elif rule_type == ValidationType.CUSTOM:
            if rule.validator and not rule.validator(value):
                return ValidationError(
                    field=field_name,
                    message=rule.message or f"{field_name} failed custom validation",
                    rule="custom",
                    value=value
                )
        
        return None
    
    def _check_type(self, value: Any, expected: Union[Type, str]) -> bool:
        """Check if value is of expected type."""
        if expected is None:
            return True
        
        type_map = {
            'str': str, 'string': str,
            'int': int, 'integer': int,
            'float': float, 'number': float,
            'bool': bool, 'boolean': bool,
            'list': list, 'array': list,
            'dict': dict, 'object': dict
        }
        
        if isinstance(expected, str):
            expected = type_map.get(expected.lower(), expected)
        
        return isinstance(value, expected)
    
    def _check_range(
        self,
        value: Any,
        min_val: Optional[float],
        max_val: Optional[float]
    ) -> bool:
        """Check if numeric value is in range."""
        try:
            num = float(value)
            if min_val is not None and num < min_val:
                return False
            if max_val is not None and num > max_val:
                return False
            return True
        except (TypeError, ValueError):
            return False


class JSONSchemaValidatorAction(BaseAction):
    """Validate data against JSON Schema specification."""
    action_type = "json_schema_validator"
    display_name = "JSON Schema验证"
    description = "基于JSON Schema的数据验证"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute JSON Schema validation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: Data to validate
                - schema: JSON Schema dict
        
        Returns:
            ActionResult with validation result.
        """
        data = params.get('data')
        schema = params.get('schema')
        
        if data is None:
            return ActionResult(success=False, message="data is required")
        if not isinstance(schema, dict):
            return ActionResult(success=False, message="schema must be a dict")
        
        errors = self._validate_with_schema(data, schema, path="$")
        
        return ActionResult(
            success=len(errors) == 0,
            message=f"{'Valid' if not errors else f'{len(errors)} validation errors'}",
            data={'errors': errors}
        )
    
    def _validate_with_schema(
        self,
        data: Any,
        schema: Dict,
        path: str
    ) -> List[Dict]:
        """Recursively validate data against schema."""
        errors = []
        
        # Type validation
        json_type = schema.get('type')
        if json_type:
            type_map = {
                'string': (str,), 'number': (int, float),
                'integer': (int,), 'boolean': (bool),
                'array': (list,), 'object': (dict,), 'null': (type(None),)
            }
            expected = type_map.get(json_type)
            if expected and not isinstance(data, expected):
                errors.append({
                    'path': path,
                    'message': f"Expected {json_type}, got {type(data).__name__}"
                })
                return errors
        
        # Enum validation
        if 'enum' in schema:
            if data not in schema['enum']:
                errors.append({
                    'path': path,
                    'message': f"Value must be one of {schema['enum']}"
                })
        
        # String validations
        if isinstance(data, str) and 'pattern' in schema:
            if not re.match(schema['pattern'], data):
                errors.append({
                    'path': path,
                    'message': f"String does not match pattern {schema['pattern']}"
                })
        
        # Object validations
        if isinstance(data, dict) and 'properties' in schema:
            for prop, prop_schema in schema['properties'].items():
                if prop in data:
                    errors.extend(
                        self._validate_with_schema(
                            data[prop],
                            prop_schema,
                            f"{path}.{prop}"
                        )
                    )
        
        return errors

"""Data validator action module for RabAI AutoClick.

Provides comprehensive data validation with support for
schemas, type checking, range validation, and custom rules.
"""

import sys
import os
import re
from typing import Any, Dict, List, Optional, Callable, Union
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ValidationError(Exception):
    """Validation error exception."""
    def __init__(self, field: str, message: str):
        self.field = field
        self.message = message
        super().__init__(f"{field}: {message}")


@dataclass
class ValidationRule:
    """A single validation rule."""
    field: str
    rule_type: str
    params: Dict[str, Any] = field(default_factory=dict)
    message: Optional[str] = None


@dataclass
class ValidationResult:
    """Result of validation."""
    valid: bool
    errors: List[Dict[str, str]]
    warnings: List[Dict[str, str]]


class DataValidatorAction(BaseAction):
    """Data validator action for validating data against rules.
    
    Supports type checking, range validation, pattern matching,
    custom validators, and schema-based validation.
    """
    action_type = "data_validator"
    display_name = "数据校验"
    description = "数据验证与规则校验"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data validation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                data: Data to validate
                rules: List of validation rules
                schema: Schema definition (alternative to rules)
                stop_on_first_error: Stop at first error (default False).
        
        Returns:
            ActionResult with validation result.
        """
        data = params.get('data')
        rules = params.get('rules', [])
        schema = params.get('schema')
        stop_on_first = params.get('stop_on_first_error', False)
        
        if data is None:
            return ActionResult(success=False, message="No data provided")
        
        if schema:
            rules = self._schema_to_rules(schema)
        
        validation_rules = [self._parse_rule(r) for r in rules]
        
        result = self._validate(data, validation_rules, stop_on_first)
        
        return ActionResult(
            success=result.valid,
            message=f"{'Valid' if result.valid else 'Invalid'}: {len(result.errors)} errors",
            data={
                'valid': result.valid,
                'errors': result.errors,
                'warnings': result.warnings,
                'error_count': len(result.errors),
                'warning_count': len(result.warnings)
            }
        )
    
    def _schema_to_rules(self, schema: Dict[str, Any]) -> List[Dict]:
        """Convert schema to validation rules."""
        rules = []
        
        for field_name, field_def in schema.items():
            if isinstance(field_def, dict):
                if 'type' in field_def:
                    rules.append({
                        'field': field_name,
                        'rule_type': 'type',
                        'params': {'expected': field_def['type']}
                    })
                if 'required' in field_def:
                    rules.append({
                        'field': field_name,
                        'rule_type': 'required',
                        'params': {}
                    })
                if 'min' in field_def:
                    rules.append({
                        'field': field_name,
                        'rule_type': 'min',
                        'params': {'value': field_def['min']}
                    })
                if 'max' in field_def:
                    rules.append({
                        'field': field_name,
                        'rule_type': 'max',
                        'params': {'value': field_def['max']}
                    })
                if 'pattern' in field_def:
                    rules.append({
                        'field': field_name,
                        'rule_type': 'pattern',
                        'params': {'pattern': field_def['pattern']}
                    })
                if 'enum' in field_def:
                    rules.append({
                        'field': field_name,
                        'rule_type': 'enum',
                        'params': {'values': field_def['enum']}
                    })
            else:
                rules.append({
                    'field': field_name,
                    'rule_type': 'type',
                    'params': {'expected': field_def}
                })
        
        return rules
    
    def _parse_rule(self, rule_def: Union[Dict, str]) -> ValidationRule:
        """Parse rule definition."""
        if isinstance(rule_def, str):
            parts = rule_def.split(':', 1)
            return ValidationRule(
                field=parts[0],
                rule_type=parts[1] if len(parts) > 1 else 'required',
                params={}
            )
        
        return ValidationRule(
            field=rule_def.get('field', ''),
            rule_type=rule_def.get('rule_type', 'required'),
            params=rule_def.get('params', {}),
            message=rule_def.get('message')
        )
    
    def _validate(
        self,
        data: Any,
        rules: List[ValidationRule],
        stop_on_first: bool
    ) -> ValidationResult:
        """Validate data against rules."""
        errors = []
        warnings = []
        
        items = data if isinstance(data, list) else [data]
        
        for item_idx, item in enumerate(items):
            if not isinstance(item, dict):
                continue
            
            for rule in rules:
                error = self._validate_rule(item, rule, item_idx)
                
                if error:
                    if rule.rule_type == 'warning':
                        warnings.append(error)
                    else:
                        errors.append(error)
                        if stop_on_first:
                            return ValidationResult(
                                valid=False,
                                errors=errors,
                                warnings=warnings
                            )
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )
    
    def _validate_rule(
        self,
        item: Dict,
        rule: ValidationRule,
        item_idx: int
    ) -> Optional[Dict[str, str]]:
        """Validate single rule against item."""
        field_path = rule.field
        value = self._get_nested(item, field_path)
        prefix = f"[{item_idx}]." if len([item_idx]) > 1 else ""
        
        if rule.rule_type == 'required':
            if value is None or value == '':
                return {
                    'field': prefix + field_path,
                    'rule': 'required',
                    'message': rule.message or f"Field is required"
                }
        
        elif rule.rule_type == 'type':
            expected = rule.params.get('expected')
            if value is not None and not self._check_type(value, expected):
                return {
                    'field': prefix + field_path,
                    'rule': 'type',
                    'message': rule.message or f"Expected {expected}, got {type(value).__name__}"
                }
        
        elif rule.rule_type == 'min':
            min_val = rule.params.get('value')
            if value is not None and value < min_val:
                return {
                    'field': prefix + field_path,
                    'rule': 'min',
                    'message': rule.message or f"Value {value} is less than minimum {min_val}"
                }
        
        elif rule.rule_type == 'max':
            max_val = rule.params.get('value')
            if value is not None and value > max_val:
                return {
                    'field': prefix + field_path,
                    'rule': 'max',
                    'message': rule.message or f"Value {value} exceeds maximum {max_val}"
                }
        
        elif rule.rule_type == 'min_length':
            min_len = rule.params.get('value')
            if value is not None and len(value) < min_len:
                return {
                    'field': prefix + field_path,
                    'rule': 'min_length',
                    'message': rule.message or f"Length {len(value)} is less than minimum {min_len}"
                }
        
        elif rule.rule_type == 'max_length':
            max_len = rule.params.get('value')
            if value is not None and len(value) > max_len:
                return {
                    'field': prefix + field_path,
                    'rule': 'max_length',
                    'message': rule.message or f"Length {len(value)} exceeds maximum {max_len}"
                }
        
        elif rule.rule_type == 'pattern':
            pattern = rule.params.get('pattern')
            if value is not None and pattern:
                if not re.match(pattern, str(value)):
                    return {
                        'field': prefix + field_path,
                        'rule': 'pattern',
                        'message': rule.message or f"Value does not match pattern {pattern}"
                    }
        
        elif rule.rule_type == 'enum':
            allowed = rule.params.get('values', [])
            if value is not None and value not in allowed:
                return {
                    'field': prefix + field_path,
                    'rule': 'enum',
                    'message': rule.message or f"Value must be one of {allowed}"
                }
        
        elif rule.rule_type == 'email':
            if value is not None:
                email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
                if not re.match(email_pattern, str(value)):
                    return {
                        'field': prefix + field_path,
                        'rule': 'email',
                        'message': rule.message or "Invalid email format"
                    }
        
        elif rule.rule_type == 'url':
            if value is not None:
                url_pattern = r'^https?://[\w\.-]+\.\w+'
                if not re.match(url_pattern, str(value)):
                    return {
                        'field': prefix + field_path,
                        'rule': 'url',
                        'message': rule.message or "Invalid URL format"
                    }
        
        elif rule.rule_type == 'uuid':
            if value is not None:
                uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                if not re.match(uuid_pattern, str(value).lower()):
                    return {
                        'field': prefix + field_path,
                        'rule': 'uuid',
                        'message': rule.message or "Invalid UUID format"
                    }
        
        elif rule.rule_type == 'range':
            min_val = rule.params.get('min')
            max_val = rule.params.get('max')
            if value is not None:
                if min_val is not None and value < min_val:
                    return {
                        'field': prefix + field_path,
                        'rule': 'range',
                        'message': rule.message or f"Value {value} below range"
                    }
                if max_val is not None and value > max_val:
                    return {
                        'field': prefix + field_path,
                        'rule': 'range',
                        'message': rule.message or f"Value {value} above range"
                    }
        
        elif rule.rule_type == 'custom':
            validator_func = rule.params.get('func')
            if validator_func and value is not None:
                try:
                    if not validator_func(value):
                        return {
                            'field': prefix + field_path,
                            'rule': 'custom',
                            'message': rule.message or "Custom validation failed"
                        }
                except Exception as e:
                    return {
                        'field': prefix + field_path,
                        'rule': 'custom',
                        'message': rule.message or f"Validation error: {str(e)}"
                    }
        
        return None
    
    def _get_nested(self, data: Dict, path: str) -> Any:
        """Get nested value using dot notation."""
        parts = path.split('.')
        current = data
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
        return current
    
    def _check_type(self, value: Any, expected: str) -> bool:
        """Check if value matches expected type."""
        type_map = {
            'str': str,
            'string': str,
            'int': int,
            'integer': int,
            'float': float,
            'number': (int, float),
            'bool': bool,
            'boolean': bool,
            'list': list,
            'array': list,
            'dict': dict,
            'object': dict,
            'null': type(None)
        }
        
        expected_type = type_map.get(expected, object)
        
        if expected == 'number':
            return isinstance(value, expected_type)
        
        return isinstance(value, expected_type)

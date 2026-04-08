"""Validator action module for RabAI AutoClick.

Provides data validation actions with configurable rules
for schemas, constraints, and custom validators.
"""

import sys
import os
import re
import time
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class ValidationError:
    """A single validation error.
    
    Attributes:
        field: Field name that failed validation.
        rule: Rule that was violated.
        message: Human-readable error message.
        value: The invalid value.
    """
    field: str
    rule: str
    message: str
    value: Any = None


@dataclass
class ValidationResult:
    """Result of a validation operation.
    
    Attributes:
        valid: Whether validation passed.
        errors: List of validation errors.
        record: The validated record.
    """
    valid: bool
    errors: List[ValidationError]
    record: Dict[str, Any]


class Validator:
    """Data validation engine with multiple rule types."""
    
    TYPE_RULES = {
        'string': lambda v: isinstance(v, str),
        'int': lambda v: isinstance(v, int) and not isinstance(v, bool),
        'float': lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
        'bool': lambda v: isinstance(v, bool),
        'list': lambda v: isinstance(v, list),
        'dict': lambda v: isinstance(v, dict),
        'email': lambda v: isinstance(v, str) and bool(re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', v)),
        'url': lambda v: isinstance(v, str) and bool(re.match(r'^https?://', v)),
        'phone': lambda v: isinstance(v, str) and bool(re.match(r'^[\d\s\-\+\(\)]+$', v)),
        'ipv4': lambda v: isinstance(v, str) and bool(re.match(r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$', v)),
        'uuid': lambda v: isinstance(v, str) and bool(re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', v, re.I)),
    }
    
    def __init__(self):
        self._custom_rules: Dict[str, Callable] = {}
    
    def register_rule(self, name: str, validator: Callable[[Any], bool]) -> None:
        """Register a custom validation rule.
        
        Args:
            name: Rule name.
            validator: Callable that returns True if valid.
        """
        self._custom_rules[name] = validator
    
    def validate_record(
        self,
        record: Dict[str, Any],
        schema: Dict[str, Any]
    ) -> ValidationResult:
        """Validate a record against a schema.
        
        Args:
            record: Record to validate.
            schema: Schema definition.
        
        Returns:
            ValidationResult with errors if any.
        """
        errors: List[ValidationError] = []
        
        for field_name, rules in schema.items():
            value = record.get(field_name)
            field_errors = self._validate_field(field_name, value, rules)
            errors.extend(field_errors)
        
        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            record=record
        )
    
    def _validate_field(self, field_name: str, value: Any, rules: Union[str, List, Dict]) -> List[ValidationError]:
        """Validate a single field.
        
        Args:
            field_name: Name of the field.
            value: Value to validate.
            rules: Validation rules.
        
        Returns:
            List of ValidationErrors.
        """
        errors: List[ValidationError] = []
        
        if isinstance(rules, str):
            rules = [rules]
        
        if isinstance(rules, list):
            for rule in rules:
                if rule in self.TYPE_RULES:
                    if not self.TYPE_RULES[rule](value):
                        errors.append(ValidationError(
                            field=field_name,
                            rule=rule,
                            message=f"Field '{field_name}' must be of type {rule}",
                            value=value
                        ))
                elif rule in self._custom_rules:
                    if not self._custom_rules[rule](value):
                        errors.append(ValidationError(
                            field=field_name,
                            rule=rule,
                            message=f"Field '{field_name}' failed custom rule '{rule}'",
                            value=value
                        ))
                elif rule == 'required':
                    if value is None:
                        errors.append(ValidationError(
                            field=field_name,
                            rule='required',
                            message=f"Field '{field_name}' is required",
                            value=value
                        ))
        
        elif isinstance(rules, dict):
            rule_dict = rules
            
            if 'type' in rule_dict:
                type_name = rule_dict['type']
                if type_name in self.TYPE_RULES:
                    if not self.TYPE_RULES[type_name](value):
                        errors.append(ValidationError(
                            field=field_name,
                            rule=f"type:{type_name}",
                            message=f"Field '{field_name}' must be of type {type_name}",
                            value=value
                        ))
            
            if 'required' in rule_dict and rule_dict['required']:
                if value is None:
                    errors.append(ValidationError(
                        field=field_name,
                        rule='required',
                        message=f"Field '{field_name}' is required",
                        value=value
                    ))
                    return errors
            
            if value is None:
                return errors
            
            if 'min' in rule_dict and isinstance(value, (int, float)):
                if value < rule_dict['min']:
                    errors.append(ValidationError(
                        field=field_name,
                        rule='min',
                        message=f"Field '{field_name}' must be >= {rule_dict['min']}",
                        value=value
                    ))
            
            if 'max' in rule_dict and isinstance(value, (int, float)):
                if value > rule_dict['max']:
                    errors.append(ValidationError(
                        field=field_name,
                        rule='max',
                        message=f"Field '{field_name}' must be <= {rule_dict['max']}",
                        value=value
                    ))
            
            if 'min_length' in rule_dict and isinstance(value, (str, list)):
                if len(value) < rule_dict['min_length']:
                    errors.append(ValidationError(
                        field=field_name,
                        rule='min_length',
                        message=f"Field '{field_name}' must have length >= {rule_dict['min_length']}",
                        value=value
                    ))
            
            if 'max_length' in rule_dict and isinstance(value, (str, list)):
                if len(value) > rule_dict['max_length']:
                    errors.append(ValidationError(
                        field=field_name,
                        rule='max_length',
                        message=f"Field '{field_name}' must have length <= {rule_dict['max_length']}",
                        value=value
                    ))
            
            if 'pattern' in rule_dict and isinstance(value, str):
                if not re.match(rule_dict['pattern'], value):
                    errors.append(ValidationError(
                        field=field_name,
                        rule='pattern',
                        message=f"Field '{field_name}' does not match pattern",
                        value=value
                    ))
            
            if 'enum' in rule_dict:
                if value not in rule_dict['enum']:
                    errors.append(ValidationError(
                        field=field_name,
                        rule='enum',
                        message=f"Field '{field_name}' must be one of {rule_dict['enum']}",
                        value=value
                    ))
            
            if 'custom' in rule_dict:
                custom_func = rule_dict['custom']
                try:
                    if isinstance(custom_func, str):
                        custom_func = eval(f"lambda x: {custom_func}")
                    if not custom_func(value):
                        errors.append(ValidationError(
                            field=field_name,
                            rule='custom',
                            message=f"Field '{field_name}' failed custom validation",
                            value=value
                        ))
                except Exception:
                    errors.append(ValidationError(
                        field=field_name,
                        rule='custom',
                        message=f"Field '{field_name}' custom validation error",
                        value=value
                    ))
        
        return errors
    
    def validate_batch(
        self,
        records: List[Dict[str, Any]],
        schema: Dict[str, Any]
    ) -> List[ValidationResult]:
        """Validate multiple records.
        
        Args:
            records: Records to validate.
            schema: Schema definition.
        
        Returns:
            List of ValidationResults.
        """
        return [self.validate_record(record, schema) for record in records]


# Global validator
_validator = Validator()


class ValidateRecordAction(BaseAction):
    """Validate a single record against a schema."""
    action_type = "validate_record"
    display_name = "验证记录"
    description = "验证单条数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Validate a record.
        
        Args:
            context: Execution context.
            params: Dict with keys: record, schema.
        
        Returns:
            ActionResult with validation result.
        """
        record = params.get('record', {})
        schema = params.get('schema', {})
        
        if not record:
            return ActionResult(success=False, message="record is required")
        
        if not schema:
            return ActionResult(success=False, message="schema is required")
        
        result = _validator.validate_record(record, schema)
        
        return ActionResult(
            success=result.valid,
            message=f"Validation {'passed' if result.valid else 'failed'}",
            data={
                "valid": result.valid,
                "errors": [
                    {"field": e.field, "rule": e.rule, "message": e.message, "value": str(e.value)}
                    for e in result.errors
                ]
            }
        )


class ValidateBatchAction(BaseAction):
    """Validate multiple records against a schema."""
    action_type = "validate_batch"
    display_name = "批量验证"
    description = "批量验证数据"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Validate batch.
        
        Args:
            context: Execution context.
            params: Dict with keys: records, schema.
        
        Returns:
            ActionResult with batch validation results.
        """
        records = params.get('records', [])
        schema = params.get('schema', {})
        
        if not records:
            return ActionResult(success=False, message="records list is required")
        
        if not schema:
            return ActionResult(success=False, message="schema is required")
        
        results = _validator.validate_batch(records, schema)
        
        valid_count = sum(1 for r in results if r.valid)
        invalid_count = len(results) - valid_count
        
        error_summary = defaultdict(list)
        for r in results:
            for e in r.errors:
                error_summary[f"{e.field}.{e.rule}"].append(e.message)
        
        return ActionResult(
            success=invalid_count == 0,
            message=f"Validated {len(records)} records: {valid_count} valid, {invalid_count} invalid",
            data={
                "total": len(records),
                "valid": valid_count,
                "invalid": invalid_count,
                "results": [
                    {"valid": r.valid, "errors": [{"field": e.field, "rule": e.rule, "message": e.message} for e in r.errors]}
                    for r in results
                ],
                "error_summary": dict(error_summary)
            }
        )


class RegisterCustomRuleAction(BaseAction):
    """Register a custom validation rule."""
    action_type = "register_custom_rule"
    display_name = "注册验证规则"
    description = "注册自定义验证规则"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Register a custom rule.
        
        Args:
            context: Execution context.
            params: Dict with keys: name, expression.
        
        Returns:
            ActionResult with registration status.
        """
        name = params.get('name', '')
        expression = params.get('expression', '')
        
        if not name:
            return ActionResult(success=False, message="name is required")
        
        if not expression:
            return ActionResult(success=False, message="expression is required")
        
        try:
            func = eval(f"lambda x: {expression}")
            _validator.register_rule(name, func)
            
            return ActionResult(
                success=True,
                message=f"Custom rule '{name}' registered",
                data={"name": name}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Invalid expression: {str(e)}")


class CheckTypeAction(BaseAction):
    """Check the type of a value."""
    action_type = "check_type"
    display_name = "类型检查"
    description = "检查数据类型"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Check value type.
        
        Args:
            context: Execution context.
            params: Dict with keys: value, expected_type.
        
        Returns:
            ActionResult with type check result.
        """
        value = params.get('value', None)
        expected_type = params.get('expected_type', 'string')
        
        if expected_type not in Validator.TYPE_RULES:
            return ActionResult(success=False, message=f"Unknown type: {expected_type}")
        
        checker = Validator.TYPE_RULES[expected_type]
        matches = checker(value)
        
        return ActionResult(
            success=True,
            message=f"Type check: {'passed' if matches else 'failed'}",
            data={
                "matches": matches,
                "value": value,
                "expected_type": expected_type,
                "actual_type": type(value).__name__
            }
        )

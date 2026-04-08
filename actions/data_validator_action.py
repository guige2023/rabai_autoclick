# Copyright (c) 2024. coded by claude
"""Data Validator Action Module.

Provides data validation utilities for API requests and responses
with support for schema validation, type checking, and custom rules.
"""
from typing import Optional, Dict, Any, List, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
import re
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ValidationRule(Enum):
    REQUIRED = "required"
    TYPE = "type"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    MIN_VALUE = "min_value"
    MAX_VALUE = "max_value"
    PATTERN = "pattern"
    ENUM = "enum"
    CUSTOM = "custom"


@dataclass
class FieldValidator:
    name: str
    rules: List[ValidationRule]
    field_type: Optional[type] = None
    required: bool = False
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    pattern: Optional[str] = None
    enum_values: Optional[List[Any]] = None
    custom_validator: Optional[Callable[[Any], bool]] = None
    error_message: Optional[str] = None


@dataclass
class ValidationError:
    field: str
    rule: ValidationRule
    message: str


@dataclass
class ValidationResult:
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)


class DataValidator:
    def __init__(self, validators: Optional[List[FieldValidator]] = None):
        self.validators = validators or []

    def add_validator(self, validator: FieldValidator) -> None:
        self.validators.append(validator)

    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        errors: List[ValidationError] = []
        for validator in self.validators:
            field_errors = self._validate_field(validator, data)
            errors.extend(field_errors)
        return ValidationResult(valid=len(errors) == 0, errors=errors)

    def _validate_field(self, validator: FieldValidator, data: Dict[str, Any]) -> List[ValidationError]:
        errors: List[ValidationError] = []
        value = data.get(validator.name)
        if value is None:
            if validator.required:
                errors.append(ValidationError(
                    field=validator.name,
                    rule=ValidationRule.REQUIRED,
                    message=validator.error_message or f"Field '{validator.name}' is required",
                ))
            return errors
        for rule in validator.rules:
            error = self._check_rule(validator, rule, value)
            if error:
                errors.append(error)
        return errors

    def _check_rule(self, validator: FieldValidator, rule: ValidationRule, value: Any) -> Optional[ValidationError]:
        if rule == ValidationRule.TYPE:
            if validator.field_type and not isinstance(value, validator.field_type):
                return ValidationError(
                    field=validator.name,
                    rule=rule,
                    message=f"Field '{validator.name}' must be of type {validator.field_type.__name__}",
                )
        elif rule == ValidationRule.MIN_LENGTH:
            if validator.min_length is not None and len(value) < validator.min_length:
                return ValidationError(
                    field=validator.name,
                    rule=rule,
                    message=f"Field '{validator.name}' must be at least {validator.min_length} characters",
                )
        elif rule == ValidationRule.MAX_LENGTH:
            if validator.max_length is not None and len(value) > validator.max_length:
                return ValidationError(
                    field=validator.name,
                    rule=rule,
                    message=f"Field '{validator.name}' must be at most {validator.max_length} characters",
                )
        elif rule == ValidationRule.MIN_VALUE:
            if validator.min_value is not None and value < validator.min_value:
                return ValidationError(
                    field=validator.name,
                    rule=rule,
                    message=f"Field '{validator.name}' must be at least {validator.min_value}",
                )
        elif rule == ValidationRule.MAX_VALUE:
            if validator.max_value is not None and value > validator.max_value:
                return ValidationError(
                    field=validator.name,
                    rule=rule,
                    message=f"Field '{validator.name}' must be at most {validator.max_value}",
                )
        elif rule == ValidationRule.PATTERN:
            if validator.pattern and isinstance(value, str):
                if not re.match(validator.pattern, value):
                    return ValidationError(
                        field=validator.name,
                        rule=rule,
                        message=f"Field '{validator.name}' does not match required pattern",
                    )
        elif rule == ValidationRule.ENUM:
            if validator.enum_values and value not in validator.enum_values:
                return ValidationError(
                    field=validator.name,
                    rule=rule,
                    message=f"Field '{validator.name}' must be one of {validator.enum_values}",
                )
        elif rule == ValidationRule.CUSTOM:
            if validator.custom_validator and not validator.custom_validator(value):
                return ValidationError(
                    field=validator.name,
                    rule=rule,
                    message=validator.error_message or f"Field '{validator.name}' failed custom validation",
                )
        return None

"""Data Validator Action Module.

Validate data against schemas with comprehensive rule support.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

T = TypeVar("T")


class ValidationType(Enum):
    """Type of validation rule."""
    REQUIRED = "required"
    TYPE = "type"
    MIN = "min"
    MAX = "max"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    PATTERN = "pattern"
    ENUM = "enum"
    CUSTOM = "custom"
    EMAIL = "email"
    URL = "url"
    IP = "ip"


@dataclass
class ValidationError:
    """Single validation error."""
    field: str
    rule: ValidationType
    message: str
    value: Any = None


@dataclass
class ValidationResult:
    """Result of validation."""
    valid: bool
    errors: list[ValidationError] = field(default_factory=list)

    def add_error(self, error: ValidationError) -> None:
        self.errors.append(error)
        self.valid = False


@dataclass
class ValidationRule:
    """Single validation rule."""
    field: str
    validation_type: ValidationType
    value: Any = None
    message: str | None = None
    custom_validator: Callable[[Any], bool] | None = None


class DataValidator:
    """Data validator with multiple rule types."""

    def __init__(self) -> None:
        self._rules: list[ValidationRule] = []

    def add_rule(self, rule: ValidationRule) -> DataValidator:
        """Add a validation rule."""
        self._rules.append(rule)
        return self

    def required(self, field: str, message: str | None = None) -> DataValidator:
        """Add required field rule."""
        return self.add_rule(ValidationRule(field, ValidationType.REQUIRED, message=message))

    def type_check(self, field: str, expected_type: type, message: str | None = None) -> DataValidator:
        """Add type check rule."""
        return self.add_rule(ValidationRule(field, ValidationType.TYPE, expected_type, message))

    def min_value(self, field: str, min_val: float, message: str | None = None) -> DataValidator:
        """Add minimum value rule."""
        return self.add_rule(ValidationRule(field, ValidationType.MIN, min_val, message))

    def max_value(self, field: str, max_val: float, message: str | None = None) -> DataValidator:
        """Add maximum value rule."""
        return self.add_rule(ValidationRule(field, ValidationType.MAX, max_val, message))

    def min_length(self, field: str, length: int, message: str | None = None) -> DataValidator:
        """Add minimum length rule."""
        return self.add_rule(ValidationRule(field, ValidationType.MIN_LENGTH, length, message))

    def max_length(self, field: str, length: int, message: str | None = None) -> DataValidator:
        """Add maximum length rule."""
        return self.add_rule(ValidationRule(field, ValidationType.MAX_LENGTH, length, message))

    def pattern(self, field: str, regex: str, message: str | None = None) -> DataValidator:
        """Add pattern match rule."""
        return self.add_rule(ValidationRule(field, ValidationType.PATTERN, regex, message))

    def enum_value(self, field: str, allowed: list, message: str | None = None) -> DataValidator:
        """Add enum/allowed values rule."""
        return self.add_rule(ValidationRule(field, ValidationType.ENUM, allowed, message))

    def email(self, field: str, message: str | None = None) -> DataValidator:
        """Add email format rule."""
        return self.add_rule(ValidationRule(field, ValidationType.EMAIL, message=message))

    def url(self, field: str, message: str | None = None) -> DataValidator:
        """Add URL format rule."""
        return self.add_rule(ValidationRule(field, ValidationType.URL, message=message))

    def custom(self, field: str, validator: Callable[[Any], bool], message: str | None = None) -> DataValidator:
        """Add custom validator rule."""
        return self.add_rule(ValidationRule(field, ValidationType.CUSTOM, validator, message))

    def validate(self, data: dict) -> ValidationResult:
        """Validate data against all rules."""
        result = ValidationResult(valid=True)
        for rule in self._rules:
            value = data.get(rule.field)
            error = self._validate_rule(rule, value)
            if error:
                result.add_error(error)
        return result

    def _validate_rule(self, rule: ValidationRule, value: Any) -> ValidationError | None:
        """Validate a single rule."""
        msg = rule.message or f"Validation failed for {rule.field} on rule {rule.validation_type.value}"
        if rule.validation_type == ValidationType.REQUIRED:
            if value is None or (isinstance(value, str) and not value.strip()):
                return ValidationError(rule.field, rule.validation_type, msg, value)
        elif rule.validation_type == ValidationType.TYPE:
            if value is not None and not isinstance(value, rule.value):
                return ValidationError(rule.field, rule.validation_type, msg, value)
        elif rule.validation_type == ValidationType.MIN:
            if value is not None and value < rule.value:
                return ValidationError(rule.field, rule.validation_type, msg, value)
        elif rule.validation_type == ValidationType.MAX:
            if value is not None and value > rule.value:
                return ValidationError(rule.field, rule.validation_type, msg, value)
        elif rule.validation_type == ValidationType.MIN_LENGTH:
            if value is not None and len(value) < rule.value:
                return ValidationError(rule.field, rule.validation_type, msg, value)
        elif rule.validation_type == ValidationType.MAX_LENGTH:
            if value is not None and len(value) > rule.value:
                return ValidationError(rule.field, rule.validation_type, msg, value)
        elif rule.validation_type == ValidationType.PATTERN:
            if value is not None and not re.match(rule.value, str(value)):
                return ValidationError(rule.field, rule.validation_type, msg, value)
        elif rule.validation_type == ValidationType.ENUM:
            if value is not None and value not in rule.value:
                return ValidationError(rule.field, rule.validation_type, msg, value)
        elif rule.validation_type == ValidationType.EMAIL:
            if value is not None:
                pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
                if not re.match(pattern, str(value)):
                    return ValidationError(rule.field, rule.validation_type, msg, value)
        elif rule.validation_type == ValidationType.URL:
            if value is not None:
                pattern = r"^https?://[^\s/$.?#].[^\s]*$"
                if not re.match(pattern, str(value), re.IGNORECASE):
                    return ValidationError(rule.field, rule.validation_type, msg, value)
        elif rule.validation_type == ValidationType.CUSTOM:
            if value is not None and rule.custom_validator:
                if not rule.custom_validator(value):
                    return ValidationError(rule.field, rule.validation_type, msg, value)
        return None

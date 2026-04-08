"""
Data Validator Action Module.

Provides comprehensive data validation with schema support,
 custom validators, and detailed error reporting.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Optional, Type, Union
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ValidationType(Enum):
    """Type of validation to perform."""
    REQUIRED = "required"
    TYPE = "type"
    RANGE = "range"
    LENGTH = "length"
    PATTERN = "pattern"
    ENUM = "enum"
    CUSTOM = "custom"
    SCHEMA = "schema"


@dataclass
class ValidationRule:
    """A single validation rule."""
    field: str
    validation_type: ValidationType
    rule_value: Any = None
    error_message: Optional[str] = None
    severity: str = "error"


@dataclass
class ValidationError:
    """A single validation error."""
    field: str
    message: str
    value: Any = None
    severity: str = "error"


@dataclass
class ValidationResult:
    """Result of validation."""
    valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)
    validated_at: Optional[str] = None


class DataValidatorAction:
    """
    Multi-purpose data validator with schema and custom rule support.

    Validates data against configured rules and returns detailed
    error reports with field-level information.

    Example:
        validator = DataValidatorAction()
        validator.add_rule("email", ValidationRule(
            field="email",
            validation_type=ValidationType.PATTERN,
            rule_value=r"^[a-zA-Z0-9_.+-]+@[a-z]+\.[a-z]+$",
            error_message="Invalid email format",
        ))
        result = validator.validate({"email": "test@example.com"})
    """

    def __init__(self) -> None:
        self._rules: list[ValidationRule] = []
        self._custom_validators: dict[str, Callable[[Any], bool]] = {}

    def add_rule(
        self,
        field: str,
        validation_type: ValidationType,
        rule_value: Any = None,
        error_message: Optional[str] = None,
        severity: str = "error",
    ) -> "DataValidatorAction":
        """Add a validation rule."""
        rule = ValidationRule(
            field=field,
            validation_type=validation_type,
            rule_value=rule_value,
            error_message=error_message,
            severity=severity,
        )
        self._rules.append(rule)
        return self

    def add_required_fields(
        self,
        fields: list[str],
    ) -> "DataValidatorAction":
        """Add required field validation."""
        for field_name in fields:
            self.add_rule(
                field=field_name,
                validation_type=ValidationType.REQUIRED,
                error_message=f"Field '{field_name}' is required",
            )
        return self

    def add_type_validation(
        self,
        field: str,
        expected_type: Type,
    ) -> "DataValidatorAction":
        """Add type validation for a field."""
        self.add_rule(
            field=field,
            validation_type=ValidationType.TYPE,
            rule_value=expected_type,
            error_message=f"Field '{field}' must be of type {expected_type.__name__}",
        )
        return self

    def add_range_validation(
        self,
        field: str,
        min_value: Optional[Union[int, float]] = None,
        max_value: Optional[Union[int, float]] = None,
    ) -> "DataValidatorAction":
        """Add numeric range validation."""
        self.add_rule(
            field=field,
            validation_type=ValidationType.RANGE,
            rule_value={"min": min_value, "max": max_value},
            error_message=f"Field '{field}' must be between {min_value} and {max_value}",
        )
        return self

    def add_length_validation(
        self,
        field: str,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
    ) -> "DataValidatorAction":
        """Add string length validation."""
        self.add_rule(
            field=field,
            validation_type=ValidationType.LENGTH,
            rule_value={"min": min_length, "max": max_length},
            error_message=f"Field '{field}' length must be between {min_length} and {max_length}",
        )
        return self

    def add_pattern_validation(
        self,
        field: str,
        pattern: str,
        error_message: Optional[str] = None,
    ) -> "DataValidatorAction":
        """Add regex pattern validation."""
        self.add_rule(
            field=field,
            validation_type=ValidationType.PATTERN,
            rule_value=pattern,
            error_message=error_message or f"Field '{field}' does not match required pattern",
        )
        return self

    def add_enum_validation(
        self,
        field: str,
        allowed_values: list[Any],
    ) -> "DataValidatorAction":
        """Add enum/allowed values validation."""
        self.add_rule(
            field=field,
            validation_type=ValidationType.ENUM,
            rule_value=allowed_values,
            error_message=f"Field '{field}' must be one of: {allowed_values}",
        )
        return self

    def add_custom_validator(
        self,
        field: str,
        validator_func: Callable[[Any], bool],
        error_message: str,
        severity: str = "error",
    ) -> "DataValidatorAction":
        """Add a custom validation function."""
        self._custom_validators[field] = validator_func
        self.add_rule(
            field=field,
            validation_type=ValidationType.CUSTOM,
            rule_value=validator_func,
            error_message=error_message,
            severity=severity,
        )
        return self

    def validate(self, data: dict[str, Any]) -> ValidationResult:
        """Validate data against all configured rules."""
        errors: list[ValidationError] = []
        warnings: list[ValidationError] = []

        for rule in self._rules:
            value = data.get(rule.field)
            error = self._validate_rule(rule, value)

            if error:
                if rule.severity == "warning":
                    warnings.append(error)
                else:
                    errors.append(error)

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    def _validate_rule(self, rule: ValidationRule, value: Any) -> Optional[ValidationError]:
        """Validate a single rule against a value."""
        from datetime import datetime

        if rule.validation_type == ValidationType.REQUIRED:
            if value is None or value == "":
                return ValidationError(
                    field=rule.field,
                    message=rule.error_message or f"Field '{rule.field}' is required",
                    value=value,
                    severity=rule.severity,
                )

        elif rule.validation_type == ValidationType.TYPE:
            if value is not None and not isinstance(value, rule.rule_value):
                return ValidationError(
                    field=rule.field,
                    message=rule.error_message or f"Field '{rule.field}' must be of type {rule.rule_value.__name__}",
                    value=value,
                    severity=rule.severity,
                )

        elif rule.validation_type == ValidationType.RANGE:
            if value is not None:
                range_config = rule.rule_value
                min_val = range_config.get("min")
                max_val = range_config.get("max")
                if min_val is not None and value < min_val:
                    return ValidationError(field=rule.field, message=rule.error_message, value=value, severity=rule.severity)
                if max_val is not None and value > max_val:
                    return ValidationError(field=rule.field, message=rule.error_message, value=value, severity=rule.severity)

        elif rule.validation_type == ValidationType.LENGTH:
            if value is not None:
                range_config = rule.rule_value
                min_len = range_config.get("min")
                max_len = range_config.get("max")
                if min_len is not None and len(value) < min_len:
                    return ValidationError(field=rule.field, message=rule.error_message, value=value, severity=rule.severity)
                if max_len is not None and len(value) > max_len:
                    return ValidationError(field=rule.field, message=rule.error_message, value=value, severity=rule.severity)

        elif rule.validation_type == ValidationType.PATTERN:
            if value is not None:
                try:
                    if not re.match(rule.rule_value, str(value)):
                        return ValidationError(field=rule.field, message=rule.error_message, value=value, severity=rule.severity)
                except re.error:
                    logger.warning(f"Invalid regex pattern: {rule.rule_value}")

        elif rule.validation_type == ValidationType.ENUM:
            if value is not None and value not in rule.rule_value:
                return ValidationError(field=rule.field, message=rule.error_message, value=value, severity=rule.severity)

        elif rule.validation_type == ValidationType.CUSTOM:
            try:
                if not rule.rule_value(value):
                    return ValidationError(field=rule.field, message=rule.error_message, value=value, severity=rule.severity)
            except Exception as e:
                return ValidationError(field=rule.field, message=f"Custom validation error: {e}", value=value, severity=rule.severity)

        return None

    def clear_rules(self) -> None:
        """Clear all validation rules."""
        self._rules.clear()
        self._custom_validators.clear()

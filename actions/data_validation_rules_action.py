"""Data Validation Rules Action module.

Provides declarative data validation with rule-based
validation rules, custom validators, and comprehensive
error reporting.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

import numpy as np


class ValidationSeverity(Enum):
    """Severity levels for validation errors."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationError:
    """A single validation error."""

    field: str
    rule: str
    message: str
    severity: ValidationSeverity = ValidationSeverity.ERROR
    value: Any = None
    params: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "field": self.field,
            "rule": self.rule,
            "message": self.message,
            "severity": self.severity.value,
            "value": str(self.value) if self.value is not None else None,
            "params": self.params,
        }


@dataclass
class ValidationResult:
    """Result of validation operation."""

    is_valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)
    validated_at: float = field(default_factory=lambda: __import__("time").time())

    def add_error(self, error: ValidationError) -> None:
        """Add an error."""
        if error.severity == ValidationSeverity.WARNING:
            self.warnings.append(error)
        else:
            self.errors.append(error)
        if error.severity != ValidationSeverity.INFO:
            self.is_valid = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "is_valid": self.is_valid,
            "error_count": len(self.errors),
            "warning_count": len(self.warnings),
            "errors": [e.to_dict() for e in self.errors],
            "warnings": [e.to_dict() for e in self.warnings],
        }


class ValidationRule:
    """Base validation rule."""

    def __init__(
        self,
        field: str,
        message: str = "",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ):
        self.field = field
        self.message = message or f"Validation failed for {field}"
        self.severity = severity

    def validate(self, data: dict[str, Any]) -> Optional[ValidationError]:
        """Validate the field."""
        raise NotImplementedError


class RequiredRule(ValidationRule):
    """Field is required rule."""

    def validate(self, data: dict[str, Any]) -> Optional[ValidationError]:
        """Check if field is present and not None/empty."""
        if self.field not in data or data[self.field] is None:
            return ValidationError(
                field=self.field,
                rule="required",
                message=self.message or f"Field '{self.field}' is required",
                severity=self.severity,
            )

        value = data[self.field]
        if isinstance(value, str) and not value.strip():
            return ValidationError(
                field=self.field,
                rule="required",
                message=self.message or f"Field '{self.field}' cannot be empty",
                severity=self.severity,
                value=value,
            )

        return None


class TypeRule(ValidationRule):
    """Field type validation rule."""

    def __init__(
        self,
        field: str,
        expected_type: type | tuple[type, ...],
        message: str = "",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ):
        super().__init__(field, message, severity)
        self.expected_type = expected_type

    def validate(self, data: dict[str, Any]) -> Optional[ValidationError]:
        """Check field type."""
        if self.field not in data:
            return None

        value = data[self.field]
        if value is None:
            return None

        if not isinstance(value, self.expected_type):
            return ValidationError(
                field=self.field,
                rule="type",
                message=self.message or f"Field '{self.field}' must be of type {self.expected_type}",
                severity=self.severity,
                value=type(value).__name__,
                params={"expected": str(self.expected_type)},
            )

        return None


class RangeRule(ValidationRule):
    """Numeric range validation rule."""

    def __init__(
        self,
        field: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        message: str = "",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ):
        super().__init__(field, message, severity)
        self.min_value = min_value
        self.max_value = max_value

    def validate(self, data: dict[str, Any]) -> Optional[ValidationError]:
        """Check numeric range."""
        if self.field not in data:
            return None

        value = data[self.field]
        if value is None:
            return None

        try:
            num_value = float(value)
        except (ValueError, TypeError):
            return ValidationError(
                field=self.field,
                rule="range",
                message=f"Field '{self.field}' is not a number",
                severity=self.severity,
                value=value,
            )

        if self.min_value is not None and num_value < self.min_value:
            return ValidationError(
                field=self.field,
                rule="range",
                message=self.message or f"Field '{self.field}' must be >= {self.min_value}",
                severity=self.severity,
                value=num_value,
                params={"min": self.min_value},
            )

        if self.max_value is not None and num_value > self.max_value:
            return ValidationError(
                field=self.field,
                rule="range",
                message=self.message or f"Field '{self.field}' must be <= {self.max_value}",
                severity=self.severity,
                value=num_value,
                params={"max": self.max_value},
            )

        return None


class LengthRule(ValidationRule):
    """String length validation rule."""

    def __init__(
        self,
        field: str,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        message: str = "",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ):
        super().__init__(field, message, severity)
        self.min_length = min_length
        self.max_length = max_length

    def validate(self, data: dict[str, Any]) -> Optional[ValidationError]:
        """Check string length."""
        if self.field not in data:
            return None

        value = data[self.field]
        if value is None:
            return None

        str_value = str(value)
        length = len(str_value)

        if self.min_length is not None and length < self.min_length:
            return ValidationError(
                field=self.field,
                rule="length",
                message=self.message or f"Field '{self.field}' must be at least {self.min_length} characters",
                severity=self.severity,
                value=length,
                params={"min_length": self.min_length},
            )

        if self.max_length is not None and length > self.max_length:
            return ValidationError(
                field=self.field,
                rule="length",
                message=self.message or f"Field '{self.field}' must be at most {self.max_length} characters",
                severity=self.severity,
                value=length,
                params={"max_length": self.max_length},
            )

        return None


class PatternRule(ValidationRule):
    """Regex pattern validation rule."""

    def __init__(
        self,
        field: str,
        pattern: str,
        message: str = "",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ):
        super().__init__(field, message, severity)
        self.pattern = re.compile(pattern)

    def validate(self, data: dict[str, Any]) -> Optional[ValidationError]:
        """Check regex pattern match."""
        if self.field not in data:
            return None

        value = data[self.field]
        if value is None:
            return None

        str_value = str(value)

        if not self.pattern.match(str_value):
            return ValidationError(
                field=self.field,
                rule="pattern",
                message=self.message or f"Field '{self.field}' does not match required pattern",
                severity=self.severity,
                value=str_value,
                params={"pattern": self.pattern.pattern},
            )

        return None


class EmailRule(PatternRule):
    """Email format validation rule."""

    EMAIL_PATTERN = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

    def __init__(
        self,
        field: str,
        message: str = "",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ):
        super().__init__(field, self.EMAIL_PATTERN, message, severity)
        self.rule = "email"


class URLRule(PatternRule):
    """URL format validation rule."""

    URL_PATTERN = r"^https?://[^\s/$.?#].[^\s]*$"

    def __init__(
        self,
        field: str,
        message: str = "",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ):
        super().__init__(field, self.URL_PATTERN, message, severity)
        self.rule = "url"


class OneOfRule(ValidationRule):
    """Value must be one of allowed values."""

    def __init__(
        self,
        field: str,
        allowed_values: list[Any],
        message: str = "",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ):
        super().__init__(field, message, severity)
        self.allowed_values = allowed_values

    def validate(self, data: dict[str, Any]) -> Optional[ValidationError]:
        """Check if value is in allowed list."""
        if self.field not in data:
            return None

        value = data[self.field]
        if value is None:
            return None

        if value not in self.allowed_values:
            return ValidationError(
                field=self.field,
                rule="one_of",
                message=self.message or f"Field '{self.field}' must be one of {self.allowed_values}",
                severity=self.severity,
                value=value,
                params={"allowed": self.allowed_values},
            )

        return None


class CustomRule(ValidationRule):
    """Custom validation function."""

    def __init__(
        self,
        field: str,
        validator: Callable[[Any], tuple[bool, str]],
        message: str = "",
        severity: ValidationSeverity = ValidationSeverity.ERROR,
    ):
        super().__init__(field, message, severity)
        self.validator = validator

    def validate(self, data: dict[str, Any]) -> Optional[ValidationError]:
        """Run custom validator."""
        if self.field not in data:
            return None

        value = data[self.field]

        try:
            is_valid, error_msg = self.validator(value)
        except Exception as e:
            is_valid = False
            error_msg = str(e)

        if not is_valid:
            return ValidationError(
                field=self.field,
                rule="custom",
                message=error_msg or self.message,
                severity=self.severity,
                value=value,
            )

        return None


class DataValidator:
    """Main data validation interface."""

    def __init__(self):
        self._rules: list[ValidationRule] = []

    def add_rule(self, rule: ValidationRule) -> "DataValidator":
        """Add a validation rule.

        Args:
            rule: ValidationRule to add

        Returns:
            Self for chaining
        """
        self._rules.append(rule)
        return self

    def required(self, field: str) -> "DataValidator":
        """Add required rule."""
        return self.add_rule(RequiredRule(field))

    def type_of(self, field: str, expected_type: type) -> "DataValidator":
        """Add type rule."""
        return self.add_rule(TypeRule(field, expected_type))

    def range(self, field: str, min_value: float = None, max_value: float = None) -> "DataValidator":
        """Add range rule."""
        return self.add_rule(RangeRule(field, min_value, max_value))

    def length(
        self,
        field: str,
        min_length: int = None,
        max_length: int = None,
    ) -> "DataValidator":
        """Add length rule."""
        return self.add_rule(LengthRule(field, min_length, max_length))

    def pattern(self, field: str, regex: str) -> "DataValidator":
        """Add pattern rule."""
        return self.add_rule(PatternRule(field, regex))

    def email(self, field: str) -> "DataValidator":
        """Add email rule."""
        return self.add_rule(EmailRule(field))

    def url(self, field: str) -> "DataValidator":
        """Add URL rule."""
        return self.add_rule(URLRule(field))

    def one_of(self, field: str, allowed: list[Any]) -> "DataValidator":
        """Add one_of rule."""
        return self.add_rule(OneOfRule(field, allowed))

    def custom(self, field: str, validator: Callable) -> "DataValidator":
        """Add custom rule."""
        return self.add_rule(CustomRule(field, validator))

    def validate(self, data: dict[str, Any]) -> ValidationResult:
        """Validate data against all rules.

        Args:
            data: Data to validate

        Returns:
            ValidationResult
        """
        result = ValidationResult(is_valid=True)

        for rule in self._rules:
            error = rule.validate(data)
            if error:
                result.add_error(error)

        return result

    def validate_batch(
        self,
        data_list: list[dict[str, Any]],
    ) -> list[ValidationResult]:
        """Validate multiple records.

        Args:
            data_list: List of data records

        Returns:
            List of ValidationResults
        """
        return [self.validate(data) for data in data_list]

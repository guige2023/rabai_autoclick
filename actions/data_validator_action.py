"""
Data Validator Action Module.

Validates data against schemas, business rules, and constraints with
comprehensive error reporting and auto-correction capabilities.

Author: RabAi Team
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


class ValidationSeverity(Enum):
    """Severity levels for validation errors."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationError:
    """A single validation error."""
    field: str
    message: str
    severity: ValidationSeverity
    value: Any = None
    rule: str = ""
    suggested_fix: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "field": self.field,
            "message": self.message,
            "severity": self.severity.value,
            "value": str(self.value) if self.value is not None else None,
            "rule": self.rule,
            "suggested_fix": self.suggested_fix,
        }


@dataclass
class ValidationResult:
    """Result of validating data against a schema."""
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    validated_at: datetime = field(default_factory=datetime.now)
    record_count: int = 0
    field_count: int = 0

    @property
    def error_count(self) -> int:
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)

    @property
    def is_valid(self) -> bool:
        return self.valid and len(self.errors) == 0

    def get_errors(self, field: Optional[str] = None) -> List[ValidationError]:
        if field is None:
            return self.errors
        return [e for e in self.errors if e.field == field]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "valid": self.is_valid,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "errors": [e.to_dict() for e in self.errors],
            "warnings": [e.to_dict() for e in self.warnings],
            "validated_at": self.validated_at.isoformat(),
            "record_count": self.record_count,
            "field_count": self.field_count,
        }


class Validator:
    """Base validator interface."""

    def validate(self, value: Any, context: Dict[str, Any]) -> Optional[ValidationError]:
        raise NotImplementedError


class RequiredValidator(Validator):
    """Validates that a field is present and not null."""

    def validate(self, value: Any, context: Dict[str, Any]) -> Optional[ValidationError]:
        if value is None:
            return ValidationError(
                field=context.get("field", ""),
                message="Field is required",
                severity=ValidationSeverity.ERROR,
                value=value,
                rule="required",
            )
        return None


class TypeValidator(Validator):
    """Validates field type."""

    def __init__(self, expected_type: type):
        self.expected_type = expected_type

    def validate(self, value: Any, context: Dict[str, Any]) -> Optional[ValidationError]:
        if value is None:
            return None
        if not isinstance(value, self.expected_type):
            return ValidationError(
                field=context.get("field", ""),
                message=f"Expected {self.expected_type.__name__}, got {type(value).__name__}",
                severity=ValidationSeverity.ERROR,
                value=value,
                rule="type_check",
            )
        return None


class RangeValidator(Validator):
    """Validates numeric or string length within a range."""

    def __init__(
        self,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
    ):
        self.min_val = min_val
        self.max_val = max_val
        self.min_length = min_length
        self.max_length = max_length

    def validate(self, value: Any, context: Dict[str, Any]) -> Optional[ValidationError]:
        if value is None:
            return None
        field_name = context.get("field", "")

        if isinstance(value, (int, float)):
            if self.min_val is not None and value < self.min_val:
                return ValidationError(
                    field=field_name,
                    message=f"Value {value} is below minimum {self.min_val}",
                    severity=ValidationSeverity.ERROR,
                    value=value,
                    rule="range_min",
                )
            if self.max_val is not None and value > self.max_val:
                return ValidationError(
                    field=field_name,
                    message=f"Value {value} exceeds maximum {self.max_val}",
                    severity=ValidationSeverity.ERROR,
                    value=value,
                    rule="range_max",
                )

        if isinstance(value, str):
            if self.min_length is not None and len(value) < self.min_length:
                return ValidationError(
                    field=field_name,
                    message=f"Length {len(value)} is below minimum {self.min_length}",
                    severity=ValidationSeverity.ERROR,
                    value=value,
                    rule="length_min",
                )
            if self.max_length is not None and len(value) > self.max_length:
                return ValidationError(
                    field=field_name,
                    message=f"Length {len(value)} exceeds maximum {self.max_length}",
                    severity=ValidationSeverity.ERROR,
                    value=value,
                    rule="length_max",
                )

        return None


class PatternValidator(Validator):
    """Validates string against regex pattern."""

    def __init__(self, pattern: str, flags: int = 0):
        self.pattern = re.compile(pattern, flags)

    def validate(self, value: Any, context: Dict[str, Any]) -> Optional[ValidationError]:
        if value is None:
            return None
        field_name = context.get("field", "")
        if not isinstance(value, str):
            return ValidationError(
                field=field_name,
                message=f"Expected string for pattern validation, got {type(value).__name__}",
                severity=ValidationSeverity.ERROR,
                value=value,
                rule="pattern_type",
            )
        if not self.pattern.match(value):
            return ValidationError(
                field=field_name,
                message=f"Value does not match pattern {self.pattern.pattern}",
                severity=ValidationSeverity.ERROR,
                value=value,
                rule="pattern_match",
            )
        return None


class EnumValidator(Validator):
    """Validates value is in allowed set."""

    def __init__(self, allowed_values: List[Any]):
        self.allowed_values = allowed_values

    def validate(self, value: Any, context: Dict[str, Any]) -> Optional[ValidationError]:
        if value is None:
            return None
        field_name = context.get("field", "")
        if value not in self.allowed_values:
            return ValidationError(
                field=field_name,
                message=f"Value '{value}' not in allowed set {self.allowed_values}",
                severity=ValidationSeverity.ERROR,
                value=value,
                rule="enum_check",
                suggested_fix=f"Use one of: {self.allowed_values[0]}",
            )
        return None


class EmailValidator(PatternValidator):
    """Validates email format."""

    def __init__(self):
        super().__init__(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")


class URLValidator(PatternValidator):
    """Validates URL format."""

    def __init__(self):
        super().__init__(r"^https?://[^\s/$.?#].[^\s]*$")


class DataValidator:
    """
    Data validation engine with schema and rule support.

    Validates structured data against defined schemas and business rules
    with comprehensive error reporting.

    Example:
        >>> validator = DataValidator()
        >>> validator.add_field("email", RequiredValidator(), EmailValidator())
        >>> validator.add_field("age", TypeValidator(int), RangeValidator(min_val=0, max_val=150))
        >>> result = validator.validate({"email": "test@example.com", "age": 25})
    """

    def __init__(self):
        self._field_validators: Dict[str, List[Validator]] = {}
        self._record_validators: List[Callable] = []

    def add_field(self, field_name: str, *validators: Validator) -> "DataValidator":
        """Add validators for a field."""
        if field_name not in self._field_validators:
            self._field_validators[field_name] = []
        self._field_validators[field_name].extend(validators)
        return self

    def add_record_validator(
        self,
        validator_fn: Callable[[Dict[str, Any]], Optional[ValidationError]],
    ) -> "DataValidator":
        """Add a record-level validator function."""
        self._record_validators.append(validator_fn)
        return self

    def validate(self, data: Union[Dict, List[Dict]]) -> ValidationResult:
        """Validate data against defined schema."""
        if isinstance(data, dict):
            return self._validate_single(data)
        elif isinstance(data, list):
            return self._validate_batch(data)
        else:
            return ValidationResult(
                valid=False,
                errors=[
                    ValidationError(
                        field="",
                        message=f"Unsupported data type: {type(data).__name__}",
                        severity=ValidationSeverity.ERROR,
                        rule="type_check",
                    )
                ],
            )

    def _validate_single(self, record: Dict[str, Any]) -> ValidationResult:
        """Validate a single record."""
        errors: List[ValidationError] = []
        warnings: List[ValidationError] = []
        validated_fields: Set[str] = set()

        for field_name, validators in self._field_validators.items():
            value = record.get(field_name)
            validated_fields.add(field_name)
            context = {"field": field_name, "record": record}

            for validator in validators:
                error = validator.validate(value, context)
                if error:
                    if error.severity == ValidationSeverity.WARNING:
                        warnings.append(error)
                    else:
                        errors.append(error)

        # Record-level validators
        for validator_fn in self._record_validators:
            error = validator_fn(record)
            if error:
                if error.severity == ValidationSeverity.WARNING:
                    warnings.append(error)
                else:
                    errors.append(error)

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            record_count=1,
            field_count=len(validated_fields),
        )

    def _validate_batch(self, records: List[Dict]) -> ValidationResult:
        """Validate a batch of records."""
        all_errors: List[ValidationError] = []
        all_warnings: List[ValidationError] = []

        for record in records:
            result = self._validate_single(record)
            all_errors.extend(result.errors)
            all_warnings.extend(result.warnings)

        return ValidationResult(
            valid=len(all_errors) == 0,
            errors=all_errors,
            warnings=all_warnings,
            record_count=len(records),
            field_count=len(self._field_validators),
        )


def create_validator(config: Optional[Dict[str, Any]] = None) -> DataValidator:
    """Factory to create a configured validator from schema dict."""
    validator = DataValidator()
    if not config:
        return validator

    for field_name, rules in config.items():
        validators = []
        for rule in rules:
            rule_type = rule.get("type")
            if rule_type == "required":
                validators.append(RequiredValidator())
            elif rule_type == "type":
                validators.append(TypeValidator(eval(rule["expected"])))
            elif rule_type == "range":
                validators.append(RangeValidator(
                    min_val=rule.get("min"),
                    max_val=rule.get("max"),
                    min_length=rule.get("min_length"),
                    max_length=rule.get("max_length"),
                ))
            elif rule_type == "pattern":
                validators.append(PatternValidator(rule["pattern"]))
            elif rule_type == "enum":
                validators.append(EnumValidator(rule["values"]))
            elif rule_type == "email":
                validators.append(EmailValidator())
            elif rule_type == "url":
                validators.append(URLValidator())

        validator.add_field(field_name, *validators)

    return validator

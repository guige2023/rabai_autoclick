"""
Data Validate Action Module.

Comprehensive data validation framework for automation with
schema validation, cross-field validation, and custom validators.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union

logger = logging.getLogger(__name__)


class ValidationErrorType(Enum):
    """Types of validation errors."""
    REQUIRED = "required"
    TYPE = "type"
    RANGE = "range"
    PATTERN = "pattern"
    CHOICE = "choice"
    CUSTOM = "custom"
    CROSS_FIELD = "cross_field"
    LENGTH = "length"


@dataclass
class ValidationError:
    """A single validation error."""
    field: str
    error_type: ValidationErrorType
    message: str
    value: Any = None
    constraint: Any = None


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    def add_error(
        self,
        field: str,
        error_type: ValidationErrorType,
        message: str,
        value: Any = None,
        constraint: Any = None,
    ) -> None:
        """Add a validation error."""
        self.errors.append(ValidationError(
            field=field,
            error_type=error_type,
            message=message,
            value=value,
            constraint=constraint,
        ))
        self.valid = False

    def add_warning(self, message: str) -> None:
        """Add a warning."""
        self.warnings.append(message)

    @property
    def error_messages(self) -> List[str]:
        """Get flat list of error messages."""
        return [e.message for e in self.errors]

    @property
    def field_errors(self) -> Dict[str, List[str]]:
        """Get errors grouped by field."""
        result: Dict[str, List[str]] = {}
        for err in self.errors:
            if err.field not in result:
                result[err.field] = []
            result[err.field].append(err.message)
        return result


@dataclass
class FieldValidator:
    """Validator for a single field."""
    field_name: str
    required: bool = False
    field_type: Optional[type] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    choices: Optional[Set[Any]] = None
    custom_validators: List[Callable[[Any], bool]] = field(default_factory=list)
    custom_messages: Dict[str, str] = field(default_factory=dict)


@dataclass
class CrossFieldValidator:
    """Validator that spans multiple fields."""
    name: str
    fields: List[str]
    validator_fn: Callable[[Dict[str, Any]], bool]
    message: str


class DataValidateAction:
    """
    Comprehensive data validation for automation.

    Supports field-level validation, cross-field validation,
    custom validators, and detailed error reporting.

    Example:
        validator = DataValidateAction()
        validator.add_field("email", required=True, field_type=str, pattern=r"^[^@]+@[^@]+$")
        validator.add_field("age", field_type=int, min_value=0, max_value=150)
        validator.add_cross_field(
            CrossFieldValidator(
                name="date_range",
                fields=["start_date", "end_date"],
                validator_fn=lambda d: d["end_date"] >= d["start_date"],
                message="end_date must be after start_date",
            )
        )

        result = validator.validate({"email": "test@example.com", "age": 30})
        if not result.valid:
            print(result.error_messages)
    """

    def __init__(self) -> None:
        self._field_validators: Dict[str, FieldValidator] = {}
        self._cross_field_validators: List[CrossFieldValidator] = []

    def add_field(
        self,
        field_name: str,
        required: bool = False,
        field_type: Optional[type] = None,
        min_value: Optional[Any] = None,
        max_value: Optional[Any] = None,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        pattern: Optional[str] = None,
        choices: Optional[Set[Any]] = None,
    ) -> "DataValidateAction":
        """Add a field validator."""
        self._field_validators[field_name] = FieldValidator(
            field_name=field_name,
            required=required,
            field_type=field_type,
            min_value=min_value,
            max_value=max_value,
            min_length=min_length,
            max_length=max_length,
            pattern=pattern,
            choices=choices,
        )
        return self

    def add_custom_validator(
        self,
        field_name: str,
        validator_fn: Callable[[Any], bool],
        error_message: str = "Custom validation failed",
    ) -> "DataValidateAction":
        """Add a custom validator to a field."""
        if field_name not in self._field_validators:
            self._field_validators[field_name] = FieldValidator(field_name=field_name)

        self._field_validators[field_name].custom_validators.append(validator_fn)
        if error_message:
            self._field_validators[field_name].custom_messages[len(
                self._field_validators[field_name].custom_validators
            ) - 1] = error_message

        return self

    def add_cross_field(
        self,
        validator: CrossFieldValidator,
    ) -> "DataValidateAction":
        """Add a cross-field validator."""
        self._cross_field_validators.append(validator)
        return self

    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate data against all field and cross-field validators."""
        result = ValidationResult(valid=True)

        # Field-level validation
        for field_name, validator in self._field_validators.items():
            self._validate_field(field_name, validator, data, result)

        # Cross-field validation
        for cross_validator in self._cross_field_validators:
            self._validate_cross_field(cross_validator, data, result)

        return result

    def _validate_field(
        self,
        field_name: str,
        validator: FieldValidator,
        data: Dict[str, Any],
        result: ValidationResult,
    ) -> None:
        """Validate a single field."""
        value = data.get(field_name)

        # Required check
        if value is None or value == "":
            if validator.required:
                result.add_error(
                    field_name,
                    ValidationErrorType.REQUIRED,
                    f"Field '{field_name}' is required",
                )
            return

        # Type check
        if validator.field_type is not None:
            if not isinstance(value, validator.field_type):
                # Try coercion
                try:
                    if validator.field_type == bool:
                        pass  # bool is special
                    elif validator.field_type in (int, float):
                        validator.field_type(value)
                except (ValueError, TypeError):
                    result.add_error(
                        field_name,
                        ValidationErrorType.TYPE,
                        f"Field '{field_name}' must be of type {validator.field_type.__name__}",
                        value=value,
                        constraint=validator.field_type.__name__,
                    )
                    return

        # Range check (for numeric types)
        if validator.min_value is not None and isinstance(value, (int, float)):
            if value < validator.min_value:
                result.add_error(
                    field_name,
                    ValidationErrorType.RANGE,
                    f"Field '{field_name}' must be >= {validator.min_value}",
                    value=value,
                    constraint=validator.min_value,
                )

        if validator.max_value is not None and isinstance(value, (int, float)):
            if value > validator.max_value:
                result.add_error(
                    field_name,
                    ValidationErrorType.RANGE,
                    f"Field '{field_name}' must be <= {validator.max_value}",
                    value=value,
                    constraint=validator.max_value,
                )

        # Length check (for strings/sequences)
        if validator.min_length is not None:
            if len(value) < validator.min_length:
                result.add_error(
                    field_name,
                    ValidationErrorType.LENGTH,
                    f"Field '{field_name}' must have length >= {validator.min_length}",
                    value=value,
                    constraint=validator.min_length,
                )

        if validator.max_length is not None:
            if len(value) > validator.max_length:
                result.add_error(
                    field_name,
                    ValidationErrorType.LENGTH,
                    f"Field '{field_name}' must have length <= {validator.max_length}",
                    value=value,
                    constraint=validator.max_length,
                )

        # Pattern check
        if validator.pattern is not None and isinstance(value, str):
            if not re.match(validator.pattern, value):
                result.add_error(
                    field_name,
                    ValidationErrorType.PATTERN,
                    f"Field '{field_name}' does not match pattern '{validator.pattern}'",
                    value=value,
                    constraint=validator.pattern,
                )

        # Choices check
        if validator.choices is not None:
            if value not in validator.choices:
                result.add_error(
                    field_name,
                    ValidationErrorType.CHOICE,
                    f"Field '{field_name}' must be one of {validator.choices}",
                    value=value,
                    constraint=list(validator.choices),
                )

        # Custom validators
        for i, cv in enumerate(validator.custom_validators):
            try:
                if not cv(value):
                    msg = validator.custom_messages.get(i, f"Custom validation failed for '{field_name}'")
                    result.add_error(
                        field_name,
                        ValidationErrorType.CUSTOM,
                        msg,
                        value=value,
                    )
            except Exception as e:
                result.add_error(
                    field_name,
                    ValidationErrorType.CUSTOM,
                    f"Custom validator error: {e}",
                    value=value,
                )

    def _validate_cross_field(
        self,
        validator: CrossFieldValidator,
        data: Dict[str, Any],
        result: ValidationResult,
    ) -> None:
        """Validate across multiple fields."""
        try:
            if not validator.validator_fn(data):
                result.add_error(
                    field=",".join(validator.fields),
                    error_type=ValidationErrorType.CROSS_FIELD,
                    message=validator.message,
                )
        except Exception as e:
            result.add_error(
                field=",".join(validator.fields),
                error_type=ValidationErrorType.CROSS_FIELD,
                message=f"Cross-field validation error: {e}",
            )

    def validate_many(
        self,
        data_list: List[Dict[str, Any]],
    ) -> List[ValidationResult]:
        """Validate multiple records."""
        return [self.validate(data) for data in data_list]

    def get_valid_records(
        self,
        data_list: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Filter to only valid records."""
        return [d for d, r in zip(data_list, self.validate_many(data_list)) if r.valid]

    def get_invalid_records(
        self,
        data_list: List[Dict[str, Any]],
    ) -> List[tuple[Dict[str, Any], ValidationResult]]:
        """Get invalid records with their error results."""
        return [
            (d, r) for d, r in zip(data_list, self.validate_many(data_list))
            if not r.valid
        ]

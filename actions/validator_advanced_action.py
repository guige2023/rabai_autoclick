"""Advanced validator action module.

Provides schema validation, cross-field validation, and custom
validation rules for complex data structures.
"""

from __future__ import annotations

import re
import logging
from typing import Optional, Dict, Any, List, Callable, Union
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class ValidationLevel(Enum):
    """Severity level of validation."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationError:
    """A single validation error."""
    field: str
    message: str
    level: ValidationLevel = ValidationLevel.ERROR
    code: Optional[str] = None


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)

    def add_error(self, field: str, message: str, code: Optional[str] = None) -> None:
        self.errors.append(ValidationError(field=field, message=message, level=ValidationLevel.ERROR, code=code))
        self.valid = False

    def add_warning(self, field: str, message: str, code: Optional[str] = None) -> None:
        self.warnings.append(ValidationError(field=field, message=message, level=ValidationLevel.WARNING, code=code))


@dataclass
class FieldValidator:
    """Validator for a single field."""
    name: str
    required: bool = False
    field_type: Optional[type] = None
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    choices: Optional[List[Any]] = None
    custom_validator: Optional[Callable[[Any], bool]] = None
    error_message: str = "Validation failed"


class AdvancedValidatorAction:
    """Advanced validation engine.

    Validates data against schemas and custom rules.

    Example:
        validator = AdvancedValidatorAction()
        validator.field("email", required=True, pattern=r"[\w.-]+@[\w.-]+")
        validator.field("age", field_type=int, min_value=0, max_value=150)
        result = validator.validate(data)
    """

    def __init__(self) -> None:
        """Initialize advanced validator."""
        self._field_validators: Dict[str, FieldValidator] = {}
        self._cross_field_validators: List[Callable[[Dict[str, Any]], List[ValidationError]]] = []

    def field(
        self,
        name: str,
        required: bool = False,
        field_type: Optional[type] = None,
        min_value: Optional[Union[int, float]] = None,
        max_value: Optional[Union[int, float]] = None,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        pattern: Optional[str] = None,
        choices: Optional[List[Any]] = None,
        custom: Optional[Callable[[Any], bool]] = None,
        message: str = "Validation failed",
    ) -> "AdvancedValidatorAction":
        """Define a field validator.

        Args:
            name: Field name.
            required: Field is required.
            field_type: Expected Python type.
            min_value: Minimum numeric value.
            max_value: Maximum numeric value.
            min_length: Minimum string length.
            max_length: Maximum string length.
            pattern: Regex pattern.
            choices: Allowed values.
            custom: Custom validation function.
            message: Error message on failure.

        Returns:
            Self for chaining.
        """
        self._field_validators[name] = FieldValidator(
            name=name,
            required=required,
            field_type=field_type,
            min_value=min_value,
            max_value=max_value,
            min_length=min_length,
            max_length=max_length,
            pattern=pattern,
            choices=choices,
            custom_validator=custom,
            error_message=message,
        )
        return self

    def cross_field(
        self,
        validator: Callable[[Dict[str, Any]], List[ValidationError]],
    ) -> "AdvancedValidatorAction":
        """Add a cross-field validation function.

        Args:
            validator: Function that takes full record and returns errors.

        Returns:
            Self for chaining.
        """
        self._cross_field_validators.append(validator)
        return self

    def validate(
        self,
        data: Dict[str, Any],
    ) -> ValidationResult:
        """Validate data against all rules.

        Args:
            data: Record to validate.

        Returns:
            ValidationResult with errors and warnings.
        """
        result = ValidationResult(valid=True)

        for name, field_val in self._field_validators.items():
            value = data.get(name)
            self._validate_field(name, value, field_val, result)

        for validator in self._cross_field_validators:
            errors = validator(data)
            for err in errors:
                result.errors.append(err)
            if errors:
                result.valid = False

        return result

    def _validate_field(
        self,
        name: str,
        value: Any,
        validator: FieldValidator,
        result: ValidationResult,
    ) -> None:
        """Validate a single field."""
        if value is None or (isinstance(value, str) and not value.strip()):
            if validator.required:
                result.add_error(name, f"{name} is required", code="REQUIRED")
            return

        if validator.field_type and value is not None:
            if validator.field_type == int:
                try:
                    value = int(value)
                except (ValueError, TypeError):
                    result.add_error(name, f"{name} must be an integer", code="TYPE")
                    return
            elif validator.field_type == float:
                try:
                    value = float(value)
                except (ValueError, TypeError):
                    result.add_error(name, f"{name} must be a number", code="TYPE")
                    return
            elif validator.field_type == str and not isinstance(value, str):
                result.add_error(name, f"{name} must be a string", code="TYPE")
                return

        if validator.min_value is not None and isinstance(value, (int, float)):
            if value < validator.min_value:
                result.add_error(name, f"{name} must be >= {validator.min_value}", code="MIN_VALUE")
                return

        if validator.max_value is not None and isinstance(value, (int, float)):
            if value > validator.max_value:
                result.add_error(name, f"{name} must be <= {validator.max_value}", code="MAX_VALUE")
                return

        if validator.min_length is not None and isinstance(value, str):
            if len(value) < validator.min_length:
                result.add_error(name, f"{name} must be at least {validator.min_length} chars", code="MIN_LENGTH")
                return

        if validator.max_length is not None and isinstance(value, str):
            if len(value) > validator.max_length:
                result.add_error(name, f"{name} must be at most {validator.max_length} chars", code="MAX_LENGTH")
                return

        if validator.pattern is not None:
            if not re.match(validator.pattern, str(value)):
                result.add_error(name, validator.error_message, code="PATTERN")
                return

        if validator.choices is not None:
            if value not in validator.choices:
                result.add_error(name, f"{name} must be one of {validator.choices}", code="CHOICES")
                return

        if validator.custom_validator is not None:
            try:
                if not validator.custom_validator(value):
                    result.add_error(name, validator.error_message, code="CUSTOM")
            except Exception as e:
                result.add_error(name, f"Custom validation error: {e}", code="CUSTOM_ERROR")

    def validate_batch(
        self,
        data: List[Dict[str, Any]],
    ) -> List[ValidationResult]:
        """Validate a batch of records.

        Args:
            data: List of records.

        Returns:
            List of ValidationResults.
        """
        return [self.validate(record) for record in data]

"""
Data Validation Action Module.

Provides comprehensive data validation with custom rules,
cross-field validation, and validation error reporting.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging
import re

logger = logging.getLogger(__name__)


class ValidationType(Enum):
    """Validation types."""
    REQUIRED = "required"
    TYPE = "type"
    RANGE = "range"
    PATTERN = "pattern"
    ENUM = "enum"
    CUSTOM = "custom"
    CROSS_FIELD = "cross_field"


class ValidationSeverity(Enum):
    """Validation severity."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationError:
    """Single validation error."""
    field: str
    error_type: ValidationType
    message: str
    severity: ValidationSeverity = ValidationSeverity.ERROR
    value: Any = None
    constraint: Any = None


@dataclass
class ValidationResult:
    """Result of validation."""
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    validated_at: datetime = field(default_factory=datetime.now)

    @property
    def error_count(self) -> int:
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)

    def get_errors_by_field(self, field: str) -> List[ValidationError]:
        return [e for e in self.errors if e.field == field]

    def get_field_errors(self, field: str) -> str:
        """Get formatted error messages for field."""
        return "; ".join(e.message for e in self.get_errors_by_field(field))


@dataclass
class FieldValidator:
    """Validator for a single field."""
    field_name: str
    validators: List[Tuple[ValidationType, Any, str]] = field(default_factory=list)
    required: bool = False
    custom_validators: List[Callable] = field(default_factory=list)

    def add_validator(
        self,
        validation_type: ValidationType,
        constraint: Any,
        message: str
    ):
        """Add a validator."""
        self.validators.append((validation_type, constraint, message))

    def validate(self, value: Any) -> List[ValidationError]:
        """Validate a value."""
        errors = []

        if value is None or value == "":
            if self.required:
                errors.append(ValidationError(
                    field=self.field_name,
                    error_type=ValidationType.REQUIRED,
                    message=f"{self.field_name} is required"
                ))
            return errors

        for val_type, constraint, message in self.validators:
            error = self._validate_single(value, val_type, constraint, message)
            if error:
                errors.append(error)

        for custom_validator in self.custom_validators:
            try:
                if not custom_validator(value):
                    errors.append(ValidationError(
                        field=self.field_name,
                        error_type=ValidationType.CUSTOM,
                        message=f"{self.field_name} failed custom validation"
                    ))
            except Exception as e:
                errors.append(ValidationError(
                    field=self.field_name,
                    error_type=ValidationType.CUSTOM,
                    message=f"{self.field_name} validation error: {str(e)}"
                ))

        return errors

    def _validate_single(
        self,
        value: Any,
        validation_type: ValidationType,
        constraint: Any,
        message: str
    ) -> Optional[ValidationError]:
        """Validate with single constraint."""
        if validation_type == ValidationType.TYPE:
            if not isinstance(value, constraint):
                return ValidationError(
                    field=self.field_name,
                    error_type=validation_type,
                    message=message or f"Expected {constraint.__name__}",
                    value=value,
                    constraint=constraint
                )

        elif validation_type == ValidationType.RANGE:
            if isinstance(value, (int, float)):
                min_val, max_val = constraint
                if value < min_val or value > max_val:
                    return ValidationError(
                        field=self.field_name,
                        error_type=validation_type,
                        message=message or f"Value must be between {min_val} and {max_val}",
                        value=value,
                        constraint=constraint
                    )

        elif validation_type == ValidationType.PATTERN:
            if isinstance(value, str):
                if not re.match(constraint, value):
                    return ValidationError(
                        field=self.field_name,
                        error_type=validation_type,
                        message=message or f"Pattern mismatch",
                        value=value,
                        constraint=constraint
                    )

        elif validation_type == ValidationType.ENUM:
            if value not in constraint:
                return ValidationError(
                    field=self.field_name,
                    error_type=validation_type,
                    message=message or f"Value must be one of {constraint}",
                    value=value,
                    constraint=constraint
                )

        return None


class SchemaValidator:
    """Validates data against a schema."""

    def __init__(self):
        self.field_validators: Dict[str, FieldValidator] = {}
        self.cross_field_validators: List[Tuple[str, str, Callable]] = []

    def add_field(self, validator: FieldValidator):
        """Add field validator."""
        self.field_validators[validator.field_name] = validator

    def add_cross_field_validator(
        self,
        field1: str,
        field2: str,
        validator: Callable[[Any, Any], bool],
        message: str
    ):
        """Add cross-field validator."""
        self.cross_field_validators.append((field1, field2, validator, message))

    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate data against schema."""
        errors = []
        warnings = []

        for field_name, validator in self.field_validators.items():
            value = data.get(field_name)
            field_errors = validator.validate(value)
            errors.extend(field_errors)

        for field1, field2, validator, message in self.cross_field_validators:
            value1 = data.get(field1)
            value2 = data.get(field2)
            try:
                if not validator(value1, value2):
                    errors.append(ValidationError(
                        field=f"{field1},{field2}",
                        error_type=ValidationType.CROSS_FIELD,
                        message=message
                    ))
            except Exception as e:
                errors.append(ValidationError(
                    field=f"{field1},{field2}",
                    error_type=ValidationType.CROSS_FIELD,
                    message=f"Cross-field validation error: {str(e)}"
                ))

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )


class ValidationReporter:
    """Reports validation results."""

    def __init__(self):
        self.history: List[ValidationResult] = []

    def add_result(self, result: ValidationResult):
        """Add validation result to history."""
        self.history.append(result)

    def get_summary(self, since: Optional[datetime] = None) -> Dict[str, Any]:
        """Get validation summary."""
        results = self.history
        if since:
            results = [r for r in results if r.validated_at >= since]

        total = len(results)
        valid_count = sum(1 for r in results if r.valid)

        field_error_counts: Dict[str, int] = {}
        for result in results:
            for error in result.errors:
                field_error_counts[error.field] = field_error_counts.get(error.field, 0) + 1

        return {
            "total_validations": total,
            "valid_count": valid_count,
            "invalid_count": total - valid_count,
            "success_rate": valid_count / total if total > 0 else 0,
            "top_error_fields": sorted(
                field_error_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10]
        }


def main():
    """Demonstrate data validation."""
    schema = SchemaValidator()

    schema.add_field(FieldValidator(
        field_name="email",
        required=True
    ).add_validator(
        ValidationType.PATTERN,
        r"^[\w\.]+@[\w\.]+$",
        "Invalid email format"
    ))

    schema.add_field(FieldValidator(
        field_name="age",
        required=True
    ).add_validator(
        ValidationType.TYPE,
        int,
        "Age must be an integer"
    ).add_validator(
        ValidationType.RANGE,
        (0, 150),
        "Age must be between 0 and 150"
    ))

    result = schema.validate({"email": "test@example.com", "age": 25})
    print(f"Valid: {result.valid}, Errors: {result.error_count}")

    result = schema.validate({"email": "invalid", "age": 200})
    print(f"Valid: {result.valid}, Errors: {result.error_count}")


if __name__ == "__main__":
    main()

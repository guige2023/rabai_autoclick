"""API Request Validator.

This module provides request validation:
- Schema validation
- Field constraints
- Custom validators
- Error aggregation

Example:
    >>> from actions.api_request_validator_action import RequestValidator
    >>> validator = RequestValidator(schema={"name": str, "age": int})
    >>> errors = validator.validate(request_body)
"""

from __future__ import annotations

import re
import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """A single validation error."""
    field: str
    message: str
    code: str
    value: Any = None


@dataclass
class ValidationResult:
    """Result of validation."""
    valid: bool
    errors: list[ValidationError]
    warnings: list[str]


class FieldValidator:
    """Validator for a single field."""

    def __init__(self, field_name: str) -> None:
        self._field_name = field_name
        self._rules: list[tuple[str, Any, str]] = []

    def required(self, message: str = "Field is required") -> "FieldValidator":
        """Add required rule."""
        self._rules.append(("required", True, message))
        return self

    def type_of(self, expected_type: type, message: str = "Invalid type") -> "FieldValidator":
        """Add type check rule."""
        self._rules.append(("type", expected_type, message))
        return self

    def min_length(self, length: int, message: str = "Too short") -> "FieldValidator":
        """Add minimum length rule."""
        self._rules.append(("min_length", length, message))
        return self

    def max_length(self, length: int, message: str = "Too long") -> "FieldValidator":
        """Add maximum length rule."""
        self._rules.append(("max_length", length, message))
        return self

    def pattern(self, regex: str, message: str = "Invalid format") -> "FieldValidator":
        """Add regex pattern rule."""
        self._rules.append(("pattern", re.compile(regex), message))
        return self

    def in_values(self, values: list, message: str = "Invalid value") -> "FieldValidator":
        """Add allowed values rule."""
        self._rules.append(("in", values, message))
        return self

    def custom(self, func: Callable[[Any], bool], message: str = "Invalid value") -> "FieldValidator":
        """Add custom validator."""
        self._rules.append(("custom", func, message))
        return self

    def validate(self, value: Any) -> list[ValidationError]:
        """Validate a value."""
        errors = []

        for rule_type, rule_value, message in self._rules:
            if rule_type == "required" and value is None:
                errors.append(ValidationError(field=self._field_name, message=message, code="required", value=value))
            elif rule_type == "type" and value is not None and not isinstance(value, rule_value):
                errors.append(ValidationError(field=self._field_name, message=message, code="type", value=value))
            elif rule_type == "min_length" and isinstance(value, (str, list)) and len(value) < rule_value:
                errors.append(ValidationError(field=self._field_name, message=message, code="min_length", value=value))
            elif rule_type == "max_length" and isinstance(value, (str, list)) and len(value) > rule_value:
                errors.append(ValidationError(field=self._field_name, message=message, code="max_length", value=value))
            elif rule_type == "pattern" and value is not None and not rule_value.match(str(value)):
                errors.append(ValidationError(field=self._field_name, message=message, code="pattern", value=value))
            elif rule_type == "in" and value not in rule_value:
                errors.append(ValidationError(field=self._field_name, message=message, code="in", value=value))
            elif rule_type == "custom" and value is not None:
                try:
                    if not rule_value(value):
                        errors.append(ValidationError(field=self._field_name, message=message, code="custom", value=value))
                except Exception as e:
                    errors.append(ValidationError(field=self._field_name, message=str(e), code="custom_error", value=value))

        return errors


class RequestValidator:
    """Validates API requests against schemas."""

    def __init__(self) -> None:
        """Initialize the request validator."""
        self._field_validators: dict[str, FieldValidator] = {}
        self._lock = threading.Lock()
        self._stats = {"validations": 0, "failures": 0}

    def add_field(
        self,
        field_name: str,
        validator: FieldValidator,
    ) -> None:
        """Add a field validator.

        Args:
            field_name: Field name.
            validator: FieldValidator instance.
        """
        with self._lock:
            self._field_validators[field_name] = validator

    def add_schema_fields(self, schema: dict[str, dict[str, Any]]) -> None:
        """Add fields from a schema dict.

        Args:
            schema: Dict mapping field names to rule configs.
        """
        for field_name, rules in schema.items():
            fv = FieldValidator(field_name)

            if rules.get("required"):
                fv.required()
            if "type" in rules:
                fv.type_of(rules["type"])
            if "min_length" in rules:
                fv.min_length(rules["min_length"])
            if "max_length" in rules:
                fv.max_length(rules["max_length"])
            if "pattern" in rules:
                fv.pattern(rules["pattern"])
            if "in" in rules:
                fv.in_values(rules["in"])

            self.add_field(field_name, fv)

    def validate(self, data: dict[str, Any]) -> ValidationResult:
        """Validate request data.

        Args:
            data: Request data dict.

        Returns:
            ValidationResult.
        """
        with self._lock:
            self._stats["validations"] += 1

        all_errors = []

        for field_name, validator in self._field_validators.items():
            value = data.get(field_name)
            errors = validator.validate(value)
            all_errors.extend(errors)

        result = ValidationResult(
            valid=len(all_errors) == 0,
            errors=all_errors,
            warnings=[],
        )

        if all_errors:
            with self._lock:
                self._stats["failures"] += 1

        return result

    def validate_and_raise(self, data: dict[str, Any]) -> None:
        """Validate and raise exception if invalid.

        Args:
            data: Request data.

        Raises:
            ValidationError: If validation fails.
        """
        result = self.validate(data)
        if not result.valid:
            error_msgs = [f"{e.field}: {e.message}" for e in result.errors]
            raise ValueError(f"Validation failed: {', '.join(error_msgs)}")

    def get_stats(self) -> dict[str, int]:
        """Get validation statistics."""
        with self._lock:
            return dict(self._stats)

"""Data validation utilities for API inputs and outputs.

This module provides comprehensive data validation:
- Schema-based validation
- Type checking
- Range and constraint validation
- Custom validator functions

Example:
    >>> from actions.data_validator_action import Validator, Schema
    >>> schema = Schema({"name": str, "age": int})
    >>> validator = Validator(schema)
    >>> validator.validate({"name": "Alice", "age": 30})
"""

from __future__ import annotations

import re
import logging
from typing import Any, Optional, Callable
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """A validation error."""
    field: str
    message: str
    value: Any = None


@dataclass
class ValidationResult:
    """Result of a validation operation."""
    valid: bool
    errors: list[ValidationError] = field(default_factory=list)

    @property
    def error_messages(self) -> list[str]:
        """Get flat list of error messages."""
        return [e.message for e in self.errors]


class FieldValidator:
    """Validator for a single field."""

    def __init__(self, field_name: str) -> None:
        self.field_name = field_name
        self._validators: list[Callable[[Any], Optional[str]]] = []

    def required(self) -> FieldValidator:
        """Field cannot be None or empty."""
        def validate(value: Any) -> Optional[str]:
            if value is None or (isinstance(value, str) and not value.strip()):
                return f"{self.field_name} is required"
            return None
        self._validators.append(validate)
        return self

    def type_check(self, expected_type: type) -> FieldValidator:
        """Validate field type."""
        def validate(value: Any) -> Optional[str]:
            if value is not None and not isinstance(value, expected_type):
                return f"{self.field_name} must be {expected_type.__name__}"
            return None
        self._validators.append(validate)
        return self

    def range_check(
        self,
        min_val: Optional[Any] = None,
        max_val: Optional[Any] = None,
    ) -> FieldValidator:
        """Validate numeric range."""
        def validate(value: Any) -> Optional[str]:
            if value is None:
                return None
            if min_val is not None and value < min_val:
                return f"{self.field_name} must be >= {min_val}"
            if max_val is not None and value > max_val:
                return f"{self.field_name} must be <= {max_val}"
            return None
        self._validators.append(validate)
        return self

    def length_check(
        self,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
    ) -> FieldValidator:
        """Validate string/list length."""
        def validate(value: Any) -> Optional[str]:
            if value is None:
                return None
            length = len(value)
            if min_length is not None and length < min_length:
                return f"{self.field_name} length must be >= {min_length}"
            if max_length is not None and length > max_length:
                return f"{self.field_name} length must be <= {max_length}"
            return None
        self._validators.append(validate)
        return self

    def pattern(self, regex: str) -> FieldValidator:
        """Validate string pattern."""
        compiled = re.compile(regex)
        def validate(value: Any) -> Optional[str]:
            if value is None:
                return None
            if not isinstance(value, str):
                return f"{self.field_name} must be a string for pattern matching"
            if not compiled.match(value):
                return f"{self.field_name} does not match pattern {regex}"
            return None
        self._validators.append(validate)
        return self

    def one_of(self, choices: list[Any]) -> FieldValidator:
        """Validate value is one of choices."""
        def validate(value: Any) -> Optional[str]:
            if value is None:
                return None
            if value not in choices:
                return f"{self.field_name} must be one of {choices}"
            return None
        self._validators.append(validate)
        return self

    def custom(self, func: Callable[[Any], bool]) -> FieldValidator:
        """Add a custom validator function."""
        def validate(value: Any) -> Optional[str]:
            try:
                if not func(value):
                    return f"{self.field_name} failed custom validation"
            except Exception as e:
                return f"{self.field_name} validation error: {e}"
            return None
        self._validators.append(validate)
        return self

    def validate(self, value: Any) -> list[ValidationError]:
        """Run all validators.

        Returns:
            List of ValidationErrors (empty if valid).
        """
        errors = []
        for validator in self._validators:
            error_msg = validator(value)
            if error_msg:
                errors.append(ValidationError(
                    field=self.field_name,
                    message=error_msg,
                    value=value,
                ))
        return errors


class SchemaValidator:
    """Validate data against a schema."""

    def __init__(self, schema: dict[str, Any]) -> None:
        self.schema = schema

    def validate(self, data: dict[str, Any]) -> ValidationResult:
        """Validate data against schema.

        Args:
            data: Data to validate.

        Returns:
            ValidationResult with errors if any.
        """
        errors: list[ValidationError] = []
        for field_name, rules in self.schema.items():
            value = data.get(field_name)
            if isinstance(rules, dict):
                errors.extend(self._validate_field_rules(field_name, value, rules))
            elif isinstance(rules, type):
                if value is not None and not isinstance(value, rules):
                    errors.append(ValidationError(
                        field=field_name,
                        message=f"must be {rules.__name__}",
                        value=value,
                    ))
        return ValidationResult(valid=len(errors) == 0, errors=errors)

    def _validate_field_rules(
        self,
        field_name: str,
        value: Any,
        rules: dict[str, Any],
    ) -> list[ValidationError]:
        """Validate a field against its rules."""
        errors = []
        validator = FieldValidator(field_name)
        if rules.get("required"):
            validator.required()
        if "type" in rules:
            validator.type_check(rules["type"])
        if "min" in rules or "max" in rules:
            validator.range_check(min_val=rules.get("min"), max_val=rules.get("max"))
        if "min_length" in rules or "max_length" in rules:
            validator.length_check(
                min_length=rules.get("min_length"),
                max_length=rules.get("max_length"),
            )
        if "pattern" in rules:
            validator.pattern(rules["pattern"])
        if "choices" in rules:
            validator.one_of(rules["choices"])
        errors.extend(validator.validate(value))
        return errors


class Validator:
    """Main validation class with fluent API."""

    def __init__(self, schema: Optional[dict[str, Any]] = None) -> None:
        self.schema = schema
        self._field_validators: dict[str, FieldValidator] = {}

    def field(self, name: str) -> FieldValidator:
        """Get or create a field validator."""
        if name not in self._field_validators:
            self._field_validators[name] = FieldValidator(name)
        return self._field_validators[name]

    def validate(self, data: dict[str, Any]) -> ValidationResult:
        """Validate data against configured schema.

        Returns:
            ValidationResult with errors if any.
        """
        all_errors: list[ValidationError] = []
        for field_name, validator in self._field_validators.items():
            value = data.get(field_name)
            all_errors.extend(validator.validate(value))
        if self.schema:
            schema_result = SchemaValidator(self.schema).validate(data)
            all_errors.extend(schema_result.errors)
        return ValidationResult(valid=len(all_errors) == 0, errors=all_errors)


def validate_json_schema(data: Any, schema: dict[str, Any]) -> ValidationResult:
    """Validate data against a JSON-like schema.

    Args:
        data: Data to validate.
        schema: Schema definition.

    Returns:
        ValidationResult.
    """
    validator = Validator(schema)
    return validator.validate(data if isinstance(data, dict) else {})

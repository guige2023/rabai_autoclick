"""
Data Validator Action Module.

Schema-based data validation with custom rules.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Type, Union


@dataclass
class ValidationError:
    """A validation error."""
    field: str
    message: str
    code: str = ""
    value: Any = None


@dataclass
class ValidationResult:
    """Result of validation."""
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)

    def __bool__(self) -> bool:
        return self.valid


class FieldValidator:
    """Validator for a single field."""

    def __init__(
        self,
        field_name: str,
        rules: Optional[List[Callable[[Any], Optional[str]]]] = None,
    ) -> None:
        self.field_name = field_name
        self.rules = rules or []

    def add_rule(
        self,
        rule: Callable[[Any], Optional[str]],
    ) -> "FieldValidator":
        """Add a validation rule."""
        self.rules.append(rule)
        return self

    def validate(self, value: Any) -> List[ValidationError]:
        """Validate field value."""
        errors = []

        for rule in self.rules:
            error_msg = rule(value)
            if error_msg:
                errors.append(ValidationError(
                    field=self.field_name,
                    message=error_msg,
                    code="RULE_FAILED",
                    value=value,
                ))

        return errors


class DataValidatorAction:
    """
    Schema-based data validator.

    Supports required, type, range, pattern, and custom validators.
    """

    def __init__(self) -> None:
        self._validators: Dict[str, FieldValidator] = {}
        self._required_fields: set = set()

    def add_field(
        self,
        field_name: str,
        required: bool = False,
        field_type: Optional[Type] = None,
        min_value: Optional[Union[int, float]] = None,
        max_value: Optional[Union[int, float]] = None,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
        pattern: Optional[str] = None,
        custom: Optional[Callable[[Any], Optional[str]]] = None,
    ) -> "DataValidatorAction":
        """
        Add a field with validation rules.

        Args:
            field_name: Name of field
            required: Is required
            field_type: Expected type
            min_value: Minimum numeric value
            max_value: Maximum numeric value
            min_length: Minimum string/list length
            max_length: Maximum string/list length
            pattern: Regex pattern
            custom: Custom validation function

        Returns:
            Self for chaining
        """
        validator = FieldValidator(field_name)
        self._validators[field_name] = validator

        if required:
            self._required_fields.add(field_name)

        if field_type is not None:
            validator.add_rule(self._type_rule(field_type))

        if min_value is not None:
            validator.add_rule(self._min_value_rule(min_value))

        if max_value is not None:
            validator.add_rule(self._max_value_rule(max_value))

        if min_length is not None:
            validator.add_rule(self._min_length_rule(min_length))

        if max_length is not None:
            validator.add_rule(self._max_length_rule(max_length))

        if pattern is not None:
            validator.add_rule(self._pattern_rule(pattern))

        if custom is not None:
            validator.add_rule(custom)

        return self

    def _type_rule(
        self,
        expected_type: Type,
    ) -> Callable[[Any], Optional[str]]:
        """Create type validation rule."""
        def rule(value: Any) -> Optional[str]:
            if value is None:
                return None
            if not isinstance(value, expected_type):
                return f"Expected {expected_type.__name__}, got {type(value).__name__}"
            return None
        return rule

    def _min_value_rule(
        self,
        min_val: Union[int, float],
    ) -> Callable[[Any], Optional[str]]:
        """Create minimum value rule."""
        def rule(value: Any) -> Optional[str]:
            if value is None:
                return None
            if value < min_val:
                return f"Value {value} is less than minimum {min_val}"
            return None
        return rule

    def _max_value_rule(
        self,
        max_val: Union[int, float],
    ) -> Callable[[Any], Optional[str]]:
        """Create maximum value rule."""
        def rule(value: Any) -> Optional[str]:
            if value is None:
                return None
            if value > max_val:
                return f"Value {value} is greater than maximum {max_val}"
            return None
        return rule

    def _min_length_rule(
        self,
        min_len: int,
    ) -> Callable[[Any], Optional[str]]:
        """Create minimum length rule."""
        def rule(value: Any) -> Optional[str]:
            if value is None:
                return None
            if hasattr(value, "__len__") and len(value) < min_len:
                return f"Length {len(value)} is less than minimum {min_len}"
            return None
        return rule

    def _max_length_rule(
        self,
        max_len: int,
    ) -> Callable[[Any], Optional[str]]:
        """Create maximum length rule."""
        def rule(value: Any) -> Optional[str]:
            if value is None:
                return None
            if hasattr(value, "__len__") and len(value) > max_len:
                return f"Length {len(value)} is greater than maximum {max_len}"
            return None
        return rule

    def _pattern_rule(
        self,
        pattern: str,
    ) -> Callable[[Any], Optional[str]]:
        """Create pattern matching rule."""
        compiled = re.compile(pattern)
        def rule(value: Any) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, str) and not compiled.match(value):
                return f"Value does not match pattern: {pattern}"
            return None
        return rule

    def validate(
        self,
        data: Dict[str, Any],
    ) -> ValidationResult:
        """
        Validate data against schema.

        Args:
            data: Data to validate

        Returns:
            ValidationResult
        """
        errors: List[ValidationError] = []

        for field_name in self._required_fields:
            if field_name not in data or data[field_name] is None:
                errors.append(ValidationError(
                    field=field_name,
                    message=f"Required field '{field_name}' is missing",
                    code="REQUIRED",
                ))

        for field_name, validator in self._validators.items():
            if field_name in data:
                field_errors = validator.validate(data[field_name])
                errors.extend(field_errors)

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
        )

    def get_errors_by_field(
        self,
        result: ValidationResult,
    ) -> Dict[str, List[str]]:
        """Get errors grouped by field."""
        grouped: Dict[str, List[str]] = {}
        for error in result.errors:
            if error.field not in grouped:
                grouped[error.field] = []
            grouped[error.field].append(error.message)
        return grouped

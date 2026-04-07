"""Validation utilities for RabAI AutoClick.

Provides:
- Schema validation
- Type checking validators
- Composite validators
- Field validators
- Validation error handling
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Optional,
    Pattern,
    Set,
    TypeVar,
    Union,
)


T = TypeVar("T")


class ValidationError(Exception):
    """Raised when validation fails."""

    def __init__(self, message: str, field: Optional[str] = None) -> None:
        self.message = message
        self.field = field
        super().__init__(message)

    def __repr__(self) -> str:
        if self.field:
            return f"ValidationError({self.field}: {self.message})"
        return f"ValidationError({self.message})"


class Validator(Generic[T]):
    """Base validator class."""

    def validate(self, value: T) -> None:
        """Validate value. Raises ValidationError on failure."""
        raise NotImplementedError

    def __call__(self, value: T) -> None:
        self.validate(value)


class TypeValidator(Validator[T]):
    """Validator that checks type."""

    def __init__(self, expected_type: type | tuple[type, ...]) -> None:
        self.expected_type = expected_type

    def validate(self, value: T) -> None:
        if not isinstance(value, self.expected_type):  # type: ignore
            raise ValidationError(
                f"Expected {self.expected_type}, got {type(value)}"
            )


class RangeValidator(Validator[T]):
    """Validator for numeric ranges."""

    def __init__(
        self,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> None:
        self.min_val = min_val
        self.max_val = max_val

    def validate(self, value: T) -> None:
        if self.min_val is not None and value < self.min_val:
            raise ValidationError(f"Value {value} is less than {self.min_val}")
        if self.max_val is not None and value > self.max_val:
            raise ValidationError(f"Value {value} is greater than {self.max_val}")


class LengthValidator(Validator[T]):
    """Validator for string/collection length."""

    def __init__(
        self,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
    ) -> None:
        self.min_length = min_length
        self.max_length = max_length

    def validate(self, value: T) -> None:
        length = len(value)  # type: ignore
        if self.min_length is not None and length < self.min_length:
            raise ValidationError(
                f"Length {length} is less than minimum {self.min_length}"
            )
        if self.max_length is not None and length > self.max_length:
            raise ValidationError(
                f"Length {length} is greater than maximum {self.max_length}"
            )


class PatternValidator(Validator[str]):
    """Validator for string patterns."""

    def __init__(self, pattern: str | Pattern[str], message: Optional[str] = None) -> None:
        if isinstance(pattern, str):
            self.pattern = re.compile(pattern)
        else:
            self.pattern = pattern
        self.message = message

    def validate(self, value: str) -> None:
        if not self.pattern.match(value):
            msg = self.message or f"String does not match pattern {self.pattern.pattern}"
            raise ValidationError(msg)


class ChoiceValidator(Validator[T]):
    """Validator for enum-like choices."""

    def __init__(self, choices: Set[T]) -> None:
        self.choices = choices

    def validate(self, value: T) -> None:
        if value not in self.choices:
            raise ValidationError(f"Value {value} not in allowed choices: {self.choices}")


class RequiredValidator(Validator[Any]):
    """Validator that checks for None and empty values."""

    def validate(self, value: Any) -> None:
        if value is None:
            raise ValidationError("Value is required but was None")
        if isinstance(value, (str, list, dict, tuple, set)) and len(value) == 0:
            raise ValidationError("Value is required but was empty")


class EmailValidator(Validator[str]):
    """Validator for email addresses."""

    EMAIL_PATTERN = re.compile(
        r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    )

    def validate(self, value: str) -> None:
        if not self.EMAIL_PATTERN.match(value):
            raise ValidationError(f"Invalid email address: {value}")


class URLValidator(Validator[str]):
    """Validator for URLs."""

    URL_PATTERN = re.compile(
        r"^https?://"
        r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|"
        r"localhost|"
        r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"
        r"(?::\d+)?"
        r"(?:/?|[/?]\S+)$",
        re.IGNORECASE,
    )

    def validate(self, value: str) -> None:
        if not self.URL_PATTERN.match(value):
            raise ValidationError(f"Invalid URL: {value}")


class CustomValidator(Validator[T]):
    """Validator that uses a custom function."""

    def __init__(
        self,
        func: Callable[[T], bool],
        message: str = "Custom validation failed",
    ) -> None:
        self.func = func
        self.message = message

    def validate(self, value: T) -> None:
        if not self.func(value):
            raise ValidationError(self.message)


class CompositeValidator(Validator[T]):
    """Combines multiple validators."""

    def __init__(
        self,
        *validators: Validator[Any],
        require_all: bool = True,
    ) -> None:
        self.validators = validators
        self.require_all = require_all

    def validate(self, value: T) -> None:
        errors: List[ValidationError] = []
        for validator in self.validators:
            try:
                validator.validate(value)
            except ValidationError as e:
                if self.require_all:
                    raise
                errors.append(e)

        if not self.require_all and errors:
            raise errors[0]


class OptionalValidator(Validator[Optional[T]]):
    """Wraps validator to make value optional (None passes)."""

    def __init__(self, validator: Validator[T]) -> None:
        self.validator = validator

    def validate(self, value: Optional[T]) -> None:
        if value is None:
            return
        self.validator.validate(value)


class SchemaValidator:
    """Validates data against a schema.

    Example:
        schema = SchemaValidator({
            "name": RequiredValidator(),
            "age": CompositeValidator(TypeValidator(int), RangeValidator(0, 150)),
            "email": CompositeValidator(TypeValidator(str), EmailValidator()),
        })

        schema.validate({"name": "John", "age": 30, "email": "john@example.com"})
    """

    def __init__(self, fields: Dict[str, Validator[Any]]) -> None:
        self.fields = fields

    def validate(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data against schema.

        Args:
            data: Data to validate.

        Returns:
            Validated data.

        Raises:
            ValidationError: If validation fails.
        """
        errors: Dict[str, List[str]] = {}

        for field_name, validator in self.fields.items():
            try:
                if field_name in data:
                    validator.validate(data[field_name])
                else:
                    if isinstance(validator, RequiredValidator):
                        raise ValidationError(f"Missing required field: {field_name}")
            except ValidationError as e:
                if field_name not in errors:
                    errors[field_name] = []
                errors[field_name].append(e.message)

        if errors:
            error_messages = [
                f"{field}: {', '.join(msgs)}"
                for field, msgs in errors.items()
            ]
            raise ValidationError("; ".join(error_messages))

        return data


@dataclass
class ValidationResult:
    """Result of a validation operation."""

    is_valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    field_errors: Dict[str, List[str]] = field(default_factory=dict)

    @property
    def error_messages(self) -> List[str]:
        return [e.message for e in self.errors]


def validate(
    value: T,
    *validators: Validator[Any],
    raise_on_error: bool = False,
) -> ValidationResult:
    """Validate value with multiple validators.

    Args:
        value: Value to validate.
        *validators: Validators to apply.
        raise_on_error: If True, raise ValidationError on failure.

    Returns:
        ValidationResult.
    """
    result = ValidationResult(is_valid=True)
    for validator in validators:
        try:
            validator.validate(value)
        except ValidationError as e:
            result.is_valid = False
            result.errors.append(e)

    if not result.is_valid and raise_on_error:
        raise result.errors[0]

    return result

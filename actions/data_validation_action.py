"""Data validation action module.

Provides comprehensive data validation functionality
with support for schemas, custom validators, and type checking.
"""

from __future__ import annotations

import re
import logging
from typing import Any, Optional, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, date

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ValidationError(Exception):
    """Validation error exception."""
    def __init__(self, message: str, field: Optional[str] = None):
        super().__init__(message)
        self.field = field
        self.message = message


class ValidationResult:
    """Result of validation operation."""
    def __init__(self):
        self.valid = True
        self.errors: list[ValidationError] = []

    def add_error(self, message: str, field: Optional[str] = None) -> None:
        """Add validation error."""
        self.errors.append(ValidationError(message, field))
        self.valid = False

    def merge(self, other: ValidationResult) -> None:
        """Merge another result."""
        self.errors.extend(other.errors)
        self.valid = self.valid and other.valid


@dataclass
class FieldValidator:
    """Single field validator."""
    name: str
    required: bool = False
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    choices: Optional[list[Any]] = None
    custom_validator: Optional[Callable[[Any], bool]] = None
    error_message: Optional[str] = None


class StringValidator:
    """String field validator."""

    @staticmethod
    def validate(value: Any, field: FieldValidator) -> ValidationResult:
        """Validate string value."""
        result = ValidationResult()

        if value is None or value == "":
            if field.required:
                result.add_error(f"{field.name} is required", field.name)
            return result

        if not isinstance(value, str):
            result.add_error(f"{field.name} must be a string", field.name)
            return result

        if field.min_length is not None and len(value) < field.min_length:
            result.add_error(
                f"{field.name} must be at least {field.min_length} characters",
                field.name
            )

        if field.max_length is not None and len(value) > field.max_length:
            result.add_error(
                f"{field.name} must be at most {field.max_length} characters",
                field.name
            )

        if field.pattern is not None and not re.match(field.pattern, value):
            msg = field.error_message or f"{field.name} has invalid format"
            result.add_error(msg, field.name)

        if field.choices is not None and value not in field.choices:
            result.add_error(
                f"{field.name} must be one of: {', '.join(str(c) for c in field.choices)}",
                field.name
            )

        if field.custom_validator is not None and not field.custom_validator(value):
            msg = field.error_message or f"{field.name} failed custom validation"
            result.add_error(msg, field.name)

        return result


class NumberValidator:
    """Number field validator."""

    @staticmethod
    def validate(value: Any, field: FieldValidator) -> ValidationResult:
        """Validate numeric value."""
        result = ValidationResult()

        if value is None:
            if field.required:
                result.add_error(f"{field.name} is required", field.name)
            return result

        if not isinstance(value, (int, float)):
            result.add_error(f"{field.name} must be a number", field.name)
            return result

        if isinstance(value, int) and field.min_length is not None and value < field.min_length:
            result.add_error(f"{field.name} must be at least {field.min_length}", field.name)

        if isinstance(value, int) and field.max_length is not None and value > field.max_length:
            result.add_error(f"{field.name} must be at most {field.max_length}", field.name)

        if field.choices is not None and value not in field.choices:
            result.add_error(
                f"{field.name} must be one of: {', '.join(str(c) for c in field.choices)}",
                field.name
            )

        return result


class ListValidator:
    """List field validator."""

    @staticmethod
    def validate(value: Any, field: FieldValidator) -> ValidationResult:
        """Validate list value."""
        result = ValidationResult()

        if value is None:
            if field.required:
                result.add_error(f"{field.name} is required", field.name)
            return result

        if not isinstance(value, list):
            result.add_error(f"{field.name} must be a list", field.name)
            return result

        if field.min_length is not None and len(value) < field.min_length:
            result.add_error(
                f"{field.name} must have at least {field.min_length} items",
                field.name
            )

        if field.max_length is not None and len(value) > field.max_length:
            result.add_error(
                f"{field.name} must have at most {field.max_length} items",
                field.name
            )

        return result


class DictValidator:
    """Dictionary field validator."""

    @staticmethod
    def validate(value: Any, field: FieldValidator) -> ValidationResult:
        """Validate dict value."""
        result = ValidationResult()

        if value is None:
            if field.required:
                result.add_error(f"{field.name} is required", field.name)
            return result

        if not isinstance(value, dict):
            result.add_error(f"{field.name} must be a dictionary", field.name)

        return result


class SchemaValidator:
    """Schema-based validator."""

    def __init__(self, schema: dict[str, FieldValidator]):
        """Initialize schema validator.

        Args:
            schema: Dictionary of field name -> FieldValidator
        """
        self.schema = schema

    def validate(self, data: dict[str, Any]) -> ValidationResult:
        """Validate data against schema.

        Args:
            data: Data to validate

        Returns:
            ValidationResult
        """
        result = ValidationResult()

        for field_name, validator in self.schema.items():
            value = data.get(field_name)

            if isinstance(value, str):
                field_result = StringValidator.validate(value, validator)
            elif isinstance(value, (int, float)):
                field_result = NumberValidator.validate(value, validator)
            elif isinstance(value, list):
                field_result = ListValidator.validate(value, validator)
            elif isinstance(value, dict):
                field_result = DictValidator.validate(value, validator)
            else:
                field_result = ValidationResult()
                if value is None and not validator.required:
                    pass
                else:
                    field_result.add_error(
                        f"{field_name} has unsupported type: {type(value).__name__}",
                        field_name
                    )

            result.merge(field_result)

        return result


class EmailValidator:
    """Email address validator."""

    EMAIL_PATTERN = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

    @staticmethod
    def validate(value: str) -> bool:
        """Validate email format.

        Args:
            value: Email address

        Returns:
            True if valid
        """
        if not value:
            return False
        return re.match(EmailValidator.EMAIL_PATTERN, value) is not None


class URLValidator:
    """URL validator."""

    URL_PATTERN = r"^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$"

    @staticmethod
    def validate(value: str) -> bool:
        """Validate URL format.

        Args:
            value: URL string

        Returns:
            True if valid
        """
        if not value:
            return False
        return re.match(URLValidator.URL_PATTERN, value) is not None


class IPAddressValidator:
    """IP address validator."""

    IPV4_PATTERN = r"^(\d{1,3}\.){3}\d{1,3}$"
    IPV6_PATTERN = r"^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$"

    @staticmethod
    def validate_ipv4(value: str) -> bool:
        """Validate IPv4 address."""
        if not re.match(IPAddressValidator.IPV4_PATTERN, value):
            return False
        parts = value.split(".")
        return all(0 <= int(part) <= 255 for part in parts)

    @staticmethod
    def validate_ipv6(value: str) -> bool:
        """Validate IPv6 address."""
        return re.match(IPAddressValidator.IPV6_PATTERN, value) is not None

    @staticmethod
    def validate(value: str) -> bool:
        """Validate IP address (v4 or v6)."""
        return IPAddressValidator.validate_ipv4(value) or IPAddressValidator.validate_ipv6(value)


def create_string_field(
    name: str,
    required: bool = False,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    pattern: Optional[str] = None,
) -> FieldValidator:
    """Create string field validator.

    Args:
        name: Field name
        required: Is required
        min_length: Minimum length
        max_length: Maximum length
        pattern: Regex pattern

    Returns:
        FieldValidator
    """
    return FieldValidator(
        name=name,
        required=required,
        min_length=min_length,
        max_length=max_length,
        pattern=pattern,
    )


def create_email_field(name: str, required: bool = False) -> FieldValidator:
    """Create email field validator.

    Args:
        name: Field name
        required: Is required

    Returns:
        FieldValidator
    """
    return FieldValidator(
        name=name,
        required=required,
        pattern=EmailValidator.EMAIL_PATTERN,
        error_message=f"{name} must be a valid email address",
    )

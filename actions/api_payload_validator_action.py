"""API payload validation and sanitization action."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional, Sequence


class ValidationType(str, Enum):
    """Type of validation."""

    REQUIRED = "required"
    TYPE = "type"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    MIN_VALUE = "min_value"
    MAX_VALUE = "max_value"
    PATTERN = "pattern"
    ENUM = "enum"
    EMAIL = "email"
    URL = "url"
    IP_ADDRESS = "ip_address"
    UUID = "uuid"
    CUSTOM = "custom"


@dataclass
class ValidationRule:
    """A single validation rule."""

    field_name: str
    validation_type: ValidationType
    value: Any = None
    message: str = ""
    custom_validator: Optional[Callable[[Any], bool]] = None


@dataclass
class ValidationError:
    """A validation error."""

    field: str
    message: str
    validation_type: ValidationType
    value: Any = None


@dataclass
class ValidationResult:
    """Result of validation."""

    is_valid: bool
    errors: list[ValidationError]
    validated_at: datetime = field(default_factory=datetime.now)


class APIPayloadValidatorAction:
    """Validates and sanitizes API payloads."""

    def __init__(self):
        """Initialize validator."""
        self._rules: list[ValidationRule] = []
        self._sanitizers: dict[str, Callable[[Any], Any]] = {}

    def add_rule(self, rule: ValidationRule) -> None:
        """Add a validation rule."""
        self._rules.append(rule)

    def add_rules(self, rules: list[ValidationRule]) -> None:
        """Add multiple validation rules."""
        self._rules.extend(rules)

    def set_sanitizer(
        self,
        field_name: str,
        sanitizer: Callable[[Any], Any],
    ) -> None:
        """Set a sanitizer for a field."""
        self._sanitizers[field_name] = sanitizer

    def _validate_required(
        self,
        value: Any,
        rule: ValidationRule,
    ) -> Optional[ValidationError]:
        """Validate required field."""
        if value is None or value == "":
            return ValidationError(
                field=rule.field_name,
                message=rule.message or f"{rule.field_name} is required",
                validation_type=ValidationType.REQUIRED,
                value=value,
            )
        return None

    def _validate_type(
        self,
        value: Any,
        rule: ValidationRule,
    ) -> Optional[ValidationError]:
        """Validate type."""
        expected_type = rule.value
        if value is not None and not isinstance(value, expected_type):
            return ValidationError(
                field=rule.field_name,
                message=rule.message or f"{rule.field_name} must be of type {expected_type.__name__}",
                validation_type=ValidationType.TYPE,
                value=value,
            )
        return None

    def _validate_min_length(
        self,
        value: Any,
        rule: ValidationRule,
    ) -> Optional[ValidationError]:
        """Validate minimum length."""
        if value is not None and len(value) < rule.value:
            return ValidationError(
                field=rule.field_name,
                message=rule.message or f"{rule.field_name} must be at least {rule.value} characters",
                validation_type=ValidationType.MIN_LENGTH,
                value=value,
            )
        return None

    def _validate_max_length(
        self,
        value: Any,
        rule: ValidationRule,
    ) -> Optional[ValidationError]:
        """Validate maximum length."""
        if value is not None and len(value) > rule.value:
            return ValidationError(
                field=rule.field_name,
                message=rule.message or f"{rule.field_name} must be at most {rule.value} characters",
                validation_type=ValidationType.MAX_LENGTH,
                value=value,
            )
        return None

    def _validate_min_value(
        self,
        value: Any,
        rule: ValidationRule,
    ) -> Optional[ValidationError]:
        """Validate minimum value."""
        if value is not None and value < rule.value:
            return ValidationError(
                field=rule.field_name,
                message=rule.message or f"{rule.field_name} must be at least {rule.value}",
                validation_type=ValidationType.MIN_VALUE,
                value=value,
            )
        return None

    def _validate_max_value(
        self,
        value: Any,
        rule: ValidationRule,
    ) -> Optional[ValidationError]:
        """Validate maximum value."""
        if value is not None and value > rule.value:
            return ValidationError(
                field=rule.field_name,
                message=rule.message or f"{rule.field_name} must be at most {rule.value}",
                validation_type=ValidationType.MAX_VALUE,
                value=value,
            )
        return None

    def _validate_pattern(
        self,
        value: Any,
        rule: ValidationRule,
    ) -> Optional[ValidationError]:
        """Validate against regex pattern."""
        if value is not None:
            pattern = rule.value
            if not re.match(pattern, str(value)):
                return ValidationError(
                    field=rule.field_name,
                    message=rule.message or f"{rule.field_name} does not match required pattern",
                    validation_type=ValidationType.PATTERN,
                    value=value,
                )
        return None

    def _validate_enum(
        self,
        value: Any,
        rule: ValidationRule,
    ) -> Optional[ValidationError]:
        """Validate against enum values."""
        if value is not None and value not in rule.value:
            return ValidationError(
                field=rule.field_name,
                message=rule.message or f"{rule.field_name} must be one of {rule.value}",
                validation_type=ValidationType.ENUM,
                value=value,
            )
        return None

    def _validate_email(self, value: Any, rule: ValidationRule) -> Optional[ValidationError]:
        """Validate email format."""
        if value is not None:
            pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            if not re.match(pattern, str(value)):
                return ValidationError(
                    field=rule.field_name,
                    message=rule.message or f"{rule.field_name} is not a valid email",
                    validation_type=ValidationType.EMAIL,
                    value=value,
                )
        return None

    def _validate_url(self, value: Any, rule: ValidationRule) -> Optional[ValidationError]:
        """Validate URL format."""
        if value is not None:
            pattern = r"^https?://[^\s/$.?#].[^\s]*$"
            if not re.match(pattern, str(value)):
                return ValidationError(
                    field=rule.field_name,
                    message=rule.message or f"{rule.field_name} is not a valid URL",
                    validation_type=ValidationType.URL,
                    value=value,
                )
        return None

    def _validate_uuid(self, value: Any, rule: ValidationRule) -> Optional[ValidationError]:
        """Validate UUID format."""
        if value is not None:
            pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
            if not re.match(pattern, str(value).lower()):
                return ValidationError(
                    field=rule.field_name,
                    message=rule.message or f"{rule.field_name} is not a valid UUID",
                    validation_type=ValidationType.UUID,
                    value=value,
                )
        return None

    def _validate_custom(
        self,
        value: Any,
        rule: ValidationRule,
    ) -> Optional[ValidationError]:
        """Validate using custom validator."""
        if rule.custom_validator and value is not None:
            try:
                if not rule.custom_validator(value):
                    return ValidationError(
                        field=rule.field_name,
                        message=rule.message or f"{rule.field_name} failed custom validation",
                        validation_type=ValidationType.CUSTOM,
                        value=value,
                    )
            except Exception as e:
                return ValidationError(
                    field=rule.field_name,
                    message=str(e),
                    validation_type=ValidationType.CUSTOM,
                    value=value,
                )
        return None

    def validate(self, payload: dict[str, Any]) -> ValidationResult:
        """Validate a payload against all rules.

        Args:
            payload: Payload to validate.

        Returns:
            ValidationResult with any errors.
        """
        errors = []

        for rule in self._rules:
            value = payload.get(rule.field_name)

            error: Optional[ValidationError] = None

            if rule.validation_type == ValidationType.REQUIRED:
                error = self._validate_required(value, rule)
            elif rule.validation_type == ValidationType.TYPE:
                error = self._validate_type(value, rule)
            elif rule.validation_type == ValidationType.MIN_LENGTH:
                error = self._validate_min_length(value, rule)
            elif rule.validation_type == ValidationType.MAX_LENGTH:
                error = self._validate_max_length(value, rule)
            elif rule.validation_type == ValidationType.MIN_VALUE:
                error = self._validate_min_value(value, rule)
            elif rule.validation_type == ValidationType.MAX_VALUE:
                error = self._validate_max_value(value, rule)
            elif rule.validation_type == ValidationType.PATTERN:
                error = self._validate_pattern(value, rule)
            elif rule.validation_type == ValidationType.ENUM:
                error = self._validate_enum(value, rule)
            elif rule.validation_type == ValidationType.EMAIL:
                error = self._validate_email(value, rule)
            elif rule.validation_type == ValidationType.URL:
                error = self._validate_url(value, rule)
            elif rule.validation_type == ValidationType.UUID:
                error = self._validate_uuid(value, rule)
            elif rule.validation_type == ValidationType.CUSTOM:
                error = self._validate_custom(value, rule)

            if error:
                errors.append(error)

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
        )

    def sanitize(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Sanitize a payload using configured sanitizers."""
        result = payload.copy()

        for field_name, sanitizer in self._sanitizers.items():
            if field_name in result:
                try:
                    result[field_name] = sanitizer(result[field_name])
                except Exception:
                    pass

        return result

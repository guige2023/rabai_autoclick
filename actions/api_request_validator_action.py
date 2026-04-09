"""
API Request Validator Action Module.

Request validation with field-level validation rules,
cross-field constraints, and custom validators.
"""

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ValidationType(Enum):
    """Validation types."""
    REQUIRED = "required"
    TYPE = "type"
    MIN = "min"
    MAX = "max"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    PATTERN = "pattern"
    ENUM = "enum"
    EMAIL = "email"
    URL = "url"
    CUSTOM = "custom"


@dataclass
class ValidationRule:
    """
    Single validation rule.

    Attributes:
        field: Field name to validate.
        rule_type: Type of validation.
        value: Validation value (threshold, pattern, etc.).
        message: Error message on validation failure.
        validator_func: Custom validation function.
    """
    field: str
    rule_type: ValidationType
    value: Any = None
    message: str = ""
    validator_func: Optional[Callable] = None


@dataclass
class ValidationError:
    """Validation error."""
    field: str
    rule_type: ValidationType
    message: str
    value: Any = None


@dataclass
class ValidationResult:
    """Validation result."""
    valid: bool
    errors: list[ValidationError]
    warnings: list[str]


class APIRequestValidatorAction:
    """
    Request validation with comprehensive rules.

    Example:
        validator = APIRequestValidatorAction()
        validator.required("email")
        validator.email("email")
        validator.range("age", min=0, max=150)
        result = validator.validate(request_data)
    """

    def __init__(self):
        """Initialize request validator."""
        self._rules: list[ValidationRule] = []
        self._cross_field_rules: list[Callable] = []

    def required(self, field: str, message: str = "") -> "APIRequestValidatorAction":
        """Add required field rule."""
        self._rules.append(ValidationRule(
            field=field,
            rule_type=ValidationType.REQUIRED,
            message=message or f"Field '{field}' is required"
        ))
        return self

    def type_check(self, field: str, expected_type: type, message: str = "") -> "APIRequestValidatorAction":
        """Add type validation rule."""
        self._rules.append(ValidationRule(
            field=field,
            rule_type=ValidationType.TYPE,
            value=expected_type,
            message=message or f"Field '{field}' must be of type {expected_type.__name__}"
        ))
        return self

    def range(
        self,
        field: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
        message: str = ""
    ) -> "APIRequestValidatorAction":
        """Add numeric range validation."""
        if min_val is not None:
            self._rules.append(ValidationRule(
                field=field,
                rule_type=ValidationType.MIN,
                value=min_val,
                message=message or f"Field '{field}' must be >= {min_val}"
            ))
        if max_val is not None:
            self._rules.append(ValidationRule(
                field=field,
                rule_type=ValidationType.MAX,
                value=max_val,
                message=message or f"Field '{field}' must be <= {max_val}"
            ))
        return self

    def length(
        self,
        field: str,
        min_len: Optional[int] = None,
        max_len: Optional[int] = None,
        message: str = ""
    ) -> "APIRequestValidatorAction":
        """Add string length validation."""
        if min_len is not None:
            self._rules.append(ValidationRule(
                field=field,
                rule_type=ValidationType.MIN_LENGTH,
                value=min_len,
                message=message or f"Field '{field}' must be at least {min_len} characters"
            ))
        if max_len is not None:
            self._rules.append(ValidationRule(
                field=field,
                rule_type=ValidationType.MAX_LENGTH,
                value=max_len,
                message=message or f"Field '{field}' must be at most {max_len} characters"
            ))
        return self

    def pattern(self, field: str, regex: str, message: str = "") -> "APIRequestValidatorAction":
        """Add regex pattern validation."""
        self._rules.append(ValidationRule(
            field=field,
            rule_type=ValidationType.PATTERN,
            value=regex,
            message=message or f"Field '{field}' does not match pattern"
        ))
        return self

    def email(self, field: str, message: str = "") -> "APIRequestValidatorAction":
        """Add email validation."""
        self._rules.append(ValidationRule(
            field=field,
            rule_type=ValidationType.EMAIL,
            message=message or f"Field '{field}' is not a valid email"
        ))
        return self

    def url(self, field: str, message: str = "") -> "APIRequestValidatorAction":
        """Add URL validation."""
        self._rules.append(ValidationRule(
            field=field,
            rule_type=ValidationType.URL,
            message=message or f"Field '{field}' is not a valid URL"
        ))
        return self

    def enum_val(self, field: str, allowed: list, message: str = "") -> "APIRequestValidatorAction":
        """Add enum/allowed values validation."""
        self._rules.append(ValidationRule(
            field=field,
            rule_type=ValidationType.ENUM,
            value=allowed,
            message=message or f"Field '{field}' must be one of {allowed}"
        ))
        return self

    def custom(
        self,
        field: str,
        validator: Callable[[Any], bool],
        message: str = ""
    ) -> "APIRequestValidatorAction":
        """Add custom validation function."""
        self._rules.append(ValidationRule(
            field=field,
            rule_type=ValidationType.CUSTOM,
            validator_func=validator,
            message=message or f"Field '{field}' failed custom validation"
        ))
        return self

    def add_rule(self, rule: ValidationRule) -> "APIRequestValidatorAction":
        """Add a pre-built ValidationRule."""
        self._rules.append(rule)
        return self

    def add_cross_field(
        self,
        validator: Callable[[dict], Optional[str]]
    ) -> "APIRequestValidatorAction":
        """
        Add cross-field validation.

        Args:
            validator: Function that takes data dict and returns error message or None.
        """
        self._cross_field_rules.append(validator)
        return self

    def validate(self, data: dict) -> ValidationResult:
        """
        Validate data against all rules.

        Args:
            data: Data to validate.

        Returns:
            ValidationResult with errors.
        """
        errors = []
        warnings = []

        for rule in self._rules:
            value = data.get(rule.field)

            error = self._validate_rule(rule, value)
            if error:
                errors.append(error)

        for cross_validator in self._cross_field_rules:
            try:
                error_msg = cross_validator(data)
                if error_msg:
                    warnings.append(error_msg)
            except Exception as e:
                logger.error(f"Cross-field validation error: {e}")

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors,
            warnings=warnings
        )

    def _validate_rule(self, rule: ValidationRule, value: Any) -> Optional[ValidationError]:
        """Validate single rule against value."""
        rule_type = rule.rule_type

        if rule_type == ValidationType.REQUIRED:
            if value is None or value == "":
                return ValidationError(
                    field=rule.field,
                    rule_type=rule_type,
                    message=rule.message,
                    value=value
                )

        if value is None:
            return None

        if rule_type == ValidationType.TYPE:
            if not isinstance(value, rule.value):
                return ValidationError(
                    field=rule.field,
                    rule_type=rule_type,
                    message=rule.message,
                    value=value
                )

        elif rule_type == ValidationType.MIN:
            try:
                if float(value) < rule.value:
                    return ValidationError(
                        field=rule.field,
                        rule_type=rule_type,
                        message=rule.message,
                        value=value
                    )
            except (ValueError, TypeError):
                return ValidationError(
                    field=rule.field,
                    rule_type=rule_type,
                    message=f"Field '{rule.field}' must be numeric",
                    value=value
                )

        elif rule_type == ValidationType.MAX:
            try:
                if float(value) > rule.value:
                    return ValidationError(
                        field=rule.field,
                        rule_type=rule_type,
                        message=rule.message,
                        value=value
                    )
            except (ValueError, TypeError):
                return ValidationError(
                    field=rule.field,
                    rule_type=rule_type,
                    message=f"Field '{rule.field}' must be numeric",
                    value=value
                )

        elif rule_type == ValidationType.MIN_LENGTH:
            if len(str(value)) < rule.value:
                return ValidationError(
                    field=rule.field,
                    rule_type=rule_type,
                    message=rule.message,
                    value=value
                )

        elif rule_type == ValidationType.MAX_LENGTH:
            if len(str(value)) > rule.value:
                return ValidationError(
                    field=rule.field,
                    rule_type=rule_type,
                    message=rule.message,
                    value=value
                )

        elif rule_type == ValidationType.PATTERN:
            if not re.match(rule.value, str(value)):
                return ValidationError(
                    field=rule.field,
                    rule_type=rule_type,
                    message=rule.message,
                    value=value
                )

        elif rule_type == ValidationType.EMAIL:
            email_pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
            if not re.match(email_pattern, str(value)):
                return ValidationError(
                    field=rule.field,
                    rule_type=rule_type,
                    message=rule.message,
                    value=value
                )

        elif rule_type == ValidationType.URL:
            url_pattern = r"^https?://[\w\.-]+\.\w+"
            if not re.match(url_pattern, str(value)):
                return ValidationError(
                    field=rule.field,
                    rule_type=rule_type,
                    message=rule.message,
                    value=value
                )

        elif rule_type == ValidationType.ENUM:
            if value not in rule.value:
                return ValidationError(
                    field=rule.field,
                    rule_type=rule_type,
                    message=rule.message,
                    value=value
                )

        elif rule_type == ValidationType.CUSTOM:
            if rule.validator_func and not rule.validator_func(value):
                return ValidationError(
                    field=rule.field,
                    rule_type=rule_type,
                    message=rule.message,
                    value=value
                )

        return None

    def clear(self) -> None:
        """Clear all rules."""
        self._rules.clear()
        self._cross_field_rules.clear()

    def get_rules(self) -> list[dict]:
        """Get all validation rules."""
        return [
            {
                "field": r.field,
                "type": r.rule_type.value,
                "value": r.value,
                "message": r.message
            }
            for r in self._rules
        ]

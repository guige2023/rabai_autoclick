"""Data Validator Action Module.

Provides schema validation, type checking, range validation,
and custom validation rules for data records.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Pattern, Set, Union
import logging

logger = logging.getLogger(__name__)


class ValidationType(Enum):
    """Validation type."""
    REQUIRED = "required"
    TYPE = "type"
    RANGE = "range"
    PATTERN = "pattern"
    ENUM = "enum"
    CUSTOM = "custom"
    LENGTH = "length"


@dataclass
class ValidationRule:
    """Single validation rule."""
    field: str
    validation_type: ValidationType
    rule: Any = None
    message: str = ""
    validator: Optional[Callable[[Any], bool]] = None


@dataclass
class ValidationError:
    """Validation error."""
    field: str
    message: str
    value: Any = None


@dataclass
class ValidationResult:
    """Validation result."""
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)


class DataValidatorAction:
    """Data validator with multiple validation types.

    Example:
        validator = DataValidatorAction()

        validator.add_rule(ValidationRule(
            field="email",
            validation_type=ValidationType.PATTERN,
            rule=r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$",
            message="Invalid email format"
        ))

        validator.add_rule(ValidationRule(
            field="age",
            validation_type=ValidationType.RANGE,
            rule={"min": 0, "max": 150},
            message="Age must be between 0 and 150"
        ))

        result = validator.validate({
            "email": "user@example.com",
            "age": 25
        })
    """

    def __init__(self) -> None:
        self._rules: List[ValidationRule] = []
        self._compiled_patterns: Dict[str, Pattern] = {}

    def add_rule(self, rule: ValidationRule) -> "DataValidatorAction":
        """Add validation rule.

        Returns self for chaining.
        """
        self._rules.append(rule)
        return self

    def add_required(self, field: str) -> "DataValidatorAction":
        """Add required field rule."""
        self.add_rule(ValidationRule(
            field=field,
            validation_type=ValidationType.REQUIRED,
            message=f"{field} is required"
        ))
        return self

    def add_type_check(
        self,
        field: str,
        expected_type: type,
    ) -> "DataValidatorAction":
        """Add type check rule."""
        self.add_rule(ValidationRule(
            field=field,
            validation_type=ValidationType.TYPE,
            rule=expected_type,
            message=f"{field} must be of type {expected_type.__name__}"
        ))
        return self

    def add_range(
        self,
        field: str,
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> "DataValidatorAction":
        """Add range validation rule."""
        self.add_rule(ValidationRule(
            field=field,
            validation_type=ValidationType.RANGE,
            rule={"min": min_val, "max": max_val},
            message=f"{field} must be between {min_val} and {max_val}"
        ))
        return self

    def add_pattern(
        self,
        field: str,
        pattern: str,
        message: Optional[str] = None,
    ) -> "DataValidatorAction":
        """Add pattern match rule."""
        self.add_rule(ValidationRule(
            field=field,
            validation_type=ValidationType.PATTERN,
            rule=pattern,
            message=message or f"{field} does not match required pattern"
        ))
        return self

    def add_enum(
        self,
        field: str,
        allowed_values: List[Any],
    ) -> "DataValidatorAction":
        """Add enum validation rule."""
        self.add_rule(ValidationRule(
            field=field,
            validation_type=ValidationType.ENUM,
            rule=allowed_values,
            message=f"{field} must be one of {allowed_values}"
        ))
        return self

    def add_length(
        self,
        field: str,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None,
    ) -> "DataValidatorAction":
        """Add length validation rule."""
        self.add_rule(ValidationRule(
            field=field,
            validation_type=ValidationType.LENGTH,
            rule={"min": min_length, "max": max_length},
            message=f"{field} length must be between {min_length} and {max_length}"
        ))
        return self

    def add_custom(
        self,
        field: str,
        validator: Callable[[Any], bool],
        message: str,
    ) -> "DataValidatorAction":
        """Add custom validation rule."""
        self.add_rule(ValidationRule(
            field=field,
            validation_type=ValidationType.CUSTOM,
            validator=validator,
            message=message
        ))
        return self

    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        """Validate data against all rules.

        Args:
            data: Data record to validate

        Returns:
            ValidationResult with errors if any
        """
        errors: List[ValidationError] = []

        for rule in self._rules:
            error = self._validate_rule(data, rule)
            if error:
                errors.append(error)

        return ValidationResult(
            valid=len(errors) == 0,
            errors=errors
        )

    def validate_batch(
        self,
        data_list: List[Dict[str, Any]],
    ) -> List[ValidationResult]:
        """Validate batch of records.

        Returns:
            List of ValidationResults
        """
        return [self.validate(data) for data in data_list]

    def _validate_rule(
        self,
        data: Dict[str, Any],
        rule: ValidationRule,
    ) -> Optional[ValidationError]:
        """Validate single rule."""
        value = data.get(rule.field)

        if rule.validation_type == ValidationType.REQUIRED:
            if value is None or (isinstance(value, str) and not value.strip()):
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} is required",
                    value=value
                )

        elif rule.validation_type == ValidationType.TYPE:
            if value is not None and not isinstance(value, rule.rule):
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} must be {rule.rule.__name__}",
                    value=value
                )

        elif rule.validation_type == ValidationType.RANGE:
            if value is not None:
                range_spec = rule.rule
                min_val = range_spec.get("min")
                max_val = range_spec.get("max")

                if min_val is not None and value < min_val:
                    return ValidationError(
                        field=rule.field,
                        message=rule.message or f"{rule.field} must be >= {min_val}",
                        value=value
                    )

                if max_val is not None and value > max_val:
                    return ValidationError(
                        field=rule.field,
                        message=rule.message or f"{rule.field} must be <= {max_val}",
                        value=value
                    )

        elif rule.validation_type == ValidationType.PATTERN:
            if value is not None:
                pattern = rule.rule
                if pattern not in self._compiled_patterns:
                    self._compiled_patterns[pattern] = re.compile(pattern)

                if not self._compiled_patterns[pattern].match(str(value)):
                    return ValidationError(
                        field=rule.field,
                        message=rule.message,
                        value=value
                    )

        elif rule.validation_type == ValidationType.ENUM:
            if value is not None and value not in rule.rule:
                return ValidationError(
                    field=rule.field,
                    message=rule.message,
                    value=value
                )

        elif rule.validation_type == ValidationType.LENGTH:
            if value is not None:
                length_spec = rule.rule
                length = len(value)
                min_len = length_spec.get("min")
                max_len = length_spec.get("max")

                if min_len is not None and length < min_len:
                    return ValidationError(
                        field=rule.field,
                        message=rule.message or f"{rule.field} length must be >= {min_len}",
                        value=value
                    )

                if max_len is not None and length > max_len:
                    return ValidationError(
                        field=rule.field,
                        message=rule.message or f"{rule.field} length must be <= {max_len}",
                        value=value
                    )

        elif rule.validation_type == ValidationType.CUSTOM:
            if value is not None and rule.validator:
                try:
                    if not rule.validator(value):
                        return ValidationError(
                            field=rule.field,
                            message=rule.message,
                            value=value
                        )
                except Exception as e:
                    logger.error(f"Custom validator error for {rule.field}: {e}")
                    return ValidationError(
                        field=rule.field,
                        message=f"Validation error: {str(e)}",
                        value=value
                    )

        return None

    def clear_rules(self) -> None:
        """Clear all validation rules."""
        self._rules.clear()
        self._compiled_patterns.clear()

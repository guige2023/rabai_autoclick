"""Data Validator Action Module.

Provides data validation with support for schemas, type checking,
range validation, and custom validation rules.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Type, Union

logger = logging.getLogger(__name__)


class ValidationType(Enum):
    TYPE = "type"
    REQUIRED = "required"
    MIN = "min"
    MAX = "max"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    PATTERN = "pattern"
    ENUM = "enum"
    CUSTOM = "custom"


@dataclass
class ValidationError:
    field: str
    validation_type: ValidationType
    message: str
    value: Any = None
    expected: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    validated_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def add_error(self, error: ValidationError) -> None:
        self.valid = False
        self.errors.append(error)

    def add_warning(self, warning: str) -> None:
        self.warnings.append(warning)


@dataclass
class FieldValidator:
    name: str
    validation_type: ValidationType
    constraint: Any = None
    message: Optional[str] = None
    validator_fn: Optional[Callable[[Any], bool]] = None
    required: bool = False


class SchemaValidator:
    def __init__(self):
        self._field_validators: Dict[str, List[FieldValidator]] = {}

    def add_field(self, name: str, validator: FieldValidator) -> None:
        if name not in self._field_validators:
            self._field_validators[name] = []
        self._field_validators[name].append(validator)

    def add_required_field(self, name: str, field_type: Type) -> None:
        self.add_field(name, FieldValidator(
            name=name,
            validation_type=ValidationType.TYPE,
            constraint=field_type,
            required=True,
        ))

    def add_optional_field(self, name: str, field_type: Type) -> None:
        self.add_field(name, FieldValidator(
            name=name,
            validation_type=ValidationType.TYPE,
            constraint=field_type,
            required=False,
        ))

    def add_range_constraint(self, name: str, min_val: Any = None, max_val: Any = None) -> None:
        if min_val is not None:
            self.add_field(name, FieldValidator(
                name=name,
                validation_type=ValidationType.MIN,
                constraint=min_val,
            ))
        if max_val is not None:
            self.add_field(name, FieldValidator(
                name=name,
                validation_type=ValidationType.MAX,
                constraint=max_val,
            ))

    def add_pattern_constraint(self, name: str, pattern: str) -> None:
        self.add_field(name, FieldValidator(
            name=name,
            validation_type=ValidationType.PATTERN,
            constraint=re.compile(pattern),
        ))

    def add_enum_constraint(self, name: str, allowed_values: List[Any]) -> None:
        self.add_field(name, FieldValidator(
            name=name,
            validation_type=ValidationType.ENUM,
            constraint=set(allowed_values),
        ))

    def add_custom_validator(
        self,
        name: str,
        validator_fn: Callable[[Any], bool],
        message: str = "Custom validation failed",
    ) -> None:
        self.add_field(name, FieldValidator(
            name=name,
            validation_type=ValidationType.CUSTOM,
            validator_fn=validator_fn,
            message=message,
        ))

    def validate(self, data: Dict[str, Any]) -> ValidationResult:
        result = ValidationResult(valid=True)

        for field_name, validators in self._field_validators.items():
            value = data.get(field_name)
            field_valid = True

            for validator in validators:
                if validator.validation_type == ValidationType.REQUIRED:
                    if value is None or value == "":
                        result.add_error(ValidationError(
                            field=field_name,
                            validation_type=ValidationType.REQUIRED,
                            message=validator.message or f"Field '{field_name}' is required",
                            value=value,
                        ))
                        field_valid = False
                        break

                elif value is None:
                    continue

                elif validator.validation_type == ValidationType.TYPE:
                    if not isinstance(value, validator.constraint):
                        result.add_error(ValidationError(
                            field=field_name,
                            validation_type=ValidationType.TYPE,
                            message=validator.message or f"Field '{field_name}' must be of type {validator.constraint.__name__}",
                            value=type(value).__name__,
                            expected=validator.constraint.__name__,
                        ))
                        field_valid = False

                elif validator.validation_type == ValidationType.MIN:
                    if value < validator.constraint:
                        result.add_error(ValidationError(
                            field=field_name,
                            validation_type=ValidationType.MIN,
                            message=validator.message or f"Field '{field_name}' must be >= {validator.constraint}",
                            value=value,
                            expected=f">= {validator.constraint}",
                        ))
                        field_valid = False

                elif validator.validation_type == ValidationType.MAX:
                    if value > validator.constraint:
                        result.add_error(ValidationError(
                            field=field_name,
                            validation_type=ValidationType.MAX,
                            message=validator.message or f"Field '{field_name}' must be <= {validator.constraint}",
                            value=value,
                            expected=f"<= {validator.constraint}",
                        ))
                        field_valid = False

                elif validator.validation_type == ValidationType.MIN_LENGTH:
                    if len(value) < validator.constraint:
                        result.add_error(ValidationError(
                            field=field_name,
                            validation_type=ValidationType.MIN_LENGTH,
                            message=validator.message or f"Field '{field_name}' length must be >= {validator.constraint}",
                            value=len(value),
                            expected=f">= {validator.constraint}",
                        ))
                        field_valid = False

                elif validator.validation_type == ValidationType.MAX_LENGTH:
                    if len(value) > validator.constraint:
                        result.add_error(ValidationError(
                            field=field_name,
                            validation_type=ValidationType.MAX_LENGTH,
                            message=validator.message or f"Field '{field_name}' length must be <= {validator.constraint}",
                            value=len(value),
                            expected=f"<= {validator.constraint}",
                        ))
                        field_valid = False

                elif validator.validation_type == ValidationType.PATTERN:
                    if not validator.constraint.match(str(value)):
                        result.add_error(ValidationError(
                            field=field_name,
                            validation_type=ValidationType.PATTERN,
                            message=validator.message or f"Field '{field_name}' does not match pattern",
                            value=value,
                            expected=str(validator.constraint.pattern),
                        ))
                        field_valid = False

                elif validator.validation_type == ValidationType.ENUM:
                    if value not in validator.constraint:
                        result.add_error(ValidationError(
                            field=field_name,
                            validation_type=ValidationType.ENUM,
                            message=validator.message or f"Field '{field_name}' must be one of {validator.constraint}",
                            value=value,
                            expected=list(validator.constraint),
                        ))
                        field_valid = False

                elif validator.validation_type == ValidationType.CUSTOM:
                    try:
                        if not validator.validator_fn(value):
                            result.add_error(ValidationError(
                                field=field_name,
                                validation_type=ValidationType.CUSTOM,
                                message=validator.message or "Custom validation failed",
                                value=value,
                            ))
                            field_valid = False
                    except Exception as e:
                        result.add_error(ValidationError(
                            field=field_name,
                            validation_type=ValidationType.CUSTOM,
                            message=f"Custom validator error: {e}",
                            value=value,
                        ))
                        field_valid = False

        return result


def validate_email(email: str) -> bool:
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))


def validate_url(url: str) -> bool:
    pattern = r'^https?://[^\s/$.?#].[^\s]*$'
    return bool(re.match(pattern, url))


def validate_phone(phone: str) -> bool:
    pattern = r'^\+?1?\d{9,15}$'
    return bool(re.match(pattern, phone.replace(r'[\s\-\(\)]', '')))

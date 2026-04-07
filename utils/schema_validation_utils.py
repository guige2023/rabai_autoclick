"""Schema validation utilities for Python data structures."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Callable

__all__ = [
    "ValidationError",
    "Validator",
    "Schema",
    "FieldValidator",
    "validate_dict",
    "validate_instance",
]


class ValidationError(Exception):
    """Raised when validation fails."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("; ".join(errors))


@dataclass
class FieldValidator:
    """A single field validation rule."""
    name: str
    validate: Callable[[Any], bool]
    message: str = "invalid"

    def check(self, value: Any) -> tuple[bool, str]:
        try:
            ok = self.validate(value)
            return ok, self.message if not ok else ""
        except Exception as e:
            return False, str(e)


class Schema:
    """A validation schema for dictionaries."""

    def __init__(self, **field_validators: FieldValidator) -> None:
        self._fields = field_validators

    def validate(self, data: dict[str, Any]) -> tuple[bool, list[str]]:
        errors: list[str] = []
        for name, validator in self._fields.items():
            ok, msg = validator.check(data.get(name))
            if not ok:
                errors.append(f"{name}: {msg}")
        return len(errors) == 0, errors

    def __call__(self, data: dict[str, Any]) -> dict[str, Any]:
        ok, errors = self.validate(data)
        if not ok:
            raise ValidationError(errors)
        return data


class Validator:
    """Collection of common validation rules."""

    @staticmethod
    def required() -> FieldValidator:
        return FieldValidator(
            name="required",
            validate=lambda v: v is not None,
            message="field is required",
        )

    @staticmethod
    def type_of(expected: type | tuple[type, ...]) -> FieldValidator:
        return FieldValidator(
            name=f"type_{expected.__name__}",
            validate=lambda v: isinstance(v, expected),
            message=f"expected {expected.__name__}",
        )

    @staticmethod
    def min_value(min_val: int | float) -> FieldValidator:
        return FieldValidator(
            name=f"min_{min_val}",
            validate=lambda v: v >= min_val,
            message=f"must be >= {min_val}",
        )

    @staticmethod
    def max_value(max_val: int | float) -> FieldValidator:
        return FieldValidator(
            name=f"max_{max_val}",
            validate=lambda v: v <= max_val,
            message=f"must be <= {max_val}",
        )

    @staticmethod
    def min_length(min_len: int) -> FieldValidator:
        return FieldValidator(
            name=f"minLen_{min_len}",
            validate=lambda v: len(v) >= min_len,
            message=f"length must be >= {min_len}",
        )

    @staticmethod
    def max_length(max_len: int) -> FieldValidator:
        return FieldValidator(
            name=f"maxLen_{max_len}",
            validate=lambda v: len(v) <= max_len,
            message=f"length must be <= {max_len}",
        )

    @staticmethod
    def pattern(regex: str, message: str = "pattern mismatch") -> FieldValidator:
        compiled = re.compile(regex)
        return FieldValidator(
            name=f"pattern_{regex}",
            validate=lambda v: bool(compiled.match(str(v))),
            message=message,
        )

    @staticmethod
    def one_of(choices: list[Any]) -> FieldValidator:
        return FieldValidator(
            name=f"oneOf_{choices}",
            validate=lambda v: v in choices,
            message=f"must be one of {choices}",
        )

    @staticmethod
    def email() -> FieldValidator:
        return FieldValidator(
            name="email",
            validate=lambda v: bool(re.match(r"^[^@]+@[^@]+\.[^@]+$", str(v))),
            message="invalid email address",
        )

    @staticmethod
    def url() -> FieldValidator:
        return FieldValidator(
            name="url",
            validate=lambda v: bool(re.match(r"^https?://", str(v))),
            message="invalid URL",
        )

    @staticmethod
    def uuid() -> FieldValidator:
        return FieldValidator(
            name="uuid",
            validate=lambda v: bool(re.match(
                r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                str(v).lower()
            )),
            message="invalid UUID",
        )

    @staticmethod
    def custom(
        fn: Callable[[Any], bool],
        message: str = "custom validation failed",
    ) -> FieldValidator:
        return FieldValidator(
            name="custom",
            validate=fn,
            message=message,
        )


def validate_dict(
    data: dict[str, Any],
    schema: dict[str, list[FieldValidator]],
) -> tuple[bool, list[str]]:
    """Validate a dictionary against a field->validators mapping."""
    errors: list[str] = []
    for field_name, validators in schema.items():
        value = data.get(field_name)
        for validator in validators:
            ok, msg = validator.check(value)
            if not ok:
                errors.append(f"{field_name}: {msg}")
    return len(errors) == 0, errors


def validate_instance(
    obj: Any,
    schema: Schema,
) -> dict[str, Any]:
    """Validate an object with a Schema."""
    if hasattr(obj, "__dict__"):
        data = vars(obj)
    elif hasattr(obj, "__slots__"):
        data = {s: getattr(obj, s) for s in obj.__slots__ if hasattr(obj, s)}
    else:
        data = dict(obj)
    return schema(data)

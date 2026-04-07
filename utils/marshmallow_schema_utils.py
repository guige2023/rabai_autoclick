"""Marshmallow schema utilities: schema definition, validation, and serialization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = [
    "Schema",
    "SchemaField",
    "ValidationError",
    "validate_schema",
]


@dataclass
class ValidationError:
    """Validation error for a specific field."""

    field: str
    message: str
    value: Any = None


class SchemaField:
    """A schema field definition."""

    def __init__(
        self,
        field_type: type,
        required: bool = False,
        default: Any = None,
        validators: list[callable] | None = None,
    ) -> None:
        self.field_type = field_type
        self.required = required
        self.default = default
        self.validators = validators or []

    def validate(self, value: Any) -> list[ValidationError]:
        errors: list[ValidationError] = []
        if value is None:
            if self.required:
                errors.append(ValidationError("field", "required"))
            return errors

        if not isinstance(value, self.field_type):
            errors.append(ValidationError(
                "field",
                f"expected {self.field_type.__name__}, got {type(value).__name__}",
                value,
            ))
            return errors

        for validator in self.validators:
            try:
                validator(value)
            except Exception as e:
                errors.append(ValidationError("field", str(e), value))

        return errors


class Schema:
    """Schema for validating and serializing data."""

    def __init__(self, fields: dict[str, SchemaField]) -> None:
        self.fields = fields

    def validate(self, data: dict[str, Any]) -> list[ValidationError]:
        """Validate data against this schema."""
        errors: list[ValidationError] = []

        for field_name, field_def in self.fields.items():
            value = data.get(field_name)
            if value is None and field_def.default is not None:
                data[field_name] = field_def.default
                value = field_def.default
            field_errors = field_def.validate(value)
            for err in field_errors:
                errors.append(ValidationError(field_name, err.message, err.value))

        return errors

    def is_valid(self, data: dict[str, Any]) -> bool:
        """Check if data is valid."""
        return len(self.validate(data)) == 0

    def load(self, data: dict[str, Any]) -> dict[str, Any]:
        """Load and deserialize data."""
        errors = self.validate(data)
        if errors:
            raise ValueError(f"Validation failed: {[str(e) for e in errors]}")
        return {k: data.get(k, v.default) for k, v in self.fields.items()}

    def dump(self, obj: Any) -> dict[str, Any]:
        """Serialize an object to a dict."""
        return {field_name: getattr(obj, field_name, None) for field_name in self.fields}


def validate_schema(schema: Schema, data: dict[str, Any]) -> list[ValidationError]:
    """Convenience function to validate data against a schema."""
    return schema.validate(data)

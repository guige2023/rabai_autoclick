"""
API Schema Action Module.

Validates API requests and responses against JSON schemas
 with automatic schema inference and custom validation rules.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


@dataclass
class SchemaField:
    """Definition of a schema field."""
    name: str
    field_type: str = "string"
    required: bool = False
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    pattern: Optional[str] = None
    enum_values: Optional[list[Any]] = None
    items: Optional["SchemaField"] = None


@dataclass
class ValidationIssue:
    """A validation issue."""
    path: str
    message: str
    severity: str = "error"


@dataclass
class SchemaValidationResult:
    """Result of schema validation."""
    valid: bool
    issues: list[ValidationIssue] = field(default_factory=list)


class APISchemaAction:
    """
    JSON Schema validation for API requests and responses.

    Validates data against defined schemas with support for
    nested objects, arrays, and custom validation rules.

    Example:
        schema = APISchemaAction()
        schema.add_field("name", field_type="string", required=True)
        schema.add_field("age", field_type="integer", minimum=0)
        result = schema.validate({"name": "John", "age": 30})
    """

    def __init__(self) -> None:
        self._fields: list[SchemaField] = []
        self._custom_validators: dict[str, Callable] = {}

    def add_field(
        self,
        name: str,
        field_type: str = "string",
        required: bool = False,
        minimum: Optional[float] = None,
        maximum: Optional[float] = None,
        pattern: Optional[str] = None,
        enum_values: Optional[list[Any]] = None,
    ) -> "APISchemaAction":
        """Add a field definition to the schema."""
        field_def = SchemaField(
            name=name,
            field_type=field_type,
            required=required,
            minimum=minimum,
            maximum=maximum,
            pattern=pattern,
            enum_values=enum_values,
        )
        self._fields.append(field_def)
        return self

    def add_custom_validator(
        self,
        field_name: str,
        validator_func: Callable[[Any], bool],
    ) -> "APISchemaAction":
        """Add a custom validator function."""
        self._custom_validators[field_name] = validator_func
        return self

    def validate(
        self,
        data: dict[str, Any],
        strict: bool = False,
    ) -> SchemaValidationResult:
        """Validate data against the schema."""
        issues: list[ValidationIssue] = []

        required_fields = [f.name for f in self._fields if f.required]
        for field_name in required_fields:
            if field_name not in data:
                issues.append(ValidationIssue(
                    path=field_name,
                    message=f"Required field '{field_name}' is missing",
                    severity="error",
                ))

        for field_def in self._fields:
            value = data.get(field_def.name)

            if value is None:
                continue

            field_issues = self._validate_field(field_def, value)
            issues.extend(field_issues)

        if strict:
            extra_fields = set(data.keys()) - {f.name for f in self._fields}
            for field_name in extra_fields:
                issues.append(ValidationIssue(
                    path=field_name,
                    message=f"Unknown field '{field_name}' in strict mode",
                    severity="warning",
                ))

        return SchemaValidationResult(
            valid=all(i.severity != "error" for i in issues),
            issues=issues,
        )

    def _validate_field(
        self,
        field_def: SchemaField,
        value: Any,
        path: str = "",
    ) -> list[ValidationIssue]:
        """Validate a single field."""
        issues: list[ValidationIssue] = []
        field_path = f"{path}.{field_def.name}" if path else field_def.name

        if field_def.field_type == "string":
            if not isinstance(value, str):
                issues.append(ValidationIssue(
                    path=field_path,
                    message=f"Expected string, got {type(value).__name__}",
                ))
            elif field_def.pattern:
                import re
                if not re.match(field_def.pattern, value):
                    issues.append(ValidationIssue(
                        path=field_path,
                        message=f"Value does not match pattern: {field_def.pattern}",
                    ))

        elif field_def.field_type == "integer":
            if not isinstance(value, int) or isinstance(value, bool):
                issues.append(ValidationIssue(
                    path=field_path,
                    message=f"Expected integer, got {type(value).__name__}",
                ))
            elif field_def.minimum is not None and value < field_def.minimum:
                issues.append(ValidationIssue(
                    path=field_path,
                    message=f"Value {value} is less than minimum {field_def.minimum}",
                ))
            elif field_def.maximum is not None and value > field_def.maximum:
                issues.append(ValidationIssue(
                    path=field_path,
                    message=f"Value {value} is greater than maximum {field_def.maximum}",
                ))

        elif field_def.field_type == "number":
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                issues.append(ValidationIssue(
                    path=field_path,
                    message=f"Expected number, got {type(value).__name__}",
                ))

        elif field_def.field_type == "boolean":
            if not isinstance(value, bool):
                issues.append(ValidationIssue(
                    path=field_path,
                    message=f"Expected boolean, got {type(value).__name__}",
                ))

        elif field_def.field_type == "array":
            if not isinstance(value, list):
                issues.append(ValidationIssue(
                    path=field_path,
                    message=f"Expected array, got {type(value).__name__}",
                ))

        if field_def.enum_values and value not in field_def.enum_values:
            issues.append(ValidationIssue(
                path=field_path,
                message=f"Value must be one of: {field_def.enum_values}",
            ))

        custom_validator = self._custom_validators.get(field_def.name)
        if custom_validator and value is not None:
            try:
                if not custom_validator(value):
                    issues.append(ValidationIssue(
                        path=field_path,
                        message="Custom validation failed",
                    ))
            except Exception as e:
                issues.append(ValidationIssue(
                    path=field_path,
                    message=f"Custom validator error: {e}",
                ))

        return issues

    def infer_schema(
        self,
        data: list[dict[str, Any]],
    ) -> "APISchemaAction":
        """Infer schema from sample data."""
        if not data:
            return self

        all_keys: set[str] = set()
        for record in data:
            all_keys.update(record.keys())

        for key in all_keys:
            values = [r.get(key) for r in data if key in r]
            non_null = [v for v in values if v is not None]

            if not non_null:
                field_type = "string"
            elif all(isinstance(v, bool) for v in non_null):
                field_type = "boolean"
            elif all(isinstance(v, int) and not isinstance(v, bool) for v in non_null):
                field_type = "integer"
            elif all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in non_null):
                field_type = "number"
            elif all(isinstance(v, list) for v in non_null):
                field_type = "array"
            elif all(isinstance(v, dict) for v in non_null):
                field_type = "object"
            else:
                field_type = "string"

            self.add_field(key, field_type=field_type)

        return self

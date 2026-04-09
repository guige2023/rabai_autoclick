"""Data Validator with Schema Support.

This module provides schema-based data validation:
- JSON Schema-like validation
- Type coercion
- Default value handling
- Complex nested validation

Example:
    >>> from actions.data_validator2_action import SchemaValidator
    >>> validator = SchemaValidator(schema={"name": {"type": str, "required": True}})
    >>> result = validator.validate(data)
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ValidationIssue:
    """A validation issue."""
    path: str
    message: str
    code: str
    value: Any = None


@dataclass
class ValidationOutcome:
    """Result of validation."""
    valid: bool
    issues: list[ValidationIssue]
    validated_data: Optional[dict] = None


class SchemaValidator:
    """Validates data against schemas."""

    def __init__(self, schema: Optional[dict] = None) -> None:
        """Initialize the validator.

        Args:
            schema: Schema definition dict.
        """
        self._schema = schema or {}
        self._lock = threading.Lock()
        self._stats = {"validations": 0, "failures": 0}

    def set_schema(self, schema: dict) -> None:
        """Set the validation schema.

        Args:
            schema: Schema definition.
        """
        with self._lock:
            self._schema = schema

    def validate(self, data: dict) -> ValidationOutcome:
        """Validate data against schema.

        Args:
            data: Data to validate.

        Returns:
            ValidationOutcome.
        """
        with self._lock:
            self._stats["validations"] += 1

        issues = []
        validated = {}

        for field_name, field_schema in self._schema.items():
            field_issues = self._validate_field(field_name, data.get(field_name), field_schema, "")
            issues.extend(field_issues)

            if not any(i.path == field_name for i in issues):
                validated[field_name] = data.get(field_name)

        result = ValidationOutcome(
            valid=len(issues) == 0,
            issues=issues,
            validated_data=validated if not issues else None,
        )

        if issues:
            with self._lock:
                self._stats["failures"] += 1

        return result

    def _validate_field(
        self,
        field_name: str,
        value: Any,
        field_schema: dict,
        path: str,
    ) -> list[ValidationIssue]:
        """Validate a single field."""
        issues = []
        current_path = f"{path}.{field_name}" if path else field_name

        if field_schema.get("required") and value is None:
            issues.append(ValidationIssue(
                path=current_path,
                message="Field is required",
                code="required",
                value=value,
            ))
            return issues

        if value is None:
            return issues

        expected_type = field_schema.get("type")
        if expected_type and not isinstance(value, expected_type):
            type_name = getattr(expected_type, "__name__", str(expected_type))
            issues.append(ValidationIssue(
                path=current_path,
                message=f"Expected {type_name}, got {type(value).__name__}",
                code="type_mismatch",
                value=value,
            ))
            return issues

        if "min" in field_schema and isinstance(value, (int, float)):
            if value < field_schema["min"]:
                issues.append(ValidationIssue(
                    path=current_path,
                    message=f"Value {value} is less than minimum {field_schema['min']}",
                    code="min_value",
                    value=value,
                ))

        if "max" in field_schema and isinstance(value, (int, float)):
            if value > field_schema["max"]:
                issues.append(ValidationIssue(
                    path=current_path,
                    message=f"Value {value} is greater than maximum {field_schema['max']}",
                    code="max_value",
                    value=value,
                ))

        if "min_length" in field_schema and isinstance(value, (str, list)):
            if len(value) < field_schema["min_length"]:
                issues.append(ValidationIssue(
                    path=current_path,
                    message=f"Length {len(value)} is less than minimum {field_schema['min_length']}",
                    code="min_length",
                    value=value,
                ))

        if "max_length" in field_schema and isinstance(value, (str, list)):
            if len(value) > field_schema["max_length"]:
                issues.append(ValidationIssue(
                    path=current_path,
                    message=f"Length {len(value)} exceeds maximum {field_schema['max_length']}",
                    code="max_length",
                    value=value,
                ))

        if "pattern" in field_schema:
            import re
            if not re.match(field_schema["pattern"], str(value)):
                issues.append(ValidationIssue(
                    path=current_path,
                    message=f"Value does not match pattern {field_schema['pattern']}",
                    code="pattern",
                    value=value,
                ))

        if "enum" in field_schema:
            if value not in field_schema["enum"]:
                issues.append(ValidationIssue(
                    path=current_path,
                    message=f"Value not in allowed values: {field_schema['enum']}",
                    code="enum",
                    value=value,
                ))

        if "items" in field_schema and isinstance(value, list):
            for i, item in enumerate(value):
                item_issues = self._validate_field(
                    f"{current_path}[{i}]",
                    item,
                    field_schema["items"],
                    current_path,
                )
                issues.extend(item_issues)

        if "properties" in field_schema and isinstance(value, dict):
            for sub_field, sub_schema in field_schema["properties"].items():
                sub_issues = self._validate_field(
                    sub_field,
                    value.get(sub_field),
                    sub_schema,
                    current_path,
                )
                issues.extend(sub_issues)

        return issues

    def validate_batch(
        self,
        records: list[dict],
    ) -> list[ValidationOutcome]:
        """Validate a batch of records.

        Args:
            records: List of records.

        Returns:
            List of ValidationOutcomes.
        """
        return [self.validate(r) for r in records]

    def get_stats(self) -> dict[str, int]:
        """Get validator statistics."""
        with self._lock:
            return dict(self._stats)

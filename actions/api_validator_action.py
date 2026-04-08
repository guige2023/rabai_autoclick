"""
API validator module for request/response validation.

Supports schema validation, parameter validation, and custom validation rules.
"""
from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class ValidationType(Enum):
    """Validation types."""
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
    CUSTOM = "custom"


@dataclass
class ValidationRule:
    """A validation rule."""
    field: str
    validation_type: ValidationType
    value: Any = None
    message: str = ""
    condition: Optional[Callable] = None


@dataclass
class ValidationError:
    """A validation error."""
    field: str
    message: str
    code: str = ""


@dataclass
class ValidationResult:
    """Result of validation."""
    valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    validated_at: float = field(default_factory=time.time)


class APIValidator:
    """
    API validator for request and response validation.

    Supports schema validation, parameter validation,
    and custom validation rules.
    """

    def __init__(self):
        self._schemas: dict[str, dict] = {}
        self._rules: dict[str, list[ValidationRule]] = {}

    def add_schema(self, name: str, schema: dict) -> None:
        """Add a validation schema."""
        self._schemas[name] = schema

    def get_schema(self, name: str) -> Optional[dict]:
        """Get a validation schema."""
        return self._schemas.get(name)

    def add_rule(
        self,
        schema_name: str,
        field: str,
        validation_type: ValidationType,
        value: Any = None,
        message: str = "",
        condition: Optional[Callable] = None,
    ) -> ValidationRule:
        """Add a validation rule."""
        rule = ValidationRule(
            field=field,
            validation_type=validation_type,
            value=value,
            message=message,
            condition=condition,
        )

        if schema_name not in self._rules:
            self._rules[schema_name] = []

        self._rules[schema_name].append(rule)
        return rule

    def validate(
        self,
        data: dict,
        schema_name: Optional[str] = None,
        rules: Optional[list[ValidationRule]] = None,
    ) -> ValidationResult:
        """Validate data against rules."""
        result = ValidationResult(valid=True)

        rules_to_use = rules or []
        if schema_name and schema_name in self._rules:
            rules_to_use = self._rules[schema_name]

        for rule in rules_to_use:
            field_value = data.get(rule.field)

            if rule.condition and not rule.condition(data):
                continue

            error = self._apply_rule(rule, field_value)
            if error:
                result.errors.append(error)
                result.valid = False

        return result

    def _apply_rule(self, rule: ValidationRule, value: Any) -> Optional[ValidationError]:
        """Apply a single validation rule."""
        if rule.validation_type == ValidationType.REQUIRED:
            if value is None or value == "":
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} is required",
                    code="required",
                )

        elif rule.validation_type == ValidationType.TYPE:
            expected_type = rule.value
            if not isinstance(value, expected_type):
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} must be of type {expected_type}",
                    code="type",
                )

        elif rule.validation_type == ValidationType.MIN_LENGTH:
            if isinstance(value, (str, list, dict)) and len(value) < rule.value:
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} must be at least {rule.value} characters",
                    code="min_length",
                )

        elif rule.validation_type == ValidationType.MAX_LENGTH:
            if isinstance(value, (str, list, dict)) and len(value) > rule.value:
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} must be at most {rule.value} characters",
                    code="max_length",
                )

        elif rule.validation_type == ValidationType.MIN_VALUE:
            if isinstance(value, (int, float)) and value < rule.value:
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} must be at least {rule.value}",
                    code="min_value",
                )

        elif rule.validation_type == ValidationType.MAX_VALUE:
            if isinstance(value, (int, float)) and value > rule.value:
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} must be at most {rule.value}",
                    code="max_value",
                )

        elif rule.validation_type == ValidationType.PATTERN:
            if isinstance(value, str) and not re.match(rule.value, value):
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} does not match pattern",
                    code="pattern",
                )

        elif rule.validation_type == ValidationType.ENUM:
            if value not in rule.value:
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} must be one of {rule.value}",
                    code="enum",
                )

        elif rule.validation_type == ValidationType.EMAIL:
            if isinstance(value, str) and not re.match(r"[^@]+@[^@]+\.[^@]+", value):
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} must be a valid email",
                    code="email",
                )

        elif rule.validation_type == ValidationType.URL:
            if isinstance(value, str) and not re.match(r"https?://[^\s]+", value):
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} must be a valid URL",
                    code="url",
                )

        elif rule.validation_type == ValidationType.IP_ADDRESS:
            if isinstance(value, str) and not re.match(r"\d+\.\d+\.\d+\.\d+", value):
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} must be a valid IP address",
                    code="ip_address",
                )

        elif rule.validation_type == ValidationType.CUSTOM:
            if rule.value and not rule.value(value):
                return ValidationError(
                    field=rule.field,
                    message=rule.message or f"{rule.field} failed custom validation",
                    code="custom",
                )

        return None

    def validate_request(
        self,
        params: dict,
        query_params: Optional[dict] = None,
        headers: Optional[dict] = None,
        body: Optional[dict] = None,
    ) -> ValidationResult:
        """Validate an API request."""
        result = ValidationResult(valid=True)
        all_errors = []

        if query_params:
            for key, value in query_params.items():
                if value is None:
                    all_errors.append(ValidationError(
                        field=f"query.{key}",
                        message=f"Query parameter {key} is required",
                        code="required",
                    ))

        if headers:
            required_headers = ["Content-Type", "Authorization"]
            for header in required_headers:
                if header not in headers:
                    all_errors.append(ValidationError(
                        field=f"header.{header}",
                        message=f"Header {header} is required",
                        code="required",
                    ))

        if body:
            for key, value in body.items():
                if value is None:
                    all_errors.append(ValidationError(
                        field=f"body.{key}",
                        message=f"Body field {key} is required",
                        code="required",
                    ))

        result.errors = all_errors
        result.valid = len(all_errors) == 0
        return result

    def validate_response(
        self,
        response: dict,
        schema_name: Optional[str] = None,
    ) -> ValidationResult:
        """Validate an API response."""
        if schema_name and schema_name in self._schemas:
            schema = self._schemas[schema_name]
            return self.validate(response, rules=self._extract_rules_from_schema(schema))

        return ValidationResult(valid=True)

    def _extract_rules_from_schema(self, schema: dict) -> list[ValidationRule]:
        """Extract validation rules from a schema."""
        rules = []

        properties = schema.get("properties", {})
        required = schema.get("required", [])

        for field_name, field_schema in properties.items():
            field_type = field_schema.get("type")
            if field_type:
                rules.append(ValidationRule(
                    field=field_name,
                    validation_type=ValidationType.TYPE,
                    value=self._map_type(field_type),
                ))

            if "minLength" in field_schema:
                rules.append(ValidationRule(
                    field=field_name,
                    validation_type=ValidationType.MIN_LENGTH,
                    value=field_schema["minLength"],
                ))

            if "maxLength" in field_schema:
                rules.append(ValidationRule(
                    field=field_name,
                    validation_type=ValidationType.MAX_LENGTH,
                    value=field_schema["maxLength"],
                ))

            if "minimum" in field_schema:
                rules.append(ValidationRule(
                    field=field_name,
                    validation_type=ValidationType.MIN_VALUE,
                    value=field_schema["minimum"],
                ))

            if "maximum" in field_schema:
                rules.append(ValidationRule(
                    field=field_name,
                    validation_type=ValidationType.MAX_VALUE,
                    value=field_schema["maximum"],
                ))

            if "pattern" in field_schema:
                rules.append(ValidationRule(
                    field=field_name,
                    validation_type=ValidationType.PATTERN,
                    value=field_schema["pattern"],
                ))

            if "enum" in field_schema:
                rules.append(ValidationRule(
                    field=field_name,
                    validation_type=ValidationType.ENUM,
                    value=field_schema["enum"],
                ))

            if field_name in required:
                rules.append(ValidationRule(
                    field=field_name,
                    validation_type=ValidationType.REQUIRED,
                ))

        return rules

    def _map_type(self, type_str: str) -> type:
        """Map schema type string to Python type."""
        type_map = {
            "string": str,
            "number": float,
            "integer": int,
            "boolean": bool,
            "array": list,
            "object": dict,
        }
        return type_map.get(type_str, str)

    def list_schemas(self) -> list[str]:
        """List all schema names."""
        return list(self._schemas.keys())

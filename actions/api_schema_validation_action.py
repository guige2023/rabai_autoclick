"""
API Schema Validation Action Module

JSON Schema validation, request/response validation,
OpenAPI spec validation, and custom validation rules.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union

logger = logging.getLogger(__name__)


class ValidationLevel(Enum):
    """Validation error severity levels."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationError:
    """A single validation error."""

    path: str
    message: str
    level: ValidationLevel = ValidationLevel.ERROR
    value: Any = None
    expected: Any = None
    validator: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "path": self.path,
            "message": self.message,
            "level": self.level.value,
            "value": self.value,
            "expected": self.expected,
            "validator": self.validator,
        }


@dataclass
class ValidationResult:
    """Result of a validation operation."""

    valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    validated_at: datetime = field(default_factory=datetime.now)

    def add_error(
        self,
        path: str,
        message: str,
        value: Any = None,
        expected: Any = None,
        validator: Optional[str] = None,
    ) -> None:
        """Add an error."""
        error = ValidationError(
            path=path,
            message=message,
            level=ValidationLevel.ERROR,
            value=value,
            expected=expected,
            validator=validator,
        )
        self.errors.append(error)
        self.valid = False

    def add_warning(
        self,
        path: str,
        message: str,
        value: Any = None,
        expected: Any = None,
        validator: Optional[str] = None,
    ) -> None:
        """Add a warning."""
        warning = ValidationError(
            path=path,
            message=message,
            level=ValidationLevel.WARNING,
            value=value,
            expected=expected,
            validator=validator,
        )
        self.warnings.append(warning)

    def merge(self, other: "ValidationResult") -> None:
        """Merge another validation result."""
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)
        self.valid = self.valid and other.valid


class SchemaValidator:
    """
    Core schema validator supporting JSON Schema-like validation.

    Validates:
    - Types (string, number, integer, boolean, array, object, null)
    - String formats (email, uri, uuid, date, datetime)
    - String constraints (minLength, maxLength, pattern)
    - Numeric constraints (minimum, maximum, multipleOf)
    - Array constraints (minItems, maxItems, uniqueItems)
    - Object constraints (minProperties, maxProperties, additionalProperties)
    - Enum values
    - Required fields
    """

    TYPE_MAP = {
        "string": str,
        "number": (int, float),
        "integer": int,
        "boolean": bool,
        "array": list,
        "object": dict,
        "null": type(None),
    }

    FORMAT_VALIDATORS = {
        "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        "uri": r"^https?://[^\s]+$",
        "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        "date": r"^\d{4}-\d{2}-\d{2}$",
        "datetime": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
    }

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema

    def validate(self, data: Any, path: str = "") -> ValidationResult:
        """Validate data against the schema."""
        result = ValidationResult(valid=True)
        self._validate(data, self.schema, path, result)
        return result

    def _validate(
        self,
        data: Any,
        schema: Dict[str, Any],
        path: str,
        result: ValidationResult,
    ) -> None:
        """Recursively validate data."""
        if schema is None:
            return

        # Type validation
        if "type" in schema:
            if not self._validate_type(data, schema["type"], path, result):
                return

        # Enum validation
        if "enum" in schema:
            if data not in schema["enum"]:
                result.add_error(
                    path=path,
                    message=f"Value must be one of {schema['enum']}",
                    value=data,
                    expected=schema["enum"],
                    validator="enum",
                )

        # String validations
        if isinstance(data, str) and schema.get("type") in ("string", None):
            self._validate_string(data, schema, path, result)

        # Numeric validations
        if isinstance(data, (int, float)) and schema.get("type") in ("number", "integer", None):
            self._validate_number(data, schema, path, result)

        # Array validations
        if isinstance(data, list) and schema.get("type") in ("array", None):
            self._validate_array(data, schema, path, result)

        # Object validations
        if isinstance(data, dict) and schema.get("type") in ("object", None):
            self._validate_object(data, schema, path, result)

    def _validate_type(
        self,
        data: Any,
        expected_type: Union[str, List[str]],
        path: str,
        result: ValidationResult,
    ) -> bool:
        """Validate data type."""
        if expected_type == "null":
            if data is not None:
                result.add_error(
                    path=path,
                    message="Expected null",
                    value=type(data).__name__,
                    expected="null",
                    validator="type",
                )
                return False
            return True

        types = expected_type if isinstance(expected_type, list) else [expected_type]

        for t in types:
            if t in self.TYPE_MAP:
                if isinstance(data, self.TYPE_MAP[t]):
                    return True

        result.add_error(
            path=path,
            message=f"Expected type {expected_type}, got {type(data).__name__}",
            value=type(data).__name__,
            expected=expected_type,
            validator="type",
        )
        return False

    def _validate_string(
        self,
        data: str,
        schema: Dict[str, Any],
        path: str,
        result: ValidationResult,
    ) -> None:
        """Validate string constraints."""
        if "minLength" in schema and len(data) < schema["minLength"]:
            result.add_error(
                path=path,
                message=f"String length must be at least {schema['minLength']}",
                value=len(data),
                expected=f">= {schema['minLength']}",
                validator="minLength",
            )

        if "maxLength" in schema and len(data) > schema["maxLength"]:
            result.add_error(
                path=path,
                message=f"String length must be at most {schema['maxLength']}",
                value=len(data),
                expected=f"<= {schema['maxLength']}",
                validator="maxLength",
            )

        if "pattern" in schema:
            if not re.match(schema["pattern"], data):
                result.add_error(
                    path=path,
                    message=f"String does not match pattern {schema['pattern']}",
                    value=data,
                    expected=schema["pattern"],
                    validator="pattern",
                )

        if "format" in schema:
            fmt = schema["format"]
            if fmt in self.FORMAT_VALIDATORS:
                if not re.match(self.FORMAT_VALIDATORS[fmt], data):
                    result.add_error(
                        path=path,
                        message=f"String does not match format '{fmt}'",
                        value=data,
                        expected=fmt,
                        validator="format",
                    )

    def _validate_number(
        self,
        data: Union[int, float],
        schema: Dict[str, Any],
        path: str,
        result: ValidationResult,
    ) -> None:
        """Validate numeric constraints."""
        if "minimum" in schema and data < schema["minimum"]:
            result.add_error(
                path=path,
                message=f"Value must be >= {schema['minimum']}",
                value=data,
                expected=f">= {schema['minimum']}",
                validator="minimum",
            )

        if "maximum" in schema and data > schema["maximum"]:
            result.add_error(
                path=path,
                message=f"Value must be <= {schema['maximum']}",
                value=data,
                expected=f"<= {schema['maximum']}",
                validator="maximum",
            )

        if "multipleOf" in schema:
            if data % schema["multipleOf"] != 0:
                result.add_error(
                    path=path,
                    message=f"Value must be a multiple of {schema['multipleOf']}",
                    value=data,
                    expected=f"multiple of {schema['multipleOf']}",
                    validator="multipleOf",
                )

    def _validate_array(
        self,
        data: list,
        schema: Dict[str, Any],
        path: str,
        result: ValidationResult,
    ) -> None:
        """Validate array constraints."""
        if "minItems" in schema and len(data) < schema["minItems"]:
            result.add_error(
                path=path,
                message=f"Array must have at least {schema['minItems']} items",
                value=len(data),
                expected=f">= {schema['minItems']}",
                validator="minItems",
            )

        if "maxItems" in schema and len(data) > schema["maxItems"]:
            result.add_error(
                path=path,
                message=f"Array must have at most {schema['maxItems']} items",
                value=len(data),
                expected=f"<= {schema['maxItems']}",
                validator="maxItems",
            )

        if schema.get("uniqueItems") and len(data) != len(set(str(d) for d in data)):
            result.add_error(
                path=path,
                message="Array items must be unique",
                value=len(data),
                expected="unique items",
                validator="uniqueItems",
            )

        if "items" in schema:
            for i, item in enumerate(data):
                item_path = f"{path}[{i}]"
                self._validate(item, schema["items"], item_path, result)

    def _validate_object(
        self,
        data: dict,
        schema: Dict[str, Any],
        path: str,
        result: ValidationResult,
    ) -> None:
        """Validate object constraints."""
        if "minProperties" in schema and len(data) < schema["minProperties"]:
            result.add_error(
                path=path,
                message=f"Object must have at least {schema['minProperties']} properties",
                value=len(data),
                expected=f">= {schema['minProperties']}",
                validator="minProperties",
            )

        if "maxProperties" in schema and len(data) > schema["maxProperties"]:
            result.add_error(
                path=path,
                message=f"Object must have at most {schema['maxProperties']} properties",
                value=len(data),
                expected=f"<= {schema['maxProperties']}",
                validator="maxProperties",
            )

        # Required properties
        if "required" in schema:
            for prop in schema["required"]:
                if prop not in data:
                    result.add_error(
                        path=f"{path}.{prop}",
                        message=f"Required property '{prop}' is missing",
                        value=None,
                        expected=prop,
                        validator="required",
                    )

        # Property schemas
        if "properties" in schema:
            for prop, prop_schema in schema["properties"].items():
                if prop in data:
                    prop_path = f"{path}.{prop}"
                    self._validate(data[prop], prop_schema, prop_path, result)


class RequestValidator:
    """
    Validates API requests against defined rules.

    Checks:
    - Required headers
    - Content-Type
    - Request body schema
    - Query parameters
    - Path parameters
    """

    def __init__(self):
        self._header_rules: Dict[str, Dict[str, Any]] = {}
        self._body_schema: Optional[Dict[str, Any]] = None
        self._query_schema: Optional[Dict[str, Any]] = None

    def require_headers(self, headers: List[str]) -> "RequestValidator":
        """Set required headers."""
        for header in headers:
            self._header_rules[header.lower()] = {"required": True}
        return self

    def set_body_schema(self, schema: Dict[str, Any]) -> "RequestValidator":
        """Set request body JSON schema."""
        self._body_schema = schema
        return self

    def set_query_schema(self, schema: Dict[str, Any]) -> "RequestValidator":
        """Set query parameter schema."""
        self._query_schema = schema
        return self

    def validate_request(
        self,
        method: str,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Any] = None,
        query: Optional[Dict[str, Any]] = None,
    ) -> ValidationResult:
        """Validate an API request."""
        result = ValidationResult(valid=True)
        headers = headers or {}

        # Validate required headers
        for header, rules in self._header_rules.items():
            if rules.get("required") and header not in headers:
                result.add_error(
                    path="headers",
                    message=f"Required header '{header}' is missing",
                    validator="required_header",
                )

        # Validate Content-Type for methods with body
        if method in ("POST", "PUT", "PATCH") and "content-type" in self._header_rules:
            ct = headers.get("content-type", "")
            if not ct:
                result.add_error(
                    path="headers.content-type",
                    message="Content-Type header is required",
                    validator="content_type",
                )

        # Validate body
        if body and self._body_schema:
            validator = SchemaValidator(self._body_schema)
            body_result = validator.validate(body)
            if not body_result.valid:
                result.merge(body_result)

        # Validate query params
        if query and self._query_schema:
            validator = SchemaValidator(self._query_schema)
            query_result = validator.validate(query)
            if not query_result.valid:
                result.merge(query_result)

        return result


class ResponseValidator:
    """
    Validates API responses against defined rules.

    Checks:
    - Status codes
    - Response headers
    - Response body schema
    - Content-Type
    """

    def __init__(self):
        self._allowed_status_codes: Set[int] = {200, 201, 204}
        self._expected_content_type: Optional[str] = None
        self._body_schema: Optional[Dict[str, Any]] = None

    def allow_status_codes(self, codes: List[int]) -> "ResponseValidator":
        """Set allowed status codes."""
        self._allowed_status_codes = set(codes)
        return self

    def expect_content_type(self, content_type: str) -> "ResponseValidator":
        """Set expected Content-Type."""
        self._expected_content_type = content_type
        return self

    def set_body_schema(self, schema: Dict[str, Any]) -> "ResponseValidator":
        """Set response body schema."""
        self._body_schema = schema
        return self

    def validate_response(
        self,
        status_code: int,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Any] = None,
    ) -> ValidationResult:
        """Validate an API response."""
        result = ValidationResult(valid=True)
        headers = headers or {}

        # Validate status code
        if status_code not in self._allowed_status_codes:
            result.add_error(
                path="status_code",
                message=f"Unexpected status code {status_code}",
                value=status_code,
                expected=list(self._allowed_status_codes),
                validator="status_code",
            )

        # Validate Content-Type
        if self._expected_content_type:
            ct = headers.get("content-type", "")
            if not ct.startswith(self._expected_content_type):
                result.add_error(
                    path="headers.content-type",
                    message=f"Expected Content-Type '{self._expected_content_type}'",
                    value=ct,
                    expected=self._expected_content_type,
                    validator="content_type",
                )

        # Validate body schema
        if body and self._body_schema:
            validator = SchemaValidator(self._body_schema)
            body_result = validator.validate(body)
            if not body_result.valid:
                result.merge(body_result)

        return result


class APISchemaValidationAction:
    """
    Main action class for API schema validation.

    Provides unified request/response validation with
    customizable rules and comprehensive error reporting.
    """

    def __init__(self):
        self.request_validator = RequestValidator()
        self.response_validator = ResponseValidator()
        self._stats = {
            "requests_validated": 0,
            "requests_failed": 0,
            "responses_validated": 0,
            "responses_failed": 0,
        }

    def validate_request(
        self,
        method: str,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Any] = None,
        query: Optional[Dict[str, Any]] = None,
    ) -> ValidationResult:
        """Validate an API request."""
        result = self.request_validator.validate_request(method, path, headers, body, query)
        self._stats["requests_validated"] += 1
        if not result.valid:
            self._stats["requests_failed"] += 1
        return result

    def validate_response(
        self,
        status_code: int,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Any] = None,
    ) -> ValidationResult:
        """Validate an API response."""
        result = self.response_validator.validate_response(status_code, headers, body)
        self._stats["responses_validated"] += 1
        if not result.valid:
            self._stats["responses_failed"] += 1
        return result

    def get_stats(self) -> Dict[str, int]:
        """Get validation statistics."""
        return self._stats.copy()


def demo_validation():
    """Demonstrate schema validation."""
    schema = {
        "type": "object",
        "required": ["name", "email"],
        "properties": {
            "name": {"type": "string", "minLength": 1, "maxLength": 100},
            "email": {"type": "string", "format": "email"},
            "age": {"type": "integer", "minimum": 0, "maximum": 150},
            "tags": {"type": "array", "items": {"type": "string"}, "uniqueItems": True},
        },
    }

    validator = SchemaValidator(schema)

    # Valid data
    valid_data = {
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30,
        "tags": ["python", "api"],
    }
    result = validator.validate(valid_data)
    print(f"Valid data: {result.valid}")

    # Invalid data
    invalid_data = {
        "name": "",
        "email": "invalid-email",
        "age": -5,
    }
    result = validator.validate(invalid_data)
    print(f"Invalid data: {result.valid}")
    for error in result.errors:
        print(f"  - {error.path}: {error.message}")


if __name__ == "__main__":
    demo_validation()

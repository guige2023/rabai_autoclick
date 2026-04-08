"""API Validator Action Module.

Provides request/response validation with schema support,
status code checking, and error response handling.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Union
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ValidationScope(Enum):
    """Validation scope."""
    REQUEST = "request"
    RESPONSE = "response"


@dataclass
class StatusCodeRule:
    """Status code validation rule."""
    code: int
    expected: bool = True
    message: str = ""


@dataclass
class ValidationSchema:
    """API validation schema."""
    required_fields: List[str] = field(default_factory=list)
    optional_fields: List[str] = field(default_factory=list)
    field_types: Dict[str, type] = field(default_factory=dict)
    status_codes: List[StatusCodeRule] = field(default_factory=list)
    custom_validators: Dict[str, Callable] = field(default_factory=dict)


class APIValidatorAction:
    """API request/response validator.

    Example:
        validator = APIValidatorAction()

        validator.add_schema("/api/users", ValidationSchema(
            required_fields=["id", "name", "email"],
            field_types={"id": int, "name": str}
        ))

        validator.add_status_code(StatusCodeRule(404, expected=False))

        result = validator.validate_response(200, response_data)
    """

    def __init__(self) -> None:
        self._schemas: Dict[str, ValidationSchema] = {}
        self._default_status_codes: List[StatusCodeRule] = [
            StatusCodeRule(200, expected=True),
            StatusCodeRule(201, expected=True),
            StatusCodeRule(400, expected=False),
            StatusCodeRule(401, expected=False),
            StatusCodeRule(403, expected=False),
            StatusCodeRule(404, expected=False),
            StatusCodeRule(500, expected=False),
        ]

    def add_schema(
        self,
        endpoint: str,
        schema: ValidationSchema,
    ) -> "APIValidatorAction":
        """Add validation schema for endpoint."""
        self._schemas[endpoint] = schema
        return self

    def add_status_code(
        self,
        code: int,
        expected: bool = True,
        message: str = "",
    ) -> "APIValidatorAction":
        """Add status code validation rule."""
        self._default_status_codes.append(
            StatusCodeRule(code, expected, message)
        )
        return self

    def validate_request(
        self,
        endpoint: str,
        data: Dict[str, Any],
    ) -> "ValidationResult":
        """Validate request data.

        Args:
            endpoint: API endpoint
            data: Request data

        Returns:
            ValidationResult
        """
        errors: List[str] = []
        schema = self._schemas.get(endpoint)

        if not schema:
            return ValidationResult(valid=True, errors=[])

        for field_name in schema.required_fields:
            if field_name not in data:
                errors.append(f"Missing required field: {field_name}")

        for field_name, value in data.items():
            if field_name in schema.field_types:
                expected_type = schema.field_types[field_name]
                if not isinstance(value, expected_type):
                    errors.append(
                        f"Field '{field_name}' must be {expected_type.__name__}"
                    )

            if field_name in schema.custom_validators:
                try:
                    if not schema.custom_validators[field_name](value):
                        errors.append(f"Field '{field_name}' failed validation")
                except Exception as e:
                    errors.append(f"Validator error for '{field_name}': {e}")

        return ValidationResult(valid=len(errors) == 0, errors=errors)

    def validate_response(
        self,
        status_code: int,
        data: Any,
        endpoint: Optional[str] = None,
    ) -> "ValidationResult":
        """Validate response data and status.

        Args:
            status_code: HTTP status code
            data: Response data
            endpoint: Optional endpoint for schema validation

        Returns:
            ValidationResult
        """
        errors: List[str] = []

        valid_codes = [
            rule for rule in self._default_status_codes
            if rule.code == status_code
        ]

        if not valid_codes:
            errors.append(f"Unknown status code: {status_code}")
        elif not valid_codes[0].expected:
            errors.append(f"Unexpected status code: {status_code}")

        schema = self._schemas.get(endpoint) if endpoint else None

        if schema:
            if isinstance(data, dict):
                for field_name in schema.required_fields:
                    if field_name not in data:
                        errors.append(f"Missing required field: {field_name}")

                for field_name, value in data.items():
                    if field_name in schema.field_types:
                        expected_type = schema.field_types[field_name]
                        if not isinstance(value, expected_type):
                            errors.append(
                                f"Field '{field_name}' must be {expected_type.__name__}"
                            )

        return ValidationResult(valid=len(errors) == 0, errors=errors)


@dataclass
class ValidationResult:
    """Validation result."""
    valid: bool
    errors: List[str] = field(default_factory=list)

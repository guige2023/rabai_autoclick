"""
API Response Validator Action.

Validates API responses against schemas, checks status codes,
and ensures data integrity with detailed error reporting.

Author: rabai_autoclick
License: MIT
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Literal, Optional, Set, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum, auto
import logging

logger = logging.getLogger(__name__)


class StatusCodeCategory(Enum):
    """HTTP status code categories."""
    INFO = auto()      # 1xx
    SUCCESS = auto()   # 2xx
    REDIRECT = auto()  # 3xx
    CLIENT_ERROR = auto()  # 4xx
    SERVER_ERROR = auto()  # 5xx
    UNKNOWN = auto()


@dataclass(frozen=True)
class StatusCode:
    """HTTP status code with category and description."""
    code: int
    category: StatusCodeCategory
    reason: str
    retryable: bool = False

    @classmethod
    def from_code(cls, code: int) -> "StatusCode":
        """Create StatusCode from integer code."""
        if 100 <= code < 200:
            return cls(code, StatusCodeCategory.INFO, "Informational", False)
        elif 200 <= code < 300:
            return cls(code, StatusCodeCategory.SUCCESS, "Success", False)
        elif 300 <= code < 400:
            return cls(code, StatusCodeCategory.REDIRECT, "Redirect", False)
        elif 400 <= code < 500:
            retryable = code in {408, 429}
            reason = {
                400: "Bad Request", 401: "Unauthorized", 403: "Forbidden",
                404: "Not Found", 408: "Request Timeout", 409: "Conflict",
                422: "Unprocessable Entity", 429: "Too Many Requests",
            }.get(code, f"Client Error {code}")
            return cls(code, StatusCodeCategory.CLIENT_ERROR, reason, retryable)
        elif 500 <= code < 600:
            retryable = True
            reason = {
                500: "Internal Server Error", 502: "Bad Gateway",
                503: "Service Unavailable", 504: "Gateway Timeout",
            }.get(code, f"Server Error {code}")
            return cls(code, StatusCodeCategory.SERVER_ERROR, reason, retryable)
        return cls(code, StatusCodeCategory.UNKNOWN, "Unknown", False)


@dataclass
class FieldValidationError:
    """Error for a specific field validation failure."""
    path: str
    expected: str
    actual: Any
    validator: str
    message: str

    def __str__(self) -> str:
        return f"[{self.path}] {self.validator}: {self.message} (expected: {self.expected}, got: {type(self.actual).__name__})"


@dataclass
class ResponseValidationResult:
    """Complete result of response validation."""
    valid: bool
    status_code: Optional[StatusCode] = None
    content_type: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)
    field_errors: List[FieldValidationError] = field(default_factory=list)
    schema_errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    validated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    response_size: int = 0
    elapsed_ms: float = 0.0

    def __bool__(self) -> bool:
        return self.valid

    def summary(self) -> str:
        status = f"Status {self.status_code.code}" if self.status_code else "No Status"
        errors = len(self.field_errors) + len(self.schema_errors)
        return f"[{'PASS' if self.valid else 'FAIL'}] {status} | {errors} error(s) | {len(self.warnings)} warning(s)"

    def is_retryable(self) -> bool:
        """Check if the response indicates a retryable failure."""
        if self.status_code and self.status_code.retryable:
            return True
        return False


class FieldValidator:
    """A single field validation rule."""

    def __init__(
        self,
        path: str,
        validator: Callable[[Any], bool],
        expected: str,
        message: str,
    ) -> None:
        self.path = path
        self.validator = validator
        self.expected = expected
        self.message = message

    def validate(self, data: Any) -> Optional[FieldValidationError]:
        """Run validation and return error if any."""
        try:
            value = self._get_nested(data, self.path)
            if not self.validator(value):
                return FieldValidationError(
                    path=self.path,
                    expected=self.expected,
                    actual=value,
                    validator=self.validator.__name__,
                    message=self.message,
                )
        except Exception as exc:
            return FieldValidationError(
                path=self.path,
                expected=self.expected,
                actual=None,
                validator=self.validator.__name__,
                message=f"Validation raised exception: {exc}",
            )
        return None

    def _get_nested(self, data: Any, path: str) -> Any:
        """Get nested value using dot notation."""
        if not path or path == ".":
            return data
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            elif isinstance(value, list):
                try:
                    idx = int(key)
                    value = value[idx]
                except (ValueError, IndexError):
                    return None
            else:
                return None
        return value


class TypeValidator:
    """Collection of common type validators."""

    @staticmethod
    def is_string(value: Any) -> bool:
        return isinstance(value, str)

    @staticmethod
    def is_integer(value: Any) -> bool:
        return isinstance(value, int) and not isinstance(value, bool)

    @staticmethod
    def is_float(value: Any) -> bool:
        return isinstance(value, (int, float)) and not isinstance(value, bool)

    @staticmethod
    def is_boolean(value: Any) -> bool:
        return isinstance(value, bool)

    @staticmethod
    def is_object(value: Any) -> bool:
        return isinstance(value, dict)

    @staticmethod
    def is_array(value: Any) -> bool:
        return isinstance(value, list)

    @staticmethod
    def is_null(value: Any) -> bool:
        return value is None

    @staticmethod
    def is_email(value: Any) -> bool:
        if not isinstance(value, str):
            return False
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return bool(re.match(pattern, value))

    @staticmethod
    def is_url(value: Any) -> bool:
        if not isinstance(value, str):
            return False
        pattern = r"^https?://[^\s/$.?#].[^\s]*$"
        return bool(re.match(pattern, value))

    @staticmethod
    def is_uuid(value: Any) -> bool:
        if not isinstance(value, str):
            return False
        pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        return bool(re.match(pattern, value.lower()))

    @staticmethod
    def is_iso_date(value: Any) -> bool:
        if not isinstance(value, str):
            return False
        pattern = r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})?)?$"
        return bool(re.match(pattern, value))

    @staticmethod
    def in_range(min_val: float, max_val: float) -> Callable[[Any], bool]:
        def validator(value: Any) -> bool:
            try:
                return min_val <= float(value) <= max_val
            except (TypeError, ValueError):
                return False
        return validator

    @staticmethod
    def matches_pattern(pattern: str) -> Callable[[Any], bool]:
        compiled = re.compile(pattern)
        def validator(value: Any) -> bool:
            return isinstance(value, str) and bool(compiled.match(value))
        return validator

    @staticmethod
    def one_of(allowed: List[Any]) -> Callable[[Any], bool]:
        def validator(value: Any) -> bool:
            return value in allowed
        return validator

    @staticmethod
    def has_keys(required_keys: List[str]) -> Callable[[Any], bool]:
        def validator(value: Any) -> bool:
            if not isinstance(value, dict):
                return False
            return all(k in value for k in required_keys)
        return validator

    @staticmethod
    def array_min_length(min_len: int) -> Callable[[Any], bool]:
        def validator(value: Any) -> bool:
            return isinstance(value, (list, str)) and len(value) >= min_len
        return validator

    @staticmethod
    def array_max_length(max_len: int) -> Callable[[Any], bool]:
        def validator(value: Any) -> bool:
            return isinstance(value, (list, str)) and len(value) <= max_len
        return validator


class APIResponseValidator:
    """
    Validates HTTP API responses against expected schemas and rules.

    Example:
        validator = APIResponseValidator()
        result = validator.validate(
            status_code=200,
            headers={"content-type": "application/json"},
            body={"user_id": 123, "email": "alice@example.com"},
            expected={
                "status_codes": [200, 201],
                "required_fields": ["user_id", "email"],
                "field_validators": {
                    "email": TypeValidator.is_email,
                },
            },
        )
        print(result.valid)
    """

    def __init__(self) -> None:
        self._field_validators: Dict[str, FieldValidator] = {}
        self._required_fields: Set[str] = set()
        self._allowed_status_codes: Set[int] = {200}
        self._forbidden_status_codes: Set[int] = set()
        self._required_headers: Set[str] = set()
        self._content_type_pattern: Optional[str] = None

    def expect_status_codes(self, codes: List[int]) -> Self:
        """Set allowed status codes."""
        self._allowed_status_codes = set(codes)
        return self

    def forbid_status_codes(self, codes: List[int]) -> Self:
        """Set explicitly forbidden status codes."""
        self._forbidden_status_codes = set(codes)
        return self

    def require_fields(self, fields: List[str]) -> Self:
        """Set required response fields."""
        self._required_fields = set(fields)
        return self

    def require_headers(self, headers: List[str]) -> Self:
        """Set required response headers."""
        self._required_headers = set(headers)
        return self

    def expect_content_type(self, pattern: str) -> Self:
        """Set expected content-type pattern (regex)."""
        self._content_type_pattern = pattern
        return self

    def add_field_validator(self, field: str, validator: Callable[[Any], bool],
                             expected: str, message: str) -> Self:
        """Add a field-level validator."""
        self._field_validators[field] = FieldValidator(field, validator, expected, message)
        return self

    def validate(
        self,
        status_code: int,
        body: Any,
        headers: Optional[Dict[str, str]] = None,
        elapsed_ms: float = 0.0,
    ) -> ResponseValidationResult:
        """
        Validate a complete HTTP response.

        Args:
            status_code: HTTP status code
            body: Response body (already parsed if JSON)
            headers: Response headers
            elapsed_ms: Response time in milliseconds

        Returns:
            ResponseValidationResult with detailed pass/fail information
        """
        headers = headers or {}
        field_errors: List[FieldValidationError] = []
        schema_errors: List[str] = []
        warnings: List[str] = []

        status = StatusCode.from_code(status_code)

        # Check forbidden status codes first
        if status_code in self._forbidden_status_codes:
            schema_errors.append(f"Forbidden status code: {status_code}")

        # Check allowed status codes
        if self._allowed_status_codes and status_code not in self._allowed_status_codes:
            schema_errors.append(
                f"Unexpected status code {status_code} (expected one of: {self._allowed_status_codes})"
            )

        # Validate required headers
        for header in self._required_headers:
            if header.lower() not in {k.lower() for k in headers}:
                schema_errors.append(f"Missing required header: {header}")

        # Check content type
        content_type = headers.get("content-type", "")
        if self._content_type_pattern and not re.search(self._content_type_pattern, content_type):
            schema_errors.append(f"Unexpected content-type: {content_type}")

        # Check required fields (only for successful responses)
        if status.category == StatusCodeCategory.SUCCESS and self._required_fields:
            if isinstance(body, dict):
                for field in self._required_fields:
                    if field not in body:
                        field_errors.append(FieldValidationError(
                            path=field,
                            expected="present",
                            actual=None,
                            validator="required",
                            message=f"Required field '{field}' is missing",
                        ))
            else:
                schema_errors.append("Response body is not a dictionary, cannot validate required fields")

        # Run field validators
        if isinstance(body, dict):
            for path, fv in self._field_validators.items():
                error = fv.validate(body)
                if error:
                    field_errors.append(error)

        # Size check warning
        content_length = headers.get("content-length", "")
        if content_length:
            try:
                size = int(content_length)
                if size > 10 * 1024 * 1024:  # 10MB
                    warnings.append(f"Large response: {size / (1024*1024):.1f}MB")
            except ValueError:
                pass

        # Timeout warning
        if elapsed_ms > 5000:
            warnings.append(f"Slow response: {elapsed_ms:.0f}ms")

        valid = (len(field_errors) == 0 and
                 len(schema_errors) == 0 and
                 status.category in (StatusCodeCategory.SUCCESS, StatusCodeCategory.INFO))

        return ResponseValidationResult(
            valid=valid,
            status_code=status,
            content_type=content_type,
            headers=headers,
            field_errors=field_errors,
            schema_errors=schema_errors,
            warnings=warnings,
            response_size=int(headers.get("content-length", 0) or 0),
            elapsed_ms=elapsed_ms,
        )


def validate_json_response(
    status_code: int,
    response_text: str,
    schema: Optional[Dict[str, Any]] = None,
    validators: Optional[Dict[str, Callable[[Any], bool]]] = None,
) -> ResponseValidationResult:
    """
    Convenience function to validate a JSON API response.

    Args:
        status_code: HTTP status code
        response_text: Raw response text
        schema: Optional JSON schema dict
        validators: Optional field name -> validator callable dict

    Returns:
        ResponseValidationResult
    """
    headers = {"content-type": "application/json"}
    try:
        body = json.loads(response_text) if response_text else {}
    except json.JSONDecodeError as exc:
        return ResponseValidationResult(
            valid=False,
            schema_errors=[f"Invalid JSON: {exc}"],
        )

    validator = APIResponseValidator().expect_status_codes([200]).require_fields(
        list(schema.keys()) if schema else []
    )
    if validators:
        for field, vfn in validators.items():
            validator.add_field_validator(field, vfn, "valid", f"Field '{field}' validation failed")

    return validator.validate(status_code, body, headers)

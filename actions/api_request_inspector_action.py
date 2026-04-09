"""
API Request Inspector Action Module

Inspects, validates, and transforms API requests before forwarding.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import re
import json
import time
from urllib.parse import urlparse, parse_qs


class ValidationLevel(Enum):
    """Validation strictness level."""
    NONE = "none"
    BASIC = "basic"
    STRICT = "strict"
    PEDANTIC = "pedantic"


class IssueSeverity(Enum):
    """Issue severity levels."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationIssue:
    """Single validation issue."""
    severity: IssueSeverity
    field: str
    message: str
    code: str


@dataclass
class RequestInspectionResult:
    """Result of request inspection."""
    valid: bool
    issues: List[ValidationIssue] = field(default_factory=list)
    transformed_body: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    processing_time_ms: float = 0.0


@dataclass
class FieldRule:
    """Rule for field validation."""
    name: str
    required: bool = False
    type_hint: Optional[str] = None  # "str", "int", "float", "bool", "list", "dict"
    pattern: Optional[str] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_values: Optional[List[Any]] = None
    custom_validator: Optional[Callable] = None


@dataclass
class SchemaRule:
    """Schema validation rule."""
    required_fields: List[str] = field(default_factory=list)
    field_rules: Dict[str, FieldRule] = field(default_factory=dict)
    allow_extra_fields: bool = True


class HeaderValidator:
    """Validates HTTP headers."""

    # Common header patterns
    CONTENT_TYPE_PATTERN = re.compile(r'^[a-z]+/[a-z0-9\-\+\.]+$', re.IGNORECASE)
    ETAG_PATTERN = re.compile(r'^W?/"[^"]*"$')
    DATE_RFC7231 = re.compile(
        r'^[A-Z][a-z]{2}, \d{2} [A-Z][a-z]{2} \d{4} \d{2}:\d{2}:\d{2} GMT$'
    )

    def __init__(self):
        self.allowed_headers: List[str] = [
            "accept", "accept-charset", "accept-encoding", "accept-language",
            "authorization", "cache-control", "content-type", "cookie",
            "date", "etag", "host", "if-match", "if-modified-since",
            "if-none-match", "origin", "referer", "user-agent", "x-api-key",
            "x-request-id", "x-correlation-id"
        ]
        self.restricted_headers: Dict[str, re.Pattern] = {
            "content-type": self.CONTENT_TYPE_PATTERN,
        }

    def validate(self, headers: Dict[str, str]) -> List[ValidationIssue]:
        """Validate headers and return issues."""
        issues = []

        for name, value in headers.items():
            lower_name = name.lower()

            # Check if header is allowed
            if lower_name not in self.allowed_headers:
                issues.append(ValidationIssue(
                    severity=IssueSeverity.WARNING,
                    field=f"header:{name}",
                    message=f"Non-standard header: {name}",
                    code="NON_STANDARD_HEADER"
                ))

            # Check restricted headers
            if lower_name in self.restricted_headers:
                pattern = self.restricted_headers[lower_name]
                if not pattern.match(value):
                    issues.append(ValidationIssue(
                        severity=IssueSeverity.WARNING,
                        field=f"header:{name}",
                        message=f"Header {name} has invalid format",
                        code="INVALID_HEADER_FORMAT"
                    ))

        return issues


class BodyValidator:
    """Validates request body."""

    def __init__(self, schema: Optional[SchemaRule] = None):
        self.schema = schema

    def validate(
        self,
        body: Optional[Dict[str, Any]],
        level: ValidationLevel
    ) -> Tuple[bool, List[ValidationIssue], Dict[str, Any]]:
        """Validate body and return issues."""
        issues = []
        transformed = body.copy() if body else {}

        if not body and level != ValidationLevel.NONE:
            issues.append(ValidationIssue(
                severity=IssueSeverity.WARNING,
                field="body",
                message="Empty request body",
                code="EMPTY_BODY"
            ))
            return len(issues) == 0, issues, transformed

        if not self.schema:
            return True, issues, transformed

        # Check required fields
        for field_name in self.schema.required_fields:
            if field_name not in body:
                issues.append(ValidationIssue(
                    severity=IssueSeverity.ERROR,
                    field=f"body.{field_name}",
                    message=f"Required field missing: {field_name}",
                    code="MISSING_REQUIRED_FIELD"
                ))

        # Validate each field
        for field_name, value in body.items():
            field_issues = self._validate_field(
                field_name, value, self.schema.field_rules.get(field_name), level
            )
            issues.extend(field_issues)

        # Check extra fields
        if not self.schema.allow_extra_fields:
            known_fields = set(self.schema.required_fields) | set(self.schema.field_rules.keys())
            for field_name in body.keys():
                if field_name not in known_fields:
                    issues.append(ValidationIssue(
                        severity=IssueSeverity.WARNING,
                        field=f"body.{field_name}",
                        message=f"Unknown field: {field_name}",
                        code="UNKNOWN_FIELD"
                    ))

        valid = all(i.severity != IssueSeverity.ERROR for i in issues)
        return valid, issues, transformed

    def _validate_field(
        self,
        name: str,
        value: Any,
        rule: Optional[FieldRule],
        level: ValidationLevel
    ) -> List[ValidationIssue]:
        """Validate a single field."""
        issues = []
        prefix = f"body.{name}"

        if not rule:
            return issues

        # Type check
        if rule.type_hint and value is not None:
            expected = rule.type_hint
            actual_type = type(value).__name__
            type_ok = (
                (expected == "int" and isinstance(value, int)) or
                (expected == "float" and isinstance(value, (int, float))) or
                (expected == "str" and isinstance(value, str)) or
                (expected == "bool" and isinstance(value, bool)) or
                (expected == "list" and isinstance(value, list)) or
                (expected == "dict" and isinstance(value, dict))
            )
            if not type_ok and level in (ValidationLevel.STRICT, ValidationLevel.PEDANTIC):
                issues.append(ValidationIssue(
                    severity=IssueSeverity.ERROR,
                    field=prefix,
                    message=f"Expected type {expected}, got {actual_type}",
                    code="TYPE_MISMATCH"
                ))

        # String pattern
        if rule.pattern and isinstance(value, str):
            if not re.match(rule.pattern, value):
                issues.append(ValidationIssue(
                    severity=IssueSeverity.ERROR,
                    field=prefix,
                    message=f"Value does not match pattern: {rule.pattern}",
                    code="PATTERN_MISMATCH"
                ))

        # Length constraints
        if rule.min_length is not None and isinstance(value, (str, list)):
            if len(value) < rule.min_length:
                issues.append(ValidationIssue(
                    severity=IssueSeverity.ERROR,
                    field=prefix,
                    message=f"Length must be >= {rule.min_length}",
                    code="MIN_LENGTH_VIOLATION"
                ))

        if rule.max_length is not None and isinstance(value, (str, list)):
            if len(value) > rule.max_length:
                issues.append(ValidationIssue(
                    severity=IssueSeverity.ERROR,
                    field=prefix,
                    message=f"Length must be <= {rule.max_length}",
                    code="MAX_LENGTH_VIOLATION"
                ))

        # Numeric constraints
        if rule.min_value is not None and isinstance(value, (int, float)):
            if value < rule.min_value:
                issues.append(ValidationIssue(
                    severity=IssueSeverity.ERROR,
                    field=prefix,
                    message=f"Value must be >= {rule.min_value}",
                    code="MIN_VALUE_VIOLATION"
                ))

        if rule.max_value is not None and isinstance(value, (int, float)):
            if value > rule.max_value:
                issues.append(ValidationIssue(
                    severity=IssueSeverity.ERROR,
                    field=prefix,
                    message=f"Value must be <= {rule.max_value}",
                    code="MAX_VALUE_VIOLATION"
                ))

        # Allowed values
        if rule.allowed_values is not None and value not in rule.allowed_values:
            issues.append(ValidationIssue(
                severity=IssueSeverity.ERROR,
                field=prefix,
                message=f"Value must be one of: {rule.allowed_values}",
                code="INVALID_VALUE"
            ))

        # Custom validator
        if rule.custom_validator:
            try:
                rule.custom_validator(value)
            except Exception as e:
                issues.append(ValidationIssue(
                    severity=IssueSeverity.ERROR,
                    field=prefix,
                    message=f"Custom validation failed: {str(e)}",
                    code="CUSTOM_VALIDATION_FAILED"
                ))

        return issues


class UrlValidator:
    """Validates URL components."""

    def __init__(self):
        self.allowed_schemes = ["http", "https"]
        self.allowed_domains: Optional[List[str]] = None
        self.blocked_domains: List[str] = []

    def validate(self, url: str) -> List[ValidationIssue]:
        """Validate URL and return issues."""
        issues = []

        try:
            parsed = urlparse(url)

            # Scheme check
            if parsed.scheme not in self.allowed_schemes:
                issues.append(ValidationIssue(
                    severity=IssueSeverity.ERROR,
                    field="url",
                    message=f"Invalid scheme: {parsed.scheme}",
                    code="INVALID_SCHEME"
                ))

            # Domain check
            if self.allowed_domains and parsed.netloc not in self.allowed_domains:
                issues.append(ValidationIssue(
                    severity=IssueSeverity.ERROR,
                    field="url",
                    message=f"Domain not in allowed list: {parsed.netloc}",
                    code="DOMAIN_NOT_ALLOWED"
                ))

            if parsed.netloc in self.blocked_domains:
                issues.append(ValidationIssue(
                    severity=IssueSeverity.ERROR,
                    field="url",
                    message=f"Domain is blocked: {parsed.netloc}",
                    code="DOMAIN_BLOCKED"
                ))

        except Exception as e:
            issues.append(ValidationIssue(
                severity=IssueSeverity.ERROR,
                field="url",
                message=f"Invalid URL: {str(e)}",
                code="MALFORMED_URL"
            ))

        return issues


class ApiRequestInspectorAction:
    """
    Inspects, validates, and transforms API requests before forwarding.

    Supports header validation, body schema validation, URL validation,
    and various transformation operations.

    Example:
        inspector = ApiRequestInspectorAction()
        inspector.add_header_validation()
        inspector.set_body_schema(SchemaRule(
            required_fields=["user_id"],
            field_rules={"email": FieldRule(pattern=r"^[\w\.]+@[\w\.]+$")}
        ))
        result = inspector.inspect(request_data, level=ValidationLevel.STRICT)
    """

    def __init__(self):
        """Initialize request inspector."""
        self.header_validator = HeaderValidator()
        self.url_validator = UrlValidator()
        self.body_validator: Optional[BodyValidator] = None
        self.transformations: List[Callable] = []
        self.validation_level: ValidationLevel = ValidationLevel.BASIC
        self._inspection_count = 0

    def set_validation_level(self, level: ValidationLevel) -> None:
        """Set global validation level."""
        self.validation_level = level

    def add_header_validation(self) -> None:
        """Enable header validation."""
        self.header_validator = HeaderValidator()

    def set_body_schema(self, schema: SchemaRule) -> None:
        """Set body validation schema."""
        self.body_validator = BodyValidator(schema)

    def set_allowed_domains(self, domains: List[str]) -> None:
        """Set allowed URL domains."""
        self.url_validator.allowed_domains = domains

    def set_allowed_schemes(self, schemes: List[str]) -> None:
        """Set allowed URL schemes."""
        self.url_validator.allowed_schemes = schemes

    def add_transformation(self, fn: Callable[[Dict], Dict]) -> None:
        """Add a body transformation function."""
        self.transformations.append(fn)

    def inspect(
        self,
        request_data: Dict[str, Any],
        level: Optional[ValidationLevel] = None
    ) -> RequestInspectionResult:
        """
        Inspect a request and validate/transform it.

        Args:
            request_data: Request data with keys: url, method, headers, body
            level: Override validation level

        Returns:
            RequestInspectionResult with validation results
        """
        start_time = time.time()
        level = level or self.validation_level
        all_issues: List[ValidationIssue] = []
        transformed_body = request_data.get("body", {})

        if level == ValidationLevel.NONE:
            return RequestInspectionResult(
                valid=True,
                issues=[],
                transformed_body=transformed_body,
                processing_time_ms=(time.time() - start_time) * 1000
            )

        # Validate URL
        url_issues = self.url_validator.validate(request_data.get("url", ""))
        all_issues.extend(url_issues)

        # Validate headers
        headers = request_data.get("headers", {})
        header_issues = self.header_validator.validate(headers)
        all_issues.extend(header_issues)

        # Validate body
        if self.body_validator:
            body = request_data.get("body")
            body_valid, body_issues, transformed_body = self.body_validator.validate(
                body, level
            )
            all_issues.extend(body_issues)
        else:
            # Basic body validation
            body = request_data.get("body")
            if not isinstance(body, (dict, list, type(None))):
                all_issues.append(ValidationIssue(
                    severity=IssueSeverity.ERROR,
                    field="body",
                    message="Body must be JSON object or array",
                    code="INVALID_BODY_TYPE"
                ))

        # Apply transformations
        for transform in self.transformations:
            try:
                transformed_body = transform(transformed_body) or transformed_body
            except Exception as e:
                all_issues.append(ValidationIssue(
                    severity=IssueSeverity.WARNING,
                    field="body",
                    message=f"Transformation failed: {str(e)}",
                    code="TRANSFORMATION_ERROR"
                ))

        # Determine validity
        has_errors = any(i.severity == IssueSeverity.ERROR for i in all_issues)
        valid = not has_errors

        processing_time = (time.time() - start_time) * 1000
        self._inspection_count += 1

        return RequestInspectionResult(
            valid=valid,
            issues=all_issues,
            transformed_body=transformed_body,
            metadata={
                "inspection_number": self._inspection_count,
                "validation_level": level.value,
                "transformation_count": len(self.transformations)
            },
            processing_time_ms=processing_time
        )

    def get_stats(self) -> Dict[str, Any]:
        """Get inspection statistics."""
        return {
            "total_inspections": self._inspection_count,
            "validation_level": self.validation_level.value,
            "transformations_enabled": len(self.transformations),
            "body_schema_configured": self.body_validator is not None
        }

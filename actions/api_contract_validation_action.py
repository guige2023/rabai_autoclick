"""
API Contract Validation Action Module

Validates API requests and responses against OpenAPI/pact
contracts, ensuring backward compatibility.

Author: RabAi Team
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

import logging

logger = logging.getLogger(__name__)


class ViolationSeverity(Enum):
    """Severity of contract violations."""

    ERROR = auto()
    WARNING = auto()
    INFO = auto()


@dataclass
class ContractViolation:
    """A single contract violation."""

    severity: ViolationSeverity
    message: str
    path: str
    expected: Optional[Any] = None
    actual: Optional[Any] = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class ValidationResult:
    """Result of contract validation."""

    passed: bool
    violations: List[ContractViolation] = field(default_factory=list)
    validated_at: float = field(default_factory=time.time)
    schema_version: Optional[str] = None


@dataclass
class FieldSchema:
    """Schema definition for a single field."""

    name: str
    field_type: str
    required: bool = False
    nullable: bool = True
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    enum_values: Optional[List[Any]] = None
    items_schema: Optional[FieldSchema] = None


@dataclass
class EndpointContract:
    """Contract definition for a single endpoint."""

    path: str
    method: str
    request_schema: Optional[Dict[str, FieldSchema]] = None
    response_schema: Optional[Dict[str, FieldSchema]] = None
    required_headers: List[str] = field(default_factory=list)
    deprecated: bool = False
    description: str = ""


@dataclass
class ContractSuite:
    """Collection of endpoint contracts."""

    name: str
    version: str
    endpoints: Dict[str, EndpointContract] = field(default_factory=dict)

    def get_endpoint(self, path: str, method: str) -> Optional[EndpointContract]:
        key = f"{method.upper()}:{path}"
        return self.endpoints.get(key)


class SchemaValidator:
    """Validates data against field schemas."""

    def __init__(self) -> None:
        pass

    def validate_field(
        self,
        field_schema: FieldSchema,
        value: Any,
        path: str,
    ) -> List[ContractViolation]:
        """Validate a single field against its schema."""
        violations = []

        if value is None:
            if field_schema.required:
                violations.append(ContractViolation(
                    severity=ViolationSeverity.ERROR,
                    message=f"Required field missing: {path}",
                    path=path,
                ))
            if not field_schema.nullable:
                violations.append(ContractViolation(
                    severity=ViolationSeverity.ERROR,
                    message=f"Non-nullable field is null: {path}",
                    path=path,
                    actual=None,
                ))
            return violations

        if field_schema.field_type == "string":
            if not isinstance(value, str):
                violations.append(ContractViolation(
                    severity=ViolationSeverity.ERROR,
                    message=f"Expected string at {path}, got {type(value).__name__}",
                    path=path,
                    expected="string",
                    actual=type(value).__name__,
                ))
            else:
                if field_schema.min_length is not None and len(value) < field_schema.min_length:
                    violations.append(ContractViolation(
                        severity=ViolationSeverity.ERROR,
                        message=f"String too short at {path}: {len(value)} < {field_schema.min_length}",
                        path=path,
                        expected=f"min_length={field_schema.min_length}",
                        actual=len(value),
                    ))
                if field_schema.max_length is not None and len(value) > field_schema.max_length:
                    violations.append(ContractViolation(
                        severity=ViolationSeverity.ERROR,
                        message=f"String too long at {path}: {len(value)} > {field_schema.max_length}",
                        path=path,
                        expected=f"max_length={field_schema.max_length}",
                        actual=len(value),
                    ))
                if field_schema.pattern and not re.match(field_schema.pattern, value):
                    violations.append(ContractViolation(
                        severity=ViolationSeverity.WARNING,
                        message=f"String pattern mismatch at {path}",
                        path=path,
                        expected=field_schema.pattern,
                        actual=value,
                    ))

        elif field_schema.field_type == "integer":
            if not isinstance(value, int) or isinstance(value, bool):
                violations.append(ContractViolation(
                    severity=ViolationSeverity.ERROR,
                    message=f"Expected integer at {path}",
                    path=path,
                    expected="integer",
                    actual=type(value).__name__,
                ))
            else:
                if field_schema.min_value is not None and value < field_schema.min_value:
                    violations.append(ContractViolation(
                        severity=ViolationSeverity.ERROR,
                        message=f"Integer below minimum at {path}",
                        path=path,
                        expected=f"min={field_schema.min_value}",
                        actual=value,
                    ))
                if field_schema.max_value is not None and value > field_schema.max_value:
                    violations.append(ContractViolation(
                        severity=ViolationSeverity.ERROR,
                        message=f"Integer above maximum at {path}",
                        path=path,
                        expected=f"max={field_schema.max_value}",
                        actual=value,
                    ))

        elif field_schema.field_type == "number":
            if not isinstance(value, (int, float)) or isinstance(value, bool):
                violations.append(ContractViolation(
                    severity=ViolationSeverity.ERROR,
                    message=f"Expected number at {path}",
                    path=path,
                    expected="number",
                    actual=type(value).__name__,
                ))

        elif field_schema.field_type == "boolean":
            if not isinstance(value, bool):
                violations.append(ContractViolation(
                    severity=ViolationSeverity.ERROR,
                    message=f"Expected boolean at {path}",
                    path=path,
                    expected="boolean",
                    actual=type(value).__name__,
                ))

        elif field_schema.field_type == "array":
            if not isinstance(value, list):
                violations.append(ContractViolation(
                    severity=ViolationSeverity.ERROR,
                    message=f"Expected array at {path}",
                    path=path,
                    expected="array",
                    actual=type(value).__name__,
                ))
            elif field_schema.items_schema:
                for i, item in enumerate(value):
                    item_violations = self.validate_field(
                        field_schema.items_schema,
                        item,
                        f"{path}[{i}]",
                    )
                    violations.extend(item_violations)

        elif field_schema.field_type == "object":
            if not isinstance(value, dict):
                violations.append(ContractViolation(
                    severity=ViolationSeverity.ERROR,
                    message=f"Expected object at {path}",
                    path=path,
                    expected="object",
                    actual=type(value).__name__,
                ))

        if field_schema.enum_values and value not in field_schema.enum_values:
            violations.append(ContractViolation(
                severity=ViolationSeverity.WARNING,
                message=f"Value not in allowed enum at {path}",
                path=path,
                expected=field_schema.enum_values,
                actual=value,
            ))

        return violations


class ContractValidationAction:
    """Action class for API contract validation."""

    def __init__(self, contract_suite: Optional[ContractSuite] = None) -> None:
        self.suite = contract_suite
        self.validator = SchemaValidator()
        self._history: List[ValidationResult] = []

    def validate_request(
        self,
        path: str,
        method: str,
        request_data: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
    ) -> ValidationResult:
        """Validate an API request against its contract."""
        violations: List[ContractViolation] = []

        if self.suite:
            endpoint = self.suite.get_endpoint(path, method)
            if endpoint:
                if endpoint.deprecated:
                    violations.append(ContractViolation(
                        severity=ViolationSeverity.WARNING,
                        message=f"Endpoint is deprecated: {method} {path}",
                        path=path,
                    ))

                if endpoint.required_headers and headers:
                    for header in endpoint.required_headers:
                        if header not in headers:
                            violations.append(ContractViolation(
                                severity=ViolationSeverity.ERROR,
                                message=f"Required header missing: {header}",
                                path=path,
                            ))

                if endpoint.request_schema:
                    for field_name, field_schema in endpoint.request_schema.items():
                        field_violations = self.validator.validate_field(
                            field_schema,
                            request_data.get(field_name),
                            f"body.{field_name}",
                        )
                        violations.extend(field_violations)
            else:
                violations.append(ContractViolation(
                    severity=ViolationSeverity.INFO,
                    message=f"No contract found for {method} {path}",
                    path=path,
                ))

        result = ValidationResult(
            passed=not any(v.severity == ViolationSeverity.ERROR for v in violations),
            violations=violations,
            schema_version=self.suite.version if self.suite else None,
        )
        self._history.append(result)
        return result

    def validate_response(
        self,
        path: str,
        method: str,
        status_code: int,
        response_data: Any,
    ) -> ValidationResult:
        """Validate an API response against its contract."""
        violations: List[ContractViolation] = []

        if self.suite:
            endpoint = self.suite.get_endpoint(path, method)
            if endpoint and endpoint.response_schema:
                status_group = (status_code // 100) * 100
                relevant_schema = None
                for status_key, schema in endpoint.response_schema.items():
                    if int(status_key) == status_code:
                        relevant_schema = schema
                        break

                if relevant_schema:
                    for field_name, field_schema in relevant_schema.items():
                        field_violations = self.validator.validate_field(
                            field_schema,
                            response_data.get(field_name) if isinstance(response_data, dict) else None,
                            f"body.{field_name}",
                        )
                        violations.extend(field_violations)

        result = ValidationResult(
            passed=not any(v.severity == ViolationSeverity.ERROR for v in violations),
            violations=violations,
            schema_version=self.suite.version if self.suite else None,
        )
        self._history.append(result)
        return result

    def get_validation_history(self) -> List[ValidationResult]:
        """Return validation history."""
        return self._history.copy()

    def get_pass_rate(self) -> float:
        """Calculate pass rate from validation history."""
        if not self._history:
            return 0.0
        passed = sum(1 for r in self._history if r.passed)
        return passed / len(self._history)

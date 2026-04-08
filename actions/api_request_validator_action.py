# Copyright (c) 2024. coded by claude
"""API Request Validator Action Module.

Validates API requests including parameter validation, body schema checking,
and request size limits.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ValidationErrorType(Enum):
    MISSING_PARAM = "missing_param"
    INVALID_TYPE = "invalid_type"
    INVALID_VALUE = "invalid_value"
    SIZE_EXCEEDED = "size_exceeded"
    MALFORMED = "malformed"


@dataclass
class ValidationIssue:
    field: str
    error_type: ValidationErrorType
    message: str


@dataclass
class ValidationOutcome:
    valid: bool
    issues: List[ValidationIssue] = field(default_factory=list)


@dataclass
class RequestValidationConfig:
    max_body_size: int = 10 * 1024 * 1024
    max_param_count: int = 100
    required_params: List[str] = field(default_factory=list)
    param_types: Dict[str, type] = field(default_factory=dict)


class APIRequestValidator:
    def __init__(self, config: Optional[RequestValidationConfig] = None):
        self.config = config or RequestValidationConfig()

    def validate(self, method: str, path: str, params: Dict[str, Any], body: Optional[Any] = None, headers: Optional[Dict[str, str]] = None) -> ValidationOutcome:
        issues: List[ValidationIssue] = []
        issues.extend(self._validate_params(params))
        if body is not None:
            issues.extend(self._validate_body(body))
        return ValidationOutcome(valid=len(issues) == 0, issues=issues)

    def _validate_params(self, params: Dict[str, Any]) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []
        if len(params) > self.config.max_param_count:
            issues.append(ValidationIssue(
                field="params",
                error_type=ValidationErrorType.SIZE_EXCEEDED,
                message=f"Too many parameters: {len(params)} > {self.config.max_param_count}",
            ))
        for required in self.config.required_params:
            if required not in params:
                issues.append(ValidationIssue(
                    field=required,
                    error_type=ValidationErrorType.MISSING_PARAM,
                    message=f"Required parameter '{required}' is missing",
                ))
        for param_name, param_value in params.items():
            if param_name in self.config.param_types:
                expected_type = self.config.param_types[param_name]
                if not isinstance(param_value, expected_type):
                    issues.append(ValidationIssue(
                        field=param_name,
                        error_type=ValidationErrorType.INVALID_TYPE,
                        message=f"Parameter '{param_name}' must be of type {expected_type.__name__}",
                    ))
        return issues

    def _validate_body(self, body: Any) -> List[ValidationIssue]:
        issues: List[ValidationIssue] = []
        body_str = str(body)
        if len(body_str) > self.config.max_body_size:
            issues.append(ValidationIssue(
                field="body",
                error_type=ValidationErrorType.SIZE_EXCEEDED,
                message=f"Request body exceeds maximum size of {self.config.max_body_size} bytes",
            ))
        if isinstance(body, dict) and len(body) > self.config.max_param_count:
            issues.append(ValidationIssue(
                field="body",
                error_type=ValidationErrorType.SIZE_EXCEEDED,
                message=f"Request body has too many fields: {len(body)} > {self.config.max_param_count}",
            ))
        return issues

    def add_required_param(self, param: str) -> None:
        if param not in self.config.required_params:
            self.config.required_params.append(param)

    def set_param_type(self, param: str, param_type: type) -> None:
        self.config.param_types[param] = param_type

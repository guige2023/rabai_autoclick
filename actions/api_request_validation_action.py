"""API Request Validation Action Module.

Validate API requests with schema and custom rules.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

from .data_validator_action import DataValidator, ValidationRule, ValidationType, ValidationResult


class ParameterLocation(Enum):
    """Parameter location."""
    PATH = "path"
    QUERY = "query"
    HEADER = "header"
    BODY = "body"


@dataclass
class ParameterDefinition:
    """API parameter definition."""
    name: str
    location: ParameterLocation
    required: bool = False
    type: type = str
    pattern: str | None = None
    min_length: int | None = None
    max_length: int | None = None
    minimum: float | None = None
    maximum: float | None = None
    enum_values: list | None = None


@dataclass
class RequestValidationRule:
    """Validation rule for a parameter."""
    parameter: ParameterDefinition
    validator: DataValidator


class APIRequestValidator:
    """Validate API requests against parameter definitions."""

    def __init__(self) -> None:
        self._rules: dict[str, list[RequestValidationRule]] = {
            "path": [],
            "query": [],
            "header": [],
            "body": []
        }
        self._validators: dict[str, DataValidator] = {}

    def add_parameter(self, param: ParameterDefinition) -> APIRequestValidator:
        """Add parameter definition."""
        rule = RequestValidationRule(param, self._create_validator(param))
        self._rules[param.location.value].append(rule)
        return self

    def _create_validator(self, param: ParameterDefinition) -> DataValidator:
        """Create validator for parameter."""
        validator = DataValidator()
        field_name = param.name
        if param.required:
            validator.required(field_name)
        validator.type_check(field_name, param.type)
        if param.pattern:
            validator.pattern(field_name, param.pattern)
        if param.min_length:
            validator.min_length(field_name, param.min_length)
        if param.max_length:
            validator.max_length(field_name, param.max_length)
        if param.minimum is not None:
            validator.min_value(field_name, param.minimum)
        if param.maximum is not None:
            validator.max_value(field_name, param.maximum)
        if param.enum_values:
            validator.enum_value(field_name, param.enum_values)
        return validator

    def validate_request(
        self,
        path_params: dict | None = None,
        query_params: dict | None = None,
        headers: dict | None = None,
        body: dict | None = None
    ) -> tuple[bool, list[str]]:
        """Validate request parameters. Returns (is_valid, errors)."""
        errors = []
        path_params = path_params or {}
        query_params = query_params or {}
        headers = headers or {}
        body = body or {}
        for rule in self._rules["path"]:
            data = path_params
            result = self._validate_param(rule, data)
            if not result.valid:
                errors.extend([f"path.{e.field}: {e.message}" for e in result.errors])
        for rule in self._rules["query"]:
            data = query_params
            result = self._validate_param(rule, data)
            if not result.valid:
                errors.extend([f"query.{e.field}: {e.message}" for e in result.errors])
        for rule in self._rules["header"]:
            data = headers
            result = self._validate_param(rule, data)
            if not result.valid:
                errors.extend([f"header.{e.field}: {e.message}" for e in result.errors])
        for rule in self._rules["body"]:
            data = body
            result = self._validate_param(rule, data)
            if not result.valid:
                errors.extend([f"body.{e.field}: {e.message}" for e in result.errors])
        return len(errors) == 0, errors

    def _validate_param(self, rule: RequestValidationRule, data: dict) -> ValidationResult:
        """Validate single parameter."""
        return rule.validator.validate(data)

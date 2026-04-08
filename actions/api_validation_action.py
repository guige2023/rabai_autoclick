"""
API Validation Action - Validates API requests and responses.

This module provides request/response validation including
schema validation, status code checking, and field verification.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class ValidationRule:
    """A validation rule for API data."""
    rule_id: str
    field_path: str
    rule_type: str
    expected: Any = None
    pattern: str | None = None


@dataclass
class ValidationResult:
    """Result of validation."""
    valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


class ResponseValidator:
    """Validates API responses."""
    
    def __init__(self) -> None:
        self._rules: list[ValidationRule] = []
    
    def add_rule(self, rule: ValidationRule) -> None:
        """Add a validation rule."""
        self._rules.append(rule)
    
    def validate_response(
        self,
        status_code: int,
        data: Any,
        expected_status: int | None = None,
    ) -> ValidationResult:
        """Validate an API response."""
        errors = []
        warnings = []
        
        if expected_status and status_code != expected_status:
            errors.append(f"Expected status {expected_status}, got {status_code}")
        
        if status_code >= 400:
            errors.append(f"Error response: status {status_code}")
        
        for rule in self._rules:
            value = self._get_nested(data, rule.field_path)
            if rule.rule_type == "required" and value is None:
                errors.append(f"Required field missing: {rule.field_path}")
            elif rule.rule_type == "equals" and value != rule.expected:
                errors.append(f"Field {rule.field_path} expected {rule.expected}, got {value}")
            elif rule.rule_type == "pattern" and rule.pattern:
                if not re.match(rule.pattern, str(value)):
                    errors.append(f"Field {rule.field_path} does not match pattern {rule.pattern}")
        
        return ValidationResult(valid=len(errors) == 0, errors=errors, warnings=warnings)
    
    def _get_nested(self, data: Any, path: str) -> Any:
        """Get nested value."""
        keys = path.split(".")
        current = data
        for key in keys:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
        return current


class APIValidationAction:
    """API validation action for automation workflows."""
    
    def __init__(self) -> None:
        self.validator = ResponseValidator()
    
    def add_required_field(self, field_path: str) -> None:
        """Add required field validation."""
        self.validator.add_rule(ValidationRule(rule_id=f"req_{field_path}", field_path=field_path, rule_type="required"))
    
    def add_status_check(self, expected: int) -> None:
        """Add expected status rule."""
        self._expected_status = expected
    
    async def validate(
        self,
        status_code: int,
        data: Any,
    ) -> ValidationResult:
        """Validate API response."""
        return self.validator.validate_response(status_code, data, getattr(self, "_expected_status", None))


__all__ = ["ValidationRule", "ValidationResult", "ResponseValidator", "APIValidationAction"]

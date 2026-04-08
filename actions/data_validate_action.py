"""
Data Validate Action - Validates data records.

This module provides data validation capabilities for
ensuring data quality and integrity.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass
class ValidationRule:
    """A validation rule."""
    field: str
    rule_type: str
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    """Result of validation."""
    valid: bool
    errors: list[str] = field(default_factory=list)


class DataValidator:
    """Validates data records."""
    
    def __init__(self) -> None:
        self._rules: list[ValidationRule] = []
    
    def add_rule(self, rule: ValidationRule) -> None:
        """Add a validation rule."""
        self._rules.append(rule)
    
    def validate(self, record: dict[str, Any]) -> ValidationResult:
        """Validate a record."""
        errors = []
        for rule in self._rules:
            value = record.get(rule.field)
            if rule.rule_type == "required" and (value is None or value == ""):
                errors.append(f"Field {rule.field} is required")
            elif rule.rule_type == "email" and value:
                if not re.match(r"^[^@]+@[^@]+\.[^@]+$", str(value)):
                    errors.append(f"Field {rule.field} is not a valid email")
            elif rule.rule_type == "min_length" and value:
                min_len = rule.params.get("value", 0)
                if len(str(value)) < min_len:
                    errors.append(f"Field {rule.field} must be at least {min_len} characters")
        return ValidationResult(valid=len(errors) == 0, errors=errors)


class DataValidateAction:
    """Data validation action for automation workflows."""
    
    def __init__(self) -> None:
        self.validator = DataValidator()
    
    def add_required(self, field: str) -> None:
        """Add required field rule."""
        self.validator.add_rule(ValidationRule(field=field, rule_type="required"))
    
    def add_email(self, field: str) -> None:
        """Add email validation rule."""
        self.validator.add_rule(ValidationRule(field=field, rule_type="email"))
    
    async def validate(self, record: dict[str, Any]) -> ValidationResult:
        """Validate a record."""
        return self.validator.validate(record)


__all__ = ["ValidationRule", "ValidationResult", "DataValidator", "DataValidateAction"]

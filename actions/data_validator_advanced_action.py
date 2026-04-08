# Copyright (c) 2024. coded by claude
"""Data Validator Advanced Action Module.

Advanced validation with cross-field validation, conditional rules,
and custom validator registration.
"""
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


@dataclass
class ValidationRule:
    name: str
    validator: Callable[[Dict[str, Any]], bool]
    error_message: str
    severity: str = "error"


@dataclass
class ValidationReport:
    passed: bool
    errors: List[str]
    warnings: List[str]
    info: List[str]


class DataValidatorAdvanced:
    def __init__(self):
        self._rules: List[ValidationRule] = []

    def add_rule(self, rule: ValidationRule) -> None:
        self._rules.append(rule)

    def add_simple_rule(
        self,
        name: str,
        field_name: str,
        validator: Callable[[Any], bool],
        error_message: str,
    ) -> None:
        def rule_fn(data: Dict[str, Any]) -> bool:
            value = data.get(field_name)
            return validator(value)
        self._rules.append(ValidationRule(name=name, validator=rule_fn, error_message=error_message))

    def validate(self, data: Dict[str, Any]) -> ValidationReport:
        errors: List[str] = []
        warnings: List[str] = []
        info: List[str] = []
        for rule in self._rules:
            try:
                if not rule.validator(data):
                    if rule.severity == "error":
                        errors.append(f"{rule.name}: {rule.error_message}")
                    elif rule.severity == "warning":
                        warnings.append(f"{rule.name}: {rule.error_message}")
                    else:
                        info.append(f"{rule.name}: {rule.error_message}")
            except Exception as e:
                errors.append(f"{rule.name}: Validation error - {e}")
        return ValidationReport(
            passed=len(errors) == 0,
            errors=errors,
            warnings=warnings,
            info=info,
        )

    def validate_cross_field(
        self,
        data: Dict[str, Any],
        field1: str,
        field2: str,
        validator: Callable[[Any, Any], bool],
        error_message: str,
    ) -> bool:
        value1 = data.get(field1)
        value2 = data.get(field2)
        return validator(value1, value2)

    def clear_rules(self) -> None:
        self._rules.clear()

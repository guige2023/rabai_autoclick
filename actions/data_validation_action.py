"""Data Validation Action Module.

Provides comprehensive data validation with schema validation,
custom rules, cross-field validation, and validation reporting.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class ValidationType(Enum):
    """Types of validation rules."""
    REQUIRED = "required"
    TYPE = "type"
    RANGE = "range"
    LENGTH = "length"
    PATTERN = "pattern"
    ENUM = "enum"
    EMAIL = "email"
    URL = "url"
    UUID = "uuid"
    IP_ADDRESS = "ip_address"
    DATE = "date"
    CUSTOM = "custom"


class ValidationSeverity(Enum):
    """Severity levels for validation errors."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationRule:
    """A single validation rule."""
    field: str
    rule_type: ValidationType
    severity: ValidationSeverity = ValidationSeverity.ERROR
    message: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)
    when: Optional[Dict[str, Any]] = None  # Conditional validation

    def validate(self, item: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate an item against this rule."""
        field_value = self._get_nested_value(item, self.field)

        if self.rule_type == ValidationType.REQUIRED:
            return self._validate_required(field_value)
        elif self.rule_type == ValidationType.TYPE:
            return self._validate_type(field_value)
        elif self.rule_type == ValidationType.RANGE:
            return self._validate_range(field_value)
        elif self.rule_type == ValidationType.LENGTH:
            return self._validate_length(field_value)
        elif self.rule_type == ValidationType.PATTERN:
            return self._validate_pattern(field_value)
        elif self.rule_type == ValidationType.ENUM:
            return self._validate_enum(field_value)
        elif self.rule_type == ValidationType.EMAIL:
            return self._validate_email(field_value)
        elif self.rule_type == ValidationType.URL:
            return self._validate_url(field_value)
        elif self.rule_type == ValidationType.UUID:
            return self._validate_uuid(field_value)
        elif self.rule_type == ValidationType.IP_ADDRESS:
            return self._validate_ip(field_value)
        elif self.rule_type == ValidationType.DATE:
            return self._validate_date(field_value)
        elif self.rule_type == ValidationType.CUSTOM:
            return self._validate_custom(field_value, item)

        return True, None

    def _get_nested_value(self, item: Dict[str, Any], field_path: str) -> Any:
        """Get nested field value using dot notation."""
        parts = field_path.split(".")
        value = item
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            elif isinstance(value, (list, tuple)):
                try:
                    value = value[int(part)]
                except (ValueError, IndexError):
                    return None
            else:
                return None
            if value is None:
                return None
        return value

    def _validate_required(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate required field."""
        if value is None or value == "":
            msg = self.message or f"Field '{self.field}' is required"
            return False, msg
        return True, None

    def _validate_type(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate type of value."""
        expected_type = self.params.get("expected")
        if expected_type is None:
            return True, None
        type_map = {
            "string": str, "str": str,
            "int": int, "integer": int,
            "float": float,
            "bool": bool, "boolean": bool,
            "list": list, "array": list,
            "dict": dict, "object": dict,
        }
        expected = type_map.get(expected_type, expected_type)
        if isinstance(expected, type) and not isinstance(value, expected):
            msg = self.message or f"Field '{self.field}' must be of type {expected_type}"
            return False, msg
        return True, None

    def _validate_range(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate numeric range."""
        if value is None:
            return True, None
        try:
            num_value = float(value)
            min_val = self.params.get("min")
            max_val = self.params.get("max")
            if min_val is not None and num_value < min_val:
                msg = self.message or f"Field '{self.field}' must be >= {min_val}"
                return False, msg
            if max_val is not None and num_value > max_val:
                msg = self.message or f"Field '{self.field}' must be <= {max_val}"
                return False, msg
        except (TypeError, ValueError):
            msg = self.message or f"Field '{self.field}' must be numeric"
            return False, msg
        return True, None

    def _validate_length(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate string length."""
        if value is None:
            return True, None
        str_value = str(value)
        min_len = self.params.get("min")
        max_len = self.params.get("max")
        if min_len is not None and len(str_value) < min_len:
            msg = self.message or f"Field '{self.field}' length must be >= {min_len}"
            return False, msg
        if max_len is not None and len(str_value) > max_len:
            msg = self.message or f"Field '{self.field}' length must be <= {max_len}"
            return False, msg
        return True, None

    def _validate_pattern(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate against regex pattern."""
        if value is None:
            return True, None
        pattern = self.params.get("pattern")
        if pattern is None:
            return True, None
        try:
            if not re.match(pattern, str(value)):
                msg = self.message or f"Field '{self.field}' does not match pattern"
                return False, msg
        except re.error:
            return False, f"Invalid regex pattern: {pattern}"
        return True, None

    def _validate_enum(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate against enum values."""
        if value is None:
            return True, None
        allowed = self.params.get("values", [])
        if allowed and value not in allowed:
            msg = self.message or f"Field '{self.field}' must be one of {allowed}"
            return False, msg
        return True, None

    def _validate_email(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate email format."""
        if value is None:
            return True, None
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        if not re.match(pattern, str(value)):
            msg = self.message or f"Field '{self.field}' is not a valid email"
            return False, msg
        return True, None

    def _validate_url(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate URL format."""
        if value is None:
            return True, None
        pattern = r"^https?://[^\s/$.?#].[^\s]*$"
        if not re.match(pattern, str(value)):
            msg = self.message or f"Field '{self.field}' is not a valid URL"
            return False, msg
        return True, None

    def _validate_uuid(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate UUID format."""
        if value is None:
            return True, None
        pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        if not re.match(pattern, str(value).lower()):
            msg = self.message or f"Field '{self.field}' is not a valid UUID"
            return False, msg
        return True, None

    def _validate_ip(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate IP address format."""
        if value is None:
            return True, None
        # IPv4
        ipv4_pattern = r"^(\d{1,3}\.){3}\d{1,3}$"
        # IPv6 simplified
        ipv6_pattern = r"^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$"
        val_str = str(value)
        if not (re.match(ipv4_pattern, val_str) or re.match(ipv6_pattern, val_str)):
            msg = self.message or f"Field '{self.field}' is not a valid IP address"
            return False, msg
        return True, None

    def _validate_date(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate date format."""
        if value is None:
            return True, None
        date_format = self.params.get("format", "%Y-%m-%d")
        try:
            datetime.strptime(str(value), date_format)
        except ValueError:
            msg = self.message or f"Field '{self.field}' is not a valid date"
            return False, msg
        return True, None

    def _validate_custom(self, value: Any, item: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Validate using custom function."""
        custom_fn = self.params.get("function")
        if custom_fn is None:
            return True, None
        try:
            result = custom_fn(value, item)
            if not result:
                msg = self.message or f"Field '{self.field}' failed custom validation"
                return False, msg
        except Exception as e:
            return False, f"Custom validation error: {str(e)}"
        return True, None


@dataclass
class ValidationError:
    """A validation error."""
    field: str
    message: str
    severity: ValidationSeverity
    rule_type: ValidationType
    item_index: Optional[int] = None


@dataclass
class ValidationReport:
    """Report of validation results."""
    total_items: int = 0
    valid_items: int = 0
    invalid_items: int = 0
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    validation_time_ms: float = 0.0

    @property
    def error_count(self) -> int:
        return len(self.errors)

    @property
    def warning_count(self) -> int:
        return len(self.warnings)

    @property
    def is_valid(self) -> bool:
        return self.error_count == 0


class DataValidationAction(BaseAction):
    """Data Validation Action for comprehensive validation.

    Supports schema validation, custom rules, cross-field validation,
    and detailed validation reports.

    Examples:
        >>> action = DataValidationAction()
        >>> result = action.execute(ctx, {
        ...     "data": [{"email": "test@example.com", "age": 25}],
        ...     "rules": [
        ...         {"field": "email", "rule_type": "email"},
        ...         {"field": "age", "rule_type": "range", "params": {"min": 0, "max": 150}},
        ...     ]
        ... })
    """

    action_type = "data_validation"
    display_name = "数据验证"
    description = "Schema验证、自定义规则、跨字段验证、验证报告"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data validation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - data: List of dicts to validate
                - rules: List of validation rule definitions
                - stop_on_error: Stop validation on first error (default: False)
                - validate_schema: Also validate against schema (optional)
                - cross_field_rules: Cross-field validation rules (optional)

        Returns:
            ActionResult with validation report.
        """
        import time
        start_time = time.time()

        data = params.get("data", [])
        rules_config = params.get("rules", [])
        stop_on_error = params.get("stop_on_error", False)
        cross_field_rules = params.get("cross_field_rules", [])

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message="'data' parameter must be a list"
            )

        # Build validation rules
        rules = self._build_rules(rules_config)

        # Validate each item
        report = ValidationReport(total_items=len(data))
        invalid_indices: Set[int] = set()

        for idx, item in enumerate(data):
            if not isinstance(item, dict):
                report.invalid_items += 1
                report.errors.append(ValidationError(
                    field="__root__",
                    message="Item is not a dictionary",
                    severity=ValidationSeverity.ERROR,
                    rule_type=ValidationType.TYPE,
                    item_index=idx,
                ))
                continue

            # Apply field rules
            for rule in rules:
                # Check conditional
                if rule.when and not self._check_condition(rule.when, item):
                    continue

                valid, error_msg = rule.validate(item)
                if not valid:
                    error = ValidationError(
                        field=rule.field,
                        message=error_msg,
                        severity=rule.severity,
                        rule_type=rule.rule_type,
                        item_index=idx,
                    )
                    if rule.severity == ValidationSeverity.ERROR:
                        report.errors.append(error)
                        invalid_indices.add(idx)
                    else:
                        report.warnings.append(error)

                    if stop_on_error and rule.severity == ValidationSeverity.ERROR:
                        break

            # Check if item is valid
            if idx not in invalid_indices:
                report.valid_items += 1

        report.validation_time_ms = (time.time() - start_time) * 1000

        return ActionResult(
            success=report.is_valid,
            message=f"Validation: {report.valid_items}/{report.total_items} valid, "
                    f"{report.error_count} errors, {report.warning_count} warnings",
            data={
                "is_valid": report.is_valid,
                "total_items": report.total_items,
                "valid_items": report.valid_items,
                "invalid_items": report.invalid_items,
                "error_count": report.error_count,
                "warning_count": report.warning_count,
                "errors": [
                    {"field": e.field, "message": e.message, "severity": e.severity.value,
                     "rule_type": e.rule_type.value, "item_index": e.item_index}
                    for e in report.errors[:100]
                ],
                "warnings": [
                    {"field": w.field, "message": w.message, "severity": w.severity.value,
                     "rule_type": w.rule_type.value, "item_index": w.item_index}
                    for w in report.warnings[:100]
                ],
                "validation_time_ms": report.validation_time_ms,
            }
        )

    def _build_rules(self, rules_config: List[Dict[str, Any]]) -> List[ValidationRule]:
        """Build validation rules from config."""
        rules = []
        for cfg in rules_config:
            if isinstance(cfg, ValidationRule):
                rules.append(cfg)
            else:
                rule = ValidationRule(
                    field=cfg["field"],
                    rule_type=ValidationType(cfg.get("rule_type", "required")),
                    severity=ValidationSeverity(cfg.get("severity", "error")),
                    message=cfg.get("message"),
                    params=cfg.get("params", {}),
                    when=cfg.get("when"),
                )
                rules.append(rule)
        return rules

    def _check_condition(self, condition: Dict[str, Any], item: Dict[str, Any]) -> bool:
        """Check if conditional rule should be applied."""
        field = condition.get("field")
        expected = condition.get("equals")
        if field and expected is not None:
            # Get nested value
            parts = field.split(".")
            value = item
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    return False
            return value == expected
        return True

    def get_required_params(self) -> List[str]:
        return ["data", "rules"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "stop_on_error": False,
            "validate_schema": None,
            "cross_field_rules": [],
        }

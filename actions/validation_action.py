"""Validation action for data validation and schema checking.

This module provides data validation with support for
type checking, range validation, and custom rules.

Example:
    >>> action = ValidationAction()
    >>> result = action.execute(value=5, rules=[{"type": "number", "min": 0, "max": 10}])
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class ValidationRule:
    """Represents a validation rule."""
    rule_type: str
    message: str = ""
    value: Any = None


@dataclass
class ValidationError:
    """Represents a validation error."""
    field: str
    message: str
    rule: str


class ValidationAction:
    """Data validation action.

    Provides validation for types, ranges, patterns,
    and custom validation rules.

    Example:
        >>> action = ValidationAction()
        >>> result = action.execute(
        ...     value="test@example.com",
        ...     rules=[{"type": "email"}]
        ... )
    """

    def __init__(self) -> None:
        """Initialize validation action."""
        self._last_errors: list[ValidationError] = []

    def execute(
        self,
        value: Any,
        rules: Optional[list[dict]] = None,
        field_name: str = "value",
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute validation.

        Args:
            value: Value to validate.
            rules: List of validation rule dicts.
            field_name: Name of field for error messages.
            **kwargs: Additional parameters.

        Returns:
            Validation result dictionary.
        """
        result: dict[str, Any] = {"success": True, "valid": True, "value": value}

        if not rules:
            return result

        errors: list[ValidationError] = []

        for rule_dict in rules:
            rule_type = rule_dict.get("type", "")
            rule_value = rule_dict.get("value")
            message = rule_dict.get("message", f"Validation failed for {rule_type}")

            is_valid = self._validate_rule(value, rule_type, rule_value)

            if not is_valid:
                errors.append(ValidationError(
                    field=field_name,
                    message=message,
                    rule=rule_type,
                ))

        self._last_errors = errors

        if errors:
            result["valid"] = False
            result["success"] = False
            result["errors"] = [
                {"field": e.field, "message": e.message, "rule": e.rule}
                for e in errors
            ]

        return result

    def _validate_rule(self, value: Any, rule_type: str, rule_value: Any) -> bool:
        """Validate a single rule.

        Args:
            value: Value to validate.
            rule_type: Type of rule.
            rule_value: Rule parameter value.

        Returns:
            True if valid.
        """
        if rule_type == "required":
            return value is not None and value != ""

        elif rule_type == "type":
            expected = rule_value.lower() if isinstance(rule_value, str) else rule_value
            if expected == "string":
                return isinstance(value, str)
            elif expected == "number" or expected == "int":
                return isinstance(value, (int, float))
            elif expected == "bool":
                return isinstance(value, bool)
            elif expected == "list":
                return isinstance(value, list)
            elif expected == "dict":
                return isinstance(value, dict)
            return type(value).__name__.lower() == expected

        elif rule_type == "min":
            try:
                return float(value) >= float(rule_value)
            except (TypeError, ValueError):
                return False

        elif rule_type == "max":
            try:
                return float(value) <= float(rule_value)
            except (TypeError, ValueError):
                return False

        elif rule_type == "min_length":
            return len(value) >= rule_value

        elif rule_type == "max_length":
            return len(value) <= rule_value

        elif rule_type == "equals":
            return value == rule_value

        elif rule_type == "not_equals":
            return value != rule_value

        elif rule_type == "in":
            return value in rule_value if isinstance(rule_value, list) else value == rule_value

        elif rule_type == "not_in":
            return value not in rule_value if isinstance(rule_value, list) else True

        elif rule_type == "contains":
            return str(rule_value) in str(value)

        elif rule_type == "email":
            pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            return bool(re.match(pattern, str(value)))

        elif rule_type == "url":
            pattern = r"^https?://[^\s<>\"]+$"
            return bool(re.match(pattern, str(value)))

        elif rule_type == "phone":
            pattern = r"^\+?[\d\s\-\(\)]+$"
            return bool(re.match(pattern, str(value)))

        elif rule_type == "ip":
            pattern = r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"
            return bool(re.match(pattern, str(value)))

        elif rule_type == "regex":
            try:
                return bool(re.search(rule_value, str(value)))
            except re.error:
                return False

        elif rule_type == "date":
            from dateutil import parser
            try:
                parser.parse(str(value))
                return True
            except Exception:
                return False

        elif rule_type == "json":
            import json
            try:
                json.loads(str(value))
                return True
            except Exception:
                return False

        elif rule_type == "uuid":
            pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
            return bool(re.match(pattern, str(value).lower()))

        elif rule_type == "credit_card":
            pattern = r"^\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}$"
            return bool(re.match(pattern, str(value)))

        return True

    def validate_dict(
        self,
        data: dict,
        schema: dict,
    ) -> dict[str, Any]:
        """Validate dictionary against schema.

        Args:
            data: Dictionary to validate.
            schema: Schema definition.

        Returns:
            Validation result.
        """
        errors: list[dict] = []

        for field_name, rules in schema.items():
            value = data.get(field_name)
            field_errors = self.execute(value, rules, field_name)
            if not field_errors.get("valid", True):
                errors.extend(field_errors.get("errors", []))

        return {
            "valid": len(errors) == 0,
            "errors": errors,
        }

    def get_errors(self) -> list[ValidationError]:
        """Get last validation errors.

        Returns:
            List of ValidationError objects.
        """
        return self._last_errors

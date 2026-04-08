"""
Data Validator Advanced Action Module.

Advanced data validation with cross-field rules, conditional validation,
custom validators, and validation error aggregation.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class ValidationError:
    """A validation error."""
    field: str
    value: Any
    rule: str
    message: str


@dataclass
class ValidationReport:
    """Complete validation report."""
    valid: bool
    record_count: int
    valid_count: int
    error_count: int
    errors: list[ValidationError]


class DataValidatorAdvancedAction(BaseAction):
    """Advanced data validation."""

    def __init__(self) -> None:
        super().__init__("data_validator_advanced")
        self._custom_validators: dict[str, Callable] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Validate records with advanced rules.

        Args:
            context: Execution context
            params: Parameters:
                - records: List of dict records
                - rules: Validation rules
                    - field: Field name
                    - rule: Rule type (required, type, range, pattern, custom)
                    - params: Rule parameters
                    - message: Error message
                - cross_field_rules: Rules comparing multiple fields
                - stop_on_first_error: Stop validation at first error

        Returns:
            ValidationReport with all errors
        """
        records = params.get("records", [])
        rules = params.get("rules", [])
        cross_field_rules = params.get("cross_field_rules", [])
        stop_on_first = params.get("stop_on_first_error", False)

        all_errors: list[ValidationError] = []
        valid_count = 0

        for record in records:
            if not isinstance(record, dict):
                continue

            record_errors = self._validate_record(record, rules, cross_field_rules)
            if record_errors:
                all_errors.extend(record_errors)
                if stop_on_first:
                    break
            else:
                valid_count += 1

        return ValidationReport(
            valid=len(all_errors) == 0,
            record_count=len(records),
            valid_count=valid_count,
            error_count=len(all_errors),
            errors=all_errors
        ).__dict__

    def _validate_record(self, record: dict, rules: list[dict], cross_field_rules: list[dict]) -> list[ValidationError]:
        """Validate a single record."""
        errors = []

        for rule in rules:
            field_name = rule.get("field", "")
            rule_type = rule.get("rule", "")
            rule_params = rule.get("params", {})
            message = rule.get("message", f"Validation failed for {field_name}")

            value = record.get(field_name)

            if rule_type == "required" and value is None:
                errors.append(ValidationError(field_name, value, rule_type, message))
                continue

            if value is None:
                continue

            if rule_type == "type":
                expected_type = rule_params.get("type", "")
                actual_type = type(value).__name__
                if expected_type == "string" and not isinstance(value, str):
                    errors.append(ValidationError(field_name, value, rule_type, message))
                elif expected_type == "number" and not isinstance(value, (int, float)):
                    errors.append(ValidationError(field_name, value, rule_type, message))
                elif expected_type == "boolean" and not isinstance(value, bool):
                    errors.append(ValidationError(field_name, value, rule_type, message))

            elif rule_type == "range":
                min_val = rule_params.get("min")
                max_val = rule_params.get("max")
                if isinstance(value, (int, float)):
                    if min_val is not None and value < min_val:
                        errors.append(ValidationError(field_name, value, rule_type, message))
                    if max_val is not None and value > max_val:
                        errors.append(ValidationError(field_name, value, rule_type, message))

            elif rule_type == "pattern":
                import re
                pattern = rule_params.get("pattern", "")
                if isinstance(value, str) and not re.match(pattern, value):
                    errors.append(ValidationError(field_name, value, rule_type, message))

            elif rule_type == "enum":
                allowed = rule_params.get("values", [])
                if value not in allowed:
                    errors.append(ValidationError(field_name, value, rule_type, message))

            elif rule_type == "custom":
                validator_name = rule_params.get("name", "")
                validator = self._custom_validators.get(validator_name)
                if validator and not validator(value):
                    errors.append(ValidationError(field_name, value, rule_type, message))

        for cross_rule in cross_field_rules:
            rule_name = cross_rule.get("name", "")
            condition = cross_rule.get("condition", "")
            then_field = cross_rule.get("then_field", "")
            then_rule = cross_rule.get("then_rule", {})
            message = cross_rule.get("message", f"Cross-field validation failed: {rule_name}")

            try:
                if eval(condition, {"r": record}):
                    value = record.get(then_field)
                    rule_type = then_rule.get("rule", "")
                    if rule_type == "required" and value is None:
                        errors.append(ValidationError(then_field, value, f"cross_{rule_type}", message))
            except Exception:
                pass

        return errors

    def register_validator(self, name: str, validator: Callable) -> None:
        """Register a custom validator function."""
        self._custom_validators[name] = validator

"""Validator action module for RabAI AutoClick.

Provides validation utilities:
- Validator: Data validation
- ValidationRule: Compose validation rules
- SchemaValidator: Schema-based validation
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import re
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ValidationError(Exception):
    """Validation error."""

    def __init__(self, field: str, message: str):
        self.field = field
        self.message = message
        super().__init__(f"{field}: {message}")


class ValidationRule:
    """Validation rule."""

    def __init__(self, name: str, validator: Callable[[Any], bool], message: str):
        self.name = name
        self.validator = validator
        self.message = message

    def validate(self, value: Any) -> Tuple[bool, Optional[str]]:
        """Validate value."""
        try:
            if self.validator(value):
                return True, None
            return False, self.message
        except Exception as e:
            return False, f"Validation error: {str(e)}"


class Validator:
    """Data validator with composable rules."""

    def __init__(self):
        self._rules: Dict[str, List[ValidationRule]] = {}

    def add_rule(self, field: str, rule: ValidationRule) -> None:
        """Add rule for field."""
        if field not in self._rules:
            self._rules[field] = []
        self._rules[field].append(rule)

    def validate(self, data: Dict[str, Any]) -> Tuple[bool, List[Dict[str, str]]]:
        """Validate data against rules."""
        errors = []

        for field, rules in self._rules.items():
            if field not in data:
                errors.append({"field": field, "error": "Missing field"})
                continue

            value = data[field]
            for rule in rules:
                valid, error_msg = rule.validate(value)
                if not valid and error_msg:
                    errors.append({"field": field, "error": error_msg})

        return len(errors) == 0, errors

    def get_rules(self, field: Optional[str] = None) -> Dict[str, List[str]]:
        """Get rule names."""
        if field:
            return {field: [r.name for r in self._rules.get(field, [])]}
        return {field: [r.name for field, rules in self._rules.items() for r in rules]}


class SchemaValidator:
    """Schema-based validator."""

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self._validator = Validator()
        self._build_from_schema()

    def _build_from_schema(self) -> None:
        """Build validator from schema."""
        for field, field_schema in self.schema.items():
            if "required" in field_schema and field_schema["required"]:
                self._validator.add_rule(
                    field,
                    ValidationRule("required", lambda v: v is not None, "Field is required"),
                )

            if "type" in field_schema:
                expected_type = field_schema["type"]
                self._validator.add_rule(
                    field,
                    ValidationRule(
                        "type_check",
                        lambda v, t=expected_type: isinstance(v, t),
                        f"Must be of type {expected_type}",
                    ),
                )

            if "min" in field_schema:
                min_val = field_schema["min"]
                self._validator.add_rule(
                    field,
                    ValidationRule("min", lambda v, m=min_val: v >= m, f"Must be >= {min_val}"),
                )

            if "max" in field_schema:
                max_val = field_schema["max"]
                self._validator.add_rule(
                    field,
                    ValidationRule("max", lambda v, m=max_val: v <= m, f"Must be <= {max_val}"),
                )

            if "pattern" in field_schema:
                pattern = field_schema["pattern"]
                self._validator.add_rule(
                    field,
                    ValidationRule("pattern", lambda v, p=pattern: bool(re.match(p, str(v))), f"Does not match pattern"),
                )

    def validate(self, data: Dict[str, Any]) -> Tuple[bool, List[Dict[str, str]]]:
        """Validate data against schema."""
        return self._validator.validate(data)


class ValidatorAction(BaseAction):
    """Validation action."""
    action_type = "validator"
    display_name = "数据验证"
    description = "数据校验"

    def __init__(self):
        super().__init__()
        self._validators: Dict[str, Validator] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "validate")

            if operation == "create":
                return self._create(params)
            elif operation == "add_rule":
                return self._add_rule(params)
            elif operation == "validate":
                return self._validate(params)
            elif operation == "validate_schema":
                return self._validate_schema(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Validator error: {str(e)}")

    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create a validator."""
        name = params.get("name", str(uuid.uuid4()))

        validator = Validator()
        self._validators[name] = validator

        return ActionResult(success=True, message=f"Validator created: {name}", data={"name": name})

    def _add_rule(self, params: Dict[str, Any]) -> ActionResult:
        """Add a validation rule."""
        name = params.get("name")
        field = params.get("field")
        rule_name = params.get("rule_name")
        rule_type = params.get("rule_type", "required")
        rule_message = params.get("message", "Invalid")

        validator = self._validators.get(name)
        if not validator:
            return ActionResult(success=False, message=f"Validator not found: {name}")

        if not field or not rule_name:
            return ActionResult(success=False, message="field and rule_name are required")

        if rule_type == "required":
            rule = ValidationRule(rule_name, lambda v: v is not None, rule_message)
        elif rule_type == "type":
            expected_type = params.get("expected_type", str)
            rule = ValidationRule(rule_name, lambda v, t=expected_type: isinstance(v, t), rule_message)
        elif rule_type == "min":
            min_val = params.get("min_value", 0)
            rule = ValidationRule(rule_name, lambda v, m=min_val: v >= m, rule_message)
        elif rule_type == "max":
            max_val = params.get("max_value", 0)
            rule = ValidationRule(rule_name, lambda v, m=max_val: v <= m, rule_message)
        elif rule_type == "pattern":
            pattern = params.get("pattern", ".*")
            rule = ValidationRule(rule_name, lambda v, p=pattern: bool(re.match(p, str(v))), rule_message)
        else:
            rule = ValidationRule(rule_name, lambda v: True, rule_message)

        validator.add_rule(field, rule)

        return ActionResult(success=True, message=f"Rule added: {rule_name}")

    def _validate(self, params: Dict[str, Any]) -> ActionResult:
        """Validate data."""
        name = params.get("name")
        data = params.get("data", {})

        validator = self._validators.get(name)
        if not validator:
            return ActionResult(success=False, message=f"Validator not found: {name}")

        valid, errors = validator.validate(data)

        return ActionResult(success=valid, message="Valid" if valid else f"{len(errors)} errors", data={"valid": valid, "errors": errors})

    def _validate_schema(self, params: Dict[str, Any]) -> ActionResult:
        """Validate against schema."""
        schema = params.get("schema", {})
        data = params.get("data", {})

        if not schema:
            return ActionResult(success=False, message="schema is required")

        validator = SchemaValidator(schema)
        valid, errors = validator.validate(data)

        return ActionResult(success=valid, message="Valid" if valid else f"{len(errors)} errors", data={"valid": valid, "errors": errors})

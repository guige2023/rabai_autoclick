"""API Validation Action Module.

Validates API requests and responses against schemas,
constraints, and business rules.
"""

from __future__ import annotations

import sys
import os
import time
import re
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class ValidationRule:
    """A validation rule."""
    field: str
    rule_type: str
    constraint: Any = None
    message: str = ""


class APIValidationAction(BaseAction):
    """
    API request/response validation.

    Validates API payloads against schemas,
    constraints, and custom rules.

    Example:
        validator = APIValidationAction()
        result = validator.execute(ctx, {"action": "validate", "data": {"email": "test@test.com"}, "rules": [{"field": "email", "rule_type": "email"}]})
    """
    action_type = "api_validation"
    display_name = "API验证"
    description = "API请求/响应验证"

    def __init__(self) -> None:
        super().__init__()
        self._rules: List[ValidationRule] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "validate":
                return self._validate(params)
            elif action == "add_rule":
                return self._add_rule(params)
            elif action == "validate_schema":
                return self._validate_schema(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Validation error: {str(e)}")

    def _validate(self, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", {})
        rules = params.get("rules", [])

        if not isinstance(data, dict):
            return ActionResult(success=False, message="data must be a dictionary")

        errors: List[Dict[str, str]] = []

        for rule_data in rules:
            rule = ValidationRule(field=rule_data.get("field", ""), rule_type=rule_data.get("rule_type", ""), constraint=rule_data.get("constraint"), message=rule_data.get("message", f"Validation failed for {rule_data.get('field')}"))

            error = self._validate_field(data, rule)
            if error:
                errors.append(error)

        if errors:
            return ActionResult(success=False, message="Validation failed", data={"errors": errors})

        return ActionResult(success=True, message="Validation passed")

    def _validate_field(self, data: Dict[str, Any], rule: ValidationRule) -> Optional[Dict[str, str]]:
        value = data.get(rule.field)

        if rule.rule_type == "required" and (value is None or value == ""):
            return {"field": rule.field, "message": rule.message or "Field is required"}

        if value is None or value == "":
            return None

        if rule.rule_type == "email":
            if not re.match(r"^[\w.-]+@[\w.-]+\.\w+$", str(value)):
                return {"field": rule.field, "message": rule.message or "Invalid email format"}

        elif rule.rule_type == "min_length":
            min_len = rule.constraint or 0
            if len(str(value)) < min_len:
                return {"field": rule.field, "message": rule.message or f"Minimum length is {min_len}"}

        elif rule.rule_type == "max_length":
            max_len = rule.constraint or float("inf")
            if len(str(value)) > max_len:
                return {"field": rule.field, "message": rule.message or f"Maximum length is {max_len}"}

        elif rule.rule_type == "min":
            if isinstance(value, (int, float)) and value < rule.constraint:
                return {"field": rule.field, "message": rule.message or f"Minimum value is {rule.constraint}"}

        elif rule.rule_type == "max":
            if isinstance(value, (int, float)) and value > rule.constraint:
                return {"field": rule.field, "message": rule.message or f"Maximum value is {rule.constraint}"}

        elif rule.rule_type == "pattern":
            if not re.match(rule.constraint, str(value)):
                return {"field": rule.field, "message": rule.message or "Pattern mismatch"}

        elif rule.rule_type == "enum":
            if rule.constraint and value not in rule.constraint:
                return {"field": rule.field, "message": rule.message or f"Value must be one of: {rule.constraint}"}

        elif rule.rule_type == "type":
            expected_type = rule.constraint
            type_map = {"string": str, "number": (int, float), "integer": int, "boolean": bool, "array": list, "object": dict}
            if expected_type in type_map:
                if not isinstance(value, type_map[expected_type]):
                    return {"field": rule.field, "message": rule.message or f"Expected type {expected_type}"}

        return None

    def _add_rule(self, params: Dict[str, Any]) -> ActionResult:
        field_name = params.get("field", "")
        rule_type = params.get("rule_type", "")
        constraint = params.get("constraint")
        message = params.get("message", "")

        if not field_name or not rule_type:
            return ActionResult(success=False, message="field and rule_type are required")

        rule = ValidationRule(field=field_name, rule_type=rule_type, constraint=constraint, message=message)
        self._rules.append(rule)

        return ActionResult(success=True, message=f"Rule added: {field_name} {rule_type}")

    def _validate_schema(self, params: Dict[str, Any]) -> ActionResult:
        data = params.get("data", {})
        schema = params.get("schema", {})

        errors: List[Dict[str, str]] = []

        required_fields = schema.get("required", [])
        properties = schema.get("properties", {})

        for field_name in required_fields:
            if field_name not in data:
                errors.append({"field": field_name, "message": f"Required field missing: {field_name}"})

        for field_name, field_schema in properties.items():
            if field_name in data:
                value = data[field_name]
                field_type = field_schema.get("type")

                if field_type == "string" and not isinstance(value, str):
                    errors.append({"field": field_name, "message": f"Expected string"})
                elif field_type in ("number", "integer") and not isinstance(value, (int, float)):
                    errors.append({"field": field_name, "message": f"Expected number"})

        if errors:
            return ActionResult(success=False, message="Schema validation failed", data={"errors": errors})

        return ActionResult(success=True, message="Schema validation passed")

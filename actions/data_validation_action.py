"""Data validation action module for RabAI AutoClick.

Provides data validation:
- DataValidationAction: Validate data
- SchemaValidatorAction: Validate against schema
- TypeValidatorAction: Validate data types
"""

import re
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataValidationAction(BaseAction):
    """Validate data against rules."""
    action_type = "data_validation"
    display_name = "数据验证"
    description = "根据规则验证数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            rules = params.get("rules", [])

            if not isinstance(data, dict):
                return ActionResult(success=False, message="data must be a dict")

            violations = []
            for rule in rules:
                field = rule.get("field", "")
                rule_type = rule.get("type", "required")
                value = data.get(field)

                if rule_type == "required" and value is None:
                    violations.append({"field": field, "rule": rule_type, "message": f"{field} is required"})
                elif rule_type == "min_length" and value and len(str(value)) < rule.get("min", 0):
                    violations.append({"field": field, "rule": rule_type, "message": f"{field} too short"})
                elif rule_type == "pattern" and value:
                    pattern = rule.get("pattern", "")
                    if not re.match(pattern, str(value)):
                        violations.append({"field": field, "rule": rule_type, "message": f"{field} doesn't match pattern"})

            return ActionResult(
                success=len(violations) == 0,
                data={
                    "valid": len(violations) == 0,
                    "violations_count": len(violations),
                    "violations": violations,
                    "rules_checked": len(rules)
                },
                message=f"Validation: {'PASSED' if len(violations) == 0 else 'FAILED'} ({len(violations)} violations)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data validation error: {str(e)}")


class SchemaValidatorAction(BaseAction):
    """Validate against schema."""
    action_type = "schema_validator"
    display_name = "Schema验证"
    description = "根据Schema验证"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            schema = params.get("schema", {})

            errors = []
            required_fields = schema.get("required", [])
            for field in required_fields:
                if field not in data:
                    errors.append(f"Missing required field: {field}")

            field_types = schema.get("properties", {})
            for field, field_schema in field_types.items():
                if field in data:
                    expected_type = field_schema.get("type", "string")
                    actual_value = data[field]
                    if expected_type == "string" and not isinstance(actual_value, str):
                        errors.append(f"{field} must be string")
                    elif expected_type == "number" and not isinstance(actual_value, (int, float)):
                        errors.append(f"{field} must be number")

            return ActionResult(
                success=len(errors) == 0,
                data={
                    "valid": len(errors) == 0,
                    "errors": errors
                },
                message=f"Schema validation: {'PASSED' if len(errors) == 0 else 'FAILED'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema validator error: {str(e)}")


class TypeValidatorAction(BaseAction):
    """Validate data types."""
    action_type = "type_validator"
    display_name = "类型验证"
    description = "验证数据类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            expected_types = params.get("expected_types", {})

            type_errors = []
            for field, expected_type in expected_types.items():
                value = data.get(field)
                actual_type = type(value).__name__

                if expected_type == "string" and not isinstance(value, str):
                    type_errors.append(f"{field}: expected {expected_type}, got {actual_type}")
                elif expected_type == "number" and not isinstance(value, (int, float)):
                    type_errors.append(f"{field}: expected {expected_type}, got {actual_type}")
                elif expected_type == "boolean" and not isinstance(value, bool):
                    type_errors.append(f"{field}: expected {expected_type}, got {actual_type}")
                elif expected_type == "array" and not isinstance(value, list):
                    type_errors.append(f"{field}: expected {expected_type}, got {actual_type}")
                elif expected_type == "object" and not isinstance(value, dict):
                    type_errors.append(f"{field}: expected {expected_type}, got {actual_type}")

            return ActionResult(
                success=len(type_errors) == 0,
                data={
                    "valid": len(type_errors) == 0,
                    "errors": type_errors,
                    "fields_validated": len(expected_types)
                },
                message=f"Type validation: {'PASSED' if len(type_errors) == 0 else 'FAILED'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Type validator error: {str(e)}")

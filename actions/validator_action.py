"""Validator action module for RabAI AutoClick.

Provides data validation operations:
- ValidateSchemaAction: Validate against schema
- ValidateRulesAction: Validate with rules
- ValidateRangeAction: Validate numeric ranges
- ValidateFormatAction: Validate format patterns
"""

import re
from typing import Any, Callable, Dict, List, Optional


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ValidateSchemaAction(BaseAction):
    """Validate data against a schema."""
    action_type = "validate_schema"
    display_name = "Schema验证"
    description = "根据Schema验证数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            schema = params.get("schema", {})
            strict = params.get("strict", False)

            if not data:
                return ActionResult(success=False, message="data is required")
            if not schema:
                return ActionResult(success=False, message="schema is required")

            errors = []

            for field_name, field_schema in schema.items():
                required = field_schema.get("required", False)
                field_type = field_schema.get("type", "string")
                value = data.get(field_name)

                if value is None:
                    if required:
                        errors.append({"field": field_name, "error": "required field is missing"})
                    continue

                if field_type == "string":
                    if not isinstance(value, str):
                        errors.append({"field": field_name, "error": f"expected string, got {type(value).__name__}"})
                elif field_type == "number" or field_type == "integer":
                    if not isinstance(value, (int, float)):
                        errors.append({"field": field_name, "error": f"expected number, got {type(value).__name__}"})
                elif field_type == "boolean":
                    if not isinstance(value, bool):
                        errors.append({"field": field_name, "error": f"expected boolean, got {type(value).__name__}"})
                elif field_type == "array":
                    if not isinstance(value, (list, tuple)):
                        errors.append({"field": field_name, "error": f"expected array, got {type(value).__name__}"})
                elif field_type == "object":
                    if not isinstance(value, dict):
                        errors.append({"field": field_name, "error": f"expected object, got {type(value).__name__}"})

                if field_type == "string" and isinstance(value, str):
                    min_length = field_schema.get("min_length")
                    max_length = field_schema.get("max_length")
                    pattern = field_schema.get("pattern")
                    if min_length and len(value) < min_length:
                        errors.append({"field": field_name, "error": f"string length {len(value)} < min_length {min_length}"})
                    if max_length and len(value) > max_length:
                        errors.append({"field": field_name, "error": f"string length {len(value)} > max_length {max_length}"})
                    if pattern and not re.match(pattern, value):
                        errors.append({"field": field_name, "error": f"string does not match pattern {pattern}"})

                if field_type in ("number", "integer") and isinstance(value, (int, float)):
                    min_val = field_schema.get("min")
                    max_val = field_schema.get("max")
                    if min_val is not None and value < min_val:
                        errors.append({"field": field_name, "error": f"value {value} < min {min_val}"})
                    if max_val is not None and value > max_val:
                        errors.append({"field": field_name, "error": f"value {value} > max {max_val}"})

                if field_type == "array" and isinstance(value, (list, tuple)):
                    min_items = field_schema.get("min_items")
                    max_items = field_schema.get("max_items")
                    if min_items and len(value) < min_items:
                        errors.append({"field": field_name, "error": f"array length {len(value)} < min_items {min_items}"})
                    if max_items and len(value) > max_items:
                        errors.append({"field": field_name, "error": f"array length {len(value)} > max_items {max_items}"})

            if strict:
                for key in data:
                    if key not in schema:
                        errors.append({"field": key, "error": "unknown field in strict mode"})

            return ActionResult(
                success=len(errors) == 0,
                message=f"Validation: {'PASSED' if len(errors) == 0 else f'FAILED ({len(errors)} errors)'}",
                data={"valid": len(errors) == 0, "errors": errors}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Schema validation failed: {str(e)}")


class ValidateRulesAction(BaseAction):
    """Validate with custom rules."""
    action_type = "validate_rules"
    display_name = "规则验证"
    description = "使用规则验证数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            rules = params.get("rules", [])

            if not data:
                return ActionResult(success=False, message="data is required")
            if not rules:
                return ActionResult(success=True, message="No rules to validate")

            errors = []

            for rule in rules:
                rule_name = rule.get("name", "unnamed")
                field = rule.get("field", "")
                rule_type = rule.get("type", "")
                params_rule = rule.get("params", {})

                parts = field.split(".")
                current = data
                for part in parts:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        errors.append({"rule": rule_name, "field": field, "error": "field not found"})
                        continue

                if rule_type == "required":
                    if current is None or (isinstance(current, str) and not current):
                        errors.append({"rule": rule_name, "field": field, "error": "required field is empty"})
                elif rule_type == "min_length":
                    min_len = params_rule.get("value", 0)
                    if not isinstance(current, (str, list, tuple)) or len(current) < min_len:
                        errors.append({"rule": rule_name, "field": field, "error": f"length < {min_len}"})
                elif rule_type == "max_length":
                    max_len = params_rule.get("value", 0)
                    if not isinstance(current, (str, list, tuple)) or len(current) > max_len:
                        errors.append({"rule": rule_name, "field": field, "error": f"length > {max_len}"})
                elif rule_type == "pattern":
                    pattern = params_rule.get("value", "")
                    if not isinstance(current, str) or not re.match(pattern, current):
                        errors.append({"rule": rule_name, "field": field, "error": f"does not match pattern {pattern}"})
                elif rule_type == "enum":
                    allowed = params_rule.get("values", [])
                    if current not in allowed:
                        errors.append({"rule": rule_name, "field": field, "error": f"value not in {allowed}"})

            return ActionResult(
                success=len(errors) == 0,
                message=f"Rules validation: {'PASSED' if len(errors) == 0 else f'FAILED ({len(errors)} errors)'}",
                data={"valid": len(errors) == 0, "errors": errors}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Rules validation failed: {str(e)}")


class ValidateRangeAction(BaseAction):
    """Validate numeric ranges."""
    action_type = "validate_range"
    display_name = "范围验证"
    description = "验证数值范围"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            field = params.get("field", "")
            min_val = params.get("min", None)
            max_val = params.get("max", None)
            exclusive = params.get("exclusive", False)

            parts = field.split(".")
            current = data
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return ActionResult(success=False, message=f"Field '{field}' not found")

            try:
                value = float(current)
            except (ValueError, TypeError):
                return ActionResult(success=False, message=f"Value '{current}' is not numeric")

            errors = []
            if min_val is not None:
                if exclusive:
                    if value <= min_val:
                        errors.append(f"value {value} must be > {min_val}")
                else:
                    if value < min_val:
                        errors.append(f"value {value} must be >= {min_val}")

            if max_val is not None:
                if exclusive:
                    if value >= max_val:
                        errors.append(f"value {value} must be < {max_val}")
                else:
                    if value > max_val:
                        errors.append(f"value {value} must be <= {max_val}")

            return ActionResult(
                success=len(errors) == 0,
                message=f"Range validation: {'PASSED' if len(errors) == 0 else 'FAILED'}",
                data={"valid": len(errors) == 0, "value": value, "errors": errors}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Range validation failed: {str(e)}")


class ValidateFormatAction(BaseAction):
    """Validate format patterns."""
    action_type = "validate_format"
    display_name = "格式验证"
    description = "验证格式模式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            field = params.get("field", "")
            format_type = params.get("format", "email")

            parts = field.split(".")
            current = data
            for part in parts:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return ActionResult(success=False, message=f"Field '{field}' not found")

            if not isinstance(current, str):
                return ActionResult(success=False, message=f"Value must be a string for format validation")

            patterns = {
                "email": r"^[\w\.-]+@[\w\.-]+\.\w+$",
                "url": r"^https?://[\w\.-]+\.\w+",
                "ipv4": r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",
                "ipv6": r"^[\w:]+$",
                "phone": r"^\+?[\d\s\-\(\)]{7,}$",
                "date": r"^\d{4}-\d{2}-\d{2}$",
                "datetime": r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}",
                "uuid": r"^[\w]{8}-[\w]{4}-[\w]{4}-[\w]{4}-[\w]{12}$",
                "hex_color": r"^#[\da-fA-F]{6}$",
                "credit_card": r"^\d{4}[\s\-]?\d{4}[\s\-]?\d{4}[\s\-]?\d{4}$",
            }

            pattern = params.get("pattern", patterns.get(format_type, ""))
            if not pattern:
                return ActionResult(success=False, message=f"Unknown format: {format_type}")

            is_valid = bool(re.match(pattern, current))

            return ActionResult(
                success=is_valid,
                message=f"Format '{format_type}': {'PASSED' if is_valid else 'FAILED'}",
                data={"valid": is_valid, "format": format_type, "value": current}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Format validation failed: {str(e)}")

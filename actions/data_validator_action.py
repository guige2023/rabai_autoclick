"""Data validator action module for RabAI AutoClick.

Provides comprehensive data validation:
- SchemaValidatorAction: Validate data against schemas
- DataTypeValidatorAction: Validate data types
- RangeValidatorAction: Validate numeric ranges
- FormatValidatorAction: Validate string formats
- CrossFieldValidatorAction: Cross-field validation
"""

import re
from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SchemaValidatorAction(BaseAction):
    """Validate data against schemas."""
    action_type = "data_schema_validator"
    display_name = "Schema验证器"
    description = "根据Schema验证数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            schema = params.get("schema", {})
            strict = params.get("strict", False)

            errors = []
            warnings = []

            required_fields = schema.get("required", [])
            field_schemas = schema.get("fields", {})

            for field in required_fields:
                if field not in data:
                    errors.append(f"Required field missing: {field}")

            for field, field_schema in field_schemas.items():
                if field not in data:
                    continue

                value = data[field]
                expected_type = field_schema.get("type")
                if expected_type:
                    type_valid = self._validate_type(value, expected_type)
                    if not type_valid:
                        errors.append(f"Field '{field}': expected {expected_type}, got {type(value).__name__}")

                constraints = field_schema.get("constraints", {})
                constraint_errors = self._validate_constraints(value, constraints)
                errors.extend([f"Field '{field}': {e}" for e in constraint_errors])

            is_valid = len(errors) == 0

            return ActionResult(
                success=is_valid,
                data={
                    "is_valid": is_valid,
                    "errors": errors,
                    "warnings": warnings,
                    "fields_validated": len(field_schemas),
                    "required_fields": len(required_fields),
                    "missing_required": [f for f in required_fields if f not in data]
                },
                message=f"Schema validation: {'passed' if is_valid else f'failed ({len(errors)} errors)'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema validator error: {str(e)}")

    def _validate_type(self, value: Any, expected_type: str) -> bool:
        type_map = {
            "string": str,
            "integer": int,
            "number": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict,
            "null": type(None)
        }
        expected = type_map.get(expected_type)
        if expected is None:
            return True
        return isinstance(value, expected)

    def _validate_constraints(self, value: Any, constraints: Dict) -> List[str]:
        errors = []
        if "min_length" in constraints and isinstance(value, (str, list)) and len(value) < constraints["min_length"]:
            errors.append(f"min_length: {len(value)} < {constraints['min_length']}")
        if "max_length" in constraints and isinstance(value, (str, list)) and len(value) > constraints["max_length"]:
            errors.append(f"max_length: {len(value)} > {constraints['max_length']}")
        if "min" in constraints and isinstance(value, (int, float)) and value < constraints["min"]:
            errors.append(f"min: {value} < {constraints['min']}")
        if "max" in constraints and isinstance(value, (int, float)) and value > constraints["max"]:
            errors.append(f"max: {value} > {constraints['max']}")
        if "pattern" in constraints and isinstance(value, str):
            if not re.match(constraints["pattern"], value):
                errors.append(f"pattern mismatch")
        if "enum" in constraints and value not in constraints["enum"]:
            errors.append(f"not in enum: {constraints['enum']}")
        return errors

    def get_required_params(self) -> List[str]:
        return ["data", "schema"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"strict": False}


class DataTypeValidatorAction(BaseAction):
    """Validate data types."""
    action_type = "data_type_validator"
    display_name = "类型验证器"
    description = "验证数据类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            expected_types = params.get("expected_types", {})
            coerce_types = params.get("coerce_types", False)

            errors = []
            validated = {}
            coerced = {}

            for field, expected_type in expected_types.items():
                value = data.get(field)
                if value is None:
                    if params.get("allow_null", False):
                        validated[field] = {"type": "null", "valid": True, "value": None}
                    else:
                        errors.append(f"Field '{field}': null value not allowed")
                        validated[field] = {"type": "null", "valid": False, "value": None}
                    continue

                actual_type = type(value).__name__
                type_valid = actual_type == expected_type

                if not type_valid and coerce_types:
                    try:
                        if expected_type == "string":
                            coerced_value = str(value)
                            coerced[field] = {"from": actual_type, "to": "string", "value": coerced_value}
                            value = coerced_value
                            type_valid = True
                        elif expected_type == "integer":
                            coerced_value = int(value)
                            coerced[field] = {"from": actual_type, "to": "integer", "value": coerced_value}
                            value = coerced_value
                            type_valid = True
                        elif expected_type == "float":
                            coerced_value = float(value)
                            coerced[field] = {"from": actual_type, "to": "float", "value": coerced_value}
                            value = coerced_value
                            type_valid = True
                    except (ValueError, TypeError):
                        type_valid = False

                validated[field] = {"type": actual_type, "valid": type_valid, "value": value}
                if not type_valid:
                    errors.append(f"Field '{field}': expected {expected_type}, got {actual_type}")

            is_valid = len(errors) == 0

            return ActionResult(
                success=is_valid,
                data={
                    "is_valid": is_valid,
                    "errors": errors,
                    "validated": validated,
                    "coerced": coerced,
                    "fields_validated": len(expected_types)
                },
                message=f"Type validation: {'passed' if is_valid else f'failed ({len(errors)} errors)'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Type validator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"expected_types": {}, "coerce_types": False, "allow_null": False}


class RangeValidatorAction(BaseAction):
    """Validate numeric ranges."""
    action_type = "data_range_validator"
    display_name = "范围验证器"
    description = "验证数值范围"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            ranges = params.get("ranges", {})
            inclusive = params.get("inclusive", True)

            errors = []
            validated = {}

            for field, range_spec in ranges.items():
                value = data.get(field)
                if value is None:
                    continue

                if not isinstance(value, (int, float)):
                    errors.append(f"Field '{field}': non-numeric value {type(value).__name__}")
                    validated[field] = {"valid": False, "reason": "non-numeric"}
                    continue

                min_val = range_spec.get("min")
                max_val = range_spec.get("max")
                min_exclusive = range_spec.get("min_exclusive", False)
                max_exclusive = range_spec.get("max_exclusive", False)

                if min_val is not None:
                    if inclusive and not min_exclusive:
                        min_ok = value >= min_val
                    else:
                        min_ok = value > min_val
                    if not min_ok:
                        errors.append(f"Field '{field}': {value} < min {min_val}")
                        validated[field] = {"valid": False, "reason": "below_min", "value": value, "min": min_val}
                        continue

                if max_val is not None:
                    if inclusive and not max_exclusive:
                        max_ok = value <= max_val
                    else:
                        max_ok = value < max_val
                    if not max_ok:
                        errors.append(f"Field '{field}': {value} > max {max_val}")
                        validated[field] = {"valid": False, "reason": "above_max", "value": value, "max": max_val}
                        continue

                validated[field] = {"valid": True, "value": value, "min": min_val, "max": max_val}

            is_valid = len(errors) == 0

            return ActionResult(
                success=is_valid,
                data={
                    "is_valid": is_valid,
                    "errors": errors,
                    "validated": validated,
                    "ranges_checked": len(ranges)
                },
                message=f"Range validation: {'passed' if is_valid else f'failed ({len(errors)} errors)'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Range validator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"ranges": {}, "inclusive": True}


class FormatValidatorAction(BaseAction):
    """Validate string formats."""
    action_type = "data_format_validator"
    display_name = "格式验证器"
    description = "验证字符串格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            formats = params.get("formats", {})

            errors = []
            validated = {}

            format_patterns = {
                "email": r"^[\w\.-]+@[\w\.-]+\.\w+$",
                "url": r"^https?://[\w\.-]+\.\w+",
                "phone": r"^\+?[\d\s\-\(\)]{10,}$",
                "ipv4": r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",
                "date_iso": r"^\d{4}-\d{2}-\d{2}$",
                "datetime_iso": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
                "uuid": r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                "hex_color": r"^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$",
                "postal_code_us": r"^\d{5}(-\d{4})?$",
                "credit_card": r"^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$",
            }

            for field, format_spec in formats.items():
                value = data.get(field)
                if value is None:
                    continue

                value_str = str(value)

                if format_spec in format_patterns:
                    pattern = format_patterns[format_spec]
                else:
                    pattern = format_spec

                is_match = bool(re.match(pattern, value_str))

                validated[field] = {"valid": is_match, "format": format_spec, "value": value_str}
                if not is_match:
                    errors.append(f"Field '{field}': format '{format_spec}' not matched")

            is_valid = len(errors) == 0

            return ActionResult(
                success=is_valid,
                data={
                    "is_valid": is_valid,
                    "errors": errors,
                    "validated": validated,
                    "formats_checked": len(formats)
                },
                message=f"Format validation: {'passed' if is_valid else f'failed ({len(errors)} errors)'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Format validator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"formats": {}}


class CrossFieldValidatorAction(BaseAction):
    """Cross-field validation."""
    action_type = "data_crossfield_validator"
    display_name = "跨字段验证器"
    description = "跨字段交叉验证"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            rules = params.get("rules", [])

            errors = []
            validated = {}

            for rule in rules:
                rule_name = rule.get("name", "unnamed")
                fields = rule.get("fields", [])
                condition = rule.get("condition", "eq")
                value = rule.get("value")
                error_msg = rule.get("error_msg", f"Cross-field rule '{rule_name}' failed")

                field_values = [data.get(f) for f in fields]

                if condition == "eq":
                    is_valid = all(fv == field_values[0] for fv in field_values)
                elif condition == "ne":
                    is_valid = len(set(str(fv) for fv in field_values)) == len(field_values)
                elif condition == "gt":
                    is_valid = all(field_values[i] > field_values[i-1] for i in range(1, len(field_values)))
                elif condition == "lt":
                    is_valid = all(field_values[i] < field_values[i-1] for i in range(1, len(field_values)))
                elif condition == "sum_eq":
                    is_valid = sum(field_values) == value
                elif condition == "sum_lt":
                    is_valid = sum(field_values) < value
                elif condition == "sum_gt":
                    is_valid = sum(field_values) > value
                elif condition == "matches":
                    pattern = rule.get("pattern", "")
                    is_valid = all(re.match(pattern, str(fv)) for fv in field_values)
                else:
                    is_valid = True

                validated[rule_name] = {"valid": is_valid, "fields": fields, "condition": condition}
                if not is_valid:
                    errors.append(error_msg)

            is_valid = len(errors) == 0

            return ActionResult(
                success=is_valid,
                data={
                    "is_valid": is_valid,
                    "errors": errors,
                    "validated": validated,
                    "rules_checked": len(rules)
                },
                message=f"Cross-field validation: {'passed' if is_valid else f'failed ({len(errors)} errors)'}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cross-field validator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"rules": []}

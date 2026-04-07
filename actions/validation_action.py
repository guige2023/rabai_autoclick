"""Validation action module for RabAI AutoClick.

Provides data validation operations:
- ValidateSchemaAction: Validate against schema
- ValidateTypeAction: Validate data types
- ValidateRangeAction: Validate numeric ranges
- ValidatePatternAction: Validate patterns
- ValidateRequiredAction: Validate required fields
- ValidateCustomAction: Custom validation rules
"""

import re
from typing import Any, Dict, List, Optional, Callable

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ValidateSchemaAction(BaseAction):
    """Validate against schema."""
    action_type = "validate_schema"
    display_name = "Schema验证"
    description = "根据Schema验证数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            schema = params.get("schema", {})

            if not records:
                return ActionResult(success=False, message="No records to validate")

            if not schema:
                return ActionResult(success=False, message="Schema is required")

            valid_records = []
            invalid_records = []
            errors_by_field = {}

            for i, record in enumerate(records):
                if not isinstance(record, dict):
                    record = {"value": record}

                record_errors = self._validate_record(record, schema)

                if record_errors:
                    invalid_records.append({"record": record, "errors": record_errors})
                    for field, error in record_errors.items():
                        if field not in errors_by_field:
                            errors_by_field[field] = []
                        errors_by_field[field].append({"record_index": i, "error": error})
                else:
                    valid_records.append(record)

            return ActionResult(
                success=True,
                message=f"Valid: {len(valid_records)}, Invalid: {len(invalid_records)}",
                data={
                    "valid_count": len(valid_records),
                    "invalid_count": len(invalid_records),
                    "valid_records": valid_records[:10],
                    "invalid_records": invalid_records[:10],
                    "errors_by_field": errors_by_field
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Schema validation error: {str(e)}")

    def _validate_record(self, record: Dict, schema: Dict) -> Dict[str, str]:
        errors = {}

        if "required" in schema:
            for field in schema["required"]:
                if field not in record or record[field] is None or record[field] == "":
                    errors[field] = f"Required field missing: {field}"

        if "properties" in schema:
            for field, field_schema in schema["properties"].items():
                if field in record:
                    field_errors = self._validate_field(record[field], field_schema, field)
                    errors.update(field_errors)

        return errors

    def _validate_field(self, value: Any, field_schema: Dict, field_name: str) -> Dict[str, str]:
        errors = {}

        if "type" in field_schema:
            expected_type = field_schema["type"]
            type_map = {
                "string": str, "number": (int, float), "integer": int,
                "boolean": bool, "array": list, "object": dict, "null": type(None)
            }
            if expected_type in type_map:
                if not isinstance(value, type_map[expected_type]):
                    errors[field_name] = f"Expected {expected_type}, got {type(value).__name__}"

        if "enum" in field_schema:
            if value not in field_schema["enum"]:
                errors[field_name] = f"Value not in enum: {field_schema['enum']}"

        if "minLength" in field_schema and isinstance(value, str):
            if len(value) < field_schema["minLength"]:
                errors[field_name] = f"String length {len(value)} < minLength {field_schema['minLength']}"

        if "maxLength" in field_schema and isinstance(value, str):
            if len(value) > field_schema["maxLength"]:
                errors[field_name] = f"String length {len(value)} > maxLength {field_schema['maxLength']}"

        if "minimum" in field_schema:
            try:
                if float(value) < field_schema["minimum"]:
                    errors[field_name] = f"Value {value} < minimum {field_schema['minimum']}"
            except:
                pass

        if "maximum" in field_schema:
            try:
                if float(value) > field_schema["maximum"]:
                    errors[field_name] = f"Value {value} > maximum {field_schema['maximum']}"
            except:
                pass

        return errors


class ValidateTypeAction(BaseAction):
    """Validate data types."""
    action_type = "validate_type"
    display_name = "类型验证"
    description = "验证数据类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            field = params.get("field", "")
            expected_type = params.get("type", "string")

            if not records:
                return ActionResult(success=False, message="No records to validate")

            type_map = {
                "string": str, "str": str,
                "number": (int, float), "num": (int, float),
                "integer": int, "int": int,
                "float": float,
                "boolean": bool, "bool": bool,
                "array": list, "list": list,
                "dict": dict, "object": dict
            }

            expected = type_map.get(expected_type.lower(), str)

            valid = []
            invalid = []

            for i, record in enumerate(records):
                value = record.get(field) if isinstance(record, dict) else record

                if isinstance(value, expected):
                    valid.append(record)
                else:
                    invalid.append({"record": record, "expected": expected_type, "actual": type(value).__name__})

            return ActionResult(
                success=True,
                message=f"Valid: {len(valid)}, Invalid: {len(invalid)}",
                data={"valid_count": len(valid), "invalid_count": len(invalid), "invalid": invalid[:10]}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Type validation error: {str(e)}")


class ValidateRangeAction(BaseAction):
    """Validate numeric ranges."""
    action_type = "validate_range"
    display_name = "范围验证"
    description = "验证数值范围"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            field = params.get("field", "")
            min_val = params.get("min", None)
            max_val = params.get("max", None)
            exclusive = params.get("exclusive", False)

            if not records:
                return ActionResult(success=False, message="No records to validate")

            valid = []
            invalid = []

            for record in records:
                value = record.get(field) if isinstance(record, dict) else record

                try:
                    num_value = float(value)
                except (TypeError, ValueError):
                    invalid.append({"record": record, "error": f"Non-numeric value: {value}"})
                    continue

                is_valid = True
                error = None

                if min_val is not None:
                    if exclusive:
                        if num_value <= min_val:
                            is_valid = False
                            error = f"{num_value} <= min {min_val}"
                    else:
                        if num_value < min_val:
                            is_valid = False
                            error = f"{num_value} < min {min_val}"

                if max_val is not None and is_valid:
                    if exclusive:
                        if num_value >= max_val:
                            is_valid = False
                            error = f"{num_value} >= max {max_val}"
                    else:
                        if num_value > max_val:
                            is_valid = False
                            error = f"{num_value} > max {max_val}"

                if is_valid:
                    valid.append(record)
                else:
                    invalid.append({"record": record, "error": error})

            return ActionResult(
                success=True,
                message=f"Valid: {len(valid)}, Invalid: {len(invalid)}",
                data={"valid_count": len(valid), "invalid_count": len(invalid), "invalid": invalid[:10]}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Range validation error: {str(e)}")


class ValidatePatternAction(BaseAction):
    """Validate patterns."""
    action_type = "validate_pattern"
    display_name = "模式验证"
    description = "验证正则模式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            field = params.get("field", "")
            pattern = params.get("pattern", "")
            regex = params.get("regex", True)

            if not records:
                return ActionResult(success=False, message="No records to validate")

            if not pattern:
                return ActionResult(success=False, message="pattern is required")

            try:
                if regex:
                    compiled = re.compile(pattern)
                else:
                    compiled = re.compile(re.escape(pattern))
            except re.error as e:
                return ActionResult(success=False, message=f"Invalid pattern: {str(e)}")

            valid = []
            invalid = []

            for record in records:
                value = record.get(field) if isinstance(record, dict) else record

                if value is None:
                    invalid.append({"record": record, "error": "None value"})
                    continue

                value_str = str(value)
                if compiled.match(value_str):
                    valid.append(record)
                else:
                    invalid.append({"record": record, "error": f"Pattern not matched: {pattern}"})

            return ActionResult(
                success=True,
                message=f"Valid: {len(valid)}, Invalid: {len(invalid)}",
                data={"valid_count": len(valid), "invalid_count": len(invalid), "invalid": invalid[:10]}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Pattern validation error: {str(e)}")


class ValidateRequiredAction(BaseAction):
    """Validate required fields."""
    action_type = "validate_required"
    display_name = "必填验证"
    description = "验证必填字段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            required_fields = params.get("fields", [])

            if not records:
                return ActionResult(success=False, message="No records to validate")

            if isinstance(required_fields, str):
                required_fields = [required_fields]

            if not required_fields:
                return ActionResult(success=False, message="fields list is required")

            valid = []
            invalid = []

            for record in records:
                if not isinstance(record, dict):
                    record = {"value": record}

                missing = []
                for field in required_fields:
                    if field not in record or record[field] is None or record[field] == "":
                        missing.append(field)

                if not missing:
                    valid.append(record)
                else:
                    invalid.append({"record": record, "missing_fields": missing})

            return ActionResult(
                success=True,
                message=f"Valid: {len(valid)}, Invalid: {len(invalid)}",
                data={"valid_count": len(valid), "invalid_count": len(invalid), "invalid": invalid[:10]}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Required validation error: {str(e)}")


class ValidateCustomAction(BaseAction):
    """Custom validation rules."""
    action_type = "validate_custom"
    display_name = "自定义验证"
    description = "自定义验证规则"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            records = params.get("records", [])
            rules = params.get("rules", [])

            if not records:
                return ActionResult(success=False, message="No records to validate")

            if not rules:
                return ActionResult(success=False, message="rules list is required")

            valid = []
            invalid = []

            for record in records:
                if not isinstance(record, dict):
                    record = {"value": record}

                record_errors = []

                for rule in rules:
                    rule_name = rule.get("name", "unnamed")
                    field = rule.get("field", "")
                    condition = rule.get("condition", "")

                    value = record.get(field) if field else record

                    try:
                        for k, v in record.items():
                            condition = condition.replace(k, repr(v))
                        passed = eval(condition, {"__builtins__": {}}, {})
                        if not passed:
                            record_errors.append(f"{rule_name}: condition failed")
                    except Exception as e:
                        record_errors.append(f"{rule_name}: {str(e)}")

                if record_errors:
                    invalid.append({"record": record, "errors": record_errors})
                else:
                    valid.append(record)

            return ActionResult(
                success=True,
                message=f"Valid: {len(valid)}, Invalid: {len(invalid)}",
                data={"valid_count": len(valid), "invalid_count": len(invalid), "invalid": invalid[:10]}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Custom validation error: {str(e)}")

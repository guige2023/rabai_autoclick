"""Data validation advanced action module for RabAI AutoClick.

Provides advanced data validation operations:
- SchemaValidationAction: Validate data against JSON schemas
- CrossFieldValidationAction: Validate relationships between fields
- BusinessRuleValidationAction: Validate business rules
- RecordValidationAction: Record-level validation with error tracking
"""

import re
from typing import Any, Dict, List, Optional, Set, Tuple
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SchemaValidationAction(BaseAction):
    """Validate data against JSON schemas."""
    action_type = "schema_validation"
    display_name = "模式验证"
    description = "根据JSON Schema验证数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            schema = params.get("schema", {})
            strict = params.get("strict", False)

            if not isinstance(data, list):
                data = [data]

            if not schema:
                return ActionResult(success=False, message="schema is required")

            violations = []
            for i, record in enumerate(data):
                if not isinstance(record, dict):
                    violations.append({"index": i, "error": "Record must be an object"})
                    continue

                record_violations = self._validate_object(record, schema, "", strict)
                for v in record_violations:
                    v["index"] = i
                violations.extend(record_violations)

            valid_count = len(data) - len([v for v in violations if "index" in v])
            invalid_indices = set(v.get("index", -1) for v in violations)

            return ActionResult(
                success=len(violations) == 0,
                message=f"Schema validation: {valid_count}/{len(data)} valid records",
                data={
                    "valid_count": valid_count,
                    "invalid_count": len(violations),
                    "violations": violations,
                    "invalid_indices": sorted(invalid_indices),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SchemaValidation error: {e}")

    def _validate_object(self, obj: Dict, schema: Dict, path: str, strict: bool) -> List[Dict]:
        violations = []

        required_fields = schema.get("required", [])
        for field in required_fields:
            if field not in obj or obj[field] is None:
                violations.append({"path": f"{path}.{field}" if path else field, "error": "required field missing"})

        properties = schema.get("properties", {})
        for field, field_schema in properties.items():
            if field not in obj:
                continue
            value = obj[field]
            field_violations = self._validate_value(value, field_schema, f"{path}.{field}" if path else field, strict)
            violations.extend(field_violations)

        if strict:
            for field in obj.keys():
                if field not in properties:
                    violations.append({"path": f"{path}.{field}" if path else field, "error": "unknown field"})

        return violations

    def _validate_value(self, value: Any, field_schema: Dict, path: str, strict: bool) -> List[Dict]:
        violations = []

        if value is None:
            if field_schema.get("required") and not field_schema.get("nullable", True):
                violations.append({"path": path, "error": "null value not allowed"})
            return violations

        expected_type = field_schema.get("type")
        if expected_type:
            type_map = {"string": str, "integer": int, "number": (int, float), "boolean": bool, "array": list, "object": dict, "null": type(None)}
            if isinstance(expected_type, list):
                valid = any(isinstance(value, type_map.get(t)) for t in expected_type if t in type_map)
            else:
                valid = isinstance(value, type_map.get(expected_type, object))
            if not valid:
                violations.append({"path": path, "error": f"expected {expected_type}, got {type(value).__name__}"})

        if "enum" in field_schema:
            if value not in field_schema["enum"]:
                violations.append({"path": path, "error": f"value must be one of {field_schema['enum']}"})

        if "minLength" in field_schema and isinstance(value, str):
            if len(value) < field_schema["minLength"]:
                violations.append({"path": path, "error": f"string too short, min length {field_schema['minLength']}"})

        if "maxLength" in field_schema and isinstance(value, str):
            if len(value) > field_schema["maxLength"]:
                violations.append({"path": path, "error": f"string too long, max length {field_schema['maxLength']}"})

        if "pattern" in field_schema and isinstance(value, str):
            if not re.match(field_schema["pattern"], value):
                violations.append({"path": path, "error": f"string does not match pattern {field_schema['pattern']}"})

        if "minimum" in field_schema and isinstance(value, (int, float)):
            if value < field_schema["minimum"]:
                violations.append({"path": path, "error": f"value below minimum {field_schema['minimum']}"})

        if "maximum" in field_schema and isinstance(value, (int, float)):
            if value > field_schema["maximum"]:
                violations.append({"path": path, "error": f"value above maximum {field_schema['maximum']}"})

        if "minItems" in field_schema and isinstance(value, list):
            if len(value) < field_schema["minItems"]:
                violations.append({"path": path, "error": f"array too short, min items {field_schema['minItems']}"})

        if "maxItems" in field_schema and isinstance(value, list):
            if len(value) > field_schema["maxItems"]:
                violations.append({"path": path, "error": f"array too long, max items {field_schema['maxItems']}"})

        if "items" in field_schema and isinstance(value, list):
            item_schema = field_schema["items"]
            for i, item in enumerate(value):
                item_violations = self._validate_value(item, item_schema, f"{path}[{i}]", strict)
                violations.extend(item_violations)

        return violations


class CrossFieldValidationAction(BaseAction):
    """Validate relationships between fields."""
    action_type = "cross_field_validation"
    display_name = "跨字段验证"
    description = "验证字段之间的关系"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            rules = params.get("rules", [])

            if not isinstance(data, list):
                data = [data]

            if not rules:
                return ActionResult(success=False, message="rules is required")

            violations = []

            for i, record in enumerate(data):
                if not isinstance(record, dict):
                    continue

                for rule in rules:
                    rule_name = rule.get("name", "unnamed")
                    condition = rule.get("condition", {})
                    error_msg = rule.get("error", "Validation failed")

                    condition_met = self._evaluate_condition(record, condition)
                    if condition_met and rule.get("must_be", True) or not condition_met and not rule.get("must_be", True):
                        pass
                    else:
                        violations.append({"index": i, "rule": rule_name, "error": error_msg, "record": record})

            valid_count = len(data) - len(violations)

            return ActionResult(
                success=len(violations) == 0,
                message=f"Cross-field validation: {valid_count}/{len(data)} valid records",
                data={"valid_count": valid_count, "violations": violations, "violation_count": len(violations)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CrossFieldValidation error: {e}")

    def _evaluate_condition(self, record: Dict, condition: Dict) -> bool:
        op = condition.get("operator")
        left_field = condition.get("left_field")
        right_field = condition.get("right_field")
        left_value = condition.get("left_value")
        right_value = condition.get("right_value")

        lval = record.get(left_field, left_value) if left_field else left_value
        rval = record.get(right_field, right_value) if right_field else right_value

        if op == "eq":
            return lval == rval
        elif op == "ne":
            return lval != rval
        elif op == "gt":
            return lval is not None and rval is not None and lval > rval
        elif op == "ge":
            return lval is not None and rval is not None and lval >= rval
        elif op == "lt":
            return lval is not None and rval is not None and lval < rval
        elif op == "le":
            return lval is not None and rval is not None and lval <= rval
        elif op == "sum_lt":
            return isinstance(lval, (int, float)) and isinstance(rval, (int, float)) and (lval + rval) < condition.get("threshold", 0)
        elif op == "len_gt":
            return len(lval) > rval if lval is not None else False
        elif op == "contains":
            return rval in lval if lval is not None else False
        elif op == "startswith":
            return str(lval).startswith(str(rval)) if lval is not None else False
        elif op == "endswith":
            return str(lval).endswith(str(rval)) if lval is not None else False

        return False


class BusinessRuleValidationAction(BaseAction):
    """Validate business rules."""
    action_type = "business_rule_validation"
    display_name = "业务规则验证"
    description = "验证业务规则"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            rules = params.get("rules", [])

            if not isinstance(data, list):
                data = [data]

            if not rules:
                return ActionResult(success=False, message="rules is required")

            violations = []

            for i, record in enumerate(data):
                if not isinstance(record, dict):
                    continue

                for rule in rules:
                    rule_id = rule.get("id", "unnamed")
                    rule_type = rule.get("type", "expression")
                    expression = rule.get("expression", "")
                    error_msg = rule.get("error", f"Business rule {rule_id} violated")

                    passed = False
                    if rule_type == "expression":
                        try:
                            passed = bool(eval(expression, {"item": record, "__builtins__": {}}))
                        except Exception:
                            passed = False
                    elif rule_type == "range":
                        field = rule.get("field")
                        min_val = rule.get("min")
                        max_val = rule.get("max")
                        val = record.get(field)
                        if val is not None:
                            passed = (min_val is None or val >= min_val) and (max_val is None or val <= max_val)
                    elif rule_type == "membership":
                        field = rule.get("field")
                        allowed = set(rule.get("allowed_values", []))
                        val = record.get(field)
                        passed = val in allowed
                    elif rule_type == "uniqueness":
                        fields = rule.get("fields", [])
                        key = tuple(record.get(f) for f in fields)
                        passed = True
                    elif rule_type == "referential":
                        ref_field = rule.get("ref_field")
                        ref_data = rule.get("ref_data", [])
                        val = record.get(ref_field)
                        passed = any(record.get(ref_field) == r.get(ref_field) for r in ref_data)

                    if not passed:
                        violations.append({"index": i, "rule_id": rule_id, "error": error_msg})

            valid_count = len(data) - len(violations)

            return ActionResult(
                success=len(violations) == 0,
                message=f"Business rule validation: {valid_count}/{len(data)} valid",
                data={"valid_count": valid_count, "violations": violations, "violation_count": len(violations)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"BusinessRuleValidation error: {e}")


class RecordValidationAction(BaseAction):
    """Record-level validation with error tracking."""
    action_type = "record_validation"
    display_name = "记录验证"
    description = "带错误追踪的记录级验证"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            validators = params.get("validators", [])
            fail_fast = params.get("fail_fast", False)

            if not isinstance(data, list):
                data = [data]

            if not validators:
                return ActionResult(success=True, message="No validators defined", data={"data": data, "valid": data, "invalid": []})

            valid_records = []
            invalid_records = []

            for i, record in enumerate(data):
                if not isinstance(record, dict):
                    if fail_fast:
                        return ActionResult(success=False, message=f"Record {i} is not a dict")
                    invalid_records.append({"index": i, "record": record, "errors": ["Record must be an object"]})
                    continue

                record_errors = []
                for validator in validators:
                    v_type = validator.get("type", "field_exists")
                    field = validator.get("field")

                    if v_type == "field_exists":
                        if field not in record:
                            record_errors.append(f"Required field '{field}' is missing")
                    elif v_type == "not_null":
                        if field in record and record[field] is None:
                            record_errors.append(f"Field '{field}' must not be null")
                    elif v_type == "not_empty":
                        if field in record and (record[field] == "" or record[field] == [] or record[field] == {}):
                            record_errors.append(f"Field '{field}' must not be empty")
                    elif v_type == "type_check":
                        expected = validator.get("expected_type")
                        type_map = {"str": str, "int": int, "float": (int, float), "bool": bool, "list": list, "dict": dict}
                        if field in record:
                            val = record[field]
                            if val is not None and not isinstance(val, type_map.get(expected, object)):
                                record_errors.append(f"Field '{field}' must be {expected}, got {type(val).__name__}")
                    elif v_type == "regex":
                        pattern = validator.get("pattern")
                        if field in record and isinstance(record[field], str):
                            if not re.match(pattern, record[field]):
                                record_errors.append(f"Field '{field}' does not match pattern {pattern}")
                    elif v_type == "range":
                        min_val = validator.get("min")
                        max_val = validator.get("max")
                        if field in record:
                            val = record[field]
                            if isinstance(val, (int, float)):
                                if min_val is not None and val < min_val:
                                    record_errors.append(f"Field '{field}' is below minimum {min_val}")
                                if max_val is not None and val > max_val:
                                    record_errors.append(f"Field '{field}' exceeds maximum {max_val}")
                    elif v_type == "custom":
                        func = validator.get("func")
                        if func:
                            try:
                                result = func(record)
                                if not result:
                                    record_errors.append(f"Custom validation failed for '{field}'")
                            except Exception as e:
                                record_errors.append(f"Custom validation error: {e}")

                if record_errors:
                    invalid_records.append({"index": i, "record": record, "errors": record_errors})
                else:
                    valid_records.append(record)

            return ActionResult(
                success=len(invalid_records) == 0,
                message=f"Record validation: {len(valid_records)} valid, {len(invalid_records)} invalid",
                data={
                    "valid": valid_records,
                    "invalid": invalid_records,
                    "valid_count": len(valid_records),
                    "invalid_count": len(invalid_records),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"RecordValidation error: {e}")

"""Data inspector action module for RabAI AutoClick.

Provides data inspection operations:
- DataInspectorAction: Inspect data structure
- TypeCheckerAction: Check data types
- SchemaValidatorAction: Validate data schemas
- DataDumperAction: Dump data for debugging
- DataTracerAction: Trace data transformations
"""

from typing import Any, Dict, List, Optional, get_type_hints
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataInspectorAction(BaseAction):
    """Inspect data structure."""
    action_type = "data_inspector"
    display_name = "数据检查"
    description = "检查数据结构"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", None)
            depth = params.get("depth", 3)
            include_values = params.get("include_values", False)

            if data is None:
                return ActionResult(success=False, message="data is required")

            inspection = {
                "type": type(data).__name__,
                "size": self._get_size(data),
                "structure": self._inspect_structure(data, depth, include_values),
                "is_empty": self._is_empty(data),
                "inspected_at": datetime.now().isoformat()
            }

            return ActionResult(
                success=True,
                data=inspection,
                message=f"Data inspected: {inspection['type']}, size={inspection['size']}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data inspector error: {str(e)}")

    def _get_size(self, data: Any) -> int:
        if isinstance(data, dict):
            return len(data)
        elif isinstance(data, (list, tuple, set)):
            return len(data)
        elif isinstance(data, str):
            return len(data)
        return 1

    def _is_empty(self, data: Any) -> bool:
        if data is None:
            return True
        if isinstance(data, (dict, list, tuple, set, str)):
            return len(data) == 0
        return False

    def _inspect_structure(self, data: Any, depth: int, include_values: bool, current_depth: int = 0) -> Any:
        if current_depth >= depth:
            return "..." if depth > 0 else None

        if isinstance(data, dict):
            result = {}
            for k, v in list(data.items())[:20]:
                if isinstance(v, (dict, list)):
                    result[k] = self._inspect_structure(v, depth, include_values, current_depth + 1)
                else:
                    result[k] = {"type": type(v).__name__}
                    if include_values:
                        result[k]["value"] = str(v)[:100]
            return result
        elif isinstance(data, list):
            if not data:
                return []
            sample = data[:3]
            return [self._inspect_structure(item, depth, include_values, current_depth + 1) for item in sample]
        else:
            return {"type": type(data).__name__, "value": str(data)[:100]} if include_values else type(data).__name__


class TypeCheckerAction(BaseAction):
    """Check data types."""
    action_type = "type_checker"
    display_name = "类型检查"
    description = "检查数据类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", None)
            expected_type = params.get("expected_type", None)
            allow_none = params.get("allow_none", True)

            if data is None:
                return ActionResult(success=False, message="data is required")

            actual_type = type(data).__name__
            type_matches = True

            if expected_type:
                type_matches = actual_type == expected_type

            checks = {
                "is_none": data is None,
                "is_dict": isinstance(data, dict),
                "is_list": isinstance(data, list),
                "is_tuple": isinstance(data, tuple),
                "is_set": isinstance(data, set),
                "is_string": isinstance(data, str),
                "is_int": isinstance(data, int),
                "is_float": isinstance(data, float),
                "is_bool": isinstance(data, bool),
                "is_dict": isinstance(data, dict)
            }

            return ActionResult(
                success=type_matches if expected_type else True,
                data={
                    "actual_type": actual_type,
                    "expected_type": expected_type,
                    "type_matches": type_matches,
                    "checks": checks,
                    "allow_none": allow_none
                },
                message=f"Type check: {actual_type}" + (f" {'matches' if type_matches else 'does not match'} {expected_type}" if expected_type else "")
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Type checker error: {str(e)}")


class SchemaValidatorAction(BaseAction):
    """Validate data schemas."""
    action_type = "schema_validator"
    display_name = "Schema验证"
    description = "验证数据Schema"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", None)
            schema = params.get("schema", {})

            if data is None:
                return ActionResult(success=False, message="data is required")

            if not isinstance(data, dict):
                return ActionResult(
                    success=False,
                    data={"error": "Data must be a dictionary"},
                    message="Data must be a dictionary"
                )

            errors = []
            warnings = []

            required_fields = schema.get("required", [])
            for field in required_fields:
                if field not in data:
                    errors.append(f"Required field '{field}' is missing")

            field_schemas = schema.get("fields", {})
            for field, field_schema in field_schemas.items():
                if field in data:
                    expected_type = field_schema.get("type")
                    if expected_type:
                        actual_type = type(data[field]).__name__
                        if actual_type != expected_type:
                            errors.append(f"Field '{field}': expected {expected_type}, got {actual_type}")

                    if field_schema.get("nullable") == False and data[field] is None:
                        errors.append(f"Field '{field}' is not nullable")

            return ActionResult(
                success=len(errors) == 0,
                data={
                    "valid": len(errors) == 0,
                    "errors": errors,
                    "warnings": warnings,
                    "error_count": len(errors),
                    "warning_count": len(warnings)
                },
                message=f"Schema validation: {'PASSED' if len(errors) == 0 else 'FAILED'} ({len(errors)} errors)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schema validator error: {str(e)}")


class DataDumperAction(BaseAction):
    """Dump data for debugging."""
    action_type = "data_dumper"
    display_name = "数据导出"
    description = "导出数据用于调试"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", None)
            format_type = params.get("format", "json")
            max_depth = params.get("max_depth", 5)
            truncate = params.get("truncate", 1000)

            if data is None:
                return ActionResult(success=False, message="data is required")

            import json

            def truncate_value(v, depth):
                if depth > max_depth:
                    return "..."
                if isinstance(v, dict):
                    return {kk: truncate_value(vv, depth + 1) for kk, vv in list(v.items())[:10]}
                if isinstance(v, list):
                    return [truncate_value(item, depth + 1) for item in v[:10]]
                if isinstance(v, str) and len(v) > truncate:
                    return v[:truncate] + "..."
                return v

            truncated = truncate_value(data, 0)

            if format_type == "json":
                dumped = json.dumps(truncated, indent=2, ensure_ascii=False)
            elif format_type == "repr":
                dumped = repr(truncated)
            else:
                dumped = str(truncated)

            return ActionResult(
                success=True,
                data={
                    "format": format_type,
                    "length": len(dumped),
                    "truncated": truncate_value(data, 0) != data,
                    "dump": dumped[:5000]
                },
                message=f"Data dumped: {len(dumped)} chars in {format_type} format"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data dumper error: {str(e)}")


class DataTracerAction(BaseAction):
    """Trace data transformations."""
    action_type = "data_tracer"
    display_name = "数据追踪"
    description = "追踪数据转换"

    def __init__(self):
        super().__init__()
        self._traces = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "trace")
            trace_id = params.get("trace_id", "default")
            data = params.get("data", None)
            transformation = params.get("transformation", "")

            if operation == "start":
                self._traces[trace_id] = {
                    "id": trace_id,
                    "steps": [],
                    "started_at": datetime.now().isoformat()
                }
                return ActionResult(
                    success=True,
                    data={"trace_id": trace_id, "started": True},
                    message=f"Trace '{trace_id}' started"
                )

            elif operation == "add":
                if trace_id not in self._traces:
                    self._traces[trace_id] = {
                        "id": trace_id,
                        "steps": [],
                        "started_at": datetime.now().isoformat()
                    }
                self._traces[trace_id]["steps"].append({
                    "data": str(data)[:200] if data else None,
                    "transformation": transformation,
                    "added_at": datetime.now().isoformat()
                })
                return ActionResult(
                    success=True,
                    data={
                        "trace_id": trace_id,
                        "step_count": len(self._traces[trace_id]["steps"])
                    },
                    message=f"Step added to trace '{trace_id}': {transformation}"
                )

            elif operation == "get":
                if trace_id not in self._traces:
                    return ActionResult(success=False, message=f"Trace '{trace_id}' not found")
                return ActionResult(
                    success=True,
                    data=self._traces[trace_id],
                    message=f"Retrieved trace '{trace_id}': {len(self._traces[trace_id]['steps'])} steps"
                )

            elif operation == "clear":
                if trace_id in self._traces:
                    del self._traces[trace_id]
                return ActionResult(
                    success=True,
                    data={"trace_id": trace_id, "cleared": True},
                    message=f"Trace '{trace_id}' cleared"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Data tracer error: {str(e)}")

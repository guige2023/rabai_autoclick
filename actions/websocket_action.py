"""JSON data processing action module for RabAI AutoClick.

Provides JSON operations:
- JsonParseAction: Parse JSON string
- JsonEncodeAction: Encode object to JSON
- JsonPathAction: Extract data using JSONPath
- JsonMergeAction: Merge multiple JSON objects
- JsonFlattenAction: Flatten nested JSON structure
- JsonValidateAction: Validate JSON against schema
"""

import json
import re
from typing import Any, Dict, List, Optional, Union

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class JsonParseAction(BaseAction):
    """Parse JSON string to Python object."""
    action_type = "json_parse"
    display_name = "JSON解析"
    description = "解析JSON字符串为Python对象"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            json_str = params.get("json_str", "")
            if not json_str:
                return ActionResult(success=False, message="json_str is required")

            try:
                data = json.loads(json_str)
                return ActionResult(
                    success=True,
                    message="JSON parsed successfully",
                    data={"data": data, "type": type(data).__name__}
                )
            except json.JSONDecodeError as e:
                return ActionResult(success=False, message=f"JSON decode error: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class JsonEncodeAction(BaseAction):
    """Encode Python object to JSON string."""
    action_type = "json_encode"
    display_name = "JSON编码"
    description = "将Python对象编码为JSON字符串"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            if data is None:
                return ActionResult(success=False, message="data is required")

            indent = params.get("indent", None)
            sort_keys = params.get("sort_keys", False)
            ensure_ascii = params.get("ensure_ascii", True)

            try:
                json_str = json.dumps(
                    data,
                    indent=indent,
                    sort_keys=sort_keys,
                    ensure_ascii=ensure_ascii
                )
                return ActionResult(
                    success=True,
                    message="JSON encoded successfully",
                    data={"json_str": json_str, "length": len(json_str)}
                )
            except (TypeError, ValueError) as e:
                return ActionResult(success=False, message=f"JSON encode error: {str(e)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")


class JsonPathAction(BaseAction):
    """Extract data using JSONPath-like patterns."""
    action_type = "json_path"
    display_name = "JSONPath提取"
    description = "使用JSONPath模式提取数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            path = params.get("path", "")
            default = params.get("default", None)

            if data is None:
                return ActionResult(success=False, message="data is required")

            if not path:
                return ActionResult(success=False, message="path is required")

            result = self._json_path(data, path)

            if result is None:
                return ActionResult(success=True, message="Path not found", data={"value": default})
            else:
                return ActionResult(success=True, message="Path extracted", data={"value": result})

        except Exception as e:
            return ActionResult(success=False, message=f"JSONPath error: {str(e)}")

    def _json_path(self, obj: Any, path: str) -> Any:
        """Simple JSONPath implementation supporting dot notation and array indices."""
        current = obj
        parts = path.replace("[", ".[").split(".")

        for part in parts:
            if not part:
                continue

            if part.startswith("[") and part.endswith("]"):
                index_str = part[1:-1]
                if index_str == "*":
                    if isinstance(current, (list, tuple)):
                        return current
                    return None
                try:
                    index = int(index_str)
                    if isinstance(current, (list, tuple)):
                        if -len(current) <= index < len(current):
                            current = current[index]
                        else:
                            return None
                    else:
                        return None
                except ValueError:
                    if isinstance(current, dict):
                        current = current.get(index_str)
                        if current is None:
                            return None
                    else:
                        return None
            else:
                if isinstance(current, dict):
                    current = current.get(part)
                    if current is None:
                        return None
                else:
                    return None

        return current


class JsonMergeAction(BaseAction):
    """Merge multiple JSON objects."""
    action_type = "json_merge"
    display_name = "JSON合并"
    description = "合并多个JSON对象"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            objects = params.get("objects", [])
            merge_arrays = params.get("merge_arrays", False)
            array_handling = params.get("array_handling", "concat")

            if not objects:
                return ActionResult(success=False, message="objects list is required")

            if len(objects) == 1:
                return ActionResult(success=True, message="Single object returned", data={"merged": objects[0]})

            result = objects[0]

            for obj in objects[1:]:
                result = self._deep_merge(result, obj, merge_arrays, array_handling)

            return ActionResult(success=True, message="JSON merged successfully", data={"merged": result})

        except Exception as e:
            return ActionResult(success=False, message=f"Merge error: {str(e)}")

    def _deep_merge(self, base: Any, update: Any, merge_arrays: bool, array_handling: str) -> Any:
        """Deep merge two objects."""
        if not isinstance(base, dict) or not isinstance(update, dict):
            return update

        result = base.copy()

        for key, value in update.items():
            if key in result:
                if isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = self._deep_merge(result[key], value, merge_arrays, array_handling)
                elif isinstance(result[key], list) and isinstance(value, list):
                    if merge_arrays:
                        if array_handling == "concat":
                            result[key] = result[key] + value
                        elif array_handling == "unique":
                            seen = set()
                            result[key] = [x for x in result[key] + value if not (str(x) in seen or seen.add(str(x)))]
                        elif array_handling == "replace":
                            result[key] = value
                    else:
                        result[key] = value
                else:
                    result[key] = value
            else:
                result[key] = value

        return result


class JsonFlattenAction(BaseAction):
    """Flatten nested JSON structure."""
    action_type = "json_flatten"
    display_name = "JSON扁平化"
    description = "将嵌套JSON结构扁平化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            separator = params.get("separator", ".")
            max_depth = params.get("max_depth", 10)

            if data is None:
                return ActionResult(success=False, message="data is required")

            flat = {}
            self._flatten_obj(data, flat, "", separator, 0, max_depth)

            return ActionResult(
                success=True,
                message=f"Flattened to {len(flat)} keys",
                data={"flattened": flat, "key_count": len(flat)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Flatten error: {str(e)}")

    def _flatten_obj(self, obj: Any, result: Dict, prefix: str, sep: str, depth: int, max_depth: int) -> None:
        """Recursively flatten object."""
        if depth >= max_depth:
            result[prefix] = obj
            return

        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{prefix}{sep}{key}" if prefix else key
                self._flatten_obj(value, result, new_key, sep, depth + 1, max_depth)
        elif isinstance(obj, (list, tuple)):
            for i, item in enumerate(obj):
                new_key = f"{prefix}[{i}]"
                self._flatten_obj(item, result, new_key, sep, depth + 1, max_depth)
        else:
            result[prefix] = obj


class JsonValidateAction(BaseAction):
    """Validate JSON against schema."""
    action_type = "json_validate"
    display_name = "JSON验证"
    description = "根据schema验证JSON"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            schema = params.get("schema", {})

            if data is None:
                return ActionResult(success=False, message="data is required")

            errors = self._validate(data, schema, "")

            if errors:
                return ActionResult(
                    success=False,
                    message=f"Validation failed with {len(errors)} errors",
                    data={"errors": errors, "valid": False}
                )
            else:
                return ActionResult(
                    success=True,
                    message="JSON is valid",
                    data={"valid": True}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Validation error: {str(e)}")

    def _validate(self, data: Any, schema: Dict, path: str) -> List[str]:
        """Simple JSON schema validation."""
        errors = []

        if "type" in schema:
            expected_type = schema["type"]
            type_map = {
                "string": str, "number": (int, float), "integer": int,
                "boolean": bool, "array": list, "object": dict, "null": type(None)
            }
            if expected_type in type_map:
                if not isinstance(data, type_map[expected_type]):
                    errors.append(f"{path}: expected {expected_type}, got {type(data).__name__}")

        if "enum" in schema:
            if data not in schema["enum"]:
                errors.append(f"{path}: value not in enum {schema['enum']}")

        if "minLength" in schema and isinstance(data, str):
            if len(data) < schema["minLength"]:
                errors.append(f"{path}: string length {len(data)} < minLength {schema['minLength']}")

        if "maxLength" in schema and isinstance(data, str):
            if len(data) > schema["maxLength"]:
                errors.append(f"{path}: string length {len(data)} > maxLength {schema['maxLength']}")

        if "minimum" in schema and isinstance(data, (int, float)):
            if data < schema["minimum"]:
                errors.append(f"{path}: {data} < minimum {schema['minimum']}")

        if "maximum" in schema and isinstance(data, (int, float)):
            if data > schema["maximum"]:
                errors.append(f"{path}: {data} > maximum {schema['maximum']}")

        if "required" in schema and isinstance(data, dict):
            for field in schema["required"]:
                if field not in data:
                    errors.append(f"{path}: missing required field '{field}'")

        if "properties" in schema and isinstance(data, dict):
            for key, prop_schema in schema["properties"].items():
                if key in data:
                    errors.extend(self._validate(data[key], prop_schema, f"{path}.{key}"))

        if "items" in schema and isinstance(data, list):
            for i, item in enumerate(data):
                errors.extend(self._validate(item, schema["items"], f"{path}[{i}]"))

        return errors

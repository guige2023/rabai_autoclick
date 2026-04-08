"""
JSON utilities - parsing, query, merge, diff, schema validation, transformation.
"""
from typing import Any, Dict, List, Optional, Union
import json
import logging
import re

logger = logging.getLogger(__name__)


class BaseAction:
    """Base class for all actions."""

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _get_path(data: Any, path: str) -> Optional[Any]:
    parts = path.split(".")
    current = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            idx = int(part)
            current = current[idx] if 0 <= idx < len(current) else None
        else:
            return None
        if current is None:
            return None
    return current


def _set_path(data: Dict[str, Any], path: str, value: Any) -> None:
    parts = path.split(".")
    current = data
    for part in parts[:-1]:
        if part not in current:
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


def _delete_path(data: Union[Dict, List], path: str) -> bool:
    parts = path.split(".")
    current = data
    for part in parts[:-1]:
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list) and part.isdigit():
            idx = int(part)
            current = current[idx] if 0 <= idx < len(current) else None
        else:
            return False
        if current is None:
            return False
    if isinstance(current, dict):
        if parts[-1] in current:
            del current[parts[-1]]
            return True
    elif isinstance(current, list) and parts[-1].isdigit():
        idx = int(parts[-1])
        if 0 <= idx < len(current):
            current.pop(idx)
            return True
    return False


def _json_diff(obj1: Any, obj2: Any, path: str = "") -> List[Dict[str, Any]]:
    diffs: List[Dict[str, Any]] = []
    if type(obj1) != type(obj2):
        diffs.append({"op": "replace", "path": path, "old": obj1, "new": obj2})
        return diffs
    if isinstance(obj1, dict):
        all_keys = set(obj1.keys()) | set(obj2.keys())
        for key in all_keys:
            new_path = f"{path}.{key}" if path else key
            if key not in obj1:
                diffs.append({"op": "add", "path": new_path, "value": obj2[key]})
            elif key not in obj2:
                diffs.append({"op": "remove", "path": new_path, "value": obj1[key]})
            else:
                diffs.extend(_json_diff(obj1[key], obj2[key], new_path))
    elif isinstance(obj1, list):
        len1, len2 = len(obj1), len(obj2)
        max_len = max(len1, len2)
        for i in range(max_len):
            p = f"{path}[{i}]"
            if i >= len1:
                diffs.append({"op": "add", "path": p, "value": obj2[i]})
            elif i >= len2:
                diffs.append({"op": "remove", "path": p, "value": obj1[i]})
            else:
                diffs.extend(_json_diff(obj1[i], obj2[i], p))
    else:
        if obj1 != obj2:
            diffs.append({"op": "replace", "path": path, "old": obj1, "new": obj2})
    return diffs


def _merge_json(base: Any, *updates: Any) -> Any:
    if isinstance(base, dict):
        result = dict(base)
        for update in updates:
            if isinstance(update, dict):
                for key, value in update.items():
                    if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                        result[key] = _merge_json(result[key], value)
                    else:
                        result[key] = value
        return result
    elif isinstance(base, list):
        return base + [u for u in updates if u not in base]
    return updates[-1] if updates else base


class JSONAction(BaseAction):
    """JSON operations.

    Provides parsing, querying, merging, diffing, schema validation, transformation.
    """

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "parse")
        data = params.get("data")
        text = params.get("text", "")
        path = params.get("path", "")

        try:
            if operation == "parse":
                if not text:
                    return {"success": False, "error": "text required"}
                parsed = json.loads(text)
                return {"success": True, "data": parsed}

            elif operation == "dumps":
                if data is None:
                    return {"success": False, "error": "data required"}
                indent = int(params.get("indent", 2))
                compact = params.get("compact", False)
                result = json.dumps(data, indent=None if compact else indent, ensure_ascii=False)
                return {"success": True, "json": result}

            elif operation == "get":
                if data is None:
                    return {"success": False, "error": "data required"}
                value = _get_path(data, path)
                return {"success": True, "value": value, "path": path, "found": value is not None}

            elif operation == "set":
                if data is None:
                    data = {}
                if not isinstance(data, dict):
                    return {"success": False, "error": "data must be a dict for set"}
                value = params.get("value")
                _set_path(data, path, value)
                return {"success": True, "data": data}

            elif operation == "delete":
                if data is None:
                    return {"success": False, "error": "data required"}
                deleted = _delete_path(data, path)
                return {"success": True, "deleted": deleted, "data": data}

            elif operation == "merge":
                if data is None:
                    return {"success": False, "error": "base data required"}
                updates = params.get("updates", [])
                result = _merge_json(data, *updates)
                return {"success": True, "data": result}

            elif operation == "diff":
                obj1 = data if data is not None else json.loads(text) if text else {}
                obj2 = params.get("other", {})
                diffs = _json_diff(obj1, obj2, path)
                return {"success": True, "diffs": diffs, "count": len(diffs), "has_changes": len(diffs) > 0}

            elif operation == "patch":
                obj = data if data is not None else {}
                diffs = params.get("diffs", [])
                for d in diffs:
                    op = d.get("op")
                    p = d.get("path", "")
                    if op == "add" or op == "replace":
                        _set_path(obj, p, d.get("value"))
                    elif op == "remove":
                        _delete_path(obj, p)
                return {"success": True, "data": obj}

            elif operation == "flatten":
                if not isinstance(data, dict):
                    return {"success": False, "error": "data must be a dict"}
                flat: Dict[str, Any] = {}

                def _flatten(obj: Any, prefix: str = "") -> None:
                    if isinstance(obj, dict):
                        for k, v in obj.items():
                            new_key = f"{prefix}.{k}" if prefix else k
                            _flatten(v, new_key)
                    elif isinstance(obj, list):
                        for i, v in enumerate(obj):
                            _flatten(v, f"{prefix}[{i}]")
                    else:
                        flat[prefix] = obj

                _flatten(data)
                return {"success": True, "data": flat}

            elif operation == "unflatten":
                if not isinstance(data, dict):
                    return {"success": False, "error": "data must be a dict"}
                result: Any = {}

                def _unflatten(src: Dict[str, Any]) -> Any:
                    out: Dict[str, Any] = {}
                    for key, value in src.items():
                        _set_path(out, key.replace("[", ".").replace("]", ""), value)
                    return out

                result = _unflatten(data)
                return {"success": True, "data": result}

            elif operation == "validate_schema":
                if data is None:
                    return {"success": False, "error": "data required"}
                schema = params.get("schema", {})
                errors: List[str] = []
                required = schema.get("required", [])
                properties = schema.get("properties", {})
                for req_field in required:
                    if req_field not in data:
                        errors.append(f"Missing required field: {req_field}")
                for field, field_schema in properties.items():
                    if field in data:
                        expected_type = field_schema.get("type")
                        value = data[field]
                        if expected_type == "string" and not isinstance(value, str):
                            errors.append(f"{field} must be string")
                        elif expected_type == "number" and not isinstance(value, (int, float)):
                            errors.append(f"{field} must be number")
                        elif expected_type == "boolean" and not isinstance(value, bool):
                            errors.append(f"{field} must be boolean")
                        elif expected_type == "array" and not isinstance(value, list):
                            errors.append(f"{field} must be array")
                        elif expected_type == "object" and not isinstance(value, dict):
                            errors.append(f"{field} must be object")
                return {"success": True, "valid": len(errors) == 0, "errors": errors}

            elif operation == "query":
                if data is None:
                    return {"success": False, "error": "data required"}
                query_path = params.get("query", path)
                results = []
                selector = query_path.lstrip("$")
                parts = selector.split(".")
                current = [data]
                for part in parts:
                    if part == "$":
                        continue
                    next_level = []
                    for item in current:
                        if isinstance(item, dict):
                            next_level.append(item.get(part))
                        elif isinstance(item, list) and part.isdigit():
                            idx = int(part)
                            if 0 <= idx < len(item):
                                next_level.append(item[idx])
                    current = next_level
                return {"success": True, "results": current, "count": len(current)}

            elif operation == "compact":
                if data is None:
                    return {"success": False, "error": "data required"}
                result = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
                return {"success": True, "json": result}

            elif operation == "pretty":
                if data is None:
                    return {"success": False, "error": "data required"}
                result = json.dumps(data, indent=2, ensure_ascii=False)
                return {"success": True, "json": result}

            elif operation == "keys":
                if not isinstance(data, dict):
                    return {"success": False, "error": "data must be a dict"}
                return {"success": True, "keys": list(data.keys()), "count": len(data)}

            elif operation == "values":
                if not isinstance(data, dict):
                    return {"success": False, "error": "data must be a dict"}
                return {"success": True, "values": list(data.values()), "count": len(data)}

            elif operation == "items":
                if not isinstance(data, dict):
                    return {"success": False, "error": "data must be a dict"}
                return {"success": True, "items": list(data.items()), "count": len(data)}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except json.JSONDecodeError as e:
            return {"success": False, "error": f"JSON parse error: {e}"}
        except Exception as e:
            logger.error(f"JSONAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """Entry point for JSON operations."""
    return JSONAction().execute(context, params)

"""Data serialization action module for RabAI AutoClick.

Provides serialization and deserialization:
- DataSerializationAction: Encode/decode data formats
- DataEncodingAction: Character encoding conversion
- DataCompressorAction: Compress/decompress data
- DataSchemaAction: Schema validation and enforcement
- DataValidatorAdvancedAction: Advanced data validation rules
"""

import time
import json
import base64
import gzip
import zlib
from typing import Any, Dict, List, Optional
from datetime import datetime
import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataSerializationAction(BaseAction):
    """Serialize and deserialize data in various formats."""
    action_type = "data_serialization"
    display_name = "数据序列化"
    description = "多格式数据序列化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "serialize")
            data = params.get("data")
            format_type = params.get("format", "json")

            if operation == "serialize":
                if data is None:
                    return ActionResult(success=False, message="data is required")

                if format_type == "json":
                    serialized = json.dumps(data, default=str, ensure_ascii=False)
                elif format_type == "json_compact":
                    serialized = json.dumps(data, default=str, separators=(",", ":"))
                elif format_type == "base64":
                    json_bytes = json.dumps(data, default=str).encode("utf-8")
                    serialized = base64.b64encode(json_bytes).decode("ascii")
                elif format_type == "url_encoded":
                    serialized = self._to_url_encoded(data)
                else:
                    return ActionResult(success=False, message=f"Unknown format: {format_type}")

                return ActionResult(
                    success=True,
                    data={"serialized": serialized, "format": format_type, "length": len(serialized)},
                    message=f"Serialized to {format_type}: {len(serialized)} chars"
                )

            elif operation == "deserialize":
                if data is None:
                    return ActionResult(success=False, message="data is required")

                data_str = str(data)
                if format_type == "json":
                    try:
                        deserialized = json.loads(data_str)
                    except json.JSONDecodeError as e:
                        return ActionResult(success=False, message=f"JSON decode error: {e}")
                elif format_type == "base64":
                    try:
                        decoded = base64.b64decode(data_str)
                        deserialized = json.loads(decoded.decode("utf-8"))
                    except Exception as e:
                        return ActionResult(success=False, message=f"Base64 decode error: {e}")
                elif format_type == "url_encoded":
                    deserialized = self._from_url_encoded(data_str)
                else:
                    return ActionResult(success=False, message=f"Unknown format: {format_type}")

                return ActionResult(
                    success=True,
                    data={"deserialized": deserialized, "format": format_type},
                    message=f"Deserialized from {format_type}"
                )

            elif operation == "validate":
                if data is None:
                    return ActionResult(success=False, message="data is required")

                try:
                    if format_type == "json":
                        json.loads(str(data))
                        return ActionResult(success=True, data={"valid": True, "format": "json"})
                    return ActionResult(success=True, data={"valid": True, "format": format_type})
                except Exception as e:
                    return ActionResult(success=False, data={"valid": False, "error": str(e)}, message=f"Validation failed: {e}")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Serialization error: {str(e)}")

    def _to_url_encoded(self, data: Dict) -> str:
        from urllib.parse import urlencode
        flat = self._flatten_dict(data)
        return urlencode(flat)

    def _from_url_encoded(self, data: str) -> Dict:
        from urllib.parse import parse_qs
        parsed = parse_qs(data)
        return {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}

    def _flatten_dict(self, d: Dict, parent_key: str = "", sep: str = ".") -> Dict:
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, str(v)))
        return dict(items)


class DataEncodingAction(BaseAction):
    """Character encoding conversion."""
    action_type = "data_encoding"
    display_name = "数据编码转换"
    description = "字符编码转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "convert")
            data = params.get("data", "")
            from_encoding = params.get("from_encoding", "utf-8")
            to_encoding = params.get("to_encoding", "utf-8")

            if operation == "convert":
                if not data:
                    return ActionResult(success=False, message="data is required")

                if isinstance(data, str):
                    data_bytes = data.encode(from_encoding)
                else:
                    data_bytes = data

                try:
                    data_str = data_bytes.decode(from_encoding)
                    converted_bytes = data_str.encode(to_encoding)
                except (UnicodeDecodeError, UnicodeEncodeError) as e:
                    data_str = data_bytes.decode(from_encoding, errors="replace")
                    converted_bytes = data_str.encode(to_encoding, errors="replace")

                return ActionResult(
                    success=True,
                    data={
                        "original_size": len(data_bytes),
                        "converted_size": len(converted_bytes),
                        "original_encoding": from_encoding,
                        "target_encoding": to_encoding
                    },
                    message=f"Converted {len(data_bytes)} bytes from {from_encoding} to {to_encoding}"
                )

            elif operation == "detect":
                if isinstance(data, bytes):
                    detected = self._detect_encoding(data)
                    return ActionResult(success=True, data={"detected": detected}, message=f"Detected: {detected}")
                return ActionResult(success=False, message="data must be bytes for detection")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Encoding error: {str(e)}")

    def _detect_encoding(self, data: bytes) -> str:
        try:
            data.decode("utf-8")
            return "utf-8"
        except:
            try:
                data.decode("latin-1")
                return "latin-1"
            except:
                return "unknown"


class DataCompressorAction(BaseAction):
    """Compress and decompress data."""
    action_type = "data_compressor"
    display_name = "数据压缩"
    description = "数据压缩解压"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "compress")
            data = params.get("data", "")
            algorithm = params.get("algorithm", "gzip")
            level = params.get("level", 6)

            if operation == "compress":
                if not data:
                    return ActionResult(success=False, message="data is required")

                if isinstance(data, str):
                    data_bytes = data.encode("utf-8")
                else:
                    data_bytes = data

                if algorithm == "gzip":
                    compressed = gzip.compress(data_bytes, compresslevel=level)
                elif algorithm == "zlib":
                    compressed = zlib.compress(data_bytes, level=level)
                elif algorithm == "deflate":
                    compressed = zlib.compress(data_bytes, level=level)
                elif algorithm == "lz4":
                    import lz4.frame
                    compressed = lz4.frame.compress(data_bytes)
                elif algorithm == "bz2":
                    import bz2
                    compressed = bz2.compress(data_bytes, compresslevel=level)
                else:
                    return ActionResult(success=False, message=f"Unknown algorithm: {algorithm}")

                compressed_b64 = base64.b64encode(compressed).decode("ascii")

                return ActionResult(
                    success=True,
                    data={
                        "original_size": len(data_bytes),
                        "compressed_size": len(compressed),
                        "ratio": round(len(compressed) / len(data_bytes), 4) if data_bytes else 0,
                        "compressed_b64": compressed_b64,
                        "algorithm": algorithm
                    },
                    message=f"Compressed {len(data_bytes)} -> {len(compressed)} bytes ({len(compressed)/len(data_bytes):.1%})"
                )

            elif operation == "decompress":
                compressed_b64 = params.get("compressed_b64", "")
                if not compressed_b64:
                    return ActionResult(success=False, message="compressed_b64 required")

                compressed = base64.b64decode(compressed_b64)

                if algorithm == "gzip":
                    decompressed = gzip.decompress(compressed)
                elif algorithm == "zlib":
                    decompressed = zlib.decompress(compressed)
                elif algorithm == "deflate":
                    decompressed = zlib.decompress(compressed)
                elif algorithm == "lz4":
                    import lz4.frame
                    decompressed = lz4.frame.decompress(compressed)
                elif algorithm == "bz2":
                    import bz2
                    decompressed = bz2.decompress(compressed)
                else:
                    return ActionResult(success=False, message=f"Unknown algorithm: {algorithm}")

                return ActionResult(
                    success=True,
                    data={
                        "decompressed": decompressed.decode("utf-8", errors="replace"),
                        "decompressed_size": len(decompressed),
                        "algorithm": algorithm
                    },
                    message=f"Decompressed to {len(decompressed)} bytes"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Compression error: {str(e)}")


class DataSchemaAction(BaseAction):
    """Schema validation and enforcement."""
    action_type = "data_schema"
    display_name = "数据模式"
    description = "数据模式验证"

    def __init__(self):
        super().__init__()
        self._schemas: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "validate")
            schema_name = params.get("schema_name", "")

            if operation == "define":
                if not schema_name:
                    return ActionResult(success=False, message="schema_name required")

                self._schemas[schema_name] = {
                    "name": schema_name,
                    "fields": params.get("fields", []),
                    "required_fields": params.get("required_fields", []),
                    "created_at": time.time()
                }

                return ActionResult(
                    success=True,
                    data={"schema": schema_name, "fields": len(params.get("fields", []))},
                    message=f"Schema '{schema_name}' defined"
                )

            elif operation == "validate":
                if not schema_name:
                    return ActionResult(success=False, message="schema_name required")

                if schema_name not in self._schemas:
                    return ActionResult(success=False, message=f"Schema '{schema_name}' not found")

                data = params.get("data", {})
                schema = self._schemas[schema_name]

                errors = []
                for field in schema["required_fields"]:
                    if field not in data or data[field] is None:
                        errors.append(f"Required field missing: {field}")

                for field_def in schema["fields"]:
                    fname = field_def.get("name")
                    if fname in data:
                        field_type = field_def.get("type", "string")
                        value = data[fname]
                        type_error = self._check_type(value, field_type)
                        if type_error:
                            errors.append(type_error)

                return ActionResult(
                    success=len(errors) == 0,
                    data={"valid": len(errors) == 0, "errors": errors, "schema": schema_name},
                    message=f"{'Valid' if not errors else 'Invalid'}: {len(errors)} errors"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={"schemas": list(self._schemas.keys())}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Schema error: {str(e)}")

    def _check_type(self, value: Any, expected_type: str) -> Optional[str]:
        if value is None:
            return None
        type_map = {
            "string": str,
            "integer": int,
            "float": (int, float),
            "boolean": bool,
            "array": list,
            "object": dict
        }
        expected = type_map.get(expected_type)
        if expected and not isinstance(value, expected):
            return f"Type error: expected {expected_type}, got {type(value).__name__}"
        return None


class DataValidatorAdvancedAction(BaseAction):
    """Advanced data validation rules."""
    action_type = "data_validator_advanced"
    display_name = "数据验证器"
    description = "高级数据验证"

    def __init__(self):
        super().__init__()
        self._validation_rules: Dict[str, List[Dict]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "validate")
            rule_set = params.get("rule_set", "default")

            if operation == "add_rules":
                rules = params.get("rules", [])
                self._validation_rules[rule_set] = self._validation_rules.get(rule_set, []) + rules
                return ActionResult(
                    success=True,
                    data={"rule_set": rule_set, "total_rules": len(self._validation_rules[rule_set])},
                    message=f"Added {len(rules)} rules to '{rule_set}'"
                )

            elif operation == "validate":
                data = params.get("data", {})
                rules = self._validation_rules.get(rule_set, [])

                if not rules:
                    return ActionResult(success=False, message=f"No rules found for '{rule_set}'")

                violations = []
                for rule in rules:
                    violation = self._apply_rule(rule, data)
                    if violation:
                        violations.append(violation)

                return ActionResult(
                    success=len(violations) == 0,
                    data={"valid": len(violations) == 0, "violations": violations},
                    message=f"{'Valid' if not violations else 'Invalid'}: {len(violations)} violations"
                )

            elif operation == "list_rules":
                return ActionResult(
                    success=True,
                    data={"rule_sets": {k: len(v) for k, v in self._validation_rules.items()}}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Validator error: {str(e)}")

    def _apply_rule(self, rule: Dict, data: Dict) -> Optional[Dict]:
        rule_type = rule.get("type", "")
        field = rule.get("field", "")

        if rule_type == "required":
            if field not in data or data[field] is None or data[field] == "":
                return {"field": field, "rule": "required", "message": f"Field '{field}' is required"}

        elif rule_type == "min_length":
            value = data.get(field, "")
            if not isinstance(value, (str, list)) or len(value) < rule.get("min", 0):
                return {"field": field, "rule": "min_length", "message": f"Field '{field}' too short"}

        elif rule_type == "max_length":
            value = data.get(field, "")
            if isinstance(value, (str, list)) and len(value) > rule.get("max", float("inf")):
                return {"field": field, "rule": "max_length", "message": f"Field '{field}' too long"}

        elif rule_type == "pattern":
            import re
            value = str(data.get(field, ""))
            pattern = rule.get("pattern", "")
            if pattern and not re.match(pattern, value):
                return {"field": field, "rule": "pattern", "message": f"Field '{field}' does not match pattern"}

        elif rule_type == "range":
            value = data.get(field, 0)
            min_val = rule.get("min")
            max_val = rule.get("max")
            if (min_val is not None and value < min_val) or (max_val is not None and value > max_val):
                return {"field": field, "rule": "range", "message": f"Field '{field}' out of range"}

        elif rule_type == "in":
            value = data.get(field)
            allowed = rule.get("values", [])
            if value not in allowed:
                return {"field": field, "rule": "in", "message": f"Field '{field}' not in allowed values"}

        return None

"""Data converter action module for RabAI AutoClick.

Provides data conversion operations:
- ConvertTypeAction: Type conversion
- ConvertFormatAction: Format conversion
- ConvertEncodingAction: Encoding conversion
- ConvertUnitAction: Unit conversion
"""

import json
import base64
from typing import Any, Dict, List, Optional


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ConvertTypeAction(BaseAction):
    """Type conversion."""
    action_type = "convert_type"
    display_name = "类型转换"
    description = "数据类型转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            value = params.get("value", None)
            target_type = params.get("target_type", "string")

            if target_type == "string":
                result = str(value)
            elif target_type == "integer":
                result = int(float(value)) if value is not None else 0
            elif target_type == "float":
                result = float(value) if value is not None else 0.0
            elif target_type == "boolean":
                if isinstance(value, bool):
                    result = value
                elif isinstance(value, str):
                    result = value.lower() in ("true", "1", "yes", "on")
                else:
                    result = bool(value)
            elif target_type == "list":
                if isinstance(value, (list, tuple)):
                    result = list(value)
                elif isinstance(value, str):
                    try:
                        result = json.loads(value)
                        if not isinstance(result, list):
                            result = [value]
                    except json.JSONDecodeError:
                        result = [value]
                else:
                    result = [value]
            elif target_type == "dict":
                if isinstance(value, dict):
                    result = value
                elif isinstance(value, str):
                    try:
                        result = json.loads(value)
                        if not isinstance(result, dict):
                            result = {"value": value}
                    except json.JSONDecodeError:
                        result = {"value": value}
                else:
                    result = {"value": value}
            else:
                return ActionResult(success=False, message=f"Unknown target_type: {target_type}")

            return ActionResult(
                success=True,
                message=f"Converted to {target_type}",
                data={"result": result, "original_type": type(value).__name__, "target_type": target_type}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Type conversion failed: {str(e)}")


class ConvertFormatAction(BaseAction):
    """Format conversion."""
    action_type = "convert_format"
    display_name = "格式转换"
    description = "数据格式转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", None)
            from_format = params.get("from_format", "json")
            to_format = params.get("to_format", "string")

            if data is None:
                return ActionResult(success=False, message="data is required")

            if from_format == "json":
                if isinstance(data, str):
                    parsed = json.loads(data)
                else:
                    parsed = data
            elif from_format == "string":
                parsed = data
            elif from_format == "list":
                parsed = data
            else:
                return ActionResult(success=False, message=f"Unknown from_format: {from_format}")

            if to_format == "json":
                result = json.dumps(parsed, ensure_ascii=False)
            elif to_format == "string":
                if isinstance(parsed, dict):
                    result = str(parsed)
                elif isinstance(parsed, (list, tuple)):
                    result = ", ".join(str(x) for x in parsed)
                else:
                    result = str(parsed)
            elif to_format == "list":
                if isinstance(parsed, (list, tuple)):
                    result = list(parsed)
                else:
                    result = [parsed]
            else:
                return ActionResult(success=False, message=f"Unknown to_format: {to_format}")

            return ActionResult(
                success=True,
                message=f"Converted from {from_format} to {to_format}",
                data={"result": result, "from": from_format, "to": to_format}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Format conversion failed: {str(e)}")


class ConvertEncodingAction(BaseAction):
    """Encoding conversion."""
    action_type = "convert_encoding"
    display_name = "编码转换"
    description = "编码转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            from_encoding = params.get("from_encoding", "utf-8")
            to_encoding = params.get("to_encoding", "base64")
            operation = params.get("operation", "encode")

            if operation == "encode":
                if to_encoding == "base64":
                    if isinstance(data, str):
                        data = data.encode(from_encoding)
                    result = base64.b64encode(data).decode("ascii")
                elif to_encoding == "hex":
                    if isinstance(data, str):
                        data = data.encode(from_encoding)
                    result = data.hex()
                elif to_encoding == "url":
                    import urllib.parse
                    result = urllib.parse.quote(data)
                else:
                    return ActionResult(success=False, message=f"Unknown to_encoding: {to_encoding}")
            else:
                if from_encoding == "base64":
                    decoded = base64.b64decode(data)
                    result = decoded.decode(to_encoding) if to_encoding != "bytes" else decoded
                elif from_encoding == "hex":
                    decoded = bytes.fromhex(data)
                    result = decoded.decode(to_encoding) if to_encoding != "bytes" else decoded
                elif from_encoding == "url":
                    import urllib.parse
                    result = urllib.parse.unquote(data)
                else:
                    return ActionResult(success=False, message=f"Unknown from_encoding: {from_encoding}")

            return ActionResult(
                success=True,
                message=f"Encoded to {to_encoding}" if operation == "encode" else f"Decoded from {from_encoding}",
                data={"result": result, "operation": operation, "encoding": to_encoding if operation == "encode" else from_encoding}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Encoding conversion failed: {str(e)}")


class ConvertUnitAction(BaseAction):
    """Unit conversion."""
    action_type = "convert_unit"
    display_name = "单位转换"
    description = "单位转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            value = params.get("value", 0)
            from_unit = params.get("from_unit", "")
            to_unit = params.get("to_unit", "")

            conversions = {
                "length": {
                    "m": 1.0,
                    "cm": 100.0,
                    "mm": 1000.0,
                    "km": 0.001,
                    "in": 39.3701,
                    "ft": 3.28084,
                    "yd": 1.09361,
                    "mi": 0.000621371,
                },
                "weight": {
                    "kg": 1.0,
                    "g": 1000.0,
                    "mg": 1000000.0,
                    "lb": 2.20462,
                    "oz": 35.274,
                },
                "temperature": {
                    "c_to_f": lambda c: c * 9/5 + 32,
                    "f_to_c": lambda f: (f - 32) * 5/9,
                    "c_to_k": lambda c: c + 273.15,
                    "k_to_c": lambda k: k - 273.15,
                },
                "time": {
                    "s": 1.0,
                    "ms": 1000.0,
                    "us": 1000000.0,
                    "min": 1/60,
                    "h": 1/3600,
                    "d": 1/86400,
                },
                "data": {
                    "B": 1.0,
                    "KB": 1024.0,
                    "MB": 1024.0**2,
                    "GB": 1024.0**3,
                    "TB": 1024.0**4,
                },
            }

            for category, units in conversions.items():
                if category == "temperature":
                    if from_unit == "c" and to_unit == "f":
                        result = units["c_to_f"](value)
                    elif from_unit == "f" and to_unit == "c":
                        result = units["f_to_c"](value)
                    elif from_unit == "c" and to_unit == "k":
                        result = units["c_to_k"](value)
                    elif from_unit == "k" and to_unit == "c":
                        result = units["k_to_c"](value)
                    else:
                        continue
                    return ActionResult(
                        success=True,
                        message=f"{value}{from_unit} = {result}{to_unit}",
                        data={"result": result, "from": f"{value}{from_unit}", "to": f"{result}{to_unit}"}
                    )
                else:
                    if from_unit in units and to_unit in units:
                        base_value = float(value) * units[from_unit]
                        result = base_value / units[to_unit]
                        return ActionResult(
                            success=True,
                            message=f"{value}{from_unit} = {result}{to_unit}",
                            data={"result": result, "from": f"{value}{from_unit}", "to": f"{result}{to_unit}"}
                        )

            return ActionResult(success=False, message=f"Unknown unit conversion: {from_unit} -> {to_unit}")

        except Exception as e:
            return ActionResult(success=False, message=f"Unit conversion failed: {str(e)}")

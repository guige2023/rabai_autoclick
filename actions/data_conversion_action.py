"""Data conversion action module for RabAI AutoClick.

Provides data conversion operations:
- TypeConversionAction: Convert between data types
- FormatConversionAction: Convert between data formats
- UnitConversionAction: Convert between units
- EncodingConversionAction: Convert between encodings
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TypeConversionAction(BaseAction):
    """Convert between data types."""
    action_type = "type_conversion"
    display_name: "类型转换"
    description: "数据类型之间的转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data")
            target_type = params.get("target_type", "str")

            if data is None:
                return ActionResult(success=False, message="data is required")

            conversions = {
                "str": lambda x: str(x),
                "int": lambda x: int(x) if not isinstance(x, bool) else int(x),
                "float": lambda x: float(x),
                "bool": lambda x: bool(x) if x is not None else False,
                "list": lambda x: [x] if not isinstance(x, list) else x,
                "dict": lambda x: {"value": x} if not isinstance(x, dict) else x,
                "tuple": lambda x: tuple([x]) if not isinstance(x, (list, tuple)) else x,
                "set": lambda x: set([x]) if not isinstance(x, (list, set)) else set(x),
            }

            if target_type not in conversions:
                return ActionResult(success=False, message=f"Unknown target_type: {target_type}")

            converted = conversions[target_type](data)

            return ActionResult(
                success=True,
                message=f"Converted {type(data).__name__} to {target_type}",
                data={"converted": converted, "original_type": type(data).__name__, "target_type": target_type},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"TypeConversion error: {e}")


class FormatConversionAction(BaseAction):
    """Convert between data formats."""
    action_type = "format_conversion"
    display_name: "格式转换"
    description: "数据格式之间的转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            from_format = params.get("from_format", "dict")
            to_format = params.get("to_format", "json")

            if from_format == "dict" and to_format == "json":
                import json
                result = json.dumps(data, ensure_ascii=False, indent=2)
            elif from_format == "json" and to_format == "dict":
                import json
                result = json.loads(data)
            elif from_format == "list" and to_format == "dict":
                keys = params.get("keys", [])
                if keys and len(keys) == len(data):
                    result = dict(zip(keys, data))
                else:
                    result = {f"item_{i}": v for i, v in enumerate(data)}
            elif from_format == "dict" and to_format == "list":
                result = [[k, v] for k, v in data.items()]
            elif from_format == "list" and to_format == "set":
                result = list(set(data))
            elif from_format == "dict" and to_format == "list":
                result = list(data.values())
            else:
                return ActionResult(success=False, message=f"Conversion {from_format}->{to_format} not supported")

            return ActionResult(
                success=True,
                message=f"Converted {from_format} to {to_format}",
                data={"result": result, "from": from_format, "to": to_format},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"FormatConversion error: {e}")


class UnitConversionAction(BaseAction):
    """Convert between units."""
    action_type = "unit_conversion"
    display_name: "单位转换"
    description: "单位之间的转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            value = params.get("value", 0)
            from_unit = params.get("from_unit", "m")
            to_unit = params.get("to_unit", "km")

            conversions = {
                ("m", "km"): lambda v: v / 1000,
                ("km", "m"): lambda v: v * 1000,
                ("m", "cm"): lambda v: v * 100,
                ("cm", "m"): lambda v: v / 100,
                ("km", "mi"): lambda v: v * 0.621371,
                ("mi", "km"): lambda v: v * 1.60934,
                ("kg", "g"): lambda v: v * 1000,
                ("g", "kg"): lambda v: v / 1000,
                ("kg", "lb"): lambda v: v * 2.20462,
                ("lb", "kg"): lambda v: v * 0.453592,
                ("c", "f"): lambda v: v * 9 / 5 + 32,
                ("f", "c"): lambda v: (v - 32) * 5 / 9,
                ("c", "k"): lambda v: v + 273.15,
                ("k", "c"): lambda v: v - 273.15,
                ("bytes", "kb"): lambda v: v / 1024,
                ("kb", "bytes"): lambda v: v * 1024,
                ("bytes", "mb"): lambda v: v / (1024 * 1024),
                ("mb", "bytes"): lambda v: v * 1024 * 1024,
                ("bytes", "gb"): lambda v: v / (1024 * 1024 * 1024),
                ("gb", "bytes"): lambda v: v * 1024 * 1024 * 1024,
                ("s", "ms"): lambda v: v * 1000,
                ("ms", "s"): lambda v: v / 1000,
                ("s", "min"): lambda v: v / 60,
                ("min", "s"): lambda v: v * 60,
                ("min", "h"): lambda v: v / 60,
                ("h", "min"): lambda v: v * 60,
            }

            key = (from_unit.lower(), to_unit.lower())
            if key in conversions:
                result = conversions[key](value)
                return ActionResult(
                    success=True,
                    message=f"Converted {value} {from_unit} -> {result} {to_unit}",
                    data={"result": round(result, 6), "from": from_unit, "to": to_unit, "original_value": value},
                )

            return ActionResult(success=False, message=f"Conversion {from_unit}->{to_unit} not found")
        except Exception as e:
            return ActionResult(success=False, message=f"UnitConversion error: {e}")


class EncodingConversionAction(BaseAction):
    """Convert between encodings."""
    action_type = "encoding_conversion"
    display_name: "编码转换"
    description: "字符编码之间的转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            from_encoding = params.get("from_encoding", "utf-8")
            to_encoding = params.get("to_encoding", "latin-1")

            if not data:
                return ActionResult(success=False, message="data is required")

            if isinstance(data, str):
                encoded = data.encode(from_encoding)
            else:
                encoded = data

            decoded = encoded.decode(to_encoding)

            return ActionResult(
                success=True,
                message=f"Decoded from {from_encoding} to {to_encoding}",
                data={"decoded": decoded, "from_encoding": from_encoding, "to_encoding": to_encoding},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"EncodingConversion error: {e}")

"""Data converter action module for RabAI AutoClick.

Provides data conversion:
- DataConverterAction: Convert data formats
- FormatConverterAction: Convert between formats
- TypeConverterAction: Convert data types
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataConverterAction(BaseAction):
    """Convert data formats."""
    action_type = "data_converter"
    display_name = "数据转换器"
    description = "转换数据格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            from_format = params.get("from_format", "dict")
            to_format = params.get("to_format", "json")

            if from_format == "dict" and to_format == "json":
                import json
                converted = json.dumps(data)
            elif from_format == "json" and to_format == "dict":
                import json
                converted = json.loads(data) if isinstance(data, str) else data
            elif from_format == "dict" and to_format == "xml":
                converted = self._dict_to_xml(data)
            elif from_format == "xml" and to_format == "dict":
                converted = self._xml_to_dict(data)
            else:
                converted = data

            return ActionResult(
                success=True,
                data={
                    "from_format": from_format,
                    "to_format": to_format,
                    "converted": converted
                },
                message=f"Converted: {from_format} -> {to_format}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data converter error: {str(e)}")

    def _dict_to_xml(self, data: Dict, root: str = "root") -> str:
        xml = f"<{root}>"
        for key, value in data.items():
            if isinstance(value, dict):
                xml += self._dict_to_xml(value, key)
            else:
                xml += f"<{key}>{value}</{key}>"
        xml += f"</{root}>"
        return xml

    def _xml_to_dict(self, xml: str) -> Dict:
        import re
        result = {}
        tags = re.findall(r"<(\w+)>(.*?)</\1>", xml, re.DOTALL)
        for tag, value in tags:
            result[tag] = value
        return result


class FormatConverterAction(BaseAction):
    """Convert between formats."""
    action_type = "format_converter"
    display_name = "格式转换器"
    description = "格式间转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            from_fmt = params.get("from", "csv")
            to_fmt = params.get("to", "json")

            if from_fmt == "csv" and to_fmt == "json":
                lines = data.strip().split("\n")
                headers = lines[0].split(",") if lines else []
                result = []
                for line in lines[1:]:
                    values = line.split(",")
                    result.append(dict(zip(headers, values)))
            elif from_fmt == "json" and to_fmt == "csv":
                import json
                items = json.loads(data) if isinstance(data, str) else data
                if items and isinstance(items, list) and len(items) > 0:
                    headers = list(items[0].keys())
                    csv = ",".join(headers) + "\n"
                    for item in items:
                        csv += ",".join(str(item.get(h, "")) for h in headers) + "\n"
                    result = csv
                else:
                    result = ""
            else:
                result = data

            return ActionResult(
                success=True,
                data={
                    "from": from_fmt,
                    "to": to_fmt,
                    "result": result
                },
                message=f"Format conversion: {from_fmt} -> {to_fmt}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Format converter error: {str(e)}")


class TypeConverterAction(BaseAction):
    """Convert data types."""
    action_type = "type_converter"
    display_name = "类型转换器"
    description = "数据类型转换"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", None)
            to_type = params.get("to_type", "string")

            if data is None:
                return ActionResult(success=False, message="data is required")

            converted = None
            if to_type == "string":
                converted = str(data)
            elif to_type == "int" or to_type == "integer":
                converted = int(float(data)) if data else 0
            elif to_type == "float":
                converted = float(data) if data else 0.0
            elif to_type == "bool" or to_type == "boolean":
                converted = bool(data) if data is not None else False
            elif to_type == "list":
                converted = list(data) if isinstance(data, (list, tuple, set)) else [data]
            elif to_type == "dict":
                if isinstance(data, dict):
                    converted = data
                elif isinstance(data, str):
                    import json
                    converted = json.loads(data)
                else:
                    converted = {"value": data}
            else:
                converted = str(data)

            return ActionResult(
                success=True,
                data={
                    "original": data,
                    "original_type": type(data).__name__,
                    "converted": converted,
                    "converted_type": to_type
                },
                message=f"Type conversion: {type(data).__name__} -> {to_type}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Type converter error: {str(e)}")

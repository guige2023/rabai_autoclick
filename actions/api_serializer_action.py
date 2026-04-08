"""API serializer action module for RabAI AutoClick.

Provides API serialization operations:
- JSONSerializerAction: Serialize data to JSON
- XMLSerializerAction: Serialize data to XML
- CSVSerializerAction: Serialize data to CSV
- MultiFormatSerializerAction: Serialize to multiple formats
- URLEncoderSerializerAction: Encode data as URL parameters
"""

import json
import csv
import io
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class JSONSerializerAction(BaseAction):
    """Serialize data to JSON format."""
    action_type = "api_json_serializer"
    display_name = "JSON序列化器"
    description = "将数据序列化为JSON格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            indent = params.get("indent", 2)
            sort_keys = params.get("sort_keys", False)
            ensure_ascii = params.get("ensure_ascii", False)
            include_fields = params.get("include_fields")
            exclude_fields = params.get("exclude_fields", [])

            if isinstance(data, list):
                serialized_list = []
                for item in data:
                    if not isinstance(item, dict):
                        serialized_list.append(item)
                        continue
                    record = dict(item)
                    if include_fields:
                        record = {k: v for k, v in record.items() if k in include_fields}
                    if exclude_fields:
                        record = {k: v for k, v in record.items() if k not in exclude_fields}
                    serialized_list.append(record)
                data = serialized_list
            elif isinstance(data, dict):
                if include_fields:
                    data = {k: v for k, v in data.items() if k in include_fields}
                if exclude_fields:
                    data = {k: v for k, v in data.items() if k not in exclude_fields}

            result = json.dumps(
                data,
                indent=indent,
                sort_keys=sort_keys,
                ensure_ascii=ensure_ascii,
                default=str
            )

            return ActionResult(
                success=True,
                data={
                    "serialized": result,
                    "format": "json",
                    "bytes_length": len(result.encode("utf-8")),
                    "indent": indent
                },
                message=f"Serialized to JSON: {len(result)} chars"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON serializer error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"indent": 2, "sort_keys": False, "ensure_ascii": False, "include_fields": None, "exclude_fields": []}


class XMLSerializerAction(BaseAction):
    """Serialize data to XML format."""
    action_type = "api_xml_serializer"
    display_name = "XML序列化器"
    description = "将数据序列化为XML格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            root_tag = params.get("root_tag", "root")
            item_tag = params.get("item_tag", "item")
            include_attributes = params.get("include_attributes", [])
            xml_declaration = params.get("xml_declaration", True)
            indent = params.get("indent", 2)

            def to_xml(obj: Any, tag: str, level: int = 0) -> str:
                indent_str = " " * (indent * level)
                if isinstance(obj, dict):
                    attrs = ""
                    children = []
                    for k, v in obj.items():
                        if k in include_attributes and isinstance(v, (str, int, float)):
                            attrs += f' {k}="{v}"'
                        else:
                            children.append(to_xml(v, k, level + 1))
                    return f"{indent_str}<{tag}{attrs}>{''.join(children)}</{tag}>"
                elif isinstance(obj, list):
                    items = [to_xml(item, item_tag, level + 1) for item in obj]
                    return f"{indent_str}<{tag}>{"".join(items)}</{tag}>"
                else:
                    return f"{indent_str}<{tag}>{obj}</{tag}>"

            if isinstance(data, list):
                root_content = "".join(to_xml(item, item_tag, 1) for item in data)
                xml_content = f"<{root_tag}>{root_content}</{root_tag}>"
            else:
                xml_content = to_xml(data, root_tag)

            if xml_declaration:
                xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_content

            return ActionResult(
                success=True,
                data={
                    "serialized": xml_content,
                    "format": "xml",
                    "bytes_length": len(xml_content.encode("utf-8"))
                },
                message=f"Serialized to XML: {len(xml_content)} chars"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"XML serializer error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"root_tag": "root", "item_tag": "item", "include_attributes": [], "xml_declaration": True, "indent": 2}


class CSVSerializerAction(BaseAction):
    """Serialize data to CSV format."""
    action_type = "api_csv_serializer"
    display_name = "CSV序列化器"
    description = "将数据序列化为CSV格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            delimiter = params.get("delimiter", ",")
            quotechar = params.get("quotechar", '"')
            quoting = params.get("quoting", "minimal")
            include_header = params.get("include_header", True)
            columns = params.get("columns")

            if not isinstance(data, list):
                data = [data]

            if not data:
                return ActionResult(success=False, message="No data to serialize")

            if columns:
                header = columns
                rows = [[row.get(col) for col in columns] for row in data if isinstance(row, dict)]
            elif isinstance(data[0], dict):
                header = list(data[0].keys())
                rows = [[row.get(col) for col in header] for row in data]
            else:
                header = ["value"]
                rows = [[item] for item in data]

            output = io.StringIO()
            writer = csv.writer(output, delimiter=delimiter, quotechar=quotechar)

            if quoting == "minimal":
                writer.quoting = csv.QUOTE_MINIMAL
            elif quoting == "all":
                writer.quoting = csv.QUOTE_ALL
            elif quoting == "nonnumeric":
                writer.quoting = csv.QUOTE_NONNUMERIC
            else:
                writer.quoting = csv.QUOTE_NONE

            if include_header:
                writer.writerow(header)
            writer.writerows(rows)

            result = output.getvalue()

            return ActionResult(
                success=True,
                data={
                    "serialized": result,
                    "format": "csv",
                    "rows": len(rows),
                    "columns": header,
                    "bytes_length": len(result.encode("utf-8"))
                },
                message=f"Serialized to CSV: {len(rows)} rows"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CSV serializer error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"delimiter": ",", "quotechar": '"', "quoting": "minimal", "include_header": True, "columns": None}


class MultiFormatSerializerAction(BaseAction):
    """Serialize to multiple formats."""
    action_type = "api_multi_format_serializer"
    display_name = "多格式序列化器"
    description = "序列化为多种格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            formats = params.get("formats", ["json"])
            default_format = params.get("default_format", "json")

            results = {}

            for fmt in formats:
                try:
                    if fmt == "json":
                        serializer = JSONSerializerAction()
                        result = serializer.execute(context, {"data": data, "indent": 2})
                    elif fmt == "xml":
                        serializer = XMLSerializerAction()
                        result = serializer.execute(context, {"data": data})
                    elif fmt == "csv":
                        serializer = CSVSerializerAction()
                        result = serializer.execute(context, {"data": data})
                    else:
                        continue

                    if result.success:
                        results[fmt] = result.data.get("serialized")
                except Exception:
                    pass

            if not results:
                return ActionResult(success=False, message="No formats serialized successfully")

            primary = results.get(default_format, results.get("json", list(results.values())[0]))

            return ActionResult(
                success=True,
                data={
                    "serialized": primary,
                    "all_formats": {fmt: len(data) for fmt, data in results.items()},
                    "formats_count": len(results),
                    "default_format": default_format
                },
                message=f"Serialized to {len(results)} formats"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Multi-format serializer error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"formats": ["json"], "default_format": "json"}


class URLEncoderSerializerAction(BaseAction):
    """Encode data as URL parameters."""
    action_type = "api_url_encoder_serializer"
    display_name = "URL编码序列化器"
    description = "将数据编码为URL参数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", {})
            include_empty = params.get("include_empty", False)
            array_format = params.get("array_format", "repeat")
            sort_params = params.get("sort_params", False)

            import urllib.parse

            encoded_pairs = []
            for key, value in data.items():
                if value is None or (value == "" and not include_empty):
                    continue

                if isinstance(value, list):
                    if array_format == "repeat":
                        for item in value:
                            encoded_pairs.append((urllib.parse.quote(str(key)), urllib.parse.quote(str(item))))
                    elif array_format == "bracket":
                        for item in value:
                            encoded_pairs.append((urllib.parse.quote(f"{key}[]"), urllib.parse.quote(str(item))))
                    elif array_format == "comma":
                        encoded_pairs.append((urllib.parse.quote(str(key)), urllib.parse.quote(",".join(str(v) for v in value))))
                else:
                    encoded_pairs.append((urllib.parse.quote(str(key)), urllib.parse.quote(str(value))))

            if sort_params:
                encoded_pairs.sort()

            encoded_str = "&".join(f"{k}={v}" for k, v in encoded_pairs)

            return ActionResult(
                success=True,
                data={
                    "serialized": encoded_str,
                    "format": "urlencoded",
                    "params_count": len(encoded_pairs),
                    "bytes_length": len(encoded_str.encode("utf-8"))
                },
                message=f"URL encoded {len(encoded_pairs)} parameters"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"URL encoder error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"include_empty": False, "array_format": "repeat", "sort_params": False}

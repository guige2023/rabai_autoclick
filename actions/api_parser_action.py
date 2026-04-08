"""API parser action module for RabAI AutoClick.

Provides API parsing operations:
- JSONParserAction: Parse JSON API responses
- XMLParserAction: Parse XML API responses
- CSVParserAction: Parse CSV API responses
- MultiFormatParserAction: Auto-detect and parse multiple formats
- StreamingParserAction: Parse streaming API responses
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


class JSONParserAction(BaseAction):
    """Parse JSON API responses."""
    action_type = "api_json_parser"
    display_name = "JSON解析器"
    description = "解析JSON API响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            extraction_path = params.get("extraction_path")
            default_value = params.get("default_value", None)
            flatten = params.get("flatten", False)
            parse_array = params.get("parse_array", False)

            if isinstance(data, str):
                try:
                    parsed = json.loads(data)
                except json.JSONDecodeError as e:
                    return ActionResult(success=False, message=f"JSON parse error: {str(e)}")
            elif isinstance(data, (dict, list)):
                parsed = data
            else:
                return ActionResult(success=False, message=f"Unsupported data type: {type(data)}")

            if extraction_path:
                parts = extraction_path.split(".")
                current = parsed
                for part in parts:
                    if isinstance(current, dict):
                        current = current.get(part, default_value)
                    elif isinstance(current, list):
                        try:
                            idx = int(part)
                            current = current[idx] if 0 <= idx < len(current) else default_value
                        except ValueError:
                            current = default_value
                    else:
                        current = default_value
                    if current is None:
                        break
                parsed = current

            if flatten and isinstance(parsed, dict):
                parsed = self._flatten_dict(parsed)

            if parse_array and isinstance(parsed, list):
                items = []
                for item in parsed:
                    if isinstance(item, str):
                        try:
                            items.append(json.loads(item))
                        except Exception:
                            items.append(item)
                    else:
                        items.append(item)
                parsed = items

            return ActionResult(
                success=True,
                data={
                    "parsed": parsed,
                    "data_type": type(parsed).__name__,
                    "extraction_path": extraction_path,
                },
                message=f"Parsed JSON: extracted {type(parsed).__name__}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"JSON parser error: {str(e)}")

    def _flatten_dict(self, d: Dict, parent_key: str = "", sep: str = ".") -> Dict:
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"extraction_path": None, "default_value": None, "flatten": False, "parse_array": False}


class XMLParserAction(BaseAction):
    """Parse XML API responses."""
    action_type = "api_xml_parser"
    display_name = "XML解析器"
    description = "解析XML API响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            xpath = params.get("xpath")
            tag_name = params.get("tag_name")
            attributes_as_dict = params.get("attributes_as_dict", True)
            flatten_text = params.get("flatten_text", True)

            import re

            if isinstance(data, bytes):
                data = data.decode("utf-8", errors="ignore")

            if not tag_name and not xpath:
                tag_name = "item"

            extracted = []

            if tag_name:
                pattern = rf"<{tag_name}([^>]*)>([^<]*)</{tag_name}>"
                matches = re.findall(pattern, data)
                for attr_str, text in matches:
                    attr_dict = {}
                    if attributes_as_dict and attr_str:
                        attr_pattern = r'(\w+)="([^"]*)"'
                        attr_matches = re.findall(attr_pattern, attr_str)
                        attr_dict = {k: v for k, v in attr_matches}

                    if flatten_text:
                        text = text.strip()
                    entry = {"_text": text} if text else {}
                    if attr_dict:
                        entry.update(attr_dict)
                    extracted.append(entry)

            return ActionResult(
                success=True,
                data={
                    "parsed": extracted,
                    "count": len(extracted),
                    "tag_name": tag_name,
                    "xpath": xpath
                },
                message=f"Extracted {len(extracted)} {tag_name} elements"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"XML parser error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"xpath": None, "tag_name": "item", "attributes_as_dict": True, "flatten_text": True}


class CSVParserAction(BaseAction):
    """Parse CSV API responses."""
    action_type = "api_csv_parser"
    display_name = "CSV解析器"
    description = "解析CSV API响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            delimiter = params.get("delimiter", ",")
            has_header = params.get("has_header", True)
            quotechar = params.get("quotechar", '"')
            max_rows = params.get("max_rows")
            columns = params.get("columns")

            if isinstance(data, str):
                csv_file = io.StringIO(data)
            elif isinstance(data, list):
                csv_file = io.StringIO("\n".join(str(row) for row in data))
            else:
                return ActionResult(success=False, message=f"Unsupported data type: {type(data)}")

            reader = csv.reader(csv_file, delimiter=delimiter, quotechar=quotechar)
            rows = list(reader)

            if not rows:
                return ActionResult(success=False, message="Empty CSV data")

            if has_header and rows:
                headers = rows[0]
                data_rows = rows[1:]
            else:
                headers = [f"col_{i}" for i in range(len(rows[0]))]
                data_rows = rows

            if max_rows:
                data_rows = data_rows[:max_rows]

            parsed = []
            for row in data_rows:
                record = {}
                for i, value in enumerate(row):
                    key = headers[i] if i < len(headers) else f"col_{i}"
                    record[key] = value.strip() if isinstance(value, str) else value
                parsed.append(record)

            if columns:
                parsed = [{k: v for k, v in record.items() if k in columns} for record in parsed]

            return ActionResult(
                success=True,
                data={
                    "parsed": parsed,
                    "count": len(parsed),
                    "headers": headers if has_header else None,
                    "columns_requested": columns
                },
                message=f"Parsed CSV: {len(parsed)} rows"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"CSV parser error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"delimiter": ",", "has_header": True, "quotechar": '"', "max_rows": None, "columns": None}


class MultiFormatParserAction(BaseAction):
    """Auto-detect and parse multiple formats."""
    action_type = "api_multi_format_parser"
    display_name = "多格式解析器"
    description = "自动检测并解析多种格式"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", "")
            preferred_format = params.get("preferred_format", "auto")
            fallback_on_error = params.get("fallback_on_error", True)
            extract_key = params.get("extract_key")

            detected_format = self._detect_format(data)

            if preferred_format != "auto" and preferred_format in ("json", "xml", "csv"):
                format_to_use = preferred_format
            else:
                format_to_use = detected_format

            if format_to_use == "json":
                parser_action = JSONParserAction()
                result = parser_action.execute(context, {
                    "data": data,
                    "extraction_path": extract_key,
                    "flatten": False
                })
            elif format_to_use == "xml":
                parser_action = XMLParserAction()
                result = parser_action.execute(context, {
                    "data": data,
                    "flatten_text": True
                })
            elif format_to_use == "csv":
                parser_action = CSVParserAction()
                result = parser_action.execute(context, {
                    "data": data,
                    "has_header": True
                })
            else:
                return ActionResult(success=False, message=f"Unknown format: {detected_format}")

            if not result.success and fallback_on_error:
                for fmt in ["json", "xml", "csv"]:
                    if fmt == format_to_use:
                        continue
                    try:
                        if fmt == "json":
                            parser = JSONParserAction()
                            result = parser.execute(context, {"data": data})
                        elif fmt == "xml":
                            parser = XMLParserAction()
                            result = parser.execute(context, {"data": data})
                        elif fmt == "csv":
                            parser = CSVParserAction()
                            result = parser.execute(context, {"data": data})
                        if result.success:
                            format_to_use = fmt
                            break
                    except Exception:
                        continue

            return ActionResult(
                success=result.success,
                data={
                    "parsed": result.data.get("parsed") if result.success else None,
                    "detected_format": detected_format,
                    "parsed_format": format_to_use,
                    "fallback_used": format_to_use != detected_format if preferred_format == "auto" else False
                },
                message=f"Multi-format parse: detected {detected_format}, parsed as {format_to_use}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Multi-format parser error: {str(e)}")

    def _detect_format(self, data: str) -> str:
        if isinstance(data, str):
            data = data.strip()
            if data.startswith("{") or data.startswith("["):
                return "json"
            if data.startswith("<"):
                return "xml"
            if "," in data and "\n" in data:
                return "csv"
        return "unknown"

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"preferred_format": "auto", "fallback_on_error": True, "extract_key": None}


class StreamingParserAction(BaseAction):
    """Parse streaming API responses."""
    action_type = "api_streaming_parser"
    display_name = "流式解析器"
    description = "解析流式API响应"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            chunks = params.get("chunks", [])
            chunk_delimiter = params.get("chunk_delimiter", "\n")
            parse_format = params.get("parse_format", "json")
            buffer_size = params.get("buffer_size", 10)

            if not chunks:
                return ActionResult(success=False, message="No chunks provided")

            parsed_items = []
            buffer = []

            for chunk in chunks:
                chunk_str = str(chunk)
                parts = chunk_str.split(chunk_delimiter)

                for part in parts:
                    if not part.strip():
                        continue

                    if parse_format == "json":
                        try:
                            parsed_items.append(json.loads(part))
                        except json.JSONDecodeError:
                            buffer.append(part)
                    elif parse_format == "csv":
                        buffer.append(part)
                        if len(buffer) >= buffer_size:
                            parsed_items.append(buffer.copy())
                            buffer = []
                    else:
                        parsed_items.append(part)

            if buffer and parse_format == "csv":
                parsed_items.extend(buffer)

            return ActionResult(
                success=True,
                data={
                    "parsed": parsed_items,
                    "chunk_count": len(chunks),
                    "item_count": len(parsed_items),
                    "parse_format": parse_format,
                    "buffered_items": len(buffer)
                },
                message=f"Streaming parse: {len(parsed_items)} items from {len(chunks)} chunks"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Streaming parser error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["chunks"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"chunk_delimiter": "\n", "parse_format": "json", "buffer_size": 10}

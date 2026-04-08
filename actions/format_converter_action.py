"""Format Converter Action Module.

Provides bidirectional format conversion between JSON, YAML, XML,
CSV, TSV, INI, TOML, and other common data serialization formats.
"""
from __future__ import annotations

import csv
import io
import json
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class FormatType(Enum):
    """Supported format types."""
    JSON = "json"
    YAML = "yaml"
    XML = "xml"
    CSV = "csv"
    TSV = "tsv"
    INI = "ini"
    TOML = "toml"
    ENV = "env"
    URL_PARAMS = "url_params"
    MULTIPART = "multipart"


@dataclass
class ConversionResult:
    """Result of format conversion."""
    success: bool
    source_format: str
    target_format: str
    output: Any
    error: Optional[str] = None
    duration_ms: float = 0.0


class FormatParser:
    """Base format parser."""

    def parse(self, data: str) -> Any:
        """Parse string data to Python object."""
        raise NotImplementedError

    def serialize(self, obj: Any) -> str:
        """Serialize Python object to string."""
        raise NotImplementedError


class JSONParser(FormatParser):
    """JSON format parser."""

    def parse(self, data: str) -> Any:
        """Parse JSON string."""
        return json.loads(data)

    def serialize(self, obj: Any) -> str:
        """Serialize to JSON string."""
        return json.dumps(obj, indent=2, ensure_ascii=False)


class YAMLParser(FormatParser):
    """YAML format parser.

    Simulated - in production use PyYAML.
    """

    def parse(self, data: str) -> Any:
        """Parse YAML string (simplified simulation)."""
        result = {}
        current_section = result
        section_stack = []
        lines = data.strip().split("\n")

        for line in lines:
            if not line or line.startswith("#"):
                continue

            indent = len(line) - len(line.lstrip())
            line = line.strip()

            if ":" in line:
                key, _, value = line.partition(":")
                key = key.strip()
                value = value.strip().strip("'\"")

                if indent == 0:
                    if value:
                        result[key] = self._parse_value(value)
                    else:
                        result[key] = {}
                        current_section = result[key]
                        section_stack = [result]
                    continue

                parent_indent = (len(section_stack) - 1) * 2 if section_stack else 0
                while indent <= parent_indent and section_stack:
                    section_stack.pop()
                    parent_indent = (len(section_stack) - 1) * 2 if section_stack else 0

                if value:
                    if section_stack:
                        section_stack[-1][key] = self._parse_value(value)
                    else:
                        result[key] = self._parse_value(value)
                else:
                    if section_stack:
                        section_stack[-1][key] = {}
                        section_stack.append(section_stack[-1][key])
                    else:
                        result[key] = {}
                        section_stack.append(result[key])

        return result

    def _parse_value(self, value: str) -> Any:
        """Parse YAML value."""
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        if value.lower() == "null":
            return None
        if value.lower() == "none":
            return None

        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            pass

        if (value.startswith("'") and value.endswith("'")) or \
           (value.startswith('"') and value.endswith('"')):
            return value[1:-1]

        return value

    def serialize(self, obj: Any, indent: int = 0) -> str:
        """Serialize to YAML string (simplified)."""
        lines = []
        prefix = "  " * indent

        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, (dict, list)):
                    lines.append(f"{prefix}{key}:")
                    lines.append(self.serialize(value, indent + 1))
                else:
                    lines.append(f"{prefix}{key}: {value}")
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    lines.append(f"{prefix}-")
                    lines.append(self.serialize(item, indent + 1))
                else:
                    lines.append(f"{prefix}- {item}")
        else:
            lines.append(f"{prefix}{obj}")

        return "\n".join(lines)


class XMLParser(FormatParser):
    """XML format parser (simplified)."""

    def parse(self, data: str) -> Any:
        """Parse XML string to dict (simplified)."""
        result = {}
        stack = [(None, result)]
        current_text = []

        tag_pattern = re.compile(r"<(\/?)([\w\-]+)([^>]*)>")
        attr_pattern = re.compile(r'([\w\-]+)="([^"]*)"')

        i = 0
        while i < len(data):
            match = tag_pattern.search(data, i)
            if not match:
                break

            text = data[i:match.start()].strip()
            if text:
                current_text.append(text)

            is_closing = match.group(1) == "/"
            tag_name = match.group(2)
            attrs_str = match.group(3)

            attrs = dict(attr_pattern.findall(attrs_str))

            if is_closing:
                parent_dict = stack[-1][1]
                text_content = " ".join(current_text).strip()
                if text_content:
                    parent_dict[tag_name] = text_content
                elif tag_name in parent_dict:
                    if not isinstance(parent_dict[tag_name], list):
                        parent_dict[tag_name] = [parent_dict[tag_name]]
                    if attrs:
                        parent_dict[tag_name].append(attrs)
                current_text = []
                stack.pop()
            else:
                new_dict = {}
                if attrs:
                    new_dict = {"_attrs": attrs}
                if stack:
                    parent_dict = stack[-1][1]
                    if tag_name in parent_dict:
                        if not isinstance(parent_dict[tag_name], list):
                            parent_dict[tag_name] = [parent_dict[tag_name]]
                        parent_dict[tag_name].append(new_dict)
                    else:
                        parent_dict[tag_name] = new_dict
                stack.append((tag_name, new_dict))

            i = match.end()

        return result

    def serialize(self, obj: Any, root: str = "root") -> str:
        """Serialize dict to XML string."""
        lines = [f'<?xml version="1.0" encoding="UTF-8"?>']
        lines.append(f"<{root}>")

        def serialize_value(value: Any, indent: int = 1) -> None:
            prefix = "  " * indent
            if isinstance(value, dict):
                for key, val in value.items():
                    if key == "_attrs":
                        continue
                    if isinstance(val, dict):
                        attrs = val.get("_attrs", {})
                        attr_str = "".join(f' {k}="{v}"' for k, v in attrs.items())
                        lines.append(f"{prefix}<{key}{attr_str}>")
                        serialize_value({k: v for k, v in val.items() if k != "_attrs"}, indent + 1)
                        lines.append(f"{prefix}</{key}>")
                    elif isinstance(val, list):
                        for item in val:
                            if isinstance(item, dict):
                                attrs = item.get("_attrs", {})
                                attr_str = "".join(f' {k}="{v}"' for k, v in attrs.items())
                                lines.append(f"{prefix}<{key}{attr_str}>")
                                serialize_value({k: v for k, v in item.items() if k != "_attrs"}, indent + 1)
                                lines.append(f"{prefix}</{key}>")
                            else:
                                lines.append(f"{prefix}<{key}>{item}</{key}>")
                    else:
                        lines.append(f"{prefix}<{key}>{val}</{key}>")

        serialize_value(obj)
        lines.append(f"</{root}>")
        return "\n".join(lines)


class CSVParser(FormatParser):
    """CSV format parser."""

    def parse(self, data: str) -> List[Dict[str, Any]]:
        """Parse CSV string to list of dicts."""
        reader = csv.DictReader(io.StringIO(data))
        return list(reader)

    def serialize(self, obj: List[Dict[str, Any]]) -> str:
        """Serialize list of dicts to CSV string."""
        if not obj:
            return ""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=obj[0].keys())
        writer.writeheader()
        writer.writerows(obj)
        return output.getvalue()


class TSVParser(FormatParser):
    """TSV format parser."""

    def parse(self, data: str) -> List[Dict[str, Any]]:
        """Parse TSV string to list of dicts."""
        reader = csv.DictReader(io.StringIO(data), delimiter="\t")
        return list(reader)

    def serialize(self, obj: List[Dict[str, Any]]) -> str:
        """Serialize list of dicts to TSV string."""
        if not obj:
            return ""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=obj[0].keys(), delimiter="\t")
        writer.writeheader()
        writer.writerows(obj)
        return output.getvalue()


class INIParser(FormatParser):
    """INI format parser."""

    def parse(self, data: str) -> Dict[str, Dict[str, str]]:
        """Parse INI string to nested dict."""
        result: Dict[str, Dict[str, str]] = {}
        current_section = None

        for line in data.strip().split("\n"):
            line = line.strip()
            if not line or line.startswith(";") or line.startswith("#"):
                continue

            if line.startswith("[") and line.endswith("]"):
                current_section = line[1:-1]
                result[current_section] = {}
                continue

            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip("'\"")

                if current_section:
                    result[current_section][key] = value
                else:
                    result[key] = value

        return result

    def serialize(self, obj: Dict[str, Any]) -> str:
        """Serialize to INI string."""
        lines = []

        for key, value in obj.items():
            if isinstance(value, dict):
                lines.append(f"[{key}]")
                for k, v in value.items():
                    lines.append(f"{k} = {v}")
                lines.append("")
            else:
                lines.append(f"{key} = {value}")

        return "\n".join(lines)


class URLParamsParser(FormatParser):
    """URL query parameters parser."""

    def parse(self, data: str) -> Dict[str, str]:
        """Parse URL query string."""
        from urllib.parse import parse_qs
        parsed = parse_qs(data)
        return {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}

    def serialize(self, obj: Dict[str, Any]) -> str:
        """Serialize to URL query string."""
        from urllib.parse import urlencode
        return urlencode(obj)


class FormatConverterAction:
    """Format conversion action supporting multiple formats.

    Example:
        action = FormatConverterAction()

        json_data = action.convert("json", {"name": "test", "value": 123})
        yaml_data = action.convert(json_data, from_format="json", to_format="yaml")
        xml_data = action.convert(yaml_data, from_format="yaml", to_format="xml")
    """

    def __init__(self):
        self._parsers: Dict[str, FormatParser] = {
            "json": JSONParser(),
            "yaml": YAMLParser(),
            "xml": XMLParser(),
            "csv": CSVParser(),
            "tsv": TSVParser(),
            "ini": INIParser(),
            "url_params": URLParamsParser(),
        }

    def convert(self, data: Any, from_format: Optional[str] = None,
                to_format: Optional[str] = None,
                params: Optional[Dict[str, Any]] = None) -> ConversionResult:
        """Convert data between formats.

        Args:
            data: Input data (string for parsing, dict/list for serialization)
            from_format: Source format (auto-detected if None)
            to_format: Target format
            params: Additional conversion parameters

        Returns:
            ConversionResult with converted output
        """
        start = time.time()
        params = params or {}

        try:
            if from_format is None:
                from_format = self._detect_format(data)

            if from_format not in self._parsers:
                return ConversionResult(
                    success=False,
                    source_format=from_format or "unknown",
                    target_format=to_format or "unknown",
                    output=None,
                    error=f"Unsupported source format: {from_format}",
                    duration_ms=(time.time() - start) * 1000
                )

            if to_format not in self._parsers:
                return ConversionResult(
                    success=False,
                    source_format=from_format,
                    target_format=to_format or "unknown",
                    output=None,
                    error=f"Unsupported target format: {to_format}",
                    duration_ms=(time.time() - start) * 1000
                )

            source_parser = self._parsers[from_format]
            target_parser = self._parsers[to_format]

            if isinstance(data, str):
                parsed = source_parser.parse(data)
            else:
                parsed = data

            if to_format == "json":
                output = target_parser.serialize(parsed)
            else:
                output = target_parser.serialize(parsed)

            return ConversionResult(
                success=True,
                source_format=from_format,
                target_format=to_format,
                output=output,
                duration_ms=(time.time() - start) * 1000
            )

        except Exception as e:
            return ConversionResult(
                success=False,
                source_format=from_format or "unknown",
                target_format=to_format or "unknown",
                output=None,
                error=str(e),
                duration_ms=(time.time() - start) * 1000
            )

    def _detect_format(self, data: str) -> str:
        """Auto-detect format from string content."""
        if isinstance(data, str):
            data = data.strip()

            if data.startswith("<?xml") or data.startswith("<"):
                return "xml"

            if data.startswith("{") or data.startswith("["):
                try:
                    json.loads(data)
                    return "json"
                except json.JSONDecodeError:
                    pass

            if ":" in data and "\n" in data:
                return "yaml"

            if "\t" in data and "," not in data.split("\n")[0]:
                return "tsv"

            if "," in data.split("\n")[0] and "\t" not in data:
                return "csv"

            if data.startswith("[") and data.endswith("]"):
                try:
                    json.loads(data)
                    return "json"
                except json.JSONDecodeError:
                    pass

        return "json"

    def parse(self, data: str, format: Optional[str] = None) -> Any:
        """Parse string data to Python object.

        Args:
            data: String data to parse
            format: Format type (auto-detected if None)

        Returns:
            Parsed Python object
        """
        if format is None:
            format = self._detect_format(data)

        if format not in self._parsers:
            raise ValueError(f"Unsupported format: {format}")

        return self._parsers[format].parse(data)

    def serialize(self, obj: Any, format: str) -> str:
        """Serialize Python object to string.

        Args:
            obj: Python object to serialize
            format: Target format

        Returns:
            Serialized string
        """
        if format not in self._parsers:
            raise ValueError(f"Unsupported format: {format}")

        return self._parsers[format].serialize(obj)


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute format conversion action.

    Args:
        context: Execution context
        params: Dict with keys:
            - operation: "convert", "parse", "serialize"
            - data: Input data (string or dict/list)
            - from_format: Source format
            - to_format: Target format
            - format: Format for parse/serialize operations

    Returns:
        Dict with success, output, source_format, target_format
    """
    operation = params.get("operation", "convert")
    action = FormatConverterAction()

    try:
        if operation == "convert":
            data = params.get("data")
            from_format = params.get("from_format")
            to_format = params.get("to_format")

            if data is None:
                return {"success": False, "message": "data required"}

            result = action.convert(data, from_format, to_format)

            return {
                "success": result.success,
                "output": result.output,
                "source_format": result.source_format,
                "target_format": result.target_format,
                "duration_ms": result.duration_ms,
                "error": result.error,
                "message": f"Converted from {result.source_format} to {result.target_format}"
            }

        elif operation == "parse":
            data = params.get("data")
            format = params.get("format")

            if data is None:
                return {"success": False, "message": "data required"}

            parsed = action.parse(data, format)
            return {
                "success": True,
                "parsed": parsed,
                "message": "Data parsed"
            }

        elif operation == "serialize":
            data = params.get("data")
            format = params.get("format")

            if data is None:
                return {"success": False, "message": "data required"}

            serialized = action.serialize(data, format)
            return {
                "success": True,
                "serialized": serialized,
                "message": f"Serialized to {format}"
            }

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Format conversion error: {str(e)}"}

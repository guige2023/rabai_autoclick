"""Data Decoder Action Module.

Provides data parsing and decoding for various formats including
JSON, XML, YAML, TOML, CSV, and binary formats.
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import struct
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

try:
    import toml
    TOML_AVAILABLE = True
except ImportError:
    TOML_AVAILABLE = False


class DataFormat(Enum):
    """Supported data formats."""
    JSON = "json"
    JSON_LINES = "jsonl"
    XML = "xml"
    YAML = "yaml"
    TOML = "toml"
    CSV = "csv"
    TSV = "tsv"
    URL_ENCODED = "url_encoded"
    PROPERTIES = "properties"
    BINARY = "binary"
    PACKED = "packed"


@dataclass
class ParseResult:
    """Result of a parsing operation."""
    success: bool
    data: Any = None
    format: DataFormat = DataFormat.JSON
    error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)


class JSONParser:
    """JSON parsing utilities."""

    @staticmethod
    def parse(data: str) -> ParseResult:
        """Parse JSON string."""
        try:
            return ParseResult(success=True, data=json.loads(data), format=DataFormat.JSON)
        except json.JSONDecodeError as e:
            return ParseResult(success=False, error=str(e), format=DataFormat.JSON)

    @staticmethod
    def parse_lines(data: str) -> ParseResult:
        """Parse JSON lines (one JSON object per line)."""
        results = []
        warnings = []
        for i, line in enumerate(data.strip().split("\n")):
            line = line.strip()
            if not line:
                continue
            try:
                results.append(json.loads(line))
            except json.JSONDecodeError as e:
                warnings.append(f"Line {i+1}: {str(e)}")

        return ParseResult(
            success=len(results) > 0,
            data=results,
            format=DataFormat.JSON_LINES,
            warnings=warnings
        )

    @staticmethod
    def parse_ndjson(data: str) -> ParseResult:
        """Parse newline-delimited JSON."""
        return JSONParser.parse_lines(data)

    @staticmethod
    def stringify(
        data: Any,
        indent: Optional[int] = None,
        sort_keys: bool = False
    ) -> str:
        """Convert data to JSON string."""
        return json.dumps(data, indent=indent, sort_keys=sort_keys)


class CSVParser:
    """CSV parsing utilities."""

    @staticmethod
    def parse(
        data: str,
        delimiter: str = ",",
        quotechar: str = '"',
        has_header: bool = True
    ) -> ParseResult:
        """Parse CSV string."""
        try:
            reader = csv.reader(io.StringIO(data), delimiter=delimiter, quotechar=quotechar)
            rows = list(reader)

            if has_header and rows:
                headers = rows[0]
                records = []
                for row in rows[1:]:
                    record = dict(zip(headers, row))
                    records.append(record)
                return ParseResult(success=True, data=records, format=DataFormat.CSV)
            else:
                return ParseResult(success=True, data=rows, format=DataFormat.CSV)

        except Exception as e:
            return ParseResult(success=False, error=str(e), format=DataFormat.CSV)

    @staticmethod
    def to_csv(
        data: List[Dict[str, Any]],
        headers: Optional[List[str]] = None,
        delimiter: str = ","
    ) -> str:
        """Convert list of dicts to CSV string."""
        if not data:
            return ""

        if headers is None:
            headers = list(data[0].keys()) if data else []

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers, delimiter=delimiter)
        writer.writeheader()
        writer.writerows(data)
        return output.getvalue()


class XMLParser:
    """Simple XML parsing utilities."""

    @staticmethod
    def parse(data: str) -> ParseResult:
        """Parse XML string to dict."""
        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(data)
            result = XMLParser._element_to_dict(root)
            return ParseResult(success=True, data=result, format=DataFormat.XML)
        except Exception as e:
            return ParseResult(success=False, error=str(e), format=DataFormat.XML)

    @staticmethod
    def _element_to_dict(element) -> Dict[str, Any]:
        """Convert XML element to dictionary."""
        result: Dict[str, Any] = {}

        # Attributes
        if element.attrib:
            result["@attributes"] = element.attrib

        # Text content
        if element.text and element.text.strip():
            if len(element) == 0:
                return element.text.strip()
            result["#text"] = element.text.strip()

        # Children
        for child in element:
            child_dict = XMLParser._element_to_dict(child)
            tag = child.tag

            if tag in result:
                if not isinstance(result[tag], list):
                    result[tag] = [result[tag]]
                result[tag].append(child_dict)
            else:
                result[tag] = child_dict

        return result

    @staticmethod
    def to_xml(data: Dict[str, Any], root_tag: str = "root") -> str:
        """Convert dictionary to XML string."""
        import xml.etree.ElementTree as ET
        import xml.dom.minidom

        def dict_to_element(parent, d):
            if isinstance(d, dict):
                for key, value in d.items():
                    if key == "@attributes":
                        for attr_name, attr_value in value.items():
                            parent.set(attr_name, str(attr_value))
                    elif key == "#text":
                        parent.text = str(value)
                    else:
                        if isinstance(value, list):
                            for item in value:
                                child = ET.SubElement(parent, key)
                                dict_to_element(child, item)
                        else:
                            child = ET.SubElement(parent, key)
                            dict_to_element(child, value)

        root = ET.Element(root_tag)
        dict_to_element(root, data)

        rough = ET.tostring(root, encoding="unicode")
        return rough


class YAMLParser:
    """YAML parsing utilities."""

    @staticmethod
    def parse(data: str) -> ParseResult:
        """Parse YAML string."""
        if not YAML_AVAILABLE:
            return ParseResult(success=False, error="YAML not available", format=DataFormat.YAML)

        try:
            result = yaml.safe_load(data)
            return ParseResult(success=True, data=result, format=DataFormat.YAML)
        except Exception as e:
            return ParseResult(success=False, error=str(e), format=DataFormat.YAML)

    @staticmethod
    def to_yaml(data: Any) -> str:
        """Convert data to YAML string."""
        if not YAML_AVAILABLE:
            raise ImportError("YAML not available")
        return yaml.dump(data, default_flow_style=False)


class TOMLParser:
    """TOML parsing utilities."""

    @staticmethod
    def parse(data: str) -> ParseResult:
        """Parse TOML string."""
        if not TOML_AVAILABLE:
            return ParseResult(success=False, error="TOML not available", format=DataFormat.TOML)

        try:
            result = toml.loads(data)
            return ParseResult(success=True, data=result, format=DataFormat.TOML)
        except Exception as e:
            return ParseResult(success=False, error=str(e), format=DataFormat.TOML)

    @staticmethod
    def to_toml(data: Dict[str, Any]) -> str:
        """Convert data to TOML string."""
        if not TOML_AVAILABLE:
            raise ImportError("TOML not available")
        return toml.dumps(data)


class BinaryParser:
    """Binary data parsing utilities."""

    @staticmethod
    def parse_fixed(data: bytes, struct_format: str) -> ParseResult:
        """Parse binary data using struct format."""
        try:
            size = struct.calcsize(struct_format)
            if len(data) < size:
                return ParseResult(success=False, error=f"Data too short: need {size}, got {len(data)}", format=DataFormat.BINARY)

            values = struct.unpack(struct_format, data[:size])
            return ParseResult(success=True, data=values, format=DataFormat.BINARY)
        except Exception as e:
            return ParseResult(success=False, error=str(e), format=DataFormat.BINARY)

    @staticmethod
    def pack_fixed(struct_format: str, *values) -> bytes:
        """Pack data using struct format."""
        return struct.pack(struct_format, *values)

    @staticmethod
    def parse_uint8(data: bytes) -> ParseResult:
        """Parse unsigned 8-bit integer."""
        return BinaryParser.parse_fixed(data, "B")

    @staticmethod
    def parse_uint16_le(data: bytes) -> ParseResult:
        """Parse unsigned 16-bit integer (little-endian)."""
        return BinaryParser.parse_fixed(data, "<H")

    @staticmethod
    def parse_uint32_le(data: bytes) -> ParseResult:
        """Parse unsigned 32-bit integer (little-endian)."""
        return BinaryParser.parse_fixed(data, "<I")


class URLEncodedParser:
    """URL-encoded data parsing."""

    @staticmethod
    def parse(data: str) -> ParseResult:
        """Parse URL-encoded string."""
        try:
            from urllib.parse import parse_qs, parse_qsl
            result = parse_qs(data)
            # Convert lists to single values where appropriate
            for key in result:
                if len(result[key]) == 1:
                    result[key] = result[key][0]
            return ParseResult(success=True, data=result, format=DataFormat.URL_ENCODED)
        except Exception as e:
            return ParseResult(success=False, error=str(e), format=DataFormat.URL_ENCODED)

    @staticmethod
    def to_url_encoded(data: Dict[str, Any]) -> str:
        """Convert dictionary to URL-encoded string."""
        from urllib.parse import urlencode
        return urlencode(data)


class PropertiesParser:
    """Java properties file parsing."""

    @staticmethod
    def parse(data: str) -> ParseResult:
        """Parse properties file content."""
        try:
            result: Dict[str, str] = {}
            for line in data.split("\n"):
                line = line.strip()
                if not line or line.startswith("#") or line.startswith("!"):
                    continue

                if "=" in line:
                    key, value = line.split("=", 1)
                elif ":" in line:
                    key, value = line.split(":", 1)
                else:
                    continue

                result[key.strip()] = value.strip()

            return ParseResult(success=True, data=result, format=DataFormat.PROPERTIES)
        except Exception as e:
            return ParseResult(success=False, error=str(e), format=DataFormat.PROPERTIES)


class DataDecoderAction:
    """Main action class for data decoding."""

    def __init__(self):
        self._parsers = {
            DataFormat.JSON: JSONParser.parse,
            DataFormat.JSON_LINES: JSONParser.parse_lines,
            DataFormat.CSV: CSVParser.parse,
            DataFormat.XML: XMLParser.parse,
            DataFormat.YAML: YAMLParser.parse,
            DataFormat.TOML: TOMLParser.parse,
            DataFormat.URL_ENCODED: URLEncodedParser.parse,
            DataFormat.PROPERTIES: PropertiesParser.parse,
            DataFormat.BINARY: BinaryParser.parse_fixed,
        }

    def parse(self, data: str, format: DataFormat, **kwargs) -> ParseResult:
        """Parse data using specified format."""
        parser = self._parsers.get(format)
        if not parser:
            return ParseResult(success=False, error=f"Unknown format: {format.value}")

        try:
            return parser(data, **kwargs)
        except TypeError:
            # Parser doesn't accept kwargs
            return parser(data)

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the data decoder action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - data: Data to parse
                - format: Data format
                - Other format-specific options

        Returns:
            Dictionary with parse results.
        """
        operation = context.get("operation", "parse")

        if operation == "parse":
            data = context.get("data", "")
            format_str = context.get("format", "json")

            try:
                fmt = DataFormat(format_str)
            except ValueError:
                return {"success": False, "error": f"Unknown format: {format_str}"}

            result = self.parse(data, fmt)

            return {
                "success": result.success,
                "data": result.data,
                "format": result.format.value,
                "error": result.error,
                "warnings": result.warnings
            }

        elif operation == "to_csv":
            data = context.get("data", [])
            headers = context.get("headers")
            delimiter = context.get("delimiter", ",")

            csv_data = CSVParser.to_csv(data, headers, delimiter)
            return {"success": True, "data": csv_data}

        elif operation == "to_xml":
            data = context.get("data", {})
            root_tag = context.get("root_tag", "root")
            xml_data = XMLParser.to_xml(data, root_tag)
            return {"success": True, "data": xml_data}

        elif operation == "to_yaml":
            if not YAML_AVAILABLE:
                return {"success": False, "error": "YAML not available"}
            data = context.get("data", {})
            yaml_data = YAMLParser.to_yaml(data)
            return {"success": True, "data": yaml_data}

        elif operation == "to_url_encoded":
            data = context.get("data", {})
            url_data = URLEncodedParser.to_url_encoded(data)
            return {"success": True, "data": url_data}

        elif operation == "detect_format":
            data = context.get("data", "").strip()
            detected = self._detect_format(data)
            return {"success": True, "format": detected.value if detected else None}

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _detect_format(self, data: str) -> Optional[DataFormat]:
        """Attempt to detect data format."""
        if not data:
            return None

        # JSON
        if data.startswith("{") or data.startswith("["):
            try:
                json.loads(data)
                return DataFormat.JSON
            except:
                pass

        # JSON Lines
        if "\n" in data and all(_is_json_line(l) for l in data.split("\n") if l.strip()):
            return DataFormat.JSON_LINES

        # XML
        if data.strip().startswith("<"):
            return DataFormat.XML

        # YAML (basic check)
        if YAML_AVAILABLE and any(c in data for c in ":-\n"):
            try:
                yaml.safe_load(data)
                return DataFormat.YAML
            except:
                pass

        # TOML
        if TOML_AVAILABLE and "=" in data and "[" in data:
            try:
                toml.loads(data)
                return DataFormat.TOML
            except:
                pass

        # CSV
        if "," in data.split("\n")[0]:
            return DataFormat.CSV

        return DataFormat.URL_ENCODED


def _is_json_line(line: str) -> bool:
    """Check if a line is valid JSON."""
    try:
        json.loads(line)
        return True
    except:
        return False

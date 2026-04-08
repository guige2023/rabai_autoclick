"""Data Parser Action Module.

Provides data parsing capabilities for various formats
including JSON, XML, CSV, and custom formats.
"""

from typing import Any, Dict, List, Optional, Callable, Union, TextIO
from dataclasses import dataclass, field
from enum import Enum
import json
import csv
import re
import xml.etree.ElementTree as ET
from io import StringIO
from datetime import datetime


class ParseFormat(Enum):
    """Supported parsing formats."""
    JSON = "json"
    XML = "xml"
    CSV = "csv"
    TSV = "tsv"
    URL_ENCODED = "url_encoded"
    FORM_DATA = "form_data"
    YAML = "yaml"
    INI = "ini"


@dataclass
class ParseResult:
    """Result of parsing operation."""
    success: bool
    data: Any = None
    error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CSVConfig:
    """Configuration for CSV parsing."""
    delimiter: str = ","
    quotechar: str = '"'
    escapechar: Optional[str] = None
    has_header: bool = True
    skip_rows: int = 0
    field_names: Optional[List[str]] = None
    encoding: str = "utf-8"


@dataclass
class XMLConfig:
    """Configuration for XML parsing."""
    attribute_prefix: str = "@"
    text_key: str = "#text"
    list_items: bool = True
    namespace_enabled: bool = False


class JSONParser:
    """Parses JSON data."""

    def __init__(self):
        self._strict = True

    def parse(
        self,
        json_str: str,
        encoding: str = "utf-8",
    ) -> ParseResult:
        """Parse JSON string."""
        try:
            if isinstance(json_str, bytes):
                json_str = json_str.decode(encoding)
            data = json.loads(json_str)
            return ParseResult(success=True, data=data)
        except json.JSONDecodeError as e:
            return ParseResult(
                success=False,
                error=f"JSON parse error: {str(e)}",
            )

    def parse_stream(
        self,
        lines: List[str],
    ) -> ParseResult:
        """Parse JSON lines (JSONL)."""
        results = []
        errors = []

        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue

            try:
                results.append(json.loads(line))
            except json.JSONDecodeError as e:
                errors.append(f"Line {i + 1}: {str(e)}")

        if errors and not results:
            return ParseResult(
                success=False,
                error=f"JSONL parse errors: {'; '.join(errors)}",
            )

        return ParseResult(
            success=True,
            data=results,
            warnings=errors if errors else [],
        )


class XMLParser:
    """Parses XML data."""

    def __init__(self, config: Optional[XMLConfig] = None):
        self.config = config or XMLConfig()

    def parse(
        self,
        xml_str: str,
    ) -> ParseResult:
        """Parse XML string."""
        try:
            if isinstance(xml_str, bytes):
                xml_str = xml_str.decode("utf-8")

            root = ET.fromstring(xml_str)
            data = self._element_to_dict(root)

            return ParseResult(success=True, data=data)
        except ET.ParseError as e:
            return ParseResult(
                success=False,
                error=f"XML parse error: {str(e)}",
            )

    def _element_to_dict(self, element: ET.Element) -> Any:
        """Convert XML element to dictionary."""
        result: Dict[str, Any] = {}

        if element.attrib:
            for key, value in element.attrib.items():
                result[f"{self.config.attribute_prefix}{key}"] = value

        if element.text and element.text.strip():
            if len(element) == 0:
                return element.text.strip()
            result[self.config.text_key] = element.text.strip()

        for child in element:
            child_data = self._element_to_dict(child)

            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_data)
            else:
                result[child.tag] = child_data

        return result

    def parse_file(
        self,
        file_path: str,
    ) -> ParseResult:
        """Parse XML file."""
        try:
            tree = ET.parse(file_path)
            root = tree.getroot()
            data = self._element_to_dict(root)
            return ParseResult(success=True, data=data)
        except Exception as e:
            return ParseResult(
                success=False,
                error=f"XML file parse error: {str(e)}",
            )


class CSVParser:
    """Parses CSV data."""

    def __init__(self, config: Optional[CSVConfig] = None):
        self.config = config or CSVConfig()

    def parse(
        self,
        csv_str: Union[str, bytes],
    ) -> ParseResult:
        """Parse CSV string."""
        try:
            if isinstance(csv_str, bytes):
                csv_str = csv_str.decode(self.config.encoding)

            if self.config.skip_rows > 0:
                lines = csv_str.splitlines()
                csv_str = "\n".join(lines[self.config.skip_rows:])

            reader = csv.reader(
                StringIO(csv_str),
                delimiter=self.config.delimiter,
                quotechar=self.config.quotechar,
                escapechar=self.config.escapechar,
            )

            rows = list(reader)

            if not rows:
                return ParseResult(success=True, data=[])

            if self.config.has_header:
                field_names = self.config.field_names or rows[0]
                data = [
                    dict(zip(field_names, row))
                    for row in rows[1:]
                ]
            else:
                data = [row for row in rows]

            return ParseResult(success=True, data=data)

        except Exception as e:
            return ParseResult(
                success=False,
                error=f"CSV parse error: {str(e)}",
            )

    def parse_with_header(
        self,
        csv_str: Union[str, bytes],
        field_names: List[str],
    ) -> ParseResult:
        """Parse CSV with provided header."""
        config = CSVConfig(field_names=field_names)
        parser = CSVParser(config)
        return parser.parse(csv_str)

    def to_csv(
        self,
        data: List[Dict[str, Any]],
        field_names: Optional[List[str]] = None,
    ) -> str:
        """Convert data to CSV string."""
        if not data:
            return ""

        if field_names is None:
            field_names = list(data[0].keys())

        output = StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=field_names,
            delimiter=self.config.delimiter,
            quotechar=self.config.quotechar,
        )

        writer.writeheader()
        writer.writerows(data)

        return output.getvalue()


class URLEncodedParser:
    """Parses URL-encoded data."""

    def parse(
        self,
        data: str,
    ) -> ParseResult:
        """Parse URL-encoded string."""
        try:
            from urllib.parse import parse_qs

            parsed = parse_qs(data, strict_parsing=True)
            result = {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}

            return ParseResult(success=True, data=result)

        except Exception as e:
            return ParseResult(
                success=False,
                error=f"URL-encoded parse error: {str(e)}",
            )

    def encode(
        self,
        data: Dict[str, Any],
    ) -> str:
        """Encode data as URL-encoded string."""
        from urllib.parse import urlencode
        return urlencode(data)


class FormDataParser:
    """Parses multipart form data."""

    def parse(
        self,
        data: str,
    ) -> ParseResult:
        """Parse form data."""
        result: Dict[str, Any] = {}
        fields = data.split("&")

        for field in fields:
            if "=" in field:
                key, value = field.split("=", 1)
                from urllib.parse import unquote
                key = unquote(key)
                value = unquote(value)
                result[key] = value

        return ParseResult(success=True, data=result)


class INIParser:
    """Parses INI configuration files."""

    def __init__(self):
        self._pattern = re.compile(r'^\[([^\]]+)\]$')
        self._kv_pattern = re.compile(r'^([^=]+)=(.*)$')

    def parse(
        self,
        ini_str: str,
    ) -> ParseResult:
        """Parse INI string."""
        try:
            result: Dict[str, Dict[str, str]] = {}
            current_section = None

            for line in ini_str.splitlines():
                line = line.strip()

                if not line or line.startswith("#") or line.startswith(";"):
                    continue

                section_match = self._pattern.match(line)
                if section_match:
                    current_section = section_match.group(1)
                    result[current_section] = {}
                    continue

                kv_match = self._kv_pattern.match(line)
                if kv_match and current_section:
                    key = kv_match.group(1).strip()
                    value = kv_match.group(2).strip()
                    result[current_section][key] = value

            return ParseResult(success=True, data=result)

        except Exception as e:
            return ParseResult(
                success=False,
                error=f"INI parse error: {str(e)}",
            )


class DataParser:
    """Main data parsing orchestrator."""

    def __init__(self):
        self.json_parser = JSONParser()
        self.csv_parser = CSVParser()
        self.xml_parser = XMLParser()
        self.url_parser = URLEncodedParser()
        self.form_parser = FormDataParser()
        self.ini_parser = INIParser()

    def parse(
        self,
        data: str,
        format: Union[ParseFormat, str],
    ) -> ParseResult:
        """Parse data with specified format."""
        if isinstance(format, str):
            format = ParseFormat(format)

        if format == ParseFormat.JSON:
            return self.json_parser.parse(data)
        elif format == ParseFormat.CSV:
            return self.csv_parser.parse(data)
        elif format == ParseFormat.TSV:
            config = CSVParser(CSVConfig(delimiter="\t"))
            return config.parse(data)
        elif format == ParseFormat.XML:
            return self.xml_parser.parse(data)
        elif format == ParseFormat.URL_ENCODED:
            return self.url_parser.parse(data)
        elif format == ParseFormat.FORM_DATA:
            return self.form_parser.parse(data)
        elif format == ParseFormat.INI:
            return self.ini_parser.parse(data)

        return ParseResult(
            success=False,
            error=f"Unknown format: {format}",
        )

    def parse_auto(
        self,
        data: str,
    ) -> ParseResult:
        """Attempt to auto-detect format and parse."""
        data = data.strip()

        if data.startswith("{") or data.startswith("["):
            return self.json_parser.parse(data)

        if data.startswith("<"):
            return self.xml_parser.parse(data)

        if "=" in data and "&" in data:
            return self.url_parser.parse(data)

        if "\t" in data.split("\n")[0]:
            return self.csv_parser.parse(data)

        if "," in data.split("\n")[0]:
            return self.csv_parser.parse(data)

        if data.startswith("["):
            return self.json_parser.parse(data)

        return ParseResult(
            success=False,
            error="Could not detect data format",
        )


class DataParserAction:
    """High-level data parser action."""

    def __init__(self, parser: Optional[DataParser] = None):
        self.parser = parser or DataParser()

    def parse(
        self,
        data: str,
        format: str,
    ) -> Any:
        """Parse data and return parsed result."""
        result = self.parser.parse(data, format)
        if not result.success:
            raise ValueError(result.error)
        return result.data

    def parse_auto(
        self,
        data: str,
    ) -> Any:
        """Auto-detect format and parse."""
        result = self.parser.parse_auto(data)
        if not result.success:
            raise ValueError(result.error)
        return result.data

    def to_json(
        self,
        data: Any,
        indent: Optional[int] = None,
    ) -> str:
        """Convert data to JSON."""
        return json.dumps(data, indent=indent, default=str)

    def to_csv(
        self,
        data: List[Dict[str, Any]],
        field_names: Optional[List[str]] = None,
    ) -> str:
        """Convert data to CSV."""
        return self.parser.csv_parser.to_csv(data, field_names)


# Module exports
__all__ = [
    "DataParserAction",
    "DataParser",
    "JSONParser",
    "CSVParser",
    "XMLParser",
    "URLEncodedParser",
    "FormDataParser",
    "INIParser",
    "ParseFormat",
    "ParseResult",
    "CSVConfig",
    "XMLConfig",
]

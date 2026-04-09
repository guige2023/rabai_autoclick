"""Data parser utilities for structured data parsing.

Supports JSON, CSV, TSV, INI, XML, and custom formats.
"""

from __future__ import annotations

import configparser
import csv
import io
import json
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generator, TextIO

logger = logging.getLogger(__name__)


class ParseError(Exception):
    """Raised when parsing fails."""

    pass


class DataFormat(Enum):
    """Supported data formats."""

    JSON = "json"
    CSV = "csv"
    TSV = "tsv"
    INI = "ini"
    XML = "xml"
    URL_ENCODED = "url_encoded"
    MULTILINE = "multiline"
    KEY_VALUE = "key_value"


@dataclass
class ParseResult:
    """Result of a parse operation."""

    success: bool
    data: Any = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class CSVOptions:
    """CSV parsing options."""

    delimiter: str = ","
    quotechar: str = '"'
    escapechar: str | None = None
    has_header: bool = True
    skip_rows: int = 0
    column_types: list[type] | None = None
    null_values: list[str] = field(default_factory=lambda: ["", "NA", "null", "None"])


class JSONParser:
    """JSON parser with streaming and error handling."""

    @staticmethod
    def parse(content: str | bytes, strict: bool = False) -> ParseResult:
        """Parse JSON content."""
        try:
            data = json.loads(content, strict=strict)
            return ParseResult(success=True, data=data)
        except json.JSONDecodeError as e:
            return ParseResult(success=False, error=str(e))

    @staticmethod
    def parse_stream(content: str | bytes) -> Generator[Any, None, None]:
        """Parse NDJSON (newline-delimited JSON) stream."""
        if isinstance(content, str):
            content = content.encode()
        for line in content.decode().splitlines():
            line = line.strip()
            if line:
                yield json.loads(line)

    @staticmethod
    def parse_safe(content: str | bytes, default: Any = None) -> Any:
        """Parse JSON with fallback to default on error."""
        try:
            return json.loads(content)
        except (json.JSONDecodeError, TypeError):
            return default

    @staticmethod
    def unparse(data: Any, indent: int | None = 2) -> str:
        """Serialize data to JSON string."""
        return json.dumps(data, indent=indent, ensure_ascii=False, default=str)


class CSVParser:
    """CSV/TSV parser with flexible options."""

    def __init__(self, options: CSVOptions | None = None) -> None:
        self.options = options or CSVOptions()

    def parse(self, content: str | TextIO) -> ParseResult:
        """Parse CSV content."""
        try:
            if isinstance(content, str):
                content = io.StringIO(content)

            reader = csv.reader(content, delimiter=self.options.delimiter, quotechar=self.options.quotechar, escapechar=self.options.escapechar)

            rows = list(reader)

            for _ in range(self.options.skip_rows):
                rows.pop(0)

            if self.options.has_header and rows:
                headers = rows.pop(0)
            else:
                headers = [f"col_{i}" for i in range(len(rows[0]))]

            parsed_rows = []
            for row in rows:
                cleaned_row = []
                for val in row:
                    if val in self.options.null_values:
                        cleaned_row.append(None)
                    else:
                        cleaned_row.append(val)
                parsed_rows.append(dict(zip(headers, cleaned_row)))

            if self.options.column_types:
                for i, col_type in enumerate(self.options.column_types):
                    if i < len(headers):
                        col_name = headers[i]
                        for row in parsed_rows:
                            if col_name in row and row[col_name] is not None:
                                try:
                                    row[col_name] = col_type(row[col_name])
                                except (ValueError, TypeError):
                                    pass

            return ParseResult(success=True, data=parsed_rows, metadata={"row_count": len(parsed_rows), "columns": headers})
        except csv.Error as e:
            return ParseResult(success=False, error=str(e))

    def parse_dicts(self, content: str | TextIO) -> list[dict[str, Any]]:
        """Parse CSV directly into list of dicts."""
        result = self.parse(content)
        if not result.success:
            raise ParseError(result.error or "Unknown error")
        return result.data

    @staticmethod
    def unparse(data: list[dict[str, Any]], output: str | TextIO | None = None, delimiter: str = ",", include_header: bool = True) -> str | None:
        """Serialize list of dicts to CSV."""
        if not data:
            return ""

        headers = list(data[0].keys())
        output_io = io.StringIO() if output is None else output
        writer = csv.DictWriter(output_io, fieldnames=headers, delimiter=delimiter)

        if include_header:
            writer.writeheader()

        writer.writerows(data)

        if output is None:
            return output_io.getvalue()
        return None


class INIParser:
    """INI file parser with section support."""

    def __init__(self) -> None:
        self.parser = configparser.ConfigParser()

    def parse(self, content: str) -> ParseResult:
        """Parse INI content."""
        try:
            self.parser.read_string(content)
            data = {section: dict(self.parser.items(section)) for section in self.parser.sections()}
            return ParseResult(success=True, data=data, metadata={"sections": list(self.parser.sections())})
        except configparser.Error as e:
            return ParseResult(success=False, error=str(e))

    def get(self, content: str, section: str, option: str, fallback: str | None = None) -> str | None:
        """Get a single value from parsed INI."""
        self.parse(content)
        try:
            return self.parser.get(section, option)
        except (configparser.NoSectionError, configparser.NoOptionError):
            return fallback

    @staticmethod
    def unparse(data: dict[str, dict[str, Any]], output: str | None = None) -> str:
        """Serialize dict of sections to INI format."""
        parser = configparser.ConfigParser()
        for section, options in data.items():
            parser.add_section(section)
            for key, value in options.items():
                parser.set(section, key, str(value))

        output_io = io.StringIO() if output is None else output
        parser.write(output_io)

        if output is None:
            return output_io.getvalue()
        return ""


class URLEncodedParser:
    """URL-encoded data parser (form data)."""

    @staticmethod
    def parse(content: str) -> dict[str, str]:
        """Parse URL-encoded string."""
        from urllib.parse import parse_qs, unquote

        parsed = parse_qs(content, keep_blank_values=True)
        return {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}

    @staticmethod
    def unparse(data: dict[str, Any]) -> str:
        """Serialize dict to URL-encoded string."""
        from urllib.parse import urlencode

        return urlencode(data, doseq=True)


class KeyValueParser:
    """Key-value pair parser for various formats."""

    @staticmethod
    def parse_lines(content: str, separator: str = ":", strip_values: bool = True) -> dict[str, str]:
        """Parse key:value lines."""
        result = {}
        for line in content.strip().splitlines():
            line = line.strip()
            if separator in line:
                key, _, value = line.partition(separator)
                result[key.strip()] = value.strip() if strip_values else value
        return result

    @staticmethod
    def parse_custom(content: str, pattern: str | None = None) -> dict[str, str]:
        """Parse with custom regex pattern.

        Args:
            content: Text content.
            pattern: Regex pattern with named groups 'key' and 'value'.
        """
        if pattern is None:
            pattern = r"^(?P<key>[^:=]+)[:=]\s*(?P<value>.+)$"

        result = {}
        for match in re.finditer(pattern, content, re.MULTILINE):
            result[match.group("key").strip()] = match.group("value").strip()
        return result


class MultilineParser:
    """Multiline record parser (paragraph mode)."""

    @staticmethod
    def parse(content: str, separator: str = "\n\n", max_records: int | None = None) -> list[str]:
        """Parse records separated by blank lines."""
        records = content.split(separator)
        if max_records:
            records = records[:max_records]
        return [r.strip() for r in records if r.strip()]

    @staticmethod
    def parse_with_marker(content: str, marker: str = "---", strip: bool = True) -> list[str]:
        """Parse records separated by marker lines."""
        marker_re = re.compile(rf"^\s*{re.escape(marker)}\s*$", re.MULTILINE)
        parts = marker_re.split(content)
        return [p.strip() if strip else p for p in parts if p.strip()]


class ParserFactory:
    """Factory for creating appropriate parser by format."""

    _parsers: dict[DataFormat, Callable] = {
        DataFormat.JSON: JSONParser,
        DataFormat.CSV: CSVParser,
        DataFormat.TSV: lambda: CSVParser(CSVOptions(delimiter="\t")),
        DataFormat.INI: INIParser,
        DataFormat.URL_ENCODED: URLEncodedParser,
        DataFormat.MULTILINE: MultilineParser,
        DataFormat.KEY_VALUE: KeyValueParser,
    }

    @classmethod
    def get(cls, format: DataFormat) -> Any:
        """Get parser instance for format."""
        parser_cls = cls._parsers.get(format)
        if parser_cls is None:
            raise ValueError(f"No parser for format: {format}")
        return parser_cls()

    @classmethod
    def detect_format(cls, content: str) -> DataFormat:
        """Detect format from content characteristics."""
        content = content.strip()

        if content.startswith("{") or content.startswith("["):
            return DataFormat.JSON

        try:
            json.loads(content)
            return DataFormat.JSON
        except json.JSONDecodeError:
            pass

        first_line = content.split("\n")[0]
        if "\t" in first_line and "," not in first_line:
            return DataFormat.TSV
        if "," in first_line:
            return DataFormat.CSV

        if "=" in first_line or first_line.startswith("["):
            return DataFormat.INI

        if "=" in first_line and "&" in content:
            return DataFormat.URL_ENCODED

        return DataFormat.MULTILINE

    @classmethod
    def auto_parse(cls, content: str | bytes) -> ParseResult:
        """Automatically detect format and parse."""
        if isinstance(content, bytes):
            content = content.decode("utf-8", errors="replace")

        fmt = cls.detect_format(content)
        parser = cls.get(fmt)
        return parser.parse(content)

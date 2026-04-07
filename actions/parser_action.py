"""parser action module for rabai_autoclick.

Provides data parsing utilities: JSON, CSV, TSV, XML, YAML, INI,
binary format parsing, and generic structured text parsing.
"""

from __future__ import annotations

import csv
import io
import json
import re
import struct
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, BinaryIO, Callable, Dict, Iterator, List, Optional, Sequence, TextIO, Tuple, Union

__all__ = [
    "parse_json",
    "parse_csv",
    "parse_tsv",
    "parse_xml",
    "parse_xml_simple",
    "parse_yaml",
    "parse_ini",
    "parse_json_lines",
    "parse_query_string",
    "parse_url",
    "parse_hex",
    "parse_binary",
    "Parser",
    "FieldParser",
    "JsonParser",
    "CsvParser",
    "ParserError",
    "parse_int",
    "parse_float",
    "parse_bool",
    "parse_date",
    "parse_jsonpath",
]


class ParserError(Exception):
    """Raised when parsing fails."""
    pass


def parse_int(value: str, base: int = 10, default: Optional[int] = None) -> Optional[int]:
    """Parse integer from string.

    Args:
        value: String value.
        base: Number base (default: 10).
        default: Default value on failure.

    Returns:
        Parsed integer or default.
    """
    try:
        return int(value.strip(), base)
    except (ValueError, TypeError):
        return default


def parse_float(value: str, default: Optional[float] = None) -> Optional[float]:
    """Parse float from string."""
    try:
        return float(value.strip())
    except (ValueError, TypeError):
        return default


def parse_bool(value: str, default: Optional[bool] = None) -> Optional[bool]:
    """Parse boolean from string."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    v = str(value).strip().lower()
    if v in ("true", "yes", "1", "on", "t", "y"):
        return True
    if v in ("false", "no", "0", "off", "f", "n", ""):
        return False
    return default


def parse_date(value: str, fmt: str = "%Y-%m-%d", default: Optional[str] = None) -> Optional[str]:
    """Parse date string, return formatted string."""
    from datetime import datetime
    try:
        dt = datetime.strptime(value.strip(), fmt)
        return dt.strftime(fmt)
    except (ValueError, TypeError):
        return default


def parse_json(value: str, default: Any = None) -> Any:
    """Parse JSON string."""
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return default


def parse_json_lines(lines: str) -> Iterator[dict]:
    """Parse newline-delimited JSON.

    Args:
        lines: Multi-line JSON text.

    Yields:
        Parsed JSON objects.
    """
    for line in lines.strip().split("\n"):
        line = line.strip()
        if line:
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                pass


def parse_csv(
    text: str,
    delimiter: str = ",",
    quotechar: str = '"',
    headers: Optional[List[str]] = None,
    skip_rows: int = 0,
) -> List[Dict[str, str]]:
    """Parse CSV text into list of dicts.

    Args:
        text: CSV text.
        delimiter: Field delimiter.
        quotechar: Quote character.
        headers: Explicit headers (auto-detect if None).
        skip_rows: Number of rows to skip at start.

    Returns:
        List of row dicts.
    """
    reader = csv.DictReader(
        io.StringIO(text.strip()),
        delimiter=delimiter,
        quotechar=quotechar,
        fieldnames=headers,
    )
    for _ in range(skip_rows):
        next(reader, None)
    return list(reader)


def parse_tsv(text: str, headers: Optional[List[str]] = None) -> List[Dict[str, str]]:
    """Parse TSV text."""
    return parse_csv(text, delimiter="\t", headers=headers)


def parse_xml(text: str) -> Any:
    """Parse XML using ElementTree."""
    import xml.etree.ElementTree as ET
    try:
        return ET.fromstring(text.strip())
    except ET.ParseError as e:
        raise ParserError(f"XML parse error: {e}")


def parse_xml_simple(text: str) -> dict:
    """Parse XML into nested dict (simple conversion).

    Args:
        text: XML text.

    Returns:
        Nested dict representation.
    """
    import xml.etree.ElementTree as ET
    root = ET.fromstring(text.strip())

    def element_to_dict(elem: ET.Element) -> dict:
        children = list(elem)
        result: dict = {"_tag": elem.tag}
        if elem.attrib:
            result["_attrs"] = dict(elem.attrib)
        if elem.text and elem.text.strip():
            result["_text"] = elem.text.strip()
        if children:
            child_dict: dict = {}
            for child in children:
                cdict = element_to_dict(child)
                tag = cdict["_tag"]
                del cdict["_tag"]
                if tag in child_dict:
                    if not isinstance(child_dict[tag], list):
                        child_dict[tag] = [child_dict[tag]]
                    child_dict[tag].append(cdict)
                else:
                    child_dict[tag] = cdict
            result.update(child_dict)
        return result

    return element_to_dict(root)


def parse_yaml(text: str, default: Any = None) -> Any:
    """Parse YAML text."""
    try:
        import yaml
        return yaml.safe_load(text)
    except Exception:
        return default


def parse_ini(text: str) -> dict:
    """Parse INI-format text into nested dict.

    Args:
        text: INI text.

    Returns:
        Dict with [section] -> {key: value}.
    """
    result: dict = OrderedDict()
    current_section = "_default"
    result[current_section] = {}
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line or line.startswith("#") or line.startswith(";"):
            continue
        if line.startswith("[") and line.endswith("]"):
            current_section = line[1:-1]
            result[current_section] = {}
            continue
        if "=" in line:
            key, _, value = line.partition("=")
            result[current_section][key.strip()] = value.strip()
    return result


def parse_query_string(query: str) -> dict:
    """Parse URL query string.

    Args:
        query: Query string (without leading ?).

    Returns:
        Dict of parameters.
    """
    from urllib.parse import parse_qs
    try:
        parsed = parse_qs(query)
        return {k: v[0] if len(v) == 1 else v for k, v in parsed.items()}
    except Exception:
        return {}


def parse_url(url: str) -> dict:
    """Parse URL into components.

    Args:
        url: URL string.

    Returns:
        Dict with scheme, netloc, path, params, query, fragment.
    """
    from urllib.parse import urlparse
    try:
        parsed = urlparse(url)
        return {
            "scheme": parsed.scheme,
            "netloc": parsed.netloc,
            "path": parsed.path,
            "params": parsed.params,
            "query": dict(parse_qs(parsed.query)) if parsed.query else {},
            "fragment": parsed.fragment,
        }
    except Exception:
        return {}


def parse_hex(value: str) -> bytes:
    """Parse hex string to bytes."""
    hex_clean = value.replace(" ", "").replace("\n", "")
    return bytes.fromhex(hex_clean)


def parse_binary(data: bytes, fmt: str) -> Tuple[Any, ...]:
    """Parse binary data using struct format.

    Args:
        data: Binary data.
        fmt: Struct format string.

    Returns:
        Unpacked values.
    """
    try:
        return struct.unpack(fmt, data[:struct.calcsize(fmt)])
    except struct.error as e:
        raise ParserError(f"Binary parse error: {e}")


def parse_jsonpath(value: Any, path: str, default: Any = None) -> Any:
    """Simple JSONPath-like extraction.

    Supports: $.key, $.key.subkey, $[0], $.key[0].

    Args:
        value: Parsed JSON object.
        path: JSONPath expression.
        default: Default value if not found.

    Returns:
        Extracted value or default.
    """
    if not path.startswith("$"):
        path = "$." + path
    parts = path[2:].split(".") if path.startswith("$.") else path.split(".")
    current = value
    for part in parts:
        if not part:
            continue
        if isinstance(current, dict):
            current = current.get(part, default)
        elif isinstance(current, (list, tuple)):
            try:
                idx = int(part.strip("[]"))
                current = current[idx] if 0 <= idx < len(current) else default
            except ValueError:
                current = default
        else:
            return default
    return current


class Parser:
    """Configurable parser with field transformations."""

    def __init__(self, schema: Optional[Dict[str, Callable]] = None) -> None:
        self.schema = schema or {}

    def parse(self, data: dict) -> dict:
        """Parse data applying field transformations."""
        result = {}
        for field_name, parser_fn in self.schema.items():
            value = data.get(field_name)
            try:
                result[field_name] = parser_fn(value) if value is not None else value
            except Exception:
                result[field_name] = value
        for key, value in data.items():
            if key not in result:
                result[key] = value
        return result

    def add_field(self, name: str, parser: Callable) -> None:
        """Add a field parser."""
        self.schema[name] = parser

    def parse_list(self, data: List[dict]) -> List[dict]:
        """Parse list of records."""
        return [self.parse(record) for record in data]


class FieldParser:
    """Helper for building field-by-field parsers."""

    def __init__(self) -> None:
        self._parsers: Dict[str, Callable] = {}

    def add(self, name: str, parser: Callable, required: bool = False) -> "FieldParser":
        """Add a field with its parser function."""
        self._parsers[name] = parser
        return self

    def add_int(self, name: str, default: Optional[int] = None, base: int = 10) -> "FieldParser":
        """Add integer field."""
        self._parsers[name] = lambda v: parse_int(str(v), base, default)
        return self

    def add_float(self, name: str, default: Optional[float] = None) -> "FieldParser":
        """Add float field."""
        self._parsers[name] = lambda v: parse_float(str(v), default)
        return self

    def add_bool(self, name: str, default: Optional[bool] = None) -> "FieldParser":
        """Add boolean field."""
        self._parsers[name] = lambda v: parse_bool(str(v), default)
        return self

    def add_json(self, name: str) -> "FieldParser":
        """Add JSON field."""
        self._parsers[name] = lambda v: parse_json(v) if isinstance(v, str) else v
        return self

    def parse(self, data: dict) -> dict:
        """Apply all field parsers to a record."""
        result = {}
        for name, parser in self._parsers.items():
            value = data.get(name)
            if value is not None:
                try:
                    result[name] = parser(value)
                except Exception:
                    result[name] = value
        return result


class JsonParser:
    """Streaming JSON parser for large files."""

    def __init__(self, path: Optional[str] = None) -> None:
        self.path = path
        self._decoder = json.JSONDecoder(object_hook=self._object_hook)

    def _object_hook(self, obj: dict) -> dict:
        """Override this for custom object parsing."""
        return obj

    def parse(self, text: str) -> Any:
        """Parse JSON text."""
        return json.loads(text)

    def parse_stream(self, text: str) -> Iterator[Any]:
        """Parse newline-delimited JSON."""
        for line in text.strip().split("\n"):
            line = line.strip()
            if line:
                yield self.parse(line)

    def parse_file(self, path: Optional[str] = None) -> Any:
        """Parse JSON from file."""
        target = path or self.path
        if not target:
            raise ParserError("No path specified")
        with open(target, "r", encoding="utf-8") as f:
            return self.parse(f.read())


class CsvParser:
    """Streaming CSV parser with type coercion."""

    def __init__(
        self,
        headers: Optional[List[str]] = None,
        types: Optional[Dict[str, Callable]] = None,
    ) -> None:
        self.headers = headers
        self.types = types or {}

    def parse(self, text: str, delimiter: str = ",") -> List[dict]:
        """Parse CSV text."""
        reader = csv.DictReader(io.StringIO(text.strip()), delimiter=delimiter)
        result = []
        for row in reader:
            typed_row = {}
            for key, value in row.items():
                if key in self.types:
                    try:
                        typed_row[key] = self.types[key](value)
                    except Exception:
                        typed_row[key] = value
                else:
                    typed_row[key] = value
            result.append(typed_row)
        return result

    def parse_stream(self, text: str, delimiter: str = ",") -> Iterator[dict]:
        """Parse CSV text as iterator."""
        reader = csv.DictReader(io.StringIO(text.strip()), delimiter=delimiter)
        for row in reader:
            yield row

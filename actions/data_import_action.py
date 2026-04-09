"""Data Import Action Module.

Provides data import capabilities supporting multiple formats including
JSON, CSV, TSV, Excel, XML, YAML with automatic format detection.
"""

from __future__ import annotations

import base64
import csv
import io
import json
import logging
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TextIO, Tuple, Union

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class ImportFormat(Enum):
    """Supported import formats."""
    AUTO = "auto"
    JSON = "json"
    CSV = "csv"
    TSV = "tsv"
    XML = "xml"
    YAML = "yaml"
    EXCEL = "excel"
    FORM_URLENCODED = "form_urlencoded"
    MULTIPART = "multipart"


class DataType(Enum):
    """Detected data types."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    NULL = "null"
    ARRAY = "array"
    OBJECT = "object"


@dataclass
class ImportOptions:
    """Options for data import."""
    format: ImportFormat = ImportFormat.AUTO
    encoding: str = "utf-8"
    delimiter: str = ","
    has_header: bool = True
    skip_rows: int = 0
    max_rows: Optional[int] = None
    flatten_nested: bool = False
    flatten_separator: str = "."
    infer_types: bool = True
    null_values: List[str] = field(default_factory=lambda: ["", "null", "NULL", "NA", "N/A", "None"])
    true_values: List[str] = field(default_factory=lambda: ["true", "True", "TRUE", "1", "yes", "Yes", "YES"])
    false_values: List[str] = field(default_factory=lambda: ["false", "False", "FALSE", "0", "no", "No", "NO"])


@dataclass
class ImportMetadata:
    """Metadata about imported data."""
    record_count: int = 0
    field_count: int = 0
    fields: List[str] = field(default_factory=list)
    detected_format: Optional[str] = None
    inferred_types: Dict[str, str] = field(default_factory=dict)
    parse_time_ms: float = 0.0
    warnings: List[str] = field(default_factory=list)


def _detect_type(value: str, options: ImportOptions) -> Tuple[Any, DataType]:
    """Infer the type of a string value."""
    if value in options.null_values:
        return None, DataType.NULL

    # Boolean
    if value in options.true_values:
        return True, DataType.BOOLEAN
    if value in options.false_values:
        return False, DataType.BOOLEAN

    # Integer
    try:
        return int(value), DataType.INTEGER
    except ValueError:
        pass

    # Float
    try:
        return float(value), DataType.FLOAT
    except ValueError:
        pass

    return value, DataType.STRING


def _flatten_record(record: Dict[str, Any], sep: str = ".") -> Dict[str, Any]:
    """Flatten a nested record."""
    result: Dict[str, Any] = {}

    def flatten(obj: Any, prefix: str = "") -> None:
        if isinstance(obj, dict):
            for key, value in obj.items():
                new_key = f"{prefix}{sep}{key}" if prefix else key
                flatten(value, new_key)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                new_key = f"{prefix}[{i}]"
                flatten(item, new_key)
        else:
            result[prefix] = obj

    flatten(record)
    return result


class DataImportAction(BaseAction):
    """Data Import Action for importing data from various formats.

    Supports auto-detection, type inference, and flexible parsing options.

    Examples:
        >>> action = DataImportAction()
        >>> result = action.execute(ctx, {
        ...     "source": "file",
        ...     "file_path": "/tmp/data.csv",
        ...     "format": "auto"
        ... })
    """

    action_type = "data_import"
    display_name = "数据导入"
    description = "多格式数据导入：JSON/CSV/XML/YAML/Excel，自动格式检测"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data import.

        Args:
            context: Execution context.
            params: Dict with keys:
                - source: 'file', 'text', or 'base64'
                - file_path: Path to file (for file source)
                - text: Raw text data (for text source)
                - base64_data: Base64 encoded data (for base64 source)
                - format: Format hint ('auto', 'json', 'csv', etc.)
                - encoding: Text encoding (default: 'utf-8')
                - delimiter: CSV delimiter (default: ',')
                - has_header: CSV has header row (default: True)
                - skip_rows: Rows to skip (default: 0)
                - max_rows: Max rows to import
                - flatten_nested: Flatten nested structures
                - infer_types: Infer column types

        Returns:
            ActionResult with imported data and metadata.
        """
        import time
        start_time = time.time()

        source = params.get("source", "text")
        file_path = params.get("file_path")
        text_data = params.get("text", "")
        base64_data = params.get("base64_data")
        format_hint = params.get("format", "auto").lower()

        # Build import options
        options = ImportOptions(
            format=ImportFormat(format_hint),
            encoding=params.get("encoding", "utf-8"),
            delimiter=params.get("delimiter", ","),
            has_header=params.get("has_header", True),
            skip_rows=params.get("skip_rows", 0),
            max_rows=params.get("max_rows"),
            flatten_nested=params.get("flatten_nested", False),
            infer_types=params.get("infer_types", True),
        )

        # Load raw data
        raw_data: Union[str, bytes]
        try:
            if source == "file":
                if not file_path:
                    return ActionResult(success=False, message="file_path required for file source")
                path = Path(file_path)
                if not path.exists():
                    return ActionResult(success=False, message=f"File not found: {file_path}")
                raw_data = path.read_bytes() if _is_binary_format(format_hint) else path.read_text(encoding=options.encoding)
            elif source == "base64":
                if not base64_data:
                    return ActionResult(success=False, message="base64_data required")
                raw_data = base64.b64decode(base64_data)
            else:  # text
                raw_data = text_data

        except Exception as e:
            return ActionResult(success=False, message=f"Failed to read data: {str(e)}")

        # Detect format
        if isinstance(raw_data, bytes):
            try:
                raw_data = raw_data.decode(options.encoding)
            except UnicodeDecodeError as e:
                return ActionResult(success=False, message=f"Encoding error: {str(e)}")

        detected_format = self._detect_format(raw_data, format_hint, options)
        metadata = ImportMetadata(detected_format=detected_format)

        # Parse data
        try:
            if detected_format == "json":
                records, meta = self._parse_json(raw_data, options)
            elif detected_format in ("csv", "tsv"):
                records, meta = self._parse_csv(raw_data, options)
            elif detected_format == "xml":
                records, meta = self._parse_xml(raw_data, options)
            elif detected_format == "yaml":
                records, meta = self._parse_yaml(raw_data, options)
            elif detected_format == "form_urlencoded":
                records, meta = self._parse_form_urlencoded(raw_data, options)
            else:
                return ActionResult(success=False, message=f"Unsupported format: {detected_format}")

            metadata.record_count = len(records)
            metadata.field_count = meta.get("field_count", 0)
            metadata.fields = meta.get("fields", [])
            metadata.inferred_types = meta.get("inferred_types", {})
            metadata.warnings = meta.get("warnings", [])

        except Exception as e:
            logger.exception(f"Parse error: {detected_format}")
            return ActionResult(success=False, message=f"Parse error: {str(e)}")

        metadata.parse_time_ms = (time.time() - start_time) * 1000

        return ActionResult(
            success=True,
            message=f"Imported {len(records)} records from {detected_format}",
            data={
                "data": records,
                "metadata": {
                    "record_count": metadata.record_count,
                    "field_count": metadata.field_count,
                    "fields": metadata.fields,
                    "detected_format": metadata.detected_format,
                    "inferred_types": metadata.inferred_types,
                    "parse_time_ms": metadata.parse_time_ms,
                    "warnings": metadata.warnings,
                }
            }
        )

    def _is_binary_format(self, format_hint: str) -> bool:
        """Check if format is binary."""
        return format_hint in ("excel", "parquet", "zip")

    def _detect_format(self, data: str, hint: str, options: ImportOptions) -> str:
        """Auto-detect data format."""
        if hint != "auto":
            return hint

        data = data.strip()

        # JSON
        if data.startswith("{") or data.startswith("["):
            try:
                json.loads(data)
                return "json"
            except json.JSONDecodeError:
                pass

        # XML
        if data.startswith("<"):
            return "xml"

        # CSV/TSV - check delimiter
        first_line = data.split("\n")[0] if "\n" in data else data
        if "\t" in first_line:
            return "tsv"
        if "," in first_line or ";" in first_line:
            return "csv"

        # YAML
        if ":" in data and "\n" in data:
            return "yaml"

        # Form URL-encoded
        if "=" in data and "&" in data:
            return "form_urlencoded"

        return "json"

    def _parse_json(self, data: str, options: ImportOptions) -> Tuple[List, Dict]:
        """Parse JSON data."""
        parsed = json.loads(data)
        records = parsed if isinstance(parsed, list) else [parsed]

        if options.flatten_nested:
            records = [_flatten_record(r, options.flatten_separator) if isinstance(r, dict) else r
                      for r in records]

        inferred = {}
        if options.infer_types and records:
            for key in records[0].keys() if isinstance(records[0], dict) else []:
                sample = records[0].get(key)
                _, dtype = _detect_type(str(sample), options)
                inferred[key] = dtype.value

        return records, {"fields": list(records[0].keys()) if records and isinstance(records[0], dict) else [],
                         "field_count": len(records[0]) if records and isinstance(records[0], dict) else 0,
                         "inferred_types": inferred}

    def _parse_csv(self, data: str, options: ImportOptions) -> Tuple[List[Dict], Dict]:
        """Parse CSV/TSV data."""
        delimiter = "\t" if options.format == ImportFormat.TSV else options.delimiter
        reader = csv.reader(io.StringIO(data), delimiter=delimiter)
        rows = list(reader)

        # Skip rows
        rows = rows[options.skip_rows:]

        # Header
        if options.has_header and rows:
            headers = rows[0]
            data_rows = rows[1:]
        else:
            headers = [f"col_{i}" for i in range(len(rows[0]))]
            data_rows = rows

        # Max rows
        if options.max_rows:
            data_rows = data_rows[:options.max_rows]

        # Build records
        records = []
        inferred = {}

        for row in data_rows:
            record = {}
            for i, value in enumerate(row):
                if i < len(headers):
                    key = headers[i]
                    if options.infer_types:
                        typed_value, dtype = _detect_type(value, options)
                        record[key] = typed_value
                        if key not in inferred:
                            inferred[key] = dtype.value
                    else:
                        record[key] = value
            records.append(record)

        return records, {
            "fields": headers,
            "field_count": len(headers),
            "inferred_types": inferred,
        }

    def _parse_xml(self, data: str, options: ImportOptions) -> Tuple[List[Dict], Dict]:
        """Parse XML data."""
        root = ET.fromstring(data)
        records = []

        # Find records - look for repeated elements
        if len(root) > 0:
            sample = root[0]
            for child in root:
                record = {}
                for subchild in child:
                    key = subchild.tag
                    value = subchild.text if subchild.text else ""
                    if options.infer_types:
                        value, _ = _detect_type(value, options)
                    record[key] = value
                records.append(record)
            fields = list(sample.attrib.keys()) + [c.tag for c in sample]
        else:
            records = [{"text": root.text or ""}]
            fields = ["text"]

        return records, {"fields": fields, "field_count": len(fields)}

    def _parse_yaml(self, data: str, options: ImportOptions) -> Tuple[List, Dict]:
        """Parse YAML data."""
        try:
            import yaml
            parsed = yaml.safe_load(data)
            records = parsed if isinstance(parsed, list) else [parsed]
            return records, {"fields": [], "field_count": 0}
        except ImportError:
            # Fallback - treat as JSON
            return self._parse_json(data, options)

    def _parse_form_urlencoded(self, data: str, options: ImportOptions) -> Tuple[List[Dict], Dict]:
        """Parse form URL-encoded data."""
        from urllib.parse import parse_qs
        parsed = parse_qs(data)
        records = [{k: v[0] if len(v) == 1 else v for k, v in parsed.items()}]
        return records, {"fields": list(parsed.keys()), "field_count": len(parsed)}

    def get_required_params(self) -> List[str]:
        return ["text"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "source": "text",
            "file_path": None,
            "base64_data": None,
            "format": "auto",
            "encoding": "utf-8",
            "delimiter": ",",
            "has_header": True,
            "skip_rows": 0,
            "max_rows": None,
            "flatten_nested": False,
            "infer_types": True,
        }

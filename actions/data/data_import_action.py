"""
Data Import Action Module.

Data import utilities for automation supporting multiple formats
including JSON, CSV, XML, YAML with schema validation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import csv
import io
import json
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, BinaryIO, Dict, List, Optional, TextIO, Union

logger = logging.getLogger(__name__)


class ImportFormat(Enum):
    """Supported import formats."""
    JSON = "json"
    JSON_LINES = "jsonl"
    CSV = "csv"
    TSV = "tsv"
    YAML = "yaml"
    XML = "xml"


@dataclass
class ImportConfig:
    """Configuration for import operation."""
    format: ImportFormat = ImportFormat.JSON
    encoding: str = "utf-8"
    header_row: int = 0
    skip_rows: int = 0
    delimiter: str = ","
    quote_char: str = '"'
    strict_schema: bool = False
    schema: Optional[Dict[str, type]] = None
    coerce_types: bool = True
    skip_invalid_rows: bool = True


@dataclass
class ImportResult:
    """Result of an import operation."""
    success: bool
    format: ImportFormat
    rows_imported: int = 0
    rows_skipped: int = 0
    rows_invalid: int = 0
    data: Any = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class DataImportAction:
    """
    Data import utilities for automation.

    Imports data from various formats with schema validation
    and error handling.

    Example:
        importer = DataImportAction()

        result = importer.import_from_file("data.csv", format=ImportFormat.CSV)
        print(f"Imported {result.rows_imported} rows")

        result = importer.import_from_string(json_str, format=ImportFormat.JSON)
    """

    def __init__(self) -> None:
        self._default_config = ImportConfig()

    def set_default_config(self, config: ImportConfig) -> None:
        """Set default import configuration."""
        self._default_config = config

    def import_from_file(
        self,
        file_path: str,
        format: Optional[ImportFormat] = None,
        config: Optional[ImportConfig] = None,
    ) -> ImportResult:
        """Import data from a file."""
        cfg = config or self._default_config

        if format is None:
            format = self._guess_format(file_path)

        try:
            with open(file_path, "r", encoding=cfg.encoding) as f:
                content = f.read()

            return self.import_from_string(content, format, cfg)

        except FileNotFoundError:
            return ImportResult(
                success=False,
                format=format,
                errors=[f"File not found: {file_path}"],
            )
        except Exception as e:
            return ImportResult(
                success=False,
                format=format,
                errors=[f"Import failed: {e}"],
            )

    def import_from_string(
        self,
        content: str,
        format: ImportFormat = ImportFormat.JSON,
        config: Optional[ImportConfig] = None,
    ) -> ImportResult:
        """Import data from a string."""
        cfg = config or self._default_config

        try:
            if format == ImportFormat.JSON:
                return self._import_json(content, cfg)
            elif format == ImportFormat.JSON_LINES:
                return self._import_jsonl(content, cfg)
            elif format == ImportFormat.CSV:
                return self._import_csv(content, cfg)
            elif format == ImportFormat.TSV:
                cfg.delimiter = "\t"
                return self._import_csv(content, cfg)
            elif format == ImportFormat.YAML:
                return self._import_yaml(content, cfg)
            elif format == ImportFormat.XML:
                return self._import_xml(content, cfg)
            else:
                return ImportResult(
                    success=False,
                    format=format,
                    errors=[f"Unsupported format: {format}"],
                )

        except Exception as e:
            logger.error(f"Import failed: {e}")
            return ImportResult(
                success=False,
                format=format,
                errors=[str(e)],
            )

    def _import_json(self, content: str, config: ImportConfig) -> ImportResult:
        """Import from JSON."""
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            return ImportResult(success=False, format=ImportFormat.JSON, errors=[f"JSON parse error: {e}"])

        if isinstance(data, dict) and "data" in data:
            data = data["data"]

        rows = data if isinstance(data, list) else [data]
        return ImportResult(
            success=True,
            format=ImportFormat.JSON,
            rows_imported=len(rows),
            data=rows if isinstance(data, list) else data,
        )

    def _import_jsonl(self, content: str, config: ImportConfig) -> ImportResult:
        """Import from JSON Lines."""
        lines = content.strip().split("\n")
        rows = []
        errors = []
        skipped = 0

        for i, line in enumerate(lines):
            if not line.strip():
                skipped += 1
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                errors.append(f"Line {i+1}: {e}")
                if config.strict_schema:
                    return ImportResult(
                        success=False,
                        format=ImportFormat.JSON_LINES,
                        rows_imported=len(rows),
                        rows_skipped=skipped,
                        rows_invalid=len(errors),
                        data=rows,
                        errors=errors,
                    )

        return ImportResult(
            success=len(errors) == 0,
            format=ImportFormat.JSON_LINES,
            rows_imported=len(rows),
            rows_skipped=skipped,
            rows_invalid=len(errors),
            data=rows,
            errors=errors,
        )

    def _import_csv(self, content: str, config: ImportConfig) -> ImportResult:
        """Import from CSV."""
        buffer = io.StringIO(content)
        reader = csv.DictReader(
            buffer,
            delimiter=config.delimiter,
            quotechar=config.quote_char,
        )

        rows = []
        errors = []
        skipped = 0

        for i, row in enumerate(reader):
            if i < config.skip_rows:
                skipped += 1
                continue

            if config.coerce_types:
                row = self._coerce_types(row)

            if config.schema:
                is_valid, err = self._validate_schema(row, config.schema)
                if not is_valid:
                    if config.skip_invalid_rows:
                        errors.append(f"Row {i+1}: {err}")
                        skipped += 1
                        continue
                    else:
                        errors.append(f"Row {i+1}: {err}")

            rows.append(row)

        return ImportResult(
            success=len(errors) == 0,
            format=ImportFormat.CSV,
            rows_imported=len(rows),
            rows_skipped=skipped,
            rows_invalid=len(errors),
            data=rows,
            errors=errors,
        )

    def _import_yaml(self, content: str, config: ImportConfig) -> ImportResult:
        """Import from YAML."""
        try:
            import yaml
        except ImportError:
            # Fallback to JSON parsing
            return self._import_json(content, config)

        try:
            data = yaml.safe_load(content)
        except Exception as e:
            return ImportResult(success=False, format=ImportFormat.YAML, errors=[f"YAML parse error: {e}"])

        rows = data if isinstance(data, list) else [data]
        return ImportResult(
            success=True,
            format=ImportFormat.YAML,
            rows_imported=len(rows),
            data=rows,
        )

    def _import_xml(self, content: str, config: ImportConfig) -> ImportResult:
        """Import from XML (simple element extraction)."""
        import re

        # Simple XML to dict conversion
        rows = []
        pattern = re.compile(r"<item>(.*?)</item>", re.DOTALL)
        for match in pattern.finditer(content):
            item_content = match.group(1)
            # Extract fields
            fields = {}
            for field_match in re.finditer(r"<(\w+)>(.*?)</\w+>", item_content, re.DOTALL):
                fields[field_match.group(1)] = field_match.group(2).strip()
            if fields:
                rows.append(fields)

        if not rows:
            # Try root element
            root_match = re.search(r"<(\w+)>(.*?)</\1>", content, re.DOTALL)
            if root_match:
                rows = [root_match.group(2)]

        return ImportResult(
            success=True,
            format=ImportFormat.XML,
            rows_imported=len(rows),
            data=rows,
        )

    def _coerce_types(self, row: Dict[str, str]) -> Dict[str, Any]:
        """Attempt to coerce string values to appropriate types."""
        result = {}
        for key, value in row.items():
            if value == "":
                result[key] = None
                continue

            # Try int
            try:
                result[key] = int(value)
                continue
            except ValueError:
                pass

            # Try float
            try:
                result[key] = float(value)
                continue
            except ValueError:
                pass

            # Try bool
            if value.lower() in ("true", "yes"):
                result[key] = True
            elif value.lower() in ("false", "no"):
                result[key] = False
            else:
                result[key] = value

        return result

    def _validate_schema(
        self,
        row: Dict[str, Any],
        schema: Dict[str, type],
    ) -> tuple[bool, Optional[str]]:
        """Validate row against schema."""
        for field_name, expected_type in schema.items():
            if field_name not in row:
                return False, f"Missing required field: {field_name}"
            value = row[field_name]
            if value is not None and not isinstance(value, expected_type):
                return False, f"Field '{field_name}' has wrong type: expected {expected_type.__name__}, got {type(value).__name__}"
        return True, None

    def _guess_format(self, file_path: str) -> ImportFormat:
        """Guess format from file extension."""
        ext = file_path.split(".")[-1].lower()
        format_map = {
            "json": ImportFormat.JSON,
            "jsonl": ImportFormat.JSON_LINES,
            "ndjson": ImportFormat.JSON_LINES,
            "csv": ImportFormat.CSV,
            "tsv": ImportFormat.TSV,
            "yaml": ImportFormat.YAML,
            "yml": ImportFormat.YAML,
            "xml": ImportFormat.XML,
        }
        return format_map.get(ext, ImportFormat.JSON)

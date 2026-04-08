"""
Data Import Action - Imports data from various formats.

This module provides data import capabilities including
CSV, JSON, Excel, and custom format parsing.
"""

from __future__ import annotations

import csv
import io
import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar
from enum import Enum


T = TypeVar("T")


class ImportFormat(Enum):
    """Supported import formats."""
    CSV = "csv"
    JSON = "json"
    JSON_LINES = "jsonl"
    TSV = "tsv"
    XML = "xml"
    HTML = "html"


@dataclass
class ImportConfig:
    """Configuration for data import."""
    format: ImportFormat = ImportFormat.CSV
    delimiter: str = ","
    quote_char: str = '"'
    encoding: str = "utf-8"
    has_headers: bool = True
    skip_rows: int = 0
    max_rows: int | None = None
    type_mapping: dict[str, Callable[[str], Any]] = field(default_factory=dict)


@dataclass
class ImportResult:
    """Result of import operation."""
    success: bool
    data: list[dict[str, Any]]
    record_count: int
    skipped_count: int
    errors: list[str] = field(default_factory=list)


class CSVImporter:
    """Imports data from CSV format."""
    
    def __init__(self, config: ImportConfig) -> None:
        self.config = config
    
    def import_data(self, content: str) -> list[dict[str, Any]]:
        """Import data from CSV string."""
        input_stream = io.StringIO(content)
        reader = csv.DictReader(input_stream, delimiter=self.config.delimiter, quotechar=self.config.quote_char)
        data = []
        for i, row in enumerate(reader):
            if self.config.max_rows and i >= self.config.max_rows:
                break
            data.append(self._process_row(row))
        return data
    
    def _process_row(self, row: dict[str, str]) -> dict[str, Any]:
        """Process a CSV row with type conversion."""
        result = {}
        for key, value in row.items():
            if key in self.config.type_mapping:
                try:
                    result[key] = self.config.type_mapping[key](value)
                except Exception:
                    result[key] = value
            else:
                result[key] = self._infer_type(value)
        return result
    
    def _infer_type(self, value: str) -> Any:
        """Infer type from string value."""
        if value is None or value == "":
            return None
        if value.lower() == "true":
            return True
        if value.lower() == "false":
            return False
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value


class JSONImporter:
    """Imports data from JSON format."""
    
    def __init__(self, config: ImportConfig) -> None:
        self.config = config
    
    def import_data(self, content: str) -> list[dict[str, Any]]:
        """Import data from JSON string."""
        data = json.loads(content)
        if isinstance(data, list):
            return data[:self.config.max_rows] if self.config.max_rows else data
        if isinstance(data, dict):
            if "data" in data:
                return data["data"]
            return [data]
        return []


class JSONLinesImporter:
    """Imports data from JSON Lines format."""
    
    def __init__(self, config: ImportConfig) -> None:
        self.config = config
    
    def import_data(self, content: str) -> list[dict[str, Any]]:
        """Import data from JSON Lines string."""
        data = []
        lines = content.strip().split("\n")
        for i, line in enumerate(lines):
            if self.config.max_rows and i >= self.config.max_rows:
                break
            if line.strip():
                try:
                    data.append(json.loads(line))
                except Exception:
                    pass
        return data


class DataImportAction:
    """Data import action for automation workflows."""
    
    def __init__(self) -> None:
        self._importers = {
            ImportFormat.CSV: CSVImporter,
            ImportFormat.JSON: JSONImporter,
            ImportFormat.JSON_LINES: JSONLinesImporter,
        }
    
    async def import_data(self, content: str, format: ImportFormat = ImportFormat.CSV, **kwargs) -> ImportResult:
        """Import data from specified format."""
        config = ImportConfig(format=format, **kwargs)
        importer_class = self._importers.get(format, JSONImporter)
        importer = importer_class(config)
        try:
            data = importer.import_data(content)
            skipped = min(config.skip_rows, len(data)) if config.skip_rows > 0 else 0
            if skipped:
                data = data[skipped:]
            return ImportResult(success=True, data=data, record_count=len(data), skipped_count=skipped)
        except Exception as e:
            return ImportResult(success=False, data=[], record_count=0, skipped_count=0, errors=[str(e)])


__all__ = ["ImportFormat", "ImportConfig", "ImportResult", "CSVImporter", "JSONImporter", "JSONLinesImporter", "DataImportAction"]

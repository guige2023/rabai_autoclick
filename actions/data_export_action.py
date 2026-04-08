"""
Data Export Action Module.

Provides data export capabilities for various formats
including CSV, JSON, XML, and Excel.
"""

from typing import Any, BinaryIO, Dict, List, Optional, TextIO
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import csv
import io
import json
import logging

logger = logging.getLogger(__name__)


class ExportFormat(Enum):
    """Export formats."""
    CSV = "csv"
    JSON = "json"
    XML = "xml"
    TSV = "tsv"
    Excel = "xlsx"


@dataclass
class ExportConfig:
    """Export configuration."""
    format: ExportFormat
    include_headers: bool = True
    delimiter: str = ","
    quote_char: str = '"'
    encoding: str = "utf-8"
    pretty_print: bool = False


@dataclass
class ExportResult:
    """Result of export operation."""
    success: bool
    format: ExportFormat
    row_count: int
    byte_count: int
    output: Optional[str] = None
    error: Optional[str] = None


class CSVExporter:
    """Exports data to CSV format."""

    def __init__(self, config: ExportConfig):
        self.config = config

    def export(self, data: List[Dict[str, Any]]) -> ExportResult:
        """Export data to CSV."""
        try:
            if not data:
                return ExportResult(
                    success=True,
                    format=ExportFormat.CSV,
                    row_count=0,
                    byte_count=0,
                    output=""
                )

            output = io.StringIO()
            fieldnames = list(data[0].keys())

            writer = csv.DictWriter(
                output,
                fieldnames=fieldnames,
                delimiter=self.config.delimiter,
                quotechar=self.config.quote_char
            )

            if self.config.include_headers:
                writer.writeheader()

            for row in data:
                writer.writerow(row)

            csv_content = output.getvalue()
            return ExportResult(
                success=True,
                format=ExportFormat.CSV,
                row_count=len(data),
                byte_count=len(csv_content.encode(self.config.encoding)),
                output=csv_content
            )

        except Exception as e:
            return ExportResult(
                success=False,
                format=ExportFormat.CSV,
                row_count=0,
                byte_count=0,
                error=str(e)
            )


class JSONExporter:
    """Exports data to JSON format."""

    def __init__(self, config: ExportConfig):
        self.config = config

    def export(self, data: List[Dict[str, Any]]) -> ExportResult:
        """Export data to JSON."""
        try:
            if self.config.pretty_print:
                json_content = json.dumps(data, indent=2, ensure_ascii=False)
            else:
                json_content = json.dumps(data, ensure_ascii=False)

            return ExportResult(
                success=True,
                format=ExportFormat.JSON,
                row_count=len(data),
                byte_count=len(json_content.encode(self.config.encoding)),
                output=json_content
            )

        except Exception as e:
            return ExportResult(
                success=False,
                format=ExportFormat.JSON,
                row_count=0,
                byte_count=0,
                error=str(e)
            )


class XMLExporter:
    """Exports data to XML format."""

    def __init__(self, config: ExportConfig):
        self.config = config

    def _dict_to_xml(self, data: Dict[str, Any], root: str = "record") -> str:
        """Convert dict to XML."""
        lines = [f"<{root}>"]

        for key, value in data.items():
            safe_key = str(key).replace(" ", "_")
            if isinstance(value, dict):
                lines.append(f"  <{safe_key}>")
                for k, v in value.items():
                    lines.append(f"    <{k}>{self._escape_xml(str(v))}</{k}>")
                lines.append(f"  </{safe_key}>")
            elif isinstance(value, list):
                for item in value:
                    lines.append(f"  <{safe_key}>{self._escape_xml(str(item))}</{safe_key}>")
            else:
                lines.append(f"  <{safe_key}>{self._escape_xml(str(value))}</{safe_key}>")

        lines.append(f"</{root}>")
        return "\n".join(lines)

    def _escape_xml(self, text: str) -> str:
        """Escape XML special characters."""
        return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;"))

    def export(self, data: List[Dict[str, Any]]) -> ExportResult:
        """Export data to XML."""
        try:
            lines = ['<?xml version="1.0" encoding="UTF-8"?>']
            lines.append("<data>")

            for record in data:
                lines.append(self._dict_to_xml(record))

            lines.append("</data>")
            xml_content = "\n".join(lines)

            return ExportResult(
                success=True,
                format=ExportFormat.XML,
                row_count=len(data),
                byte_count=len(xml_content.encode(self.config.encoding)),
                output=xml_content
            )

        except Exception as e:
            return ExportResult(
                success=False,
                format=ExportFormat.XML,
                row_count=0,
                byte_count=0,
                error=str(e)
            )


class DataExporter:
    """Main export orchestrator."""

    def __init__(self, config: ExportConfig):
        self.config = config

    def export(self, data: List[Dict[str, Any]]) -> ExportResult:
        """Export data based on format."""
        if self.config.format == ExportFormat.CSV:
            exporter = CSVExporter(self.config)
        elif self.config.format == ExportFormat.JSON:
            exporter = JSONExporter(self.config)
        elif self.config.format == ExportFormat.XML:
            exporter = XMLExporter(self.config)
        else:
            return ExportResult(
                success=False,
                format=self.config.format,
                row_count=0,
                byte_count=0,
                error=f"Unsupported format: {self.config.format}"
            )

        return exporter.export(data)

    def export_to_file(
        self,
        data: List[Dict[str, Any]],
        file_path: str
    ) -> ExportResult:
        """Export data to file."""
        result = self.export(data)

        if result.success and result.output:
            with open(file_path, "w", encoding=self.config.encoding) as f:
                f.write(result.output)

        return result


class StreamingExporter:
    """Exports data in streaming fashion for large datasets."""

    def __init__(self, config: ExportConfig):
        self.config = config
        self._buffer = io.StringIO()
        self._row_count = 0
        self._written = False

    def write_header(self, fields: List[str]):
        """Write header row."""
        if self.config.include_headers:
            writer = csv.writer(
                self._buffer,
                delimiter=self.config.delimiter,
                quotechar=self.config.quote_char
            )
            writer.writerow(fields)
            self._row_count += 1

    def write_row(self, row: Dict[str, Any]):
        """Write a single row."""
        if not self._written:
            self.write_header(list(row.keys()))
            self._written = True

        writer = csv.writer(
            self._buffer,
            delimiter=self.config.delimiter,
            quotechar=self.config.quote_char
        )
        writer.writerow(list(row.values()))
        self._row_count += 1

    def flush(self) -> str:
        """Get exported content."""
        return self._buffer.getvalue()


def main():
    """Demonstrate data export."""
    data = [
        {"name": "Alice", "age": 30, "city": "NYC"},
        {"name": "Bob", "age": 25, "city": "LA"},
        {"name": "Charlie", "age": 35, "city": "Chicago"},
    ]

    config = ExportConfig(format=ExportFormat.CSV, include_headers=True)
    exporter = DataExporter(config)

    result = exporter.export(data)
    print(f"Export success: {result.success}")
    print(f"Rows: {result.row_count}")
    print(f"Content:\n{result.output}")


if __name__ == "__main__":
    main()

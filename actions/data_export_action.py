"""
Data Export Action - Exports data to various formats.

This module provides data export capabilities including
CSV, JSON, Excel, and custom format export.
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar
from enum import Enum


T = TypeVar("T")


class ExportFormat(Enum):
    """Supported export formats."""
    CSV = "csv"
    JSON = "json"
    JSON_LINES = "jsonl"
    TSV = "tsv"
    XML = "xml"
    HTML = "html"


@dataclass
class ExportConfig:
    """Configuration for data export."""
    format: ExportFormat = ExportFormat.CSV
    include_headers: bool = True
    delimiter: str = ","
    quote_char: str = '"'
    encoding: str = "utf-8"
    line_ending: str = "\n"


@dataclass
class ExportResult:
    """Result of export operation."""
    success: bool
    content: str | bytes
    format: ExportFormat
    record_count: int
    file_size: int


class CSVExporter:
    """Exports data to CSV format."""
    
    def __init__(self, config: ExportConfig) -> None:
        self.config = config
    
    def export(
        self,
        data: list[dict[str, Any]],
    ) -> str:
        """Export data to CSV string."""
        if not data:
            return ""
        
        output = io.StringIO()
        fieldnames = list(data[0].keys())
        
        writer = csv.DictWriter(
            output,
            fieldnames=fieldnames,
            delimiter=self.config.delimiter,
            quotechar=self.config.quote_char,
            lineterminator=self.config.line_ending,
        )
        
        if self.config.include_headers:
            writer.writeheader()
        
        for record in data:
            row = {k: self._format_value(v) for k, v in record.items()}
            writer.writerow(row)
        
        return output.getvalue()
    
    def _format_value(self, value: Any) -> str:
        """Format value for CSV."""
        if value is None:
            return ""
        if isinstance(value, (list, dict)):
            return json.dumps(value)
        return str(value)


class JSONExporter:
    """Exports data to JSON format."""
    
    def __init__(self, config: ExportConfig) -> None:
        self.config = config
    
    def export(
        self,
        data: list[dict[str, Any]],
    ) -> str:
        """Export data to JSON string."""
        return json.dumps(
            data,
            indent=2,
            ensure_ascii=False,
            default=str,
        )


class JSONLinesExporter:
    """Exports data to JSON Lines format."""
    
    def __init__(self, config: ExportConfig) -> None:
        self.config = config
    
    def export(
        self,
        data: list[dict[str, Any]],
    ) -> str:
        """Export data to JSON Lines string."""
        lines = []
        for record in data:
            lines.append(json.dumps(record, ensure_ascii=False, default=str))
        return self.config.line_ending.join(lines)


class XMLExporter:
    """Exports data to XML format."""
    
    def __init__(self, config: ExportConfig) -> None:
        self.config = config
    
    def export(
        self,
        data: list[dict[str, Any]],
    ) -> str:
        """Export data to XML string."""
        lines = ['<?xml version="1.0" encoding="UTF-8"?>', "<root>"]
        
        for record in data:
            lines.append("  <record>")
            for key, value in record.items():
                safe_key = self._sanitize_xml_tag(key)
                safe_value = self._escape_xml(str(value))
                lines.append(f"    <{safe_key}>{safe_value}</{safe_key}>")
            lines.append("  </record>")
        
        lines.append("</root>")
        return self.config.line_ending.join(lines)
    
    def _sanitize_xml_tag(self, name: str) -> str:
        """Sanitize string for use as XML tag."""
        import re
        name = re.sub(r"[^a-zA-Z0-9_]", "_", name)
        if name[0].isdigit():
            name = f"_{name}"
        return name
    
    def _escape_xml(self, text: str) -> str:
        """Escape special XML characters."""
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&apos;")
        )


class HTMLExporter:
    """Exports data to HTML table format."""
    
    def __init__(self, config: ExportConfig) -> None:
        self.config = config
    
    def export(
        self,
        data: list[dict[str, Any]],
    ) -> str:
        """Export data to HTML table."""
        if not data:
            return "<table></table>"
        
        lines = ["<table>"]
        
        if self.config.include_headers:
            lines.append("  <thead><tr>")
            for key in data[0].keys():
                lines.append(f"    <th>{self._escape_html(key)}</th>")
            lines.append("  </tr></thead>")
        
        lines.append("  <tbody>")
        for record in data:
            lines.append("    <tr>")
            for value in record.values():
                lines.append(f"      <td>{self._escape_html(str(value))}</td>")
            lines.append("    </tr>")
        lines.append("  </tbody>")
        lines.append("</table>")
        
        return self.config.line_ending.join(lines)
    
    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters."""
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
        )


class DataExportAction:
    """
    Data export action for automation workflows.
    
    Example:
        action = DataExportAction()
        result = await action.export(
            records,
            format=ExportFormat.CSV,
            include_headers=True
        )
    """
    
    def __init__(self) -> None:
        self._exporters = {
            ExportFormat.CSV: CSVExporter,
            ExportFormat.JSON: JSONExporter,
            ExportFormat.JSON_LINES: JSONLinesExporter,
            ExportFormat.XML: XMLExporter,
            ExportFormat.HTML: HTMLExporter,
        }
    
    async def export(
        self,
        data: list[dict[str, Any]],
        format: ExportFormat = ExportFormat.CSV,
        **kwargs,
    ) -> ExportResult:
        """Export data to specified format."""
        config = ExportConfig(format=format, **kwargs)
        
        exporter_class = self._exporters.get(format, JSONExporter)
        exporter = exporter_class(config)
        
        try:
            content = exporter.export(data)
            return ExportResult(
                success=True,
                content=content,
                format=format,
                record_count=len(data),
                file_size=len(content),
            )
        except Exception as e:
            return ExportResult(
                success=False,
                content="",
                format=format,
                record_count=0,
                file_size=0,
            )
    
    def export_to_bytes(
        self,
        data: list[dict[str, Any]],
        format: ExportFormat = ExportFormat.CSV,
        **kwargs,
    ) -> bytes:
        """Export data as bytes."""
        import gzip
        
        config = ExportConfig(format=format, **kwargs)
        exporter_class = self._exporters.get(format, JSONExporter)
        exporter = exporter_class(config)
        
        content = exporter.export(data)
        
        if format == ExportFormat.CSV:
            return content.encode(config.encoding)
        
        return content.encode(config.encoding)


# Export public API
__all__ = [
    "ExportFormat",
    "ExportConfig",
    "ExportResult",
    "CSVExporter",
    "JSONExporter",
    "JSONLinesExporter",
    "XMLExporter",
    "HTMLExporter",
    "DataExportAction",
]

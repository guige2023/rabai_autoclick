"""
Data Exporter Action Module

Export data to multiple formats (JSON, CSV, XML, Excel, Parquet).
Schema inference, compression, and streaming export support.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class ExportFormat(Enum):
    """Supported export formats."""
    
    JSON = "json"
    JSON_LINES = "jsonl"
    CSV = "csv"
    XML = "xml"
    EXCEL = "xlsx"
    PARQUET = "parquet"
    HTML = "html"


@dataclass
class ExportConfig:
    """Configuration for export operation."""
    
    format: ExportFormat = ExportFormat.JSON
    include_headers: bool = True
    pretty_print: bool = False
    compression: Optional[str] = None
    chunk_size: int = 1000
    max_records: Optional[int] = None
    output_path: Optional[str] = None


@dataclass
class ExportResult:
    """Result of export operation."""
    
    success: bool
    file_path: Optional[str]
    records_exported: int
    bytes_written: int
    duration_ms: float
    format: str


class JSONExporter:
    """Exports data to JSON format."""
    
    def __init__(self, config: ExportConfig):
        self.config = config
    
    def export(self, data: List[Dict]) -> str:
        """Export data to JSON string."""
        records = data
        
        if self.config.pretty_print:
            return json.dumps(records, indent=2, ensure_ascii=False)
        return json.dumps(records, ensure_ascii=False)
    
    def export_streaming(self, data: Iterator[Dict]) -> Iterator[str]:
        """Export data as JSON lines."""
        for record in data:
            yield json.dumps(record, ensure_ascii=False) + "\n"


class CSVExporter:
    """Exports data to CSV format."""
    
    def __init__(self, config: ExportConfig):
        self.config = config
    
    def export(self, data: List[Dict]) -> str:
        """Export data to CSV string."""
        if not data:
            return ""
        
        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=data[0].keys(),
            extrasaction="ignore"
        )
        
        if self.config.include_headers:
            writer.writeheader()
        
        writer.writerows(data)
        return output.getvalue()
    
    def export_streaming(self, data: Iterator[Dict]) -> Iterator[str]:
        """Export data as streaming CSV."""
        output = io.StringIO()
        writer = None
        headers_written = False
        
        for record in data:
            if writer is None:
                writer = csv.DictWriter(
                    output,
                    fieldnames=record.keys(),
                    extrasaction="ignore"
                )
                if self.config.include_headers:
                    writer.writeheader()
                    yield output.getvalue()
                    output = io.StringIO()
                    writer = csv.DictWriter(
                        output,
                        fieldnames=record.keys(),
                        extrasaction="ignore"
                    )
            
            writer.writerow(record)
            yield output.getvalue()
            output = io.StringIO()
            writer = csv.DictWriter(
                output,
                fieldnames=record.keys(),
                extrasaction="ignore"
            )


class XMLExporter:
    """Exports data to XML format."""
    
    def __init__(self, config: ExportConfig):
        self.config = config
    
    def export(self, data: List[Dict]) -> str:
        """Export data to XML string."""
        root_tag = "records"
        record_tag = "record"
        
        lines = [f'<?xml version="1.0" encoding="UTF-8"?>', f"<{root_tag}>"]
        
        for record in data:
            lines.append(f"  <{record_tag}>")
            for key, value in record.items():
                escaped_value = str(value).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                lines.append(f"    <{key}>{escaped_value}</{key}>")
            lines.append(f"  </{record_tag}>")
        
        lines.append(f"</{root_tag}>")
        return "\n".join(lines)


class HTMLExporter:
    """Exports data to HTML table format."""
    
    def __init__(self, config: ExportConfig):
        self.config = config
    
    def export(self, data: List[Dict]) -> str:
        """Export data to HTML table."""
        if not data:
            return "<table></table>"
        
        lines = [
            "<!DOCTYPE html>",
            "<html>",
            "<head>",
            "  <meta charset='UTF-8'>",
            "  <style>",
            "    table { border-collapse: collapse; width: 100%; }",
            "    th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }",
            "    th { background-color: #4CAF50; color: white; }",
            "  </style>",
            "</head>",
            "<body>",
            "  <table>"
        ]
        
        lines.append("    <tr>")
        for header in data[0].keys():
            lines.append(f"      <th>{header}</th>")
        lines.append("    </tr>")
        
        for record in data:
            lines.append("    <tr>")
            for value in record.values():
                lines.append(f"      <td>{value}</td>")
            lines.append("    </tr>")
        
        lines.extend([
            "  </table>",
            "</body>",
            "</html>"
        ])
        
        return "\n".join(lines)


class DataExporterAction:
    """
    Main data exporter action handler.
    
    Provides multi-format data export with compression,
    streaming support, and configurable options.
    """
    
    def __init__(self):
        self._exporters: Dict[ExportFormat, Any] = {}
    
    def export(
        self,
        data: List[Dict],
        config: ExportConfig
    ) -> ExportResult:
        """Export data to specified format."""
        import time
        start_time = time.time()
        
        if config.format == ExportFormat.JSON:
            exporter = JSONExporter(config)
            content = exporter.export(data)
        elif config.format == ExportFormat.CSV:
            exporter = CSVExporter(config)
            content = exporter.export(data)
        elif config.format == ExportFormat.XML:
            exporter = XMLExporter(config)
            content = exporter.export(data)
        elif config.format == ExportFormat.HTML:
            exporter = HTMLExporter(config)
            content = exporter.export(data)
        else:
            return ExportResult(
                success=False,
                file_path=None,
                records_exported=0,
                bytes_written=0,
                duration_ms=0,
                format=config.format.value
            )
        
        records_exported = len(data)
        bytes_written = len(content.encode("utf-8"))
        
        file_path = None
        if config.output_path:
            if config.compression:
                import gzip
                file_path = f"{config.output_path}.{config.compression}"
                with gzip.open(file_path, "wt", encoding="utf-8") as f:
                    f.write(content)
            else:
                file_path = config.output_path
                Path(file_path).write_text(content, encoding="utf-8")
        
        duration_ms = (time.time() - start_time) * 1000
        
        return ExportResult(
            success=True,
            file_path=file_path,
            records_exported=records_exported,
            bytes_written=bytes_written,
            duration_ms=duration_ms,
            format=config.format.value
        )
    
    def export_to_file(
        self,
        data: List[Dict],
        file_path: str,
        format: Optional[ExportFormat] = None
    ) -> ExportResult:
        """Export data to file (format inferred from extension)."""
        if format is None:
            ext = Path(file_path).suffix.lower()
            format_map = {
                ".json": ExportFormat.JSON,
                ".jsonl": ExportFormat.JSON_LINES,
                ".csv": ExportFormat.CSV,
                ".xml": ExportFormat.XML,
                ".html": ExportFormat.HTML,
                ".xlsx": ExportFormat.EXCEL
            }
            format = format_map.get(ext, ExportFormat.JSON)
        
        config = ExportConfig(format=format, output_path=file_path)
        return self.export(data, config)
    
    def export_streaming(
        self,
        data: Iterator[Dict],
        config: ExportConfig
    ) -> Iterator[str]:
        """Export data as streaming output."""
        if config.format == ExportFormat.JSON:
            exporter = JSONExporter(config)
            yield from exporter.export_streaming(data)
        elif config.format == ExportFormat.CSV:
            exporter = CSVExporter(config)
            yield from exporter.export_streaming(data)
    
    def get_supported_formats(self) -> List[str]:
        """Get list of supported export formats."""
        return [f.value for f in ExportFormat]

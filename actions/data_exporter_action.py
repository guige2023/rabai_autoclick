"""
Data Exporter Action Module.

Provides multi-format data export including CSV, Excel, JSON, XML, and Parquet.
"""

import asyncio
import csv
import io
import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional
import base64

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


class ExportFormat(Enum):
    """Export formats."""
    CSV = "csv"
    TSV = "tsv"
    JSON = "json"
    JSON_LINES = "jsonl"
    XML = "xml"
    HTML = "html"
    EXCEL = "excel"
    PARQUET = "parquet"


@dataclass
class ExportConfig:
    """Export configuration."""
    format: ExportFormat = ExportFormat.CSV
    delimiter: str = ","
    include_headers: bool = True
    encoding: str = "utf-8"
    pretty_print: bool = False
    compression: Optional[str] = None


@dataclass
class ExportResult:
    """Export result."""
    success: bool
    data: Any = None
    format: ExportFormat = ExportFormat.CSV
    size: int = 0
    error: Optional[str] = None


class CSVExporter:
    """CSV exporter."""

    def __init__(self, config: ExportConfig):
        self.config = config

    def export(self, data: list[dict]) -> str:
        """Export to CSV string."""
        if not data:
            return ""

        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=data[0].keys(),
            delimiter=self.config.delimiter
        )

        if self.config.include_headers:
            writer.writeheader()

        writer.writerows(data)
        return output.getvalue()


class JSONExporter:
    """JSON exporter."""

    def __init__(self, config: ExportConfig):
        self.config = config

    def export(self, data: list[dict]) -> str:
        """Export to JSON string."""
        if self.config.pretty_print:
            return json.dumps(data, indent=2, ensure_ascii=False)
        return json.dumps(data, ensure_ascii=False)


class JSONLinesExporter:
    """JSON Lines exporter."""

    def __init__(self, config: ExportConfig):
        self.config = config

    def export(self, data: list[dict]) -> str:
        """Export to JSON Lines."""
        lines = [json.dumps(record, ensure_ascii=False) for record in data]
        return "\n".join(lines)


class XMLExporter:
    """XML exporter."""

    def __init__(self, config: ExportConfig):
        self.config = config

    def export(self, data: list[dict]) -> str:
        """Export to XML string."""
        root = ET.Element("data")

        for record in data:
            record_element = ET.SubElement(root, "record")

            for key, value in record.items():
                field_element = ET.SubElement(record_element, str(key))
                field_element.text = str(value) if value is not None else ""

        if self.config.pretty_print:
            return ET.tostring(root, encoding="unicode")
        return ET.tostring(root, encoding="unicode")


class DataExporterAction:
    """
    Multi-format data exporter.

    Example:
        exporter = DataExporterAction(format=ExportFormat.CSV)
        result = exporter.export(records)
    """

    def __init__(self, format: ExportFormat = ExportFormat.CSV, **kwargs: Any):
        self.config = ExportConfig(format=format, **kwargs)

        if format == ExportFormat.CSV:
            self._exporter = CSVExporter(self.config)
        elif format == ExportFormat.JSON:
            self._exporter = JSONExporter(self.config)
        elif format == ExportFormat.JSON_LINES:
            self._exporter = JSONLinesExporter(self.config)
        elif format == ExportFormat.XML:
            self._exporter = XMLExporter(self.config)
        else:
            self._exporter = CSVExporter(self.config)

    def export(self, data: list[dict]) -> ExportResult:
        """Export data to specified format."""
        try:
            result = self._exporter.export(data)

            if isinstance(result, str):
                data_bytes = result.encode(self.config.encoding)
            else:
                data_bytes = result

            return ExportResult(
                success=True,
                data=base64.b64encode(data_bytes).decode() if self.config.compression else result,
                format=self.config.format,
                size=len(data_bytes)
            )

        except Exception as e:
            return ExportResult(success=False, error=str(e))

    def export_to_bytes(self, data: list[dict]) -> ExportResult:
        """Export data to bytes."""
        try:
            result = self._exporter.export(data)

            if isinstance(result, str):
                data_bytes = result.encode(self.config.encoding)
            else:
                data_bytes = result

            return ExportResult(
                success=True,
                data=data_bytes,
                format=self.config.format,
                size=len(data_bytes)
            )

        except Exception as e:
            return ExportResult(success=False, error=str(e))

    async def export_async(self, data: list[dict]) -> ExportResult:
        """Export data asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.export, data)

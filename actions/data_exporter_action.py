# Copyright (c) 2024. coded by claude
"""Data Exporter Action Module.

Handles exporting data to various formats including JSON, CSV, XML, and
custom formats with support for streaming and batch processing.
"""
from typing import Optional, Dict, Any, List, TextIO, BinaryIO
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import csv
import json
import logging

logger = logging.getLogger(__name__)


class ExportFormat(Enum):
    JSON = "json"
    CSV = "csv"
    XML = "xml"
    TSV = "tsv"


@dataclass
class ExportConfig:
    format: ExportFormat
    include_header: bool = True
    encoding: str = "utf-8"
    indent: Optional[int] = 2
    datetime_format: str = "%Y-%m-%d %H:%M:%S"


@dataclass
class ExportResult:
    success: bool
    records_exported: int
    bytes_written: int
    error: Optional[str] = None


class DataExporter:
    def __init__(self, config: Optional[ExportConfig] = None):
        self.config = config or ExportConfig(format=ExportFormat.JSON)

    def export_json(self, data: List[Dict[str, Any]], output: TextIO) -> ExportResult:
        try:
            text = json.dumps(data, indent=self.config.indent, ensure_ascii=False, default=self._json_serializer)
            output.write(text)
            return ExportResult(
                success=True,
                records_exported=len(data),
                bytes_written=len(text.encode(self.config.encoding)),
            )
        except Exception as e:
            return ExportResult(success=False, records_exported=0, bytes_written=0, error=str(e))

    def export_csv(self, data: List[Dict[str, Any]], output: TextIO) -> ExportResult:
        if not data:
            return ExportResult(success=True, records_exported=0, bytes_written=0)
        try:
            fieldnames = list(data[0].keys())
            writer = csv.DictWriter(output, fieldnames=fieldnames)
            if self.config.include_header:
                writer.writeheader()
            writer.writerows(data)
            return ExportResult(
                success=True,
                records_exported=len(data),
                bytes_written=0,
            )
        except Exception as e:
            return ExportResult(success=False, records_exported=0, bytes_written=0, error=str(e))

    def export_tsv(self, data: List[Dict[str, Any]], output: TextIO) -> ExportResult:
        if not data:
            return ExportResult(success=True, records_exported=0, bytes_written=0)
        try:
            fieldnames = list(data[0].keys())
            writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter="\t")
            if self.config.include_header:
                writer.writeheader()
            writer.writerows(data)
            return ExportResult(
                success=True,
                records_exported=len(data),
                bytes_written=0,
            )
        except Exception as e:
            return ExportResult(success=False, records_exported=0, bytes_written=0, error=str(e))

    def export_xml(self, data: List[Dict[str, Any]], output: TextIO, root_name: str = "items") -> ExportResult:
        try:
            output.write(f'<?xml version="1.0" encoding="{self.config.encoding}"?>\\n')
            output.write(f"<{root_name}>\\n")
            for item in data:
                self._write_xml_item(output, item, "  ")
            output.write(f"</{root_name}>\\n")
            return ExportResult(
                success=True,
                records_exported=len(data),
                bytes_written=0,
            )
        except Exception as e:
            return ExportResult(success=False, records_exported=0, bytes_written=0, error=str(e))

    def _write_xml_item(self, output: TextIO, item: Dict[str, Any], indent: str) -> None:
        for key, value in item.items():
            safe_key = str(key).replace(" ", "_")
            if isinstance(value, dict):
                output.write(f"{indent}<{safe_key}>\\n")
                self._write_xml_item(output, value, indent + "  ")
                output.write(f"{indent}</{safe_key}>\\n")
            elif isinstance(value, list):
                for v in value:
                    output.write(f"{indent}<{safe_key}>{self._escape_xml(str(v))}</{safe_key}>\\n")
            else:
                output.write(f"{indent}<{safe_key}>{self._escape_xml(str(value))}</{safe_key}>\\n")

    def _escape_xml(self, text: str) -> str:
        return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

    def _json_serializer(self, obj: Any) -> str:
        if isinstance(obj, datetime):
            return obj.strftime(self.config.datetime_format)
        return str(obj)

    def export(self, data: List[Dict[str, Any]], output: TextIO) -> ExportResult:
        if self.config.format == ExportFormat.JSON:
            return self.export_json(data, output)
        elif self.config.format == ExportFormat.CSV:
            return self.export_csv(data, output)
        elif self.config.format == ExportFormat.TSV:
            return self.export_tsv(data, output)
        elif self.config.format == ExportFormat.XML:
            return self.export_xml(data, output)
        return ExportResult(success=False, records_exported=0, bytes_written=0, error=f"Unsupported format: {self.config.format}")

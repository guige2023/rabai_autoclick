"""Data Export Action Module.

Provides data export to multiple formats:
JSON, CSV, XML, with schema support.
"""
from __future__ import annotations

import csv
import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


class ExportFormat(Enum):
    """Export format."""
    JSON = "json"
    JSON_LINES = "jsonl"
    CSV = "csv"
    XML = "xml"
    TSV = "tsv"


@dataclass
class ExportConfig:
    """Export configuration."""
    format: ExportFormat = ExportFormat.JSON
    indent: int = 2
    include_headers: bool = True
    date_format: str = "%Y-%m-%dT%H:%M:%S"
    encoding: str = "utf-8"


class DataExportAction:
    """Data exporter to multiple formats.

    Example:
        exporter = DataExportAction()

        json_output = exporter.export(data, ExportFormat.JSON)
        csv_output = exporter.export(data, ExportFormat.CSV)

        await exporter.export_to_file(data, "output.csv")
    """

    def __init__(self, config: Optional[ExportConfig] = None) -> None:
        self.config = config or ExportConfig()

    def export(
        self,
        data: Any,
        format: Optional[ExportFormat] = None,
    ) -> str:
        """Export data to string.

        Args:
            data: Data to export
            format: Export format

        Returns:
            Exported string
        """
        format = format or self.config.format

        if format == ExportFormat.JSON:
            return self._export_json(data)
        elif format == ExportFormat.JSON_LINES:
            return self._export_jsonl(data)
        elif format == ExportFormat.CSV:
            return self._export_csv(data)
        elif format == ExportFormat.XML:
            return self._export_xml(data)
        elif format == ExportFormat.TSV:
            return self._export_tsv(data)

        return str(data)

    async def export_to_file(
        self,
        data: Any,
        filepath: str,
        format: Optional[ExportFormat] = None,
    ) -> None:
        """Export data to file.

        Args:
            data: Data to export
            filepath: Output file path
            format: Export format
        """
        content = self.export(data, format)
        output = Path(filepath)

        output.write_text(content, encoding=self.config.encoding)

    def _export_json(self, data: Any) -> str:
        """Export to JSON."""
        return json.dumps(
            data,
            indent=self.config.indent,
            default=self._json_default,
            ensure_ascii=False,
        )

    def _export_jsonl(self, data: Any) -> str:
        """Export to JSON Lines."""
        if isinstance(data, list):
            return "\n".join(json.dumps(item, default=self._json_default) for item in data)
        return json.dumps(data, default=self._json_default)

    def _export_csv(self, data: Any) -> str:
        """Export to CSV."""
        if not isinstance(data, list):
            data = [data]

        if not data:
            return ""

        output = StringIO()
        headers = list(data[0].keys()) if isinstance(data[0], dict) else []

        if self.config.include_headers and headers:
            writer = csv.DictWriter(output, fieldnames=headers)
            writer.writeheader()

        writer = csv.DictWriter(output, fieldnames=headers)
        for row in data:
            if isinstance(row, dict):
                writer.writerow(row)

        return output.getvalue()

    def _export_xml(self, data: Any) -> str:
        """Export to XML."""
        root = ET.Element("root")
        self._dict_to_xml(data, root)
        return ET.tostring(root, encoding="unicode")

    def _dict_to_xml(self, data: Any, parent: ET.Element) -> None:
        """Convert dict to XML element."""
        if isinstance(data, dict):
            for key, value in data.items():
                child = ET.SubElement(parent, str(key))
                self._dict_to_xml(value, child)
        elif isinstance(data, list):
            for item in data:
                child = ET.SubElement(parent, "item")
                self._dict_to_xml(item, child)
        else:
            parent.text = str(data)

    def _export_tsv(self, data: Any) -> str:
        """Export to TSV."""
        if not isinstance(data, list):
            data = [data]

        if not data:
            return ""

        output = StringIO()
        headers = list(data[0].keys()) if isinstance(data[0], dict) else []

        if self.config.include_headers and headers:
            output.write("\t".join(headers) + "\n")

        for row in data:
            if isinstance(row, dict):
                values = [str(row.get(h, "")) for h in headers]
                output.write("\t".join(values) + "\n")

        return output.getvalue()

    def _json_default(self, obj: Any) -> str:
        """JSON serializer for non-serializable objects."""
        if isinstance(obj, datetime):
            return obj.strftime(self.config.date_format)

        if hasattr(obj, "__dict__"):
            return obj.__dict__

        return str(obj)

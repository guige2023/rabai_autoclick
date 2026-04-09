"""Data export utilities for various formats.

This module provides data export functionality:
- Export to JSON, CSV, Excel, XML
- Streaming export for large datasets
- Format conversion
- Batch export with compression

Example:
    >>> from actions.data_exporter_action import DataExporter
    >>> exporter = DataExporter()
    >>> exporter.export(data, format="csv", output="report.csv")
"""

from __future__ import annotations

import csv
import json
import gzip
import logging
import io
from typing import Any, BinaryIO, Callable, Optional
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


class ExportFormat:
    """Supported export formats."""
    JSON = "json"
    CSV = "csv"
    XML = "xml"
    TSV = "tsv"


@dataclass
class ExportOptions:
    """Options for data export."""
    format: str = ExportFormat.JSON
    pretty: bool = False
    include_header: bool = True
    compression: Optional[str] = None
    batch_size: int = 1000


class DataExporter:
    """Export data to various formats.

    Example:
        >>> exporter = DataExporter()
        >>> exporter.export(users, format="csv", output="users.csv")
    """

    def __init__(self, default_options: Optional[ExportOptions] = None) -> None:
        self.default_options = default_options or ExportOptions()

    def export(
        self,
        data: list[dict[str, Any]],
        output: Optional[str] = None,
        format: Optional[str] = None,
        options: Optional[ExportOptions] = None,
        **kwargs: Any,
    ) -> Optional[bytes]:
        """Export data to specified format.

        Args:
            data: List of dicts to export.
            output: Output file path (optional).
            format: Export format override.
            options: Export options.
            **kwargs: Additional format-specific options.

        Returns:
            Exported bytes if no output file specified.
        """
        opts = options or self.default_options
        opts.format = format or opts.format
        if opts.compression:
            return self._export_compressed(data, opts)
        if opts.format == ExportFormat.JSON:
            return self._export_json(data, opts)
        elif opts.format == ExportFormat.CSV:
            return self._export_csv(data, opts)
        elif opts.format == ExportFormat.TSV:
            return self._export_tsv(data, opts)
        elif opts.format == ExportFormat.XML:
            return self._export_xml(data, opts)
        else:
            raise ValueError(f"Unsupported format: {opts.format}")

    def _export_json(self, data: list[dict[str, Any]], options: ExportOptions) -> bytes:
        """Export to JSON format."""
        indent = 2 if options.pretty else None
        json_str = json.dumps(data, indent=indent, ensure_ascii=False)
        return json_str.encode("utf-8")

    def _export_csv(self, data: list[dict[str, Any]], options: ExportOptions) -> bytes:
        """Export to CSV format."""
        if not data:
            return b""
        output = io.StringIO()
        fieldnames = list(data[0].keys())
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        if options.include_header:
            writer.writeheader()
        for row in data:
            writer.writerow(row)
        return output.getvalue().encode("utf-8")

    def _export_tsv(self, data: list[dict[str, Any]], options: ExportOptions) -> bytes:
        """Export to TSV format."""
        if not data:
            return b""
        output = io.StringIO()
        fieldnames = list(data[0].keys())
        writer = csv.DictWriter(output, fieldnames=fieldnames, delimiter="\t")
        if options.include_header:
            writer.writeheader()
        for row in data:
            writer.writerow(row)
        return output.getvalue().encode("utf-8")

    def _export_xml(self, data: list[dict[str, Any]], options: ExportOptions) -> bytes:
        """Export to XML format."""
        def to_xml(items: list[dict[str, Any]], root: str = "data") -> str:
            lines = [f'<?xml version="1.0" encoding="UTF-8"?>']
            lines.append(f"<{root}>")
            for item in items:
                lines.append("  <item>")
                for key, value in item.items():
                    safe_key = str(key).replace(" ", "_")
                    if isinstance(value, dict):
                        lines.append(f"    <{safe_key}>")
                        for k, v in value.items():
                            lines.append(f"      <{k}>{v}</{k}>")
                        lines.append(f"    </{safe_key}>")
                    elif isinstance(value, list):
                        for v in value:
                            lines.append(f"    <{safe_key}>{v}</{safe_key}>")
                    else:
                        lines.append(f"    <{safe_key}>{value}</{safe_key}>")
                lines.append("  </item>")
            lines.append(f"</{root}>")
            return "\n".join(lines)
        return to_xml(data).encode("utf-8")

    def _export_compressed(
        self,
        data: list[dict[str, Any]],
        options: ExportOptions,
    ) -> bytes:
        """Export with compression."""
        if options.compression == "gzip":
            raw = self.export(data, options=options, format=options.format)
            return gzip.compress(raw)
        raise ValueError(f"Unsupported compression: {options.compression}")

    def export_to_file(
        self,
        data: list[dict[str, Any]],
        filepath: str,
        **kwargs: Any,
    ) -> None:
        """Export data directly to a file.

        Args:
            data: Data to export.
            filepath: Output file path.
            **kwargs: Export options.
        """
        path = Path(filepath)
        format = path.suffix[1:] if path.suffix else kwargs.get("format", "json")
        content = self.export(data, format=format, **kwargs)
        path.write_bytes(content)
        logger.info(f"Exported {len(data)} records to {filepath}")


class StreamingExporter:
    """Export large datasets in streaming fashion."""

    def __init__(self, batch_size: int = 1000) -> None:
        self.batch_size = batch_size

    def export_streaming(
        self,
        data_iter: Callable[[], list[dict[str, Any]]],
        output: BinaryIO,
        format: str = ExportFormat.JSON,
    ) -> int:
        """Export data in streaming mode.

        Args:
            data_iter: Iterator that yields batches of data.
            output: Output file handle.
            format: Export format.

        Returns:
            Total records exported.
        """
        total = 0
        if format == ExportFormat.JSON:
            output.write(b'["')
            first = True
            while True:
                batch = data_iter()
                if not batch:
                    break
                for item in batch:
                    if not first:
                        output.write(b',')
                    output.write(json.dumps(item).encode("utf-8"))
                    first = False
                    total += 1
            output.write(b']')
        elif format == ExportFormat.CSV:
            first_batch = True
            while True:
                batch = data_iter()
                if not batch:
                    break
                output_buffer = io.StringIO()
                writer = csv.DictWriter(output_buffer, fieldnames=list(batch[0].keys()))
                if first_batch:
                    writer.writeheader()
                for row in batch:
                    writer.writerow(row)
                output.write(output_buffer.getvalue().encode("utf-8"))
                first_batch = False
                total += len(batch)
        return total


def export_json(data: list[dict[str, Any]], pretty: bool = False) -> bytes:
    """Quick JSON export.

    Args:
        data: Data to export.
        pretty: Pretty print flag.

    Returns:
        JSON bytes.
    """
    indent = 2 if pretty else None
    return json.dumps(data, indent=indent, ensure_ascii=False).encode("utf-8")


def export_csv(data: list[dict[str, Any]], include_header: bool = True) -> bytes:
    """Quick CSV export.

    Args:
        data: Data to export.
        include_header: Include column headers.

    Returns:
        CSV bytes.
    """
    if not data:
        return b""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(data[0].keys()))
    if include_header:
        writer.writeheader()
    for row in data:
        writer.writerow(row)
    return output.getvalue().encode("utf-8")

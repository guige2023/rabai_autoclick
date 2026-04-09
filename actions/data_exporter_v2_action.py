"""Data Exporter v2 with multi-format support and streaming.

This module provides data export capabilities:
- Multiple output formats (CSV, JSON, XML, Parquet, Excel)
- Streaming export for large datasets
- Batch processing
- Schema evolution support
- Compression options
"""

from __future__ import annotations

import asyncio
import csv
import gzip
import io
import json
import logging
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Generator, TypeVar
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ExportFormat(Enum):
    """Supported export formats."""

    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"
    XML = "xml"
    PARQUET = "parquet"
    EXCEL = "excel"
    TSV = "tsv"
    HTML = "html"


class CompressionType(Enum):
    """Compression types."""

    NONE = "none"
    GZIP = "gzip"
    ZIP = "zip"


@dataclass
class ExportConfig:
    """Configuration for data export."""

    format: ExportFormat = ExportFormat.CSV
    compression: CompressionType = CompressionType.NONE
    batch_size: int = 1000
    include_header: bool = True
    include_metadata: bool = False
    encoding: str = "utf-8"
    delimiter: str = ","
    quote_char: str = '"'
    escape_char: str = "\\"
    line_terminator: str = "\n"
    date_format: str = "%Y-%m-%d"
    datetime_format: str = "%Y-%m-%d %H:%M:%S"
    null_value: str = ""
    max_workers: int = 4


@dataclass
class ExportMetadata:
    """Metadata for exported data."""

    record_count: int = 0
    exported_count: int = 0
    failed_count: int = 0
    start_time: float = 0.0
    end_time: float = 0.0
    file_size: int = 0
    format: str = ""
    compression: str = ""
    columns: list[str] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)


@dataclass
class SchemaDefinition:
    """Schema definition for export."""

    columns: list[dict[str, Any]] = field(default_factory=list)

    def get_column_names(self) -> list[str]:
        """Get column names."""
        return [col["name"] for col in self.columns]

    def get_column_type(self, name: str) -> str:
        """Get column type."""
        for col in self.columns:
            if col["name"] == name:
                return col.get("type", "string")
        return "string"


class StreamingExporter:
    """Streaming data exporter for large datasets."""

    def __init__(self, config: ExportConfig, schema: SchemaDefinition | None = None):
        """Initialize streaming exporter.

        Args:
            config: Export configuration
            schema: Optional schema definition
        """
        self.config = config
        self.schema = schema
        self.metadata = ExportMetadata()
        self._buffer: list[dict] = []
        self._closed = False

    def write(self, record: dict) -> None:
        """Write a single record.

        Args:
            record: Record to write
        """
        if self._closed:
            raise IOError("Exporter is closed")

        self._buffer.append(record)
        self.metadata.record_count += 1

        if len(self._buffer) >= self.config.batch_size:
            self._flush_buffer()

    def write_batch(self, records: list[dict]) -> None:
        """Write multiple records.

        Args:
            records: Records to write
        """
        for record in records:
            self.write(record)

    def _flush_buffer(self) -> None:
        """Flush buffer to output."""
        if self._buffer:
            self.metadata.exported_count += len(self._buffer)
            self._buffer.clear()

    def close(self) -> None:
        """Close the exporter."""
        self._flush_buffer()
        self._closed = True


class DataExporterV2:
    """Advanced data exporter with multi-format support."""

    def __init__(
        self,
        config: ExportConfig | None = None,
        schema: SchemaDefinition | None = None,
    ):
        """Initialize data exporter.

        Args:
            config: Export configuration
            schema: Optional schema definition
        """
        self.config = config or ExportConfig()
        self.schema = schema

    def export(
        self,
        data: list[dict],
        output_path: str | Path,
        schema: SchemaDefinition | None = None,
    ) -> ExportMetadata:
        """Export data to a file.

        Args:
            data: Data to export
            output_path: Output file path
            schema: Optional schema override

        Returns:
            ExportMetadata with export statistics
        """
        output_path = Path(output_path)
        metadata = ExportMetadata(
            start_time=__import__("time").time(),
            format=self.config.format.value,
            compression=self.config.compression.value,
        )

        if schema:
            self.schema = schema

        if self.schema:
            metadata.columns = self.schema.get_column_names()

        try:
            if self.config.format == ExportFormat.CSV:
                self._export_csv(data, output_path, metadata)
            elif self.config.format == ExportFormat.JSON:
                self._export_json(data, output_path, metadata)
            elif self.config.format == ExportFormat.JSONL:
                self._export_jsonl(data, output_path, metadata)
            elif self.config.format == ExportFormat.XML:
                self._export_xml(data, output_path, metadata)
            elif self.config.format == ExportFormat.TSV:
                self._export_tsv(data, output_path, metadata)
            elif self.config.format == ExportFormat.HTML:
                self._export_html(data, output_path, metadata)
            elif self.config.format == ExportFormat.PARQUET:
                self._export_parquet(data, output_path, metadata)
            elif self.config.format == ExportFormat.EXCEL:
                self._export_excel(data, output_path, metadata)

            # Apply compression
            if self.config.compression == CompressionType.GZIP:
                self._compress_gzip(output_path)

            metadata.end_time = __import__("time").time()
            metadata.file_size = output_path.stat().st_size

        except Exception as e:
            logger.error(f"Export failed: {e}")
            metadata.errors.append({"error": str(e), "record_count": metadata.record_count})
            metadata.end_time = __import__("time").time()

        return metadata

    async def export_async(
        self,
        data: list[dict],
        output_path: str | Path,
        schema: SchemaDefinition | None = None,
    ) -> ExportMetadata:
        """Async version of export.

        Args:
            data: Data to export
            output_path: Output file path
            schema: Optional schema override

        Returns:
            ExportMetadata with export statistics
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            ThreadPoolExecutor(max_workers=self.config.max_workers),
            lambda: self.export(data, output_path, schema),
        )

    def export_streaming(
        self,
        data_generator: Generator[dict, None, None],
        output_path: str | Path,
        schema: SchemaDefinition | None = None,
    ) -> ExportMetadata:
        """Export data using streaming for large datasets.

        Args:
            data_generator: Generator yielding records
            output_path: Output file path
            schema: Optional schema definition

        Returns:
            ExportMetadata with export statistics
        """
        output_path = Path(output_path)
        metadata = ExportMetadata(
            start_time=__import__("time").time(),
            format=self.config.format.value,
            compression=self.config.compression.value,
        )

        if schema:
            self.schema = schema

        try:
            exporter = StreamingExporter(self.config, self.schema)

            if self.config.format == ExportFormat.CSV:
                self._export_streaming_csv(data_generator, output_path, exporter, metadata)
            elif self.config.format == ExportFormat.JSONL:
                self._export_streaming_jsonl(data_generator, output_path, exporter, metadata)
            else:
                # Fall back to batch for other formats
                data = list(data_generator)
                self.export(data, output_path, schema)

            if self.config.compression == CompressionType.GZIP:
                self._compress_gzip(output_path)

            metadata.end_time = __import__("time").time()
            metadata.file_size = output_path.stat().st_size

        except Exception as e:
            logger.error(f"Streaming export failed: {e}")
            metadata.errors.append({"error": str(e)})
            metadata.end_time = __import__("time").time()

        return metadata

    def _export_csv(
        self,
        data: list[dict],
        output_path: Path,
        metadata: ExportMetadata,
    ) -> None:
        """Export to CSV format."""
        if not data:
            return

        columns = self.schema.get_column_names() if self.schema else list(data[0].keys())

        with open(output_path, "w", newline="", encoding=self.config.encoding) as f:
            writer = csv.writer(
                f,
                delimiter=self.config.delimiter,
                quotechar=self.config.quote_char,
                escapechar=self.config.escape_char,
                lineterminator=self.config.line_terminator,
            )

            if self.config.include_header:
                writer.writerow(columns)
                metadata.exported_count += 1

            for record in data:
                try:
                    row = [self._format_value(record.get(col), col) for col in columns]
                    writer.writerow(row)
                    metadata.exported_count += 1
                except Exception as e:
                    metadata.failed_count += 1
                    metadata.errors.append({"error": str(e), "record": record})

    def _export_tsv(
        self,
        data: list[dict],
        output_path: Path,
        metadata: ExportMetadata,
    ) -> None:
        """Export to TSV format."""
        self.config.delimiter = "\t"
        self._export_csv(data, output_path, metadata)

    def _export_json(
        self,
        data: list[dict],
        output_path: Path,
        metadata: ExportMetadata,
    ) -> None:
        """Export to JSON format."""
        with open(output_path, "w", encoding=self.config.encoding) as f:
            json.dump(
                data,
                f,
                indent=2,
                ensure_ascii=False,
                default=str,
            )
        metadata.exported_count = len(data)

    def _export_jsonl(
        self,
        data: list[dict],
        output_path: Path,
        metadata: ExportMetadata,
    ) -> None:
        """Export to JSONL format."""
        with open(output_path, "w", encoding=self.config.encoding) as f:
            for record in data:
                try:
                    f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                    metadata.exported_count += 1
                except Exception as e:
                    metadata.failed_count += 1
                    metadata.errors.append({"error": str(e), "record": record})

    def _export_streaming_jsonl(
        self,
        data_generator: Generator[dict, None, None],
        output_path: Path,
        exporter: StreamingExporter,
        metadata: ExportMetadata,
    ) -> None:
        """Stream export to JSONL format."""
        with open(output_path, "w", encoding=self.config.encoding) as f:
            for record in data_generator:
                try:
                    f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                    metadata.exported_count += 1
                    metadata.record_count += 1
                except Exception as e:
                    metadata.failed_count += 1
                    metadata.errors.append({"error": str(e)})

    def _export_streaming_csv(
        self,
        data_generator: Generator[dict, None, None],
        output_path: Path,
        exporter: StreamingExporter,
        metadata: ExportMetadata,
    ) -> None:
        """Stream export to CSV format."""
        columns = None

        with open(output_path, "w", newline="", encoding=self.config.encoding) as f:
            writer = None

            for record in data_generator:
                if columns is None:
                    columns = self.schema.get_column_names() if self.schema else list(record.keys())
                    if self.config.include_header:
                        f.write(self.config.delimiter.join(columns) + self.config.line_terminator)

                try:
                    row = [self._format_value(record.get(col), col) for col in columns]
                    f.write(self.config.delimiter.join(row) + self.config.line_terminator)
                    metadata.exported_count += 1
                    metadata.record_count += 1
                except Exception as e:
                    metadata.failed_count += 1
                    metadata.errors.append({"error": str(e)})

    def _export_xml(
        self,
        data: list[dict],
        output_path: Path,
        metadata: ExportMetadata,
    ) -> None:
        """Export to XML format."""
        root = ET.Element("data")

        for record in data:
            record_elem = ET.SubElement(root, "record")
            for key, value in record.items():
                child = ET.SubElement(record_elem, key)
                child.text = str(value) if value is not None else ""
                metadata.exported_count += 1

        tree = ET.ElementTree(root)
        tree.write(output_path, encoding=self.config.encoding, xml_declaration=True)

    def _export_html(
        self,
        data: list[dict],
        output_path: Path,
        metadata: ExportMetadata,
    ) -> None:
        """Export to HTML table format."""
        if not data:
            return

        columns = self.schema.get_column_names() if self.schema else list(data[0].keys())

        html = ['<!DOCTYPE html>', '<html>', '<head>',
                f'<meta charset="{self.config.encoding}">',
                '<style>',
                'table { border-collapse: collapse; width: 100%; }',
                'th, td { border: 1px solid #ddd; padding: 8px; }',
                'th { background-color: #f2f2f2; }',
                '</style>', '</head>', '<body>',
                '<table>']

        # Header
        html.append('<thead><tr>')
        for col in columns:
            html.append(f'<th>{col}</th>')
        html.append('</tr></thead>')

        # Body
        html.append('<tbody>')
        for record in data:
            html.append('<tr>')
            for col in columns:
                value = self._format_value(record.get(col), col)
                html.append(f'<td>{value}</td>')
                metadata.exported_count += 1
            html.append('</tr>')
        html.append('</tbody>')

        html.extend(['</table>', '</body>', '</html>'])

        with open(output_path, "w", encoding=self.config.encoding) as f:
            f.write('\n'.join(html))

    def _export_parquet(
        self,
        data: list[dict],
        output_path: Path,
        metadata: ExportMetadata,
    ) -> None:
        """Export to Parquet format."""
        try:
            import pandas as pd
            import pyarrow.parquet as pq

            df = pd.DataFrame(data)
            df.to_parquet(output_path, engine="pyarrow", compression="snappy")
            metadata.exported_count = len(data)

        except ImportError:
            raise ImportError("pandas and pyarrow required for Parquet export")

    def _export_excel(
        self,
        data: list[dict],
        output_path: Path,
        metadata: ExportMetadata,
    ) -> None:
        """Export to Excel format."""
        try:
            import pandas as pd

            df = pd.DataFrame(data)
            df.to_excel(output_path, index=False, engine="openpyxl")
            metadata.exported_count = len(data)

        except ImportError:
            raise ImportError("pandas and openpyxl required for Excel export")

    def _compress_gzip(self, file_path: Path) -> None:
        """Compress file with gzip."""
        gzip_path = Path(str(file_path) + ".gz")
        with open(file_path, "rb") as f_in:
            with gzip.open(gzip_path, "wb") as f_out:
                f_out.writelines(f_in)
        file_path.unlink()

    def _format_value(self, value: Any, column_name: str | None = None) -> str:
        """Format a value for export."""
        if value is None:
            return self.config.null_value

        if isinstance(value, (int, float)):
            return str(value)

        if isinstance(value, (list, dict)):
            return json.dumps(value, ensure_ascii=False, default=str)

        import datetime
        if isinstance(value, datetime.datetime):
            return value.strftime(self.config.datetime_format)
        if isinstance(value, datetime.date):
            return value.strftime(self.config.date_format)

        return str(value)


def create_exporter(
    format: ExportFormat = ExportFormat.CSV,
    compression: CompressionType = CompressionType.NONE,
    batch_size: int = 1000,
) -> DataExporterV2:
    """Create a configured data exporter.

    Args:
        format: Export format
        compression: Compression type
        batch_size: Batch size for processing

    Returns:
        Configured DataExporterV2 instance
    """
    config = ExportConfig(
        format=format,
        compression=compression,
        batch_size=batch_size,
    )
    return DataExporterV2(config=config)

"""
Data Exporter Action Module.

Exports data to various formats including CSV, JSON, Parquet, Excel,
with configurable schemas, transformations, and compression.

Author: RabAi Team
"""

from __future__ import annotations

import io
import json
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

import pandas as pd


class ExportFormat(Enum):
    """Supported export formats."""
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    EXCEL = "excel"
    XML = "xml"
    HTML = "html"
    YAML = "yaml"
    ZIP = "zip"


@dataclass
class ExportConfig:
    """Configuration for export operations."""
    format: ExportFormat
    compression: Optional[str] = None
    encoding: str = "utf-8"
    include_index: bool = False
    include_headers: bool = True
    chunk_size: int = 10000
    delimiter: str = ","
    null_value: str = ""
    date_format: str = "%Y-%m-%d"


@dataclass
class ExportResult:
    """Result of an export operation."""
    success: bool
    format: ExportFormat
    bytes_exported: int
    rows_exported: int
    output_path: Optional[str] = None
    error: Optional[str] = None
    duration_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "format": self.format.value,
            "bytes_exported": self.bytes_exported,
            "rows_exported": self.rows_exported,
            "output_path": self.output_path,
            "error": self.error,
            "duration_ms": self.duration_ms,
        }


class DataExporter:
    """
    Data export engine supporting multiple formats.

    Exports DataFrames to CSV, JSON, Parquet, Excel with configurable
    compression, encoding, and transformation options.

    Example:
        >>> exporter = DataExporter()
        >>> result = exporter.export(df, format=ExportFormat.CSV, output_path="/tmp/data.csv")
    """

    def __init__(self):
        self._transformers: Dict[str, Callable] = {}

    def register_transformer(self, name: str, fn: Callable) -> None:
        """Register a pre-export transformation function."""
        self._transformers[name] = fn

    def export(
        self,
        df: pd.DataFrame,
        format: ExportFormat,
        output_path: Optional[str] = None,
        config: Optional[ExportConfig] = None,
        **kwargs,
    ) -> ExportResult:
        """Export DataFrame to specified format."""
        import time
        start = time.time()

        config = config or ExportConfig(format=format)
        config.format = format

        df = df.copy()
        for name, transformer in self._transformers.items():
            df = transformer(df)

        try:
            if format == ExportFormat.CSV:
                return self._export_csv(df, output_path, config, start)
            elif format == ExportFormat.JSON:
                return self._export_json(df, output_path, config, start)
            elif format == ExportFormat.PARQUET:
                return self._export_parquet(df, output_path, config, start)
            elif format == ExportFormat.EXCEL:
                return self._export_excel(df, output_path, config, start)
            elif format == ExportFormat.ZIP:
                return self._export_zip(df, output_path, config, start)
            else:
                return ExportResult(
                    success=False,
                    format=format,
                    bytes_exported=0,
                    rows_exported=0,
                    error=f"Unsupported format: {format.value}",
                )
        except Exception as e:
            return ExportResult(
                success=False,
                format=format,
                bytes_exported=0,
                rows_exported=0,
                error=str(e),
                duration_ms=(time.time() - start) * 1000,
            )

    def export_to_bytes(
        self,
        df: pd.DataFrame,
        format: ExportFormat,
        config: Optional[ExportConfig] = None,
    ) -> tuple[bytes, ExportResult]:
        """Export DataFrame to bytes (in-memory)."""
        config = config or ExportConfig(format=format)

        result = self.export(df, format, None, config)
        if not result.success:
            return b"", result

        if format == ExportFormat.CSV:
            buf = io.StringIO()
            df.to_csv(buf, index=config.include_index, encoding=config.encoding)
            return buf.getvalue().encode(config.encoding), result
        elif format == ExportFormat.JSON:
            buf = io.StringIO()
            df.to_json(buf, orient="records", date_format="iso")
            return buf.getvalue().encode(config.encoding), result
        elif format == ExportFormat.PARQUET:
            buf = io.BytesIO()
            df.to_parquet(buf)
            return buf.getvalue(), result

        return b"", result

    def _export_csv(
        self,
        df: pd.DataFrame,
        output_path: Optional[str],
        config: ExportConfig,
        start: float,
    ) -> ExportResult:
        """Export to CSV format."""
        buf = io.StringIO()
        df.to_csv(
            buf,
            index=config.include_index,
            sep=config.delimiter,
            encoding=config.encoding,
            na_rep=config.null_value,
            date_format=config.date_format,
        )
        data = buf.getvalue().encode(config.encoding)

        if output_path:
            with open(output_path, "wb") as f:
                f.write(data)

        return ExportResult(
            success=True,
            format=ExportFormat.CSV,
            bytes_exported=len(data),
            rows_exported=len(df),
            output_path=output_path,
            duration_ms=(time.time() - start) * 1000,
        )

    def _export_json(
        self,
        df: pd.DataFrame,
        output_path: Optional[str],
        config: ExportConfig,
        start: float,
    ) -> ExportResult:
        """Export to JSON format."""
        buf = io.StringIO()
        df.to_json(buf, orient="records", date_format="iso")
        data = buf.getvalue().encode(config.encoding)

        if output_path:
            with open(output_path, "wb") as f:
                f.write(data)

        return ExportResult(
            success=True,
            format=ExportFormat.JSON,
            bytes_exported=len(data),
            rows_exported=len(df),
            output_path=output_path,
            duration_ms=(time.time() - start) * 1000,
        )

    def _export_parquet(
        self,
        df: pd.DataFrame,
        output_path: Optional[str],
        config: ExportConfig,
        start: float,
    ) -> ExportResult:
        """Export to Parquet format."""
        buf = io.BytesIO()
        df.to_parquet(buf, compression=config.compression or "snappy")
        data = buf.getvalue()

        if output_path:
            with open(output_path, "wb") as f:
                f.write(data)

        return ExportResult(
            success=True,
            format=ExportFormat.PARQUET,
            bytes_exported=len(data),
            rows_exported=len(df),
            output_path=output_path,
            duration_ms=(time.time() - start) * 1000,
        )

    def _export_excel(
        self,
        df: pd.DataFrame,
        output_path: Optional[str],
        config: ExportConfig,
        start: float,
    ) -> ExportResult:
        """Export to Excel format."""
        if not output_path:
            return ExportResult(
                success=False,
                format=ExportFormat.EXCEL,
                bytes_exported=0,
                rows_exported=0,
                error="Excel export requires output_path",
            )

        df.to_excel(output_path, index=config.include_index)
        size = len(open(output_path, "rb").read())

        return ExportResult(
            success=True,
            format=ExportFormat.EXCEL,
            bytes_exported=size,
            rows_exported=len(df),
            output_path=output_path,
            duration_ms=(time.time() - start) * 1000,
        )

    def _export_zip(
        self,
        df: pd.DataFrame,
        output_path: Optional[str],
        config: ExportConfig,
        start: float,
    ) -> ExportResult:
        """Export to ZIP archive containing CSV."""
        if not output_path:
            return ExportResult(
                success=False,
                format=ExportFormat.ZIP,
                bytes_exported=0,
                rows_exported=0,
                error="ZIP export requires output_path",
            )

        buf = io.StringIO()
        df.to_csv(buf, index=config.include_index, encoding=config.encoding)
        csv_data = buf.getvalue().encode(config.encoding)

        with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("data.csv", csv_data)

        size = len(open(output_path, "rb").read())

        return ExportResult(
            success=True,
            format=ExportFormat.ZIP,
            bytes_exported=size,
            rows_exported=len(df),
            output_path=output_path,
            duration_ms=(time.time() - start) * 1000,
        )


def create_exporter() -> DataExporter:
    """Factory to create a data exporter."""
    return DataExporter()

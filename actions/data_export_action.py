"""Data export action module.

Provides data export to various formats (CSV, JSON, Excel, Parquet, XML).
Supports chunked export for large datasets and format conversion.
"""

from __future__ import annotations

import csv
import json
import logging
import io
from typing import Optional, Dict, Any, List, Union
from pathlib import Path
from dataclasses import dataclass

logger = logging.getLogger(__name__)


class ExportFormat(Enum):
    """Supported export formats."""
    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"
    XML = "xml"
    EXCEL = "xlsx"
    PARQUET = "parquet"
    TSV = "tsv"
    HTML = "html"


from enum import Enum


@dataclass
class ExportOptions:
    """Options for data export."""
    format: ExportFormat = ExportFormat.CSV
    delimiter: str = ","
    include_header: bool = True
    encoding: str = "utf-8"
    chunk_size: int = 10000
    max_rows: Optional[int] = None


class DataExportAction:
    """Data export engine.

    Exports lists of dicts to various file formats with configurable options.

    Example:
        exporter = DataExportAction()
        exporter.to_csv(data, "/tmp/export.csv")
        exporter.to_json(data, "/tmp/export.json", pretty=True)
    """

    def __init__(self, default_options: Optional[ExportOptions] = None) -> None:
        self.default_options = default_options or ExportOptions()

    def to_csv(
        self,
        data: List[Dict[str, Any]],
        path: Union[str, Path],
        options: Optional[ExportOptions] = None,
    ) -> int:
        """Export data to CSV file.

        Args:
            data: List of dicts to export.
            path: Output file path.
            options: Export options.

        Returns:
            Number of rows exported.
        """
        opts = options or self.default_options
        rows_written = 0

        with open(path, "w", newline="", encoding=opts.encoding) as f:
            if not data:
                return 0

            fieldnames = list(data[0].keys()) if data else []
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=opts.delimiter)

            if opts.include_header:
                writer.writeheader()

            for i, row in enumerate(data):
                if opts.max_rows and i >= opts.max_rows:
                    break
                writer.writerow(row)
                rows_written += 1

        logger.info("Exported %d rows to %s", rows_written, path)
        return rows_written

    def to_json(
        self,
        data: List[Dict[str, Any]],
        path: Union[str, Path],
        pretty: bool = False,
        options: Optional[ExportOptions] = None,
    ) -> int:
        """Export data to JSON file.

        Args:
            data: List of dicts to export.
            path: Output file path.
            pretty: Pretty-print JSON.
            options: Export options.

        Returns:
            Number of rows exported.
        """
        opts = options or self.default_options
        max_rows = opts.max_rows or len(data)

        with open(path, "w", encoding=opts.encoding) as f:
            indent = 2 if pretty else None
            json.dump(data[:max_rows], f, indent=indent, ensure_ascii=False, default=str)

        rows = min(len(data), max_rows)
        logger.info("Exported %d rows to %s", rows, path)
        return rows

    def to_jsonl(
        self,
        data: List[Dict[str, Any]],
        path: Union[str, Path],
        options: Optional[ExportOptions] = None,
    ) -> int:
        """Export data to JSON Lines file.

        Args:
            data: List of dicts to export.
            path: Output file path.
            options: Export options.

        Returns:
            Number of rows exported.
        """
        opts = options or self.default_options
        rows_written = 0

        with open(path, "w", encoding=opts.encoding) as f:
            for i, row in enumerate(data):
                if opts.max_rows and i >= opts.max_rows:
                    break
                f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")
                rows_written += 1

        logger.info("Exported %d rows to %s", rows_written, path)
        return rows_written

    def to_tsv(
        self,
        data: List[Dict[str, Any]],
        path: Union[str, Path],
        options: Optional[ExportOptions] = None,
    ) -> int:
        """Export data to TSV file."""
        opts = (options or self.default_options)
        opts.delimiter = "\t"
        return self.to_csv(data, path, opts)

    def to_html(
        self,
        data: List[Dict[str, Any]],
        path: Union[str, Path],
        title: str = "Data Export",
        options: Optional[ExportOptions] = None,
    ) -> int:
        """Export data to HTML table.

        Args:
            data: List of dicts to export.
            path: Output file path.
            title: HTML page title.
            options: Export options.

        Returns:
            Number of rows exported.
        """
        opts = options or self.default_options
        max_rows = opts.max_rows or len(data)
        export_data = data[:max_rows]

        if not export_data:
            return 0

        fieldnames = list(export_data[0].keys())

        html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="{opts.encoding}">
    <title>{title}</title>
    <style>
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <table>
        <thead>
            <tr>{"".join(f"<th>{h}</th>" for h in fieldnames)}</tr>
        </thead>
        <tbody>
"""
        for row in export_data:
            html += "            <tr>" + "".join(f"<td>{str(row.get(h, ''))}</td>" for h in fieldnames) + "</tr>\n"

        html += """        </tbody>
    </table>
</body>
</html>"""

        with open(path, "w", encoding=opts.encoding) as f:
            f.write(html)

        logger.info("Exported %d rows to %s", len(export_data), path)
        return len(export_data)

    def to_string(
        self,
        data: List[Dict[str, Any]],
        format: ExportFormat = ExportFormat.CSV,
        options: Optional[ExportOptions] = None,
    ) -> str:
        """Export data to string in specified format.

        Args:
            data: List of dicts to export.
            format: Output format.
            options: Export options.

        Returns:
            Exported data as string.
        """
        opts = options or self.default_options

        if format == ExportFormat.CSV:
            output = io.StringIO()
            self.to_csv(data, output, options=opts)
            return output.getvalue()
        elif format == ExportFormat.JSON:
            return json.dumps(data, indent=2, ensure_ascii=False, default=str)
        elif format == ExportFormat.JSONL:
            return "\n".join(json.dumps(row, ensure_ascii=False, default=str) for row in data)

        raise ValueError(f"Unsupported format for string export: {format}")

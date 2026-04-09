"""Data Export Action Module.

Provides data export capabilities supporting multiple formats including
JSON, CSV, Excel, XML, YAML, and custom templates with compression.
"""

from __future__ import annotations

import base64
import csv
import io
import json
import logging
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TextIO, Union

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class ExportFormat(Enum):
    """Supported export formats."""
    JSON = "json"
    CSV = "csv"
    TSV = "tsv"
    XML = "xml"
    YAML = "yaml"
    EXCEL = "excel"
    HTML = "html"
    SQL = "sql"
    PARQUET = "parquet"


class CompressionType(Enum):
    """Compression types for export."""
    NONE = "none"
    GZIP = "gzip"
    ZIP = "zip"
    BZIP2 = "bzip2"


@dataclass
class CSVOptions:
    """Options for CSV export."""
    delimiter: str = ","
    quotechar: str = '"'
    quoting: str = "minimal"  # minimal, all, nonnumeric, none
    include_header: bool = True
    newline: str = ""


@dataclass
class JSONOptions:
    """Options for JSON export."""
    indent: Optional[int] = 2
    sort_keys: bool = False
    ensure_ascii: bool = False
    compact: bool = False


@dataclass
class SQLOptions:
    """Options for SQL export."""
    table_name: str = "exported_data"
    insert_batch_size: int = 100
    if_exists: str = "append"  # append, replace, fail
    include_schema: bool = True


@dataclass
class ExportOptions:
    """Comprehensive export options."""
    format: ExportFormat = ExportFormat.JSON
    compression: CompressionType = CompressionType.NONE
    csv_options: Optional[CSVOptions] = None
    json_options: Optional[JSONOptions] = None
    sql_options: Optional[SQLOptions] = None
    file_path: Optional[str] = None
    include_metadata: bool = True
    encoding: str = "utf-8"


class DataExporter:
    """Performs data export to various formats."""

    @staticmethod
    def _flatten_dict(d: Dict[str, Any], parent_key: str = "", sep: str = ".") -> Dict[str, Any]:
        """Flatten nested dictionary."""
        items: List[Tuple[str, Any]] = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(DataExporter._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
        return dict(items)

    @staticmethod
    def to_json(data: List[Dict[str, Any]], options: JSONOptions) -> str:
        """Export data to JSON format."""
        if options.compact:
            return json.dumps(data, ensure_ascii=options.ensure_ascii, separators=(",", ":"))
        return json.dumps(data, indent=options.indent, sort_keys=options.sort_keys,
                          ensure_ascii=options.ensure_ascii)

    @staticmethod
    def to_csv(data: List[Dict[str, Any]], options: CSVOptions) -> str:
        """Export data to CSV format."""
        if not data:
            return ""

        output = io.StringIO()
        # Flatten first row to get all possible columns
        flattened_data = [DataExporter._flatten_dict(row) if isinstance(row, dict) else row
                         for row in data]

        fieldnames = []
        for row in flattened_data:
            if isinstance(row, dict):
                fieldnames.extend(row.keys())
        fieldnames = list(dict.fromkeys(fieldnames))  # Preserve order, remove dups

        quoting_map = {
            "minimal": csv.QUOTE_MINIMAL,
            "all": csv.QUOTE_ALL,
            "nonnumeric": csv.QUOTE_NONNUMERIC,
            "none": csv.QUOTE_NONE,
        }
        quoting = quoting_map.get(options.quoting, csv.QUOTE_MINIMAL)

        writer = csv.DictWriter(
            output,
            fieldnames=fieldnames,
            delimiter=options.delimiter,
            quotechar=options.quotechar,
            quoting=quoting,
            lineterminator=options.newline if options.newline else "\n",
        )

        if options.include_header:
            writer.writeheader()

        for row in flattened_data:
            if isinstance(row, dict):
                writer.writerow(row)
            elif row:
                writer.writerow({fieldnames[0]: row})

        return output.getvalue()

    @staticmethod
    def to_tsv(data: List[Dict[str, Any]]) -> str:
        """Export data to TSV format."""
        csv_options = CSVOptions(delimiter="\t", include_header=True)
        return DataExporter.to_csv(data, csv_options)

    @staticmethod
    def to_xml(data: List[Dict[str, Any]], root_name: str = "data",
               item_name: str = "item") -> str:
        """Export data to XML format."""
        root = ET.Element(root_name)
        for i, item in enumerate(data):
            record = ET.SubElement(root, item_name)
            if isinstance(item, dict):
                for key, value in item.items():
                    child = ET.SubElement(record, str(key))
                    child.text = str(value) if value is not None else ""
            elif item is not None:
                record.text = str(item)

        return ET.tostring(root, encoding="unicode")

    @staticmethod
    def to_yaml(data: List[Dict[str, Any]]) -> str:
        """Export data to YAML format (simple implementation)."""
        try:
            import yaml
            return yaml.dump(data, allow_unicode=True, sort_keys=False)
        except ImportError:
            # Fallback to simple format
            lines = []
            for i, item in enumerate(data):
                lines.append(f"- {json.dumps(item)}")
            return "\n".join(lines)

    @staticmethod
    def to_html(data: List[Dict[str, Any]], title: str = "Exported Data") -> str:
        """Export data to HTML table format."""
        if not data:
            return f"<html><head><title>{title}</title></head><body><p>No data</p></body></html>"

        flattened_data = [DataExporter._flatten_dict(row) if isinstance(row, dict) else row
                         for row in data]

        headers = []
        for row in flattened_data:
            if isinstance(row, dict):
                headers.extend(row.keys())
        headers = list(dict.fromkeys(headers))

        html = ['<html>', '<head>',
                f'<title>{title}</title>',
                '<style>',
                'table { border-collapse: collapse; width: 100%; }',
                'th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }',
                'th { background-color: #4CAF50; color: white; }',
                'tr:nth-child(even) { background-color: #f2f2f2; }',
                '</style>',
                '</head>', '<body>',
                f'<h1>{title}</h1>',
                '<table>']

        # Header
        html.append('<thead><tr>')
        for h in headers:
            html.append(f'<th>{h}</th>')
        html.append('</tr></thead>')

        # Body
        html.append('<tbody>')
        for row in flattened_data:
            html.append('<tr>')
            if isinstance(row, dict):
                for h in headers:
                    value = row.get(h, "")
                    html.append(f'<td>{value}</td>')
            else:
                html.append(f'<td>{row}</td>')
            html.append('</tr>')
        html.append('</tbody>')

        html.extend(['</table>', '</body>', '</html>'])
        return ''.join(html)

    @staticmethod
    def to_sql(data: List[Dict[str, Any]], options: SQLOptions) -> List[str]:
        """Export data as SQL INSERT statements."""
        if not data:
            return []

        flattened_data = [DataExporter._flatten_dict(row) if isinstance(row, dict) else row
                         for row in data]

        # Get columns from first row
        columns = []
        if flattened_data and isinstance(flattened_data[0], dict):
            columns = list(flattened_data[0].keys())

        statements = []
        if options.include_schema:
            create_table = [f"CREATE TABLE IF NOT EXISTS {options.table_name} ("]
            col_defs = [f"  {col} TEXT" for col in columns]
            create_table.append(", ".join(col_defs))
            create_table.append(");")
            statements.append("\n".join(create_table))

        # INSERT statements
        for i in range(0, len(flattened_data), options.insert_batch_size):
            batch = flattened_data[i:i + options.insert_batch_size]
            for row in batch:
                if isinstance(row, dict):
                    cols = ", ".join(columns)
                    values = []
                    for col in columns:
                        val = row.get(col)
                        if val is None:
                            values.append("NULL")
                        elif isinstance(val, (int, float)):
                            values.append(str(val))
                        else:
                            escaped = str(val).replace("'", "''")
                            values.append(f"'{escaped}'")
                    values_str = ", ".join(values)
                    statements.append(
                        f"INSERT INTO {options.table_name} ({cols}) VALUES ({values_str});"
                    )

        return statements

    @staticmethod
    def compress(data: Union[str, bytes], compression: CompressionType) -> bytes:
        """Compress data according to compression type."""
        if isinstance(data, str):
            data = data.encode("utf-8")

        if compression == CompressionType.GZIP:
            import gzip
            return gzip.compress(data)
        elif compression == CompressionType.BZIP2:
            import bz2
            return bz2.compress(data)
        elif compression == CompressionType.ZIP:
            buffer = io.BytesIO()
            with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr("exported_data", data)
            return buffer.getvalue()
        else:
            return data


class DataExportAction(BaseAction):
    """Data Export Action for exporting data to various formats.

    Supports JSON, CSV, XML, YAML, HTML, SQL, and compression.

    Examples:
        >>> action = DataExportAction()
        >>> result = action.execute(ctx, {
        ...     "data": [{"name": "Alice", "age": 30}],
        ...     "format": "csv",
        ...     "file_path": "/tmp/export.csv"
        ... })
    """

    action_type = "data_export"
    display_name = "数据导出"
    description = "多格式数据导出：JSON/CSV/XML/YAML/SQL/HTML"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data export.

        Args:
            context: Execution context.
            params: Dict with keys:
                - data: List of dicts to export
                - format: Export format ('json', 'csv', 'xml', 'yaml', 'html', 'sql')
                - file_path: Optional file path to write to
                - compression: Compression type ('none', 'gzip', 'zip', 'bzip2')
                - csv_options: Dict of CSV options
                - json_options: Dict of JSON options
                - sql_options: Dict of SQL options
                - include_metadata: Include export metadata
                - return_as_base64: Return data as base64 string

        Returns:
            ActionResult with exported data.
        """
        import time
        start_time = time.time()

        data = params.get("data", [])
        format_str = params.get("format", "json").lower()
        file_path = params.get("file_path")
        compression_str = params.get("compression", "none").lower()
        include_metadata = params.get("include_metadata", True)
        return_as_base64 = params.get("return_as_base64", False)

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message="'data' must be a list"
            )

        try:
            export_format = ExportFormat(format_str)
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Unsupported format: {format_str}"
            )

        try:
            compression = CompressionType(compression_str)
        except ValueError:
            compression = CompressionType.NONE

        # Export data
        metadata = {"record_count": len(data), "format": export_format.value}
        output_data: Union[str, bytes]

        if export_format == ExportFormat.JSON:
            json_opts = JSONOptions(**params.get("json_options", {}))
            output_data = DataExporter.to_json(data, json_opts)
            metadata["encoding"] = "utf-8"

        elif export_format == ExportFormat.CSV:
            csv_opts = CSVOptions(**params.get("csv_options", {}))
            output_data = DataExporter.to_csv(data, csv_opts)
            metadata["encoding"] = "utf-8"

        elif export_format == ExportFormat.TSV:
            output_data = DataExporter.to_tsv(data)
            metadata["encoding"] = "utf-8"

        elif export_format == ExportFormat.XML:
            root_name = params.get("xml_root_name", "data")
            item_name = params.get("xml_item_name", "item")
            output_data = DataExporter.to_xml(data, root_name, item_name)
            metadata["encoding"] = "utf-8"

        elif export_format == ExportFormat.YAML:
            output_data = DataExporter.to_yaml(data)
            metadata["encoding"] = "utf-8"

        elif export_format == ExportFormat.HTML:
            title = params.get("html_title", "Exported Data")
            output_data = DataExporter.to_html(data, title)
            metadata["encoding"] = "utf-8"

        elif export_format == ExportFormat.SQL:
            sql_opts = SQLOptions(**params.get("sql_options", {}))
            statements = DataExporter.to_sql(data, sql_opts)
            output_data = "\n".join(statements)
            metadata["encoding"] = "utf-8"
            metadata["statement_count"] = len(statements)

        else:
            return ActionResult(
                success=False,
                message=f"Format not implemented: {format_str}"
            )

        # Compress if requested
        if compression != CompressionType.NONE:
            output_data = DataExporter.compress(output_data, compression)
            metadata["compression"] = compression.value
            metadata["original_size"] = len(output_data)

        # Write to file if path provided
        if file_path:
            try:
                path = Path(file_path)
                path.parent.mkdir(parents=True, exist_ok=True)
                if isinstance(output_data, str):
                    path.write_text(output_data, encoding="utf-8")
                else:
                    path.write_bytes(output_data)
                metadata["file_path"] = str(path.absolute())
                metadata["file_size"] = path.stat().st_size
            except Exception as e:
                return ActionResult(
                    success=False,
                    message=f"Failed to write file: {str(e)}"
                )

        # Return result
        if return_as_base64:
            if isinstance(output_data, str):
                output_data = output_data.encode("utf-8")
            b64_data = base64.b64encode(output_data).decode()
            return ActionResult(
                success=True,
                message=f"Exported {len(data)} records to {export_format.value} (base64)",
                data={
                    "data": b64_data,
                    "metadata": metadata,
                }
            )

        return ActionResult(
            success=True,
            message=f"Exported {len(data)} records to {export_format.value}",
            data={
                "data": output_data if isinstance(output_data, str) else output_data.decode(errors="replace"),
                "metadata": metadata,
            }
        )

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "format": "json",
            "file_path": None,
            "compression": "none",
            "csv_options": None,
            "json_options": None,
            "sql_options": None,
            "include_metadata": True,
            "return_as_base64": False,
        }

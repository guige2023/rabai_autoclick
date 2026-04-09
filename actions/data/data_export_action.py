"""
Data Export Action Module.

Data export utilities for automation supporting multiple formats
including JSON, CSV, XML, YAML, and custom serializers.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import csv
import io
import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, TextIO, Union

logger = logging.getLogger(__name__)


class ExportFormat(Enum):
    """Supported export formats."""
    JSON = "json"
    JSON_LINES = "jsonl"
    CSV = "csv"
    TSV = "tsv"
    XML = "xml"
    YAML = "yaml"
    HTML = "html"
    MARKDOWN = "md"


@dataclass
class ExportConfig:
    """Configuration for export operation."""
    format: ExportFormat = ExportFormat.JSON
    indent: int = 2
    include_header: bool = True
    encoding: str = "utf-8"
    add_metadata: bool = False
    flatten_nested: bool = False
    delimiter: str = ","


@dataclass
class ExportResult:
    """Result of an export operation."""
    success: bool
    format: ExportFormat
    rows_exported: int = 0
    bytes_written: int = 0
    output: Optional[str] = None
    file_path: Optional[str] = None
    error: Optional[str] = None


class DataExportAction:
    """
    Data export utilities for automation.

    Exports data to various formats with configurable options.

    Example:
        exporter = DataExportAction()

        result = exporter.export(
            data=[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}],
            format=ExportFormat.CSV,
        )

        # Write to file
        exporter.export_to_file(data, "output.csv", format=ExportFormat.CSV)
    """

    def __init__(self) -> None:
        self._default_config = ExportConfig()

    def set_default_config(self, config: ExportConfig) -> None:
        """Set default export configuration."""
        self._default_config = config

    def export(
        self,
        data: Union[List[Dict], Dict, Any],
        format: ExportFormat = ExportFormat.JSON,
        config: Optional[ExportConfig] = None,
    ) -> ExportResult:
        """Export data to specified format."""
        cfg = config or self._default_config

        try:
            if format == ExportFormat.JSON:
                return self._export_json(data, cfg)
            elif format == ExportFormat.JSON_LINES:
                return self._export_jsonl(data, cfg)
            elif format == ExportFormat.CSV:
                return self._export_csv(data, cfg)
            elif format == ExportFormat.TSV:
                return self._export_tsv(data, cfg)
            elif format == ExportFormat.YAML:
                return self._export_yaml(data, cfg)
            elif format == ExportFormat.XML:
                return self._export_xml(data, cfg)
            elif format == ExportFormat.HTML:
                return self._export_html(data, cfg)
            elif format == ExportFormat.MARKDOWN:
                return self._export_markdown(data, cfg)
            else:
                return ExportResult(success=False, format=format, error=f"Unsupported format: {format}")

        except Exception as e:
            logger.error(f"Export failed: {e}")
            return ExportResult(success=False, format=format, error=str(e))

    def _export_json(self, data: Any, config: ExportConfig) -> ExportResult:
        """Export to JSON format."""
        metadata = {"exported_at": __import__("time").time(), "format": "json"} if config.add_metadata else {}

        if isinstance(data, list):
            output_data = data
        else:
            output_data = {"data": data, **metadata}

        json_str = json.dumps(output_data, indent=config.indent, default=str, ensure_ascii=False)
        return ExportResult(
            success=True,
            format=ExportFormat.JSON,
            rows_exported=len(data) if isinstance(data, list) else 1,
            bytes_written=len(json_str.encode()),
            output=json_str,
        )

    def _export_jsonl(self, data: Any, config: ExportConfig) -> ExportResult:
        """Export to JSON Lines format."""
        if not isinstance(data, list):
            data = [data]

        lines = []
        for item in data:
            lines.append(json.dumps(item, default=str))

        output = "\n".join(lines)
        return ExportResult(
            success=True,
            format=ExportFormat.JSON_LINES,
            rows_exported=len(lines),
            bytes_written=len(output.encode()),
            output=output,
        )

    def _export_csv(self, data: Any, config: ExportConfig) -> ExportResult:
        """Export to CSV format."""
        if not isinstance(data, list):
            data = [data]

        if not data:
            return ExportResult(success=True, format=ExportFormat.CSV, output="")

        output_buffer = io.StringIO()
        writer = csv.DictWriter(
            output_buffer,
            fieldnames=data[0].keys() if isinstance(data[0], dict) else [],
            delimiter=config.delimiter,
        )

        if config.include_header:
            writer.writeheader()

        for row in data:
            if isinstance(row, dict):
                writer.writerow(row)
            else:
                writer.writerow({"value": row})

        output = output_buffer.getvalue()
        return ExportResult(
            success=True,
            format=ExportFormat.CSV,
            rows_exported=len(data),
            bytes_written=len(output.encode()),
            output=output,
        )

    def _export_tsv(self, data: Any, config: ExportConfig) -> ExportResult:
        """Export to TSV format."""
        config.delimiter = "\t"
        return self._export_csv(data, config)

    def _export_yaml(self, data: Any, config: ExportConfig) -> ExportResult:
        """Export to YAML format."""
        try:
            import yaml
        except ImportError:
            # Fall back to simple representation
            yaml_str = json.dumps(data, indent=config.indent, default=str)
            return ExportResult(
                success=True,
                format=ExportFormat.YAML,
                rows_exported=len(data) if isinstance(data, list) else 1,
                bytes_written=len(yaml_str.encode()),
                output=yaml_str,
            )

        output = yaml.dump(data, default_flow_style=False, allow_unicode=True, indent=config.indent)
        return ExportResult(
            success=True,
            format=ExportFormat.YAML,
            rows_exported=len(data) if isinstance(data, list) else 1,
            bytes_written=len(output.encode()),
            output=output,
        )

    def _export_xml(self, data: Any, config: ExportConfig) -> ExportResult:
        """Export to XML format."""
        def dict_to_xml(tag: str, d: Dict) -> str:
            items = []
            for k, v in d.items():
                if isinstance(v, dict):
                    items.append(dict_to_xml(k, v))
                elif isinstance(v, list):
                    for item in v:
                        if isinstance(item, dict):
                            items.append(dict_to_xml(k, dict_to_xml(k, item)))
                        else:
                            items.append(f"<{k}>{item}</{k}>")
                else:
                    items.append(f"<{k}>{v}</{k}>")
            return f"<{tag}>{''.join(items)}</{tag}>"

        if isinstance(data, list):
            root = "".join(dict_to_xml("item", d) for d in data)
            xml_str = f"<root>{root}</root>"
        else:
            xml_str = dict_to_xml("root", data if isinstance(data, dict) else {"item": data})

        return ExportResult(
            success=True,
            format=ExportFormat.XML,
            rows_exported=len(data) if isinstance(data, list) else 1,
            bytes_written=len(xml_str.encode()),
            output=xml_str,
        )

    def _export_html(self, data: Any, config: ExportConfig) -> ExportResult:
        """Export to HTML table format."""
        if not isinstance(data, list):
            data = [data]

        if not data or not isinstance(data[0], dict):
            return ExportResult(success=True, format=ExportFormat.HTML, output="<table></table>")

        headers = list(data[0].keys())
        rows = []
        for item in data:
            cells = "".join(f"<td>{item.get(h, '')}</td>" for h in headers)
            rows.append(f"<tr>{cells}</tr>")

        header_cells = "".join(f"<th>{h}</th>" for h in headers)
        html = f"<table><thead><tr>{header_cells}</tr></thead><tbody>{''.join(rows)}</tbody></table>"

        return ExportResult(
            success=True,
            format=ExportFormat.HTML,
            rows_exported=len(data),
            bytes_written=len(html.encode()),
            output=html,
        )

    def _export_markdown(self, data: Any, config: ExportConfig) -> ExportResult:
        """Export to Markdown table format."""
        if not isinstance(data, list):
            data = [data]

        if not data or not isinstance(data[0], dict):
            return ExportResult(success=True, format=ExportFormat.MARKDOWN, output="")

        headers = list(data[0].keys())
        separator = "|" + "|".join("---" for _ in headers) + "|"
        header_row = "|" + "|".join(str(h) for h in headers) + "|"

        rows = []
        for item in data:
            cells = "|" + "|".join(str(item.get(h, "")) for h in headers) + "|"
            rows.append(cells)

        md = "\n".join([header_row, separator] + rows)
        return ExportResult(
            success=True,
            format=ExportFormat.MARKDOWN,
            rows_exported=len(data),
            bytes_written=len(md.encode()),
            output=md,
        )

    def export_to_file(
        self,
        data: Any,
        file_path: str,
        format: Optional[ExportFormat] = None,
        config: Optional[ExportConfig] = None,
    ) -> ExportResult:
        """Export data directly to a file."""
        if format is None:
            format = self._guess_format(file_path)

        result = self.export(data, format, config)

        if result.success and result.output:
            with open(file_path, "w", encoding=config.encoding if config else "utf-8") as f:
                f.write(result.output)
            result.file_path = file_path
            logger.info(f"Exported {result.rows_exported} rows to {file_path}")

        return result

    def _guess_format(self, file_path: str) -> ExportFormat:
        """Guess format from file extension."""
        ext = file_path.split(".")[-1].lower()
        format_map = {
            "json": ExportFormat.JSON,
            "jsonl": ExportFormat.JSON_LINES,
            "csv": ExportFormat.CSV,
            "tsv": ExportFormat.TSV,
            "xml": ExportFormat.XML,
            "yaml": ExportFormat.YAML,
            "yml": ExportFormat.YAML,
            "html": ExportFormat.HTML,
            "md": ExportFormat.MARKDOWN,
        }
        return format_map.get(ext, ExportFormat.JSON)

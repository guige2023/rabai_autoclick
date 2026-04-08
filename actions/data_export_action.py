"""Data Export Action Module.

Provides data export capabilities to various
formats including JSON, CSV, XML, and more.
"""

from typing import Any, Dict, List, Optional, Union, TextIO
from dataclasses import dataclass, field
from enum import Enum
import json
import csv
import io
from datetime import datetime


class ExportFormat(Enum):
    """Supported export formats."""
    JSON = "json"
    JSONL = "jsonl"
    CSV = "csv"
    TSV = "tsv"
    XML = "xml"
    HTML = "html"
    Markdown = "markdown"
    YAML = "yaml"


@dataclass
class ExportConfig:
    """Configuration for data export."""
    format: ExportFormat
    include_header: bool = True
    indent: Optional[int] = 2
    encoding: str = "utf-8"
    field_names: Optional[List[str]] = None
    delimiter: str = ","
    quote_char: str = '"'
    date_format: str = "%Y-%m-%d %H:%M:%S"


@dataclass
class ExportResult:
    """Result of an export operation."""
    success: bool
    content: Optional[str] = None
    bytes_written: int = 0
    records_exported: int = 0
    error: Optional[str] = None


class JSONExporter:
    """Exports data to JSON format."""

    def __init__(self, config: Optional[ExportConfig] = None):
        self.config = config or ExportConfig(format=ExportFormat.JSON)

    def export(
        self,
        data: Union[List[Dict], Dict],
    ) -> ExportResult:
        """Export data to JSON."""
        try:
            if isinstance(data, dict):
                content = json.dumps(
                    data,
                    indent=self.config.indent,
                    default=str,
                    ensure_ascii=False,
                )
            else:
                content = json.dumps(
                    data,
                    indent=self.config.indent,
                    default=str,
                    ensure_ascii=False,
                )

            return ExportResult(
                success=True,
                content=content,
                bytes_written=len(content.encode(self.config.encoding)),
                records_exported=len(data) if isinstance(data, list) else 1,
            )

        except Exception as e:
            return ExportResult(
                success=False,
                error=str(e),
            )

    def export_to_file(
        self,
        data: Union[List[Dict], Dict],
        file_path: str,
    ) -> ExportResult:
        """Export data to JSON file."""
        result = self.export(data)
        if result.success:
            try:
                with open(file_path, 'w', encoding=self.config.encoding) as f:
                    f.write(result.content)
                    result.bytes_written = len(result.content.encode(self.config.encoding))
            except Exception as e:
                result.success = False
                result.error = str(e)
        return result


class JSONLExporter:
    """Exports data to JSON Lines format."""

    def __init__(self, config: Optional[ExportConfig] = None):
        self.config = config or ExportConfig(format=ExportFormat.JSONL)

    def export(
        self,
        data: List[Dict],
    ) -> ExportResult:
        """Export data to JSONL."""
        try:
            lines = []
            for record in data:
                line = json.dumps(record, default=str, ensure_ascii=False)
                lines.append(line)

            content = "\n".join(lines)
            return ExportResult(
                success=True,
                content=content,
                bytes_written=len(content.encode(self.config.encoding)),
                records_exported=len(data),
            )

        except Exception as e:
            return ExportResult(
                success=False,
                error=str(e),
            )


class CSVExporter:
    """Exports data to CSV format."""

    def __init__(self, config: Optional[ExportConfig] = None):
        self.config = config or ExportConfig(format=ExportFormat.CSV)

    def export(
        self,
        data: List[Dict],
    ) -> ExportResult:
        """Export data to CSV."""
        try:
            if not data:
                return ExportResult(
                    success=True,
                    content="",
                    records_exported=0,
                )

            field_names = self.config.field_names or list(data[0].keys())

            output = io.StringIO()
            writer = csv.DictWriter(
                output,
                fieldnames=field_names,
                delimiter=self.config.delimiter,
                quotechar=self.config.quote_char,
                lineterminator='\n',
            )

            if self.config.include_header:
                writer.writeheader()

            for record in data:
                row = {k: record.get(k, "") for k in field_names}
                writer.writerow(row)

            content = output.getvalue()
            return ExportResult(
                success=True,
                content=content,
                bytes_written=len(content.encode(self.config.encoding)),
                records_exported=len(data),
            )

        except Exception as e:
            return ExportResult(
                success=False,
                error=str(e),
            )

    def export_to_file(
        self,
        data: List[Dict],
        file_path: str,
    ) -> ExportResult:
        """Export data to CSV file."""
        result = self.export(data)
        if result.success:
            try:
                with open(file_path, 'w', encoding=self.config.encoding, newline='') as f:
                    f.write(result.content)
            except Exception as e:
                result.success = False
                result.error = str(e)
        return result


class HTMLExporter:
    """Exports data to HTML format."""

    def __init__(self, config: Optional[ExportConfig] = None):
        self.config = config or ExportConfig(format=ExportFormat.HTML)

    def export(
        self,
        data: List[Dict],
        title: str = "Data Export",
    ) -> ExportResult:
        """Export data to HTML table."""
        try:
            if not data:
                content = self._empty_html(title)
            else:
                field_names = self.config.field_names or list(data[0].keys())

                headers = "".join(f"<th>{h}</th>" for h in field_names)
                rows = []

                for record in data:
                    cells = "".join(
                        f"<td>{self._escape_html(str(record.get(h, '')))}</td>"
                        for h in field_names
                    )
                    rows.append(f"<tr>{cells}</tr>")

                content = self._html_template(
                    title,
                    headers,
                    "".join(rows),
                )

            return ExportResult(
                success=True,
                content=content,
                bytes_written=len(content.encode(self.config.encoding)),
                records_exported=len(data),
            )

        except Exception as e:
            return ExportResult(
                success=False,
                error=str(e),
            )

    def _html_template(
        self,
        title: str,
        headers: str,
        rows: str,
    ) -> str:
        """Generate HTML template."""
        return f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="{self.config.encoding}">
    <title>{self._escape_html(title)}</title>
    <style>
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #4CAF50; color: white; }}
        tr:nth-child(even) {{ background-color: #f2f2f2; }}
    </style>
</head>
<body>
    <h1>{self._escape_html(title)}</h1>
    <p>Exported: {datetime.now().strftime(self.config.date_format)}</p>
    <table>
        <thead><tr>{headers}</tr></thead>
        <tbody>{rows}</tbody>
    </table>
</body>
</html>"""

    def _empty_html(self, title: str) -> str:
        """Generate empty HTML."""
        return f"""<!DOCTYPE html>
<html>
<head>
    <title>{self._escape_html(title)}</title>
</head>
<body>
    <h1>{self._escape_html(title)}</h1>
    <p>No data to export.</p>
</body>
</html>"""

    def _escape_html(self, text: str) -> str:
        """Escape HTML special characters."""
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
            .replace("'", "&#39;")
        )


class MarkdownExporter:
    """Exports data to Markdown format."""

    def __init__(self, config: Optional[ExportConfig] = None):
        self.config = config or ExportConfig(format=ExportFormat.Markdown)

    def export(
        self,
        data: List[Dict],
        title: str = "Data Export",
    ) -> ExportResult:
        """Export data to Markdown table."""
        try:
            if not data:
                content = f"# {title}\n\n_No data to export._"
            else:
                field_names = self.config.field_names or list(data[0].keys())

                header_row = "| " + " | ".join(field_names) + " |"
                separator_row = "| " + " | ".join(["---"] * len(field_names)) + " |"

                rows = []
                for record in data:
                    cells = " | ".join(str(record.get(h, "")) for h in field_names)
                    rows.append(f"| {cells} |")

                content = f"# {title}\n\n"
                content += f"_Exported: {datetime.now().strftime(self.config.date_format)}_\n\n"
                content += header_row + "\n"
                content += separator_row + "\n"
                content += "\n".join(rows)

            return ExportResult(
                success=True,
                content=content,
                bytes_written=len(content.encode(self.config.encoding)),
                records_exported=len(data),
            )

        except Exception as e:
            return ExportResult(
                success=False,
                error=str(e),
            )


class XMLExporter:
    """Exports data to XML format."""

    def __init__(self, config: Optional[ExportConfig] = None):
        self.config = config or ExportConfig(format=ExportFormat.XML)
        self._escape_table = str.maketrans({
            "&": "&amp;",
            "<": "&lt;",
            ">": "&gt;",
            '"': "&quot;",
            "'": "&apos;",
        })

    def export(
        self,
        data: Union[List[Dict], Dict],
        root_name: str = "data",
        item_name: str = "item",
    ) -> ExportResult:
        """Export data to XML."""
        try:
            if isinstance(data, dict):
                xml_parts = ["<items>"]
                for key, value in data.items():
                    xml_parts.append(f"<{key}>{self._escape_xml(str(value))}</{key}>")
                xml_parts.append("</items>")
            else:
                xml_parts = [f"<{root_name}>"]
                for record in data:
                    xml_parts.append(f"  <{item_name}>")
                    for key, value in record.items():
                        xml_parts.append(
                            f"    <{key}>{self._escape_xml(str(value))}</{key}>"
                        )
                    xml_parts.append(f"  </{item_name}>")
                xml_parts.append(f"</{root_name}>")

            content = "\n".join(xml_parts)
            return ExportResult(
                success=True,
                content=content,
                bytes_written=len(content.encode(self.config.encoding)),
                records_exported=len(data) if isinstance(data, list) else 1,
            )

        except Exception as e:
            return ExportResult(
                success=False,
                error=str(e),
            )

    def _escape_xml(self, text: str) -> str:
        """Escape XML special characters."""
        return text.translate(self._escape_table)


class DataExporter:
    """Main data export orchestrator."""

    def __init__(self):
        self._exporters: Dict[ExportFormat, Any] = {
            ExportFormat.JSON: JSONExporter(),
            ExportFormat.JSONL: JSONLExporter(),
            ExportFormat.CSV: CSVExporter(),
            ExportFormat.HTML: HTMLExporter(),
            ExportFormat.Markdown: MarkdownExporter(),
            ExportFormat.XML: XMLExporter(),
        }

    def export(
        self,
        data: Any,
        format: Union[ExportFormat, str],
        config: Optional[ExportConfig] = None,
    ) -> ExportResult:
        """Export data to specified format."""
        if isinstance(format, str):
            format = ExportFormat(format)

        if format == ExportFormat.YAML:
            return ExportResult(
                success=False,
                error="YAML export not implemented",
            )

        exporter = self._exporters.get(format)
        if not exporter:
            return ExportResult(
                success=False,
                error=f"Unsupported format: {format}",
            )

        return exporter.export(data)


class DataExportAction:
    """High-level data export action."""

    def __init__(self, exporter: Optional[DataExporter] = None):
        self.exporter = exporter or DataExporter()

    def export(
        self,
        data: Any,
        format: str,
        **kwargs,
    ) -> ExportResult:
        """Export data to format."""
        config = ExportConfig(format=ExportFormat(format), **kwargs)
        return self.exporter.export(data, config.format, config)

    def export_to_json(
        self,
        data: Any,
        indent: int = 2,
    ) -> str:
        """Export to JSON string."""
        result = self.exporter.export(data, ExportFormat.JSON)
        return result.content or ""

    def export_to_csv(
        self,
        data: List[Dict],
        field_names: Optional[List[str]] = None,
    ) -> str:
        """Export to CSV string."""
        config = ExportConfig(
            format=ExportFormat.CSV,
            field_names=field_names,
        )
        result = self.exporter.export(data, ExportFormat.CSV, config)
        return result.content or ""

    def export_to_html(
        self,
        data: List[Dict],
        title: str = "Data Export",
    ) -> str:
        """Export to HTML string."""
        result = self.exporter.export(data, ExportFormat.HTML)
        return result.content or ""


# Module exports
__all__ = [
    "DataExportAction",
    "DataExporter",
    "JSONExporter",
    "JSONLExporter",
    "CSVExporter",
    "HTMLExporter",
    "MarkdownExporter",
    "XMLExporter",
    "ExportFormat",
    "ExportConfig",
    "ExportResult",
]

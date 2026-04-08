"""
Data Formatter Action Module.

Format data for display and export,
supports table formatting, JSON, CSV, and custom formats.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass
from enum import Enum
import logging
import json
import csv
import io

logger = logging.getLogger(__name__)


class FormatType(Enum):
    """Output format types."""
    TABLE = "table"
    JSON = "json"
    CSV = "csv"
    MARKDOWN = "markdown"
    HTML = "html"


@dataclass
class FormatConfig:
    """Formatting configuration."""
    indent: int = 2
    include_headers: bool = True
    null_value: str = "null"
    datetime_format: str = "%Y-%m-%d %H:%M:%S"


class DataFormatterAction:
    """
    Data formatting for display and export.

    Formats structured data as tables, JSON,
    CSV, Markdown, or HTML.

    Example:
        formatter = DataFormatterAction()
        table = formatter.format_table(data, columns=["name", "age"])
        json_str = formatter.format_json(data)
    """

    def __init__(
        self,
        default_format: FormatType = FormatType.TABLE,
        config: Optional[FormatConfig] = None,
    ) -> None:
        self.default_format = default_format
        self.config = config or FormatConfig()

    def format_table(
        self,
        data: list[dict],
        columns: Optional[list[str]] = None,
        max_width: int = 80,
    ) -> str:
        """Format data as ASCII table."""
        if not data:
            return ""

        columns = columns or list(data[0].keys())

        col_widths = {col: len(col) for col in columns}
        for row in data:
            for col in columns:
                value = str(row.get(col, ""))
                col_widths[col] = max(col_widths[col], len(value))

        for col in columns:
            col_widths[col] = min(col_widths[col], max_width // max(len(columns), 1))

        separator = "+" + "+".join("-" * (col_widths[c] + 2) for c in columns) + "+"

        lines = [separator]

        header = "|" + "|".join(
            f" {col[:col_widths[col]]:<{col_widths[col]}} "
            for col in columns
        ) + "|"
        lines.append(header)
        lines.append(separator.replace("-", "="))

        for row in data:
            row_str = "|" + "|".join(
                f" {str(row.get(col, ''))[:col_widths[col]]:<{col_widths[col]}} "
                for col in columns
            ) + "|"
            lines.append(row_str)

        lines.append(separator)

        return "\n".join(lines)

    def format_json(
        self,
        data: Any,
        pretty: bool = True,
    ) -> str:
        """Format data as JSON."""
        if pretty:
            return json.dumps(data, indent=self.config.indent, default=str)
        return json.dumps(data, default=str)

    def format_csv(
        self,
        data: list[dict],
        columns: Optional[list[str]] = None,
        delimiter: str = ",",
    ) -> str:
        """Format data as CSV."""
        if not data:
            return ""

        columns = columns or list(data[0].keys())

        output = io.StringIO()
        writer = csv.DictWriter(
            output,
            fieldnames=columns,
            delimiter=delimiter,
            quoting=csv.QUOTE_MINIMAL,
        )

        if self.config.include_headers:
            writer.writeheader()

        for row in data:
            writer.writerow({k: row.get(k, "") for k in columns})

        return output.getvalue()

    def format_markdown(
        self,
        data: list[dict],
        columns: Optional[list[str]] = None,
    ) -> str:
        """Format data as Markdown table."""
        if not data:
            return ""

        columns = columns or list(data[0].keys())

        lines = []

        header = "| " + " | ".join(columns) + " |"
        separator = "| " + " | ".join("---" for _ in columns) + " |"

        lines.append(header)
        lines.append(separator)

        for row in data:
            row_str = "| " + " | ".join(
                str(row.get(col, "")) for col in columns
            ) + " |"
            lines.append(row_str)

        return "\n".join(lines)

    def format_html(
        self,
        data: list[dict],
        columns: Optional[list[str]] = None,
        table_class: str = "data-table",
    ) -> str:
        """Format data as HTML table."""
        if not data:
            return "<table></table>"

        columns = columns or list(data[0].keys())

        lines = [
            f'<table class="{table_class}">',
            "<thead><tr>",
        ]

        for col in columns:
            lines.append(f"<th>{col}</th>")
        lines.append("</tr></thead>")
        lines.append("<tbody>")

        for row in data:
            lines.append("<tr>")
            for col in columns:
                lines.append(f"<td>{row.get(col, '')}</td>")
            lines.append("</tr>")

        lines.append("</tbody>")
        lines.append("</table>")

        return "\n".join(lines)

    def format_to(
        self,
        data: Any,
        format_type: Optional[FormatType] = None,
        **kwargs: Any,
    ) -> str:
        """Format data to specified format."""
        fmt = format_type or self.default_format

        if fmt == FormatType.TABLE:
            return self.format_table(data, **kwargs)
        elif fmt == FormatType.JSON:
            return self.format_json(data, **kwargs)
        elif fmt == FormatType.CSV:
            return self.format_csv(data, **kwargs)
        elif fmt == FormatType.MARKDOWN:
            return self.format_markdown(data, **kwargs)
        elif fmt == FormatType.HTML:
            return self.format_html(data, **kwargs)
        else:
            return str(data)

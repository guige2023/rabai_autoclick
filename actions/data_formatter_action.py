"""
Data Formatter Action Module.

Formats data for output in various formats including JSON, CSV,
 XML, and custom templates with field selection and ordering.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import csv
import io
import logging

logger = logging.getLogger(__name__)


class OutputFormat(Enum):
    """Output format type."""
    JSON = "json"
    CSV = "csv"
    XML = "xml"
    TSV = "tsv"
    YAML = "yaml"
    HTML = "html"


@dataclass
class FormatConfig:
    """Configuration for data formatting."""
    format: OutputFormat = OutputFormat.JSON
    pretty_print: bool = True
    field_order: Optional[list[str]] = None
    date_format: str = "%Y-%m-%d"
    datetime_format: str = "%Y-%m-%d %H:%M:%S"
    null_value: str = ""
    indent: int = 2


class DataFormatterAction:
    """
    Data formatting engine for multiple output formats.

    Formats structured data into JSON, CSV, XML, and other formats
    with configurable field ordering and value handling.

    Example:
        formatter = DataFormatterAction(config=FormatConfig(format=OutputFormat.CSV))
        formatter.set_field_order(["id", "name", "email"])
        output = formatter.format(records)
    """

    def __init__(
        self,
        config: Optional[FormatConfig] = None,
    ) -> None:
        self.config = config or FormatConfig()

    def set_field_order(
        self,
        fields: list[str],
    ) -> "DataFormatterAction":
        """Set the field ordering for output."""
        self.config.field_order = fields
        return self

    def format(
        self,
        data: Any,
    ) -> str:
        """Format data according to configuration."""
        if self.config.format == OutputFormat.JSON:
            return self._format_json(data)
        elif self.config.format == OutputFormat.CSV:
            return self._format_csv(data)
        elif self.config.format == OutputFormat.TSV:
            return self._format_tsv(data)
        elif self.config.format == OutputFormat.XML:
            return self._format_xml(data)
        elif self.config.format == OutputFormat.YAML:
            return self._format_yaml(data)
        elif self.config.format == OutputFormat.HTML:
            return self._format_html(data)
        return str(data)

    def _format_json(self, data: Any) -> str:
        """Format data as JSON."""
        import json

        if self.config.pretty_print:
            return json.dumps(
                data,
                indent=self.config.indent,
                ensure_ascii=False,
                default=str,
            )
        return json.dumps(data, ensure_ascii=False, default=str)

    def _format_csv(self, data: Any) -> str:
        """Format data as CSV."""
        if isinstance(data, list) and data:
            if isinstance(data[0], dict):
                fieldnames = self.config.field_order or list(data[0].keys())
                output = io.StringIO()
                writer = csv.DictWriter(
                    output,
                    fieldnames=fieldnames,
                    restval=self.config.null_value,
                )
                writer.writeheader()

                for row in data:
                    formatted_row = self._format_row(row, fieldnames)
                    writer.writerow(formatted_row)

                return output.getvalue()

        elif isinstance(data, dict):
            fieldnames = self.config.field_order or list(data.keys())
            output = io.StringIO()
            writer = csv.DictWriter(
                output,
                fieldnames=fieldnames,
                restval=self.config.null_value,
            )
            writer.writeheader()
            formatted_row = self._format_row(data, fieldnames)
            writer.writerow(formatted_row)
            return output.getvalue()

        return str(data)

    def _format_tsv(self, data: Any) -> str:
        """Format data as TSV."""
        if isinstance(data, list) and data:
            if isinstance(data[0], dict):
                fieldnames = self.config.field_order or list(data[0].keys())
                output = io.StringIO()
                writer = csv.DictWriter(
                    output,
                    fieldnames=fieldnames,
                    delimiter="\t",
                    restval=self.config.null_value,
                )
                writer.writeheader()

                for row in data:
                    formatted_row = self._format_row(row, fieldnames)
                    writer.writerow(formatted_row)

                return output.getvalue()

        return str(data)

    def _format_xml(self, data: Any) -> str:
        """Format data as XML."""
        import xml.etree.ElementTree as ET

        def dict_to_xml(parent: str, data: dict) -> ET.Element:
            root = ET.Element(parent)

            for key, value in data.items():
                child = ET.SubElement(root, str(key))

                if isinstance(value, dict):
                    child.extend(dict_to_xml(key, value))
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            child.extend(dict_to_xml("item", item))
                        else:
                            item_elem = ET.SubElement(child, "item")
                            item_elem.text = str(item)
                else:
                    child.text = str(value) if value is not None else ""

            return root

        if isinstance(data, dict):
            root = dict_to_xml("root", data)
            return ET.tostring(root, encoding="unicode")

        return str(data)

    def _format_yaml(self, data: Any) -> str:
        """Format data as YAML."""
        try:
            import yaml
            return yaml.dump(data, allow_unicode=True, default_flow_style=False)
        except ImportError:
            return str(data)

    def _format_html(self, data: Any) -> str:
        """Format data as HTML table."""
        if isinstance(data, list) and data and isinstance(data[0], dict):
            fieldnames = self.config.field_order or list(data[0].keys())

            html = ['<table border="1">']
            html.append("  <thead><tr>")
            for field_name in fieldnames:
                html.append(f"    <th>{field_name}</th>")
            html.append("  </tr></thead>")
            html.append("  <tbody>")

            for row in data:
                html.append("  <tr>")
                for field_name in fieldnames:
                    value = row.get(field_name, self.config.null_value)
                    html.append(f"    <td>{value}</td>")
                html.append("  </tr>")

            html.append("  </tbody>")
            html.append("</table>")
            return "\n".join(html)

        return f"<pre>{data}</pre>"

    def _format_row(
        self,
        row: dict[str, Any],
        fieldnames: list[str],
    ) -> dict[str, str]:
        """Format a single row according to configuration."""
        formatted: dict[str, str] = {}

        for field_name in fieldnames:
            value = row.get(field_name)

            if value is None:
                formatted[field_name] = self.config.null_value
            elif isinstance(value, (int, float)):
                formatted[field_name] = str(value)
            elif isinstance(value, str):
                formatted[field_name] = value
            else:
                formatted[field_name] = str(value)

        return formatted

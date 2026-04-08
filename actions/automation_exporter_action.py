"""
Automation Exporter Action Module.

Exports automation results and reports in multiple formats
 including JSON, CSV, HTML, PDF, and custom templates.
"""

from __future__ import annotations

import os
import json
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class ExportFormat(Enum):
    """Export format types."""
    JSON = "json"
    CSV = "csv"
    HTML = "html"
    MARKDOWN = "markdown"
    XML = "xml"
    TEXT = "text"


@dataclass
class ExportConfig:
    """Configuration for export operations."""
    format: ExportFormat = ExportFormat.JSON
    pretty_print: bool = True
    include_metadata: bool = True
    date_format: str = "%Y-%m-%d %H:%M:%S"


@dataclass
class ExportResult:
    """Result of an export operation."""
    success: bool
    filepath: Optional[str] = None
    content: Optional[str] = None
    error: Optional[str] = None


class AutomationExporterAction:
    """
    Export automation results to multiple formats.

    Exports workflow results, reports, and data in various
    formats with customizable templates and metadata.

    Example:
        exporter = AutomationExporterAction()
        exporter.export(
            data=workflow_results,
            format=ExportFormat.HTML,
            template="report_template.html",
            output_path="/tmp/report.html",
        )
    """

    def __init__(
        self,
        config: Optional[ExportConfig] = None,
    ) -> None:
        self.config = config or ExportConfig()
        self._templates: dict[str, str] = {}

    def register_template(
        self,
        name: str,
        template: str,
    ) -> "AutomationExporterAction":
        """Register a custom export template."""
        self._templates[name] = template
        return self

    def export(
        self,
        data: Any,
        format: Optional[ExportFormat] = None,
        output_path: Optional[str] = None,
        template_name: Optional[str] = None,
    ) -> ExportResult:
        """Export data to specified format."""
        fmt = format or self.config.format

        try:
            if fmt == ExportFormat.JSON:
                content = self._export_json(data)
            elif fmt == ExportFormat.CSV:
                content = self._export_csv(data)
            elif fmt == ExportFormat.HTML:
                content = self._export_html(data, template_name)
            elif fmt == ExportFormat.MARKDOWN:
                content = self._export_markdown(data)
            elif fmt == ExportFormat.XML:
                content = self._export_xml(data)
            else:
                content = str(data)

            if output_path:
                os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
                with open(output_path, "w") as f:
                    f.write(content)

                return ExportResult(success=True, filepath=output_path)

            return ExportResult(success=True, content=content)

        except Exception as e:
            logger.error(f"Export failed: {e}")
            return ExportResult(success=False, error=str(e))

    def _export_json(self, data: Any) -> str:
        """Export data as JSON."""
        if self.config.pretty_print:
            return json.dumps(data, indent=2, ensure_ascii=False, default=str)
        return json.dumps(data, ensure_ascii=False, default=str)

    def _export_csv(self, data: Any) -> str:
        """Export data as CSV."""
        import csv
        import io

        if isinstance(data, list) and data and isinstance(data[0], dict):
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
            return output.getvalue()

        return str(data)

    def _export_html(
        self,
        data: Any,
        template_name: Optional[str] = None,
    ) -> str:
        """Export data as HTML."""
        if template_name and template_name in self._templates:
            return self._apply_template(template_name, data)

        html = ['<!DOCTYPE html>', '<html>', '<head>',
                '<meta charset="utf-8">', '<title>Automation Report</title>',
                '<style>', 'body { font-family: Arial, sans-serif; margin: 20px; }',
                'table { border-collapse: collapse; width: 100%; }',
                'th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }',
                'th { background-color: #4CAF50; color: white; }', '</style>',
                '</head>', '<body>', '<h1>Automation Report</h1>']

        if isinstance(data, list) and data and isinstance(data[0], dict):
            headers = list(data[0].keys())
            html.append('<table><thead><tr>')
            for h in headers:
                html.append(f'<th>{h}</th>')
            html.append('</tr></thead><tbody>')

            for row in data:
                html.append('<tr>')
                for h in headers:
                    html.append(f'<td>{row.get(h, "")}</td>')
                html.append('</tr>')
            html.append('</tbody></table>')

        elif isinstance(data, dict):
            html.append('<table><tbody>')
            for key, value in data.items():
                html.append(f'<tr><th>{key}</th><td>{value}</td></tr>')
            html.append('</tbody></table>')

        else:
            html.append(f'<pre>{data}</pre>')

        html.extend(['</body>', '</html>'])
        return '\n'.join(html)

    def _export_markdown(self, data: Any) -> str:
        """Export data as Markdown."""
        lines = ['# Automation Report', '']

        if isinstance(data, list) and data and isinstance(data[0], dict):
            headers = list(data[0].keys())
            lines.append('| ' + ' | '.join(headers) + ' |')
            lines.append('| ' + ' | '.join(['---'] * len(headers)) + ' |')

            for row in data:
                values = [str(row.get(h, '')) for h in headers]
                lines.append('| ' + ' | '.join(values) + ' |')

        elif isinstance(data, dict):
            for key, value in data.items():
                lines.append(f'**{key}**: {value}')

        else:
            lines.append(str(data))

        return '\n'.join(lines)

    def _export_xml(self, data: Any) -> str:
        """Export data as XML."""
        import xml.etree.ElementTree as ET

        def dict_to_xml(tag: str, d: dict) -> ET.Element:
            elem = ET.Element(tag)
            for key, val in d.items():
                child = ET.SubElement(elem, str(key))
                if isinstance(val, dict):
                    child.extend(dict_to_xml(key, val))
                elif isinstance(val, list):
                    for item in val:
                        if isinstance(item, dict):
                            child.extend(dict_to_xml('item', item))
                        else:
                            item_elem = ET.SubElement(child, 'item')
                            item_elem.text = str(item)
                else:
                    child.text = str(val) if val is not None else ''
            return elem

        if isinstance(data, dict):
            root = dict_to_xml('root', data)
            return ET.tostring(root, encoding='unicode')

        return f'<root>{data}</root>'

    def _apply_template(
        self,
        template_name: str,
        data: Any,
    ) -> str:
        """Apply a custom template to data."""
        template = self._templates.get(template_name, "")

        if not template:
            return str(data)

        try:
            return template.format(data=json.dumps(data, indent=2))
        except Exception:
            return str(data)

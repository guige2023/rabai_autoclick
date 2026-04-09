"""Format and serialization utilities.

Provides data formatting, conversion, and
serialization for various output formats.
"""

import base64
import json
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Union
from enum import Enum


class OutputFormat(Enum):
    """Supported output formats."""
    JSON = "json"
    XML = "xml"
    CSV = "csv"
    TSV = "tsv"
    HTML = "html"
    MARKDOWN = "markdown"
    YAML = "yaml"


@dataclass
class FormatConfig:
    """Configuration for formatting output."""
    indent: int = 2
    ensure_ascii: bool = False
    date_format: str = "%Y-%m-%d %H:%M:%S"
    null_value: str = ""


def to_json(
    data: Any,
    config: Optional[FormatConfig] = None,
) -> str:
    """Format data as JSON.

    Example:
        json_str = to_json({"key": "value"})
    """
    if config is None:
        config = FormatConfig()

    return json.dumps(
        data,
        indent=config.indent,
        ensure_ascii=config.ensure_ascii,
        default=_json_default,
    )


def _json_default(obj: Any) -> Any:
    """JSON serializer for special types."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, bytes):
        return base64.b64encode(obj).decode()
    if isinstance(obj, set):
        return list(obj)
    if hasattr(obj, "__dict__"):
        return obj.__dict__
    return str(obj)


def from_json(json_str: str) -> Any:
    """Parse JSON string to Python object."""
    return json.loads(json_str)


def to_xml(
    data: Union[Dict, List],
    root_tag: str = "root",
    item_tag: str = "item",
) -> str:
    """Format data as XML.

    Example:
        xml_str = to_xml({"name": "test"}, root_tag="data")
    """
    if isinstance(data, dict):
        root = ET.Element(root_tag)
        _dict_to_xml(data, root)
    else:
        root = ET.Element(root_tag)
        for item in data:
            _dict_to_xml({item_tag: item}, root)

    return ET.tostring(root, encoding="unicode")


def _dict_to_xml(data: Any, parent: ET.Element) -> None:
    """Convert dict/list to XML elements."""
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                child = ET.SubElement(parent, str(key))
                _dict_to_xml(value, child)
            else:
                child = ET.SubElement(parent, str(key))
                child.text = str(value) if value is not None else ""
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                _dict_to_xml(item, parent)
            else:
                child = ET.SubElement(parent, "item")
                child.text = str(item)
    else:
        parent.text = str(data) if data is not None else ""


def to_csv(
    data: List[Dict[str, Any]],
    headers: Optional[List[str]] = None,
    delimiter: str = ",",
) -> str:
    """Format data as CSV.

    Example:
        csv_str = to_csv([{"name": "A", "age": 30}, {"name": "B", "age": 25}])
    """
    if not data:
        return ""

    if headers is None:
        headers = list(data[0].keys())

    lines = [delimiter.join(headers)]

    for row in data:
        values = [str(row.get(h, "")) for h in headers]
        lines.append(delimiter.join(values))

    return "\n".join(lines)


def to_tsv(data: List[Dict[str, Any]], headers: Optional[List[str]] = None) -> str:
    """Format data as TSV."""
    return to_csv(data, headers, delimiter="\t")


def to_markdown_table(
    data: List[Dict[str, Any]],
    headers: Optional[List[str]] = None,
) -> str:
    """Format data as Markdown table.

    Example:
        md_table = to_markdown_table([{"name": "A", "v": 1}])
    """
    if not data:
        return ""

    if headers is None:
        headers = list(data[0].keys())

    header_line = "| " + " | ".join(headers) + " |"
    separator = "|" + "|".join([" --- " for _ in headers]) + "|"

    rows = []
    for row in data:
        values = [str(row.get(h, "")) for h in headers]
        rows.append("| " + " | ".join(values) + " |")

    return "\n".join([header_line, separator] + rows)


def to_html_table(
    data: List[Dict[str, Any]],
    headers: Optional[List[str]] = None,
    table_class: str = "data-table",
) -> str:
    """Format data as HTML table."""
    if not data:
        return "<table></table>"

    if headers is None:
        headers = list(data[0].keys())

    html = f'<table class="{table_class}">\n'

    html += "  <thead><tr>"
    for h in headers:
        html += f"<th>{h}</th>"
    html += "</tr></thead>\n"

    html += "  <tbody>\n"
    for row in data:
        html += "    <tr>"
        for h in headers:
            html += f"<td>{row.get(h, '')}</td>"
        html += "</tr>\n"

    html += "  </tbody>\n</table>"
    return html


def format_bytes(num_bytes: int) -> str:
    """Format byte count as human readable string.

    Example:
        format_bytes(1024 * 1024)  # "1.00 MB"
    """
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} PB"


def format_duration(seconds: float) -> str:
    """Format duration as human readable string.

    Example:
        format_duration(3665)  # "1h 1m 5s"
    """
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)

    parts = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")

    return " ".join(parts)


def format_number(num: Union[int, float], precision: int = 2) -> str:
    """Format number with thousands separator.

    Example:
        format_number(1234567)  # "1,234,567.00"
    """
    if isinstance(num, int):
        return f"{num:,}"
    return f"{num:,.{precision}f}"


def format_percentage(value: float, total: float, precision: int = 1) -> str:
    """Format as percentage.

    Example:
        format_percentage(25, 100)  # "25.0%"
    """
    if total == 0:
        return "0%"
    pct = (value / total) * 100
    return f"{pct:.{precision}f}%"


def truncate_string(s: str, max_length: int, suffix: str = "...") -> str:
    """Truncate string with suffix.

    Example:
        truncate_string("hello world", 8)  # "hello..."
    """
    if len(s) <= max_length:
        return s
    return s[:max_length - len(suffix)] + suffix


def pretty_print(data: Any, format_type: OutputFormat = OutputFormat.JSON) -> str:
    """Format data for pretty printing.

    Args:
        data: Data to format.
        format_type: Output format type.

    Returns:
        Formatted string.
    """
    if format_type == OutputFormat.JSON:
        return to_json(data)
    elif format_type == OutputFormat.XML:
        return to_xml(data)
    elif format_type == OutputFormat.CSV:
        return to_csv(data)
    elif format_type == OutputFormat.MARKDOWN:
        if isinstance(data, list):
            return to_markdown_table(data)
        return to_json(data)
    elif format_type == OutputFormat.HTML:
        if isinstance(data, list):
            return to_html_table(data)
        return to_json(data)
    else:
        return str(data)


def dataclass_to_dict(obj: Any) -> Dict[str, Any]:
    """Convert dataclass instance to dict."""
    if hasattr(obj, "__dataclass_fields__"):
        return {k: getattr(obj, k) for k in obj.__dataclass_fields__}
    return asdict(obj)


def dict_to_dataclass(data: Dict[str, Any], cls: type) -> Any:
    """Create dataclass instance from dict."""
    return cls(**{k: data.get(k) for k in cls.__dataclass_fields__})

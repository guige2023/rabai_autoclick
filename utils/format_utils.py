"""
Formatting and serialization utilities.

Provides pretty printing, table formatting, number formatting,
unit conversion, and data structure serialization.
"""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from typing import Any, Sequence


def format_number(n: float, precision: int = 2, unit: str = "") -> str:
    """Format number with precision and optional unit."""
    formatted = f"{n:.{precision}f}"
    return f"{formatted} {unit}".strip() if unit else formatted


def format_bytes(size: int) -> str:
    """Format byte size in human-readable form."""
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(size)
    for unit in units:
        if abs(size) < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"


def format_duration(seconds: float) -> str:
    """Format duration in human-readable form."""
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.0f}s"
    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    return f"{hours}h {minutes}m"


def format_percentage(value: float, total: float, precision: int = 1) -> str:
    """Format as percentage."""
    if total == 0:
        return "0.0%"
    return f"{value / total * 100:.{precision}f}%"


def format_table(
    headers: list[str],
    rows: list[list[Any]],
    align: str = "left",
) -> str:
    """
    Format data as ASCII table.

    Args:
        headers: Column headers
        rows: Data rows
        align: 'left', 'right', or 'center'

    Returns:
        Formatted table string.
    """
    if not rows:
        return ""

    # Convert all to strings
    str_headers = [str(h) for h in headers]
    str_rows = [[str(cell) for cell in row] for row in rows]

    # Compute column widths
    col_widths = [len(h) for h in str_headers]
    for row in str_rows:
        for i, cell in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(cell))

    def format_cell(cell: str, width: int) -> str:
        if align == "right":
            return cell.rjust(width)
        elif align == "center":
            return cell.center(width)
        return cell.ljust(width)

    # Build table
    lines: list[str] = []
    sep = "+" + "+".join("-" * w for w in col_widths) + "+"
    lines.append(sep)
    lines.append("|" + "|".join(format_cell(h, w) for h, w in zip(str_headers, col_widths)) + "|")
    lines.append(sep)
    for row in str_rows:
        cells = [format_cell(row[i] if i < len(row) else "", w) for i, w in enumerate(col_widths)]
        lines.append("|" + "|".join(cells) + "|")
    lines.append(sep)
    return "\n".join(lines)


def format_json(data: Any, indent: int = 2) -> str:
    """Pretty print JSON."""
    return json.dumps(data, indent=indent, ensure_ascii=False, sort_keys=False)


def parse_json(text: str) -> Any:
    """Parse JSON text."""
    return json.loads(text)


def format_xml(element: ET.Element, indent: str = "  ") -> str:
    """Format XML element as string."""
    return ET.tostring(element, encoding="unicode")


def indent_text(text: str, spaces: int = 4) -> str:
    """Indent text by spaces."""
    pad = " " * spaces
    return "\n".join(pad + line for line in text.splitlines())


def truncate(text: str, max_length: int, suffix: str = "...") -> str:
    """Truncate text to max length."""
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def word_wrap(text: str, width: int = 80) -> list[str]:
    """Wrap text to specified width."""
    words = text.split()
    lines: list[str] = []
    current_line: list[str] = []
    current_len = 0
    for word in words:
        if current_len + len(word) + len(current_line) <= width:
            current_line.append(word)
            current_len += len(word)
        else:
            if current_line:
                lines.append(" ".join(current_line))
            current_line = [word]
            current_len = len(word)
    if current_line:
        lines.append(" ".join(current_line))
    return lines


def pluralize(word: str, count: int, plural: str | None = None) -> str:
    """Add plural suffix to word based on count."""
    if plural:
        return f"{count} {plural if count != 1 else word}"
    if count == 1:
        return f"{count} {word}"
    if word.endswith("y"):
        return f"{count} {word[:-1]}ies"
    if word.endswith(("s", "x", "z", "ch", "sh")):
        return f"{count} {word}es"
    return f"{count} {word}s"


def format_list(items: list[str], conjunction: str = "and") -> str:
    """Format list with proper Oxford comma."""
    n = len(items)
    if n == 0:
        return ""
    if n == 1:
        return items[0]
    if n == 2:
        return f"{items[0]} {conjunction} {items[1]}"
    return ", ".join(items[:-1]) + f", {conjunction} {items[-1]}"


def format_dict(data: dict, indent: int = 2) -> str:
    """Format dictionary as pretty string."""
    lines: list[str] = []
    for key, value in data.items():
        if isinstance(value, dict):
            lines.append(f"{key}:")
            lines.append(format_dict(value, indent))
        elif isinstance(value, list):
            lines.append(f"{key}: {value}")
        else:
            lines.append(f"{key}: {value}")
    return "\n".join(lines)


def format_progress_bar(
    progress: float,
    width: int = 40,
    filled: str = "#",
    empty: str = "-",
) -> str:
    """Format progress bar."""
    progress = max(0.0, min(1.0, progress))
    filled_count = int(width * progress)
    empty_count = width - filled_count
    return f"[{filled * filled_count}{empty * empty_count}] {progress * 100:.1f}%"


def format_tree(
    items: dict[str, Any],
    indent: int = 0,
    prefix: str = "",
) -> list[str]:
    """Format nested dict as tree."""
    lines: list[str] = []
    keys = sorted(items.keys())
    for i, key in enumerate(keys):
        is_last = i == len(keys) - 1
        current_prefix = prefix
        if indent == 0:
            lines.append(str(key))
        else:
            connector = "`--" if is_last else "|--"
            lines.append(f"{current_prefix}{connector} {key}")
        value = items[key]
        if isinstance(value, dict):
            extension = "`   " if is_last else "|   "
            lines.extend(format_tree(value, indent + 1, prefix + extension))
    return lines


def to_csv_row(values: list[Any], delimiter: str = ",") -> str:
    """Format values as CSV row."""
    def escape(v: Any) -> str:
        s = str(v)
        if delimiter in s or '"' in s or '\n' in s:
            return f'"{s.replace("\"", "\"\"")}"'
        return s
    return delimiter.join(escape(v) for v in values)


def from_csv_row(row: str, delimiter: str = ",") -> list[str]:
    """Parse CSV row into values."""
    values: list[str] = []
    current = ""
    in_quotes = False
    i = 0
    while i < len(row):
        ch = row[i]
        if ch == '"':
            if in_quotes and i + 1 < len(row) and row[i + 1] == '"':
                current += '"'
                i += 1
            else:
                in_quotes = not in_quotes
        elif ch == delimiter and not in_quotes:
            values.append(current)
            current = ""
        else:
            current += ch
        i += 1
    values.append(current)
    return values


def unit_convert(value: float, from_unit: str, to_unit: str) -> float:
    """
    Simple unit conversion.

    Supports: length (m, km, mi, ft, in, cm, mm), weight (kg, g, lb, oz),
    temperature (C, F, K).
    """
    conversions: dict[str, dict[str, float]] = {
        # to meters
        "m": {"km": 1000, "mi": 1609.344, "ft": 0.3048, "in": 0.0254, "cm": 0.01, "mm": 0.001, "m": 1},
        # to kg
        "kg": {"g": 0.001, "lb": 0.453592, "oz": 0.0283495, "kg": 1},
    }

    # Temperature special cases
    if from_unit in "CFK" and to_unit in "CFK":
        c = 0.0
        if from_unit == "C":
            c = value
        elif from_unit == "F":
            c = (value - 32) * 5 / 9
        else:  # Kelvin
            c = value - 273.15
        if to_unit == "C":
            return c
        elif to_unit == "F":
            return c * 9 / 5 + 32
        else:  # Kelvin
            return c + 273.15

    # Find category
    for category, table in conversions.items():
        if from_unit in table and to_unit in table:
            base = value * table[from_unit]
            return base / table[to_unit]

    raise ValueError(f"Unknown unit conversion: {from_unit} to {to_unit}")

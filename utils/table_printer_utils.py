"""
Table formatting and printing utilities.

Provides functions for creating and printing formatted tables
with support for various styles, alignment, and borders.

Example:
    >>> from utils.table_printer_utils import Table, print_table
    >>> table = Table(headers=["Name", "Age"])
    >>> table.add_row(["Alice", 30])
    >>> print(table)
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Any, List, Optional, Union


class TableStyle:
    """Table styling configuration."""

    def __init__(
        self,
        border: str = "|",
        corner: str = "+",
        horizontal: str = "-",
        vertical: str = "|",
        padding: int = 1,
    ) -> None:
        """Initialize table style."""
        self.border = border
        self.corner = corner
        self.horizontal = horizontal
        self.vertical = vertical
        self.padding = padding


STYLES = {
    "simple": TableStyle(border="-", corner="+", horizontal="-", vertical=" ", padding=1),
    "rounded": TableStyle(border="─", corner="╭", corner_br="╮", corner_bl="╰", corner_br_end="╯", padding=1),
    "grid": TableStyle(border="|", corner="+", horizontal="-", vertical="|", padding=1),
    "markdown": TableStyle(border="|", corner="", horizontal="-", vertical="|", padding=1),
    "minimal": TableStyle(border="", corner="", horizontal=" ", vertical=" ", padding=1),
}


@dataclass
class Column:
    """Table column definition."""
    header: str
    width: int = 0
    align: str = "left"
    truncate: bool = True


class Table:
    """
    Formatted table for terminal output.

    Supports custom styling, alignment, and various output formats.

    Attributes:
        headers: Column headers.
        rows: Table data rows.
    """

    def __init__(
        self,
        headers: Optional[List[str]] = None,
        style: Union[str, TableStyle] = "simple",
    ) -> None:
        """
        Initialize the table.

        Args:
            headers: Column headers.
            style: Style name or TableStyle object.
        """
        self.headers: List[str] = headers or []
        self.rows: List[List[str]] = []
        self._columns: List[Column] = [Column(h) for h in self.headers]
        self.style = STYLES.get(style, style) if isinstance(style, str) else style

    def add_row(self, row: List[Any]) -> None:
        """
        Add a row to the table.

        Args:
            row: Row data (converted to strings).
        """
        str_row = [str(cell) for cell in row]
        self.rows.append(str_row)

        for i, cell in enumerate(str_row):
            if i < len(self._columns):
                self._columns[i].width = max(self._columns[i].width, len(cell))

    def add_rows(self, rows: List[List[Any]]) -> None:
        """Add multiple rows."""
        for row in rows:
            self.add_row(row)

    def set_style(self, style: str) -> None:
        """Set the table style by name."""
        if style in STYLES:
            self.style = STYLES[style]

    def column_width(self, col_idx: int) -> int:
        """Get column width including padding."""
        col = self._columns[col_idx]
        header_width = len(col.header)
        max_width = max(header_width, col.width)
        return max_width + self.style.padding * 2

    def align_column(self, col_idx: int, alignment: str) -> None:
        """
        Set column alignment.

        Args:
            col_idx: Column index.
            alignment: 'left', 'right', or 'center'.
        """
        if col_idx < len(self._columns):
            self._columns[col_idx].align = alignment

    def _pad(self, text: str, width: int, align: str) -> str:
        """Pad text to width with alignment."""
        text_len = len(text)
        padding = width - text_len

        if padding <= 0:
            return text

        if align == "left":
            return " " * padding + text
        elif align == "right":
            return text + " " * padding
        else:
            left = padding // 2
            right = padding - left
            return " " * left + text + " " * right

    def _truncate(self, text: str, width: int) -> str:
        """Truncate text to width."""
        if len(text) <= width:
            return text
        return text[: width - 3] + "..."

    def render(self) -> str:
        """
        Render the table as a string.

        Returns:
            Formatted table string.
        """
        lines: List[str] = []
        col_widths = [self.column_width(i) for i in range(len(self._columns))]

        total_width = sum(col_widths) + len(col_widths) - 1

        h_line = "+" + "+".join("-" * w for w in col_widths) + "+"
        lines.append(h_line)

        header_cells = []
        for i, col in enumerate(self._columns):
            padded = self._pad(col.header, col_widths[i], "center")
            header_cells.append(f" {padded[1:-1]} ")

        lines.append("|" + "|".join(header_cells) + "|")
        lines.append(h_line)

        for row in self.rows:
            cells = []
            for i, cell in enumerate(row):
                col = self._columns[i] if i < len(self._columns) else Column("")
                cell_width = col_widths[i] if i < len(col_widths) else len(cell)
                display_cell = cell
                if col.truncate:
                    display_cell = self._truncate(display_cell, cell_width)
                padded = self._pad(display_cell, cell_width, col.align)
                cells.append(f" {padded} ")

            lines.append("|" + "|".join(cells) + "|")

        lines.append(h_line)

        return "\n".join(lines)

    def __str__(self) -> str:
        """Get string representation."""
        return self.render()

    def __repr__(self) -> str:
        """Get detailed representation."""
        return f"Table(headers={self.headers!r}, rows={self.rows!r})"


def print_table(
    headers: List[str],
    rows: List[List[Any]],
    style: str = "simple",
    file=None,
) -> None:
    """
    Convenience function to print a table.

    Args:
        headers: Column headers.
        rows: Table rows.
        style: Table style name.
        file: Output file (default: stdout).
    """
    table = Table(headers=headers, style=style)
    table.add_rows(rows)
    print(table.render(), file=file or sys.stdout)


def format_table_row(
    values: List[Any],
    widths: List[int],
    alignments: Optional[List[str]] = None,
    separator: str = " | ",
) -> str:
    """
    Format a single table row.

    Args:
        values: Cell values.
        widths: Column widths.
        alignments: Column alignments.
        separator: Cell separator.

    Returns:
        Formatted row string.
    """
    alignments = alignments or ["left"] * len(values)
    cells = []

    for i, value in enumerate(values):
        width = widths[i] if i < len(widths) else len(str(value))
        align = alignments[i] if i < len(alignments) else "left"
        text = str(value).ljust(width) if align == "left" else str(value).rjust(width)
        cells.append(text)

    return separator.join(cells)


class MarkdownTable(Table):
    """Table with Markdown-compatible formatting."""

    def render(self) -> str:
        """Render as Markdown table."""
        lines: List[str] = []
        col_widths = [self.column_width(i) - 2 for i in range(len(self._columns))]

        headers = []
        for i, col in enumerate(self._columns):
            width = col_widths[i] if i < len(col_widths) else len(col.header)
            headers.append(col.header.ljust(width))
        lines.append("| " + " | ".join(headers) + " |")

        sep_cells = []
        for width in col_widths:
            sep_cells.append("-" * width)
        lines.append("| " + " | ".join(sep_cells) + " |")

        for row in self.rows:
            cells = []
            for i, cell in enumerate(row):
                width = col_widths[i] if i < len(col_widths) else len(cell)
                cells.append(str(cell).ljust(width))
            lines.append("| " + " | ".join(cells) + " |")

        return "\n".join(lines)


class CsvTable(Table):
    """Table that outputs as CSV."""

    def render(self) -> str:
        """Render as CSV."""
        import csv
        import io

        output = io.StringIO()
        writer = csv.writer(output)

        writer.writerow(self.headers)
        writer.writerows(self.rows)

        return output.getvalue()

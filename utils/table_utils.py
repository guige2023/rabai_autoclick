"""
Table and grid operations utilities.

Provides:
- ASCII table generation
- Table alignment and formatting
- Grid layout calculations
- Pivot operations
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator, Sequence


@dataclass
class Column:
    """Table column definition."""

    header: str
    width: int = 0
    align: str = "<"  # <, >, ^
    formatter: Callable[[Any], str] = str

    def __post_init__(self) -> None:
        if self.width == 0:
            self.width = len(self.header)


@dataclass
class TableStyle:
    """Styling options for table output."""

    border_char: str = "|"
    corner_char: str = "+"
    divider_char: str = "-"
    padding: int = 1
    show_header: bool = True
    show_border: bool = True
    alternate_row_color: bool = False


DEFAULT_STYLE = TableStyle()


def create_table(
    headers: list[str],
    rows: list[list[Any]],
    styles: TableStyle = DEFAULT_STYLE,
    formatters: list[Callable[[Any], str]] | None = None,
) -> str:
    """
    Create an ASCII table from data.

    Args:
        headers: Column headers
        rows: Row data
        styles: Table styling options
        formatters: Optional per-column formatters

    Returns:
        Formatted ASCII table string

    Example:
        >>> table = create_table(["Name", "Age"], [["Alice", 30], ["Bob", 25]])
        >>> print(table)
        +-------+-----+
        | Name  | Age |
        +-------+-----+
        | Alice | 30  |
        | Bob   | 25  |
        +-------+-----+
    """
    if not headers:
        return ""

    num_cols = len(headers)
    col_widths = [len(h) for h in headers]

    for row in rows:
        if len(row) != num_cols:
            raise ValueError(f"Row has {len(row)} columns, expected {num_cols}")
        for i, cell in enumerate(row):
            formatter = formatters[i] if formatters and i < len(formatters) else str
            cell_str = formatter(cell)
            col_widths[i] = max(col_widths[i], len(cell_str))

    lines: list[str] = []

    if styles.show_border:
        lines.append(_make_border(col_widths, styles))

    if styles.show_header:
        header_cells = [headers[i].center(col_widths[i]) for i in range(num_cols)]
        lines.append(_make_row(header_cells, col_widths, styles))
        lines.append(_make_divider(col_widths, styles))

    for row_idx, row in enumerate(rows):
        cells = []
        for i, cell in enumerate(row):
            formatter = formatters[i] if formatters and i < len(formatters) else str
            cell_str = formatter(cell)
            align = styles.alternate_row_color and row_idx % 2 == 1
            if align:
                cells.append(cell_str.ljust(col_widths[i]))
            else:
                cells.append(cell_str.ljust(col_widths[i]))
        lines.append(_make_row(cells, col_widths, styles))

    if styles.show_border:
        lines.append(_make_border(col_widths, styles))

    return "\n".join(lines)


def _make_border(col_widths: list[int], styles: TableStyle) -> str:
    parts = [styles.corner_char]
    for w in col_widths:
        parts.append(styles.divider_char * (w + 2 * styles.padding))
        parts.append(styles.corner_char)
    return "".join(parts)


def _make_divider(col_widths: list[int], styles: TableStyle) -> str:
    return _make_border(col_widths, styles)


def _make_row(cells: list[str], col_widths: list[int], styles: TableStyle) -> str:
    parts = [styles.border_char]
    for cell, width in zip(cells, col_widths):
        parts.append(" " * styles.padding)
        parts.append(cell)
        parts.append(" " * (width - len(cell) + styles.padding))
        parts.append(styles.border_char)
    return "".join(parts)


def transpose_table(headers: list[str], rows: list[list[Any]]) -> tuple[list[str], list[list[Any]]]:
    """
    Transpose a table (rows become columns and vice versa).

    Args:
        headers: Column headers
        rows: Row data

    Returns:
        Tuple of (new_headers, new_rows)
    """
    if not rows:
        return headers, rows

    new_headers = [headers[0]] + [str(row[0]) for row in rows]
    new_rows = []
    num_cols = len(headers)

    for col_idx in range(1, num_cols):
        new_row = [headers[col_idx]] + [row[col_idx] for row in rows]
        new_rows.append(new_row)

    return new_headers, new_rows


def sort_table(rows: list[list[Any]], sort_col: int, reverse: bool = False) -> list[list[Any]]:
    """
    Sort table rows by a column.

    Args:
        rows: Row data
        sort_col: Column index to sort by (0-based)
        reverse: Sort in descending order

    Returns:
        Sorted rows
    """
    return sorted(rows, key=lambda row: row[sort_col], reverse=reverse)


def filter_table(rows: list[list[Any]], predicate: Callable[[list[Any]], bool]) -> list[list[Any]]:
    """
    Filter table rows.

    Args:
        rows: Row data
        predicate: Function that returns True for rows to keep

    Returns:
        Filtered rows
    """
    return [row for row in rows if predicate(row)]


def group_by_column(rows: list[list[Any]], group_col: int) -> dict[Any, list[list[Any]]]:
    """
    Group rows by a column value.

    Args:
        rows: Row data
        group_col: Column index to group by

    Returns:
        Dictionary mapping group value to list of rows
    """
    groups: dict[Any, list[list[Any]]] = {}
    for row in rows:
        key = row[group_col]
        if key not in groups:
            groups[key] = []
        groups[key].append(row)
    return groups


def paginate_table(rows: list[list[Any]], page_size: int, page: int = 1) -> tuple[list[list[Any]], int]:
    """
    Paginate table rows.

    Args:
        rows: All row data
        page_size: Rows per page
        page: Page number (1-based)

    Returns:
        Tuple of (rows for page, total pages)
    """
    if page_size <= 0:
        raise ValueError("page_size must be positive")

    total_pages = math.ceil(len(rows) / page_size)
    page = max(1, min(page, total_pages))
    start = (page - 1) * page_size
    end = start + page_size

    return rows[start:end], total_pages


def align_columns(rows: list[list[Any]], alignments: list[str]) -> list[list[str]]:
    """
    Align cell values within columns.

    Args:
        rows: Row data
        alignments: List of alignment codes ('<', '>', '^')

    Returns:
        Rows with aligned string values
    """
    if not alignments:
        return [[str(cell) for cell in row] for row in rows]

    aligned: list[list[str]] = []
    for row in rows:
        aligned_row = []
        for i, cell in enumerate(row):
            cell_str = str(cell)
            align = alignments[i] if i < len(alignments) else "<"
            if align == ">":
                aligned_row.append(cell_str.rjust(10))
            elif align == "^":
                aligned_row.append(cell_str.center(10))
            else:
                aligned_row.append(cell_str.ljust(10))
        aligned.append(aligned_row)
    return aligned


def markdown_table(headers: list[str], rows: list[list[Any]], formatters: list[Callable[[Any], str]] | None = None) -> str:
    """
    Create a Markdown-formatted table.

    Args:
        headers: Column headers
        rows: Row data
        formatters: Optional per-column formatters

    Returns:
        Markdown table string
    """
    if not headers:
        return ""

    num_cols = len(headers)
    col_widths = [len(h) for h in headers]

    for row in rows:
        for i, cell in enumerate(row):
            formatter = formatters[i] if formatters and i < len(formatters) else str
            col_widths[i] = max(col_widths[i], len(formatter(cell)))

    lines: list[str] = []
    header_cells = [headers[i].ljust(col_widths[i]) for i in range(num_cols)]
    lines.append("| " + " | ".join(header_cells) + " |")
    lines.append("|" + "|".join("-" * (w + 2) for w in col_widths) + "|")

    for row in rows:
        cells = []
        for i, cell in enumerate(row):
            formatter = formatters[i] if formatters and i < len(formatters) else str
            cell_str = formatter(cell)
            cells.append(cell_str.ljust(col_widths[i]))
        lines.append("| " + " | ".join(cells) + " |")

    return "\n".join(lines)


def html_table(headers: list[str], rows: list[list[Any]], formatters: list[Callable[[Any], str]] | None = None, class_name: str = "") -> str:
    """
    Create an HTML table.

    Args:
        headers: Column headers
        rows: Row data
        formatters: Optional per-column formatters
        class_name: Optional CSS class name

    Returns:
        HTML table string
    """
    class_attr = f' class="{class_name}"' if class_name else ""
    lines = [f"<table{class_attr}>"]

    if headers:
        lines.append("  <thead>")
        lines.append("    <tr>")
        for h in headers:
            lines.append(f"      <th>{_escape_html(h)}</th>")
        lines.append("    </tr>")
        lines.append("  </thead>")

    lines.append("  <tbody>")
    for row in rows:
        lines.append("    <tr>")
        for i, cell in enumerate(row):
            formatter = formatters[i] if formatters and i < len(formatters) else str
            lines.append(f"      <td>{_escape_html(str(formatter(cell)))}</td>")
        lines.append("    </tr>")
    lines.append("  </tbody>")
    lines.append("</table>")

    return "\n".join(lines)


def _escape_html(text: str) -> str:
    """Escape HTML special characters."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def csv_to_rows(csv_text: str, delimiter: str = ",", quote_char: str = '"') -> tuple[list[str], list[list[str]]]:
    """
    Parse CSV text into headers and rows.

    Args:
        csv_text: CSV content
        delimiter: Field delimiter
        quote_char: Quote character

    Returns:
        Tuple of (headers, rows)
    """
    import csv
    import io

    reader = csv.reader(io.StringIO(csv_text), delimiter=delimiter, quotechar=quote_char)
    rows = list(reader)

    if not rows:
        return [], []

    headers = rows[0]
    data_rows = rows[1:]

    return headers, data_rows


def rows_to_csv(headers: list[str], rows: list[list[Any]], delimiter: str = ",", quote_char: str = '"', include_header: bool = True) -> str:
    """
    Convert rows to CSV format.

    Args:
        headers: Column headers
        rows: Row data
        delimiter: Field delimiter
        quote_char: Quote character
        include_header: Include header row

    Returns:
        CSV string
    """
    import csv
    import io

    output = io.StringIO()
    writer = csv.writer(output, delimiter=delimiter, quotechar=quote_char, quoting=csv.QUOTE_MINIMAL)

    if include_header:
        writer.writerow(headers)

    for row in rows:
        writer.writerow(row)

    return output.getvalue()


def grid_layout(items: list[Any], num_cols: int, fill_last_row: bool = False) -> list[list[Any]]:
    """
    Arrange items in a grid.

    Args:
        items: Items to arrange
        num_cols: Number of columns
        fill_last_row: Pad last row with None if incomplete

    Returns:
        List of rows, each containing num_cols items
    """
    if num_cols <= 0:
        raise ValueError("num_cols must be positive")

    num_rows = math.ceil(len(items) / num_cols)

    if fill_last_row and len(items) % num_cols != 0:
        padding = num_cols - (len(items) % num_cols)
        items = list(items) + [None] * padding

    return [items[i * num_cols : (i + 1) * num_cols] for i in range(num_rows)]

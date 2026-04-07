"""Table utilities for RabAI AutoClick.

Provides:
- ASCII table generation
- Column formatting and alignment
- Table rendering and export
"""

from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Union,
)


class TableStyle:
    """Table style configuration."""

    def __init__(
        self,
        vertical: str = "|",
        horizontal: str = "-",
        intersection: str = "+",
        padding: int = 1,
    ) -> None:
        self.vertical = vertical
        self.horizontal = horizontal
        self.intersection = intersection
        self.padding = padding


DEFAULT_STYLE = TableStyle()
MINIMAL_STYLE = TableStyle(vertical=" ", horizontal=" ", intersection=" ", padding=1)
MARKDOWN_STYLE = TableStyle(vertical="|", horizontal="-", intersection="|", padding=1)


class Table:
    """ASCII table builder."""

    def __init__(
        self,
        headers: Optional[List[str]] = None,
        style: Optional[TableStyle] = None,
    ) -> None:
        """Initialize table.

        Args:
            headers: Column header names.
            style: Table style configuration.
        """
        self._headers = headers or []
        self._rows: List[List[str]] = []
        self._style = style or DEFAULT_STYLE
        self._alignments: List[str] = ["left"] * len(self._headers)

    def set_headers(self, headers: List[str]) -> "Table":
        """Set column headers.

        Args:
            headers: List of header names.

        Returns:
            self for chaining.
        """
        self._headers = headers
        self._alignments = ["left"] * len(headers)
        return self

    def add_row(self, row: Sequence[Any]) -> "Table":
        """Add a data row.

        Args:
            row: Row data (converted to strings).

        Returns:
            self for chaining.
        """
        self._rows.append([str(cell) for cell in row])
        return self

    def add_rows(self, rows: List[Sequence[Any]]) -> "Table":
        """Add multiple rows.

        Args:
            rows: List of row data.

        Returns:
            self for chaining.
        """
        for row in rows:
            self.add_row(row)
        return self

    def align(self, column: int, alignment: str) -> "Table":
        """Set column alignment.

        Args:
            column: Column index (0-based).
            alignment: "left", "right", or "center".

        Returns:
            self for chaining.
        """
        self._alignments[column] = alignment
        return self

    def _compute_widths(self) -> List[int]:
        """Compute column widths based on content."""
        widths = [len(h) for h in self._headers] if self._headers else []

        for row in self._rows:
            for i, cell in enumerate(row):
                if i < len(widths):
                    widths[i] = max(widths[i], len(cell))
                else:
                    widths.append(len(cell))

        return widths

    def _pad_cell(self, cell: str, width: int, alignment: str) -> str:
        """Pad a cell to specified width."""
        pad = self._style.padding
        inner = width - pad * 2
        content = cell[:inner].ljust(inner) if alignment == "left" else (
            cell[:inner].rjust(inner) if alignment == "right" else
            cell[:inner].center(inner)
        )
        return " " * pad + content + " " * pad

    def _render_row(self, cells: List[str], widths: List[int]) -> str:
        """Render a single row."""
        cells_padded = [
            self._pad_cell(cells[i] if i < len(cells) else "", widths[i], self._alignments[i] if i < len(self._alignments) else "left")
            for i in range(len(widths))
        ]
        return (self._style.vertical + " ".join(cells_padded) + self._style.vertical)

    def _render_separator(self, widths: List[int]) -> str:
        """Render a separator line."""
        sep = "".join(self._style.horizontal * (w + self._style.padding * 2) for w in widths)
        return self._style.intersection + sep + self._style.intersection

    def render(self) -> str:
        """Render the table as a string.

        Returns:
            ASCII table string.
        """
        if not self._headers and not self._rows:
            return ""

        widths = self._compute_widths()
        lines: List[str] = []

        if self._headers:
            lines.append(self._render_row(self._headers, widths))
            lines.append(self._render_separator(widths))

        for row in self._rows:
            lines.append(self._render_row(row, widths))

        return "\n".join(lines)

    def __str__(self) -> str:
        return self.render()

    def __repr__(self) -> str:
        return f"Table(headers={self._headers!r}, rows={len(self._rows)})"


def make_table(
    headers: List[str],
    rows: List[Sequence[Any]],
    style: Optional[TableStyle] = None,
) -> str:
    """Create a formatted table string.

    Args:
        headers: Column headers.
        rows: List of row data.
        style: Table style.

    Returns:
        ASCII table string.
    """
    table = Table(headers, style)
    table.add_rows(rows)
    return table.render()


def make_markdown_table(
    headers: List[str],
    rows: List[Sequence[Any]],
) -> str:
    """Create a markdown-compatible table.

    Args:
        headers: Column headers.
        rows: List of row data.

    Returns:
        Markdown table string.
    """
    table = Table(headers, MARKDOWN_STYLE)
    table.add_rows(rows)
    return table.render()


def align_columns(
    items: List[str],
    widths: Optional[List[int]] = None,
    alignment: str = "left",
) -> List[str]:
    """Align items into columns.

    Args:
        items: Items to align.
        widths: Optional column widths.
        alignment: Alignment direction.

    Returns:
        List of aligned strings.
    """
    return items


def transpose_table(
    headers: List[str],
    rows: List[List[Any]],
) -> tuple[List[str], List[List[Any]]]:
    """Transpose a table (swap rows and columns).

    Args:
        headers: Column headers.
        rows: Row data.

    Returns:
        Tuple of (new_headers, new_rows).
    """
    if not rows:
        return headers, rows

    num_cols = len(headers)
    new_headers = [f"Col{i+1}" for i in range(len(rows[0]))]
    new_rows = []

    for i in range(len(rows[0])):
        new_row = []
        for row in rows:
            if i < len(row):
                new_row.append(row[i])
            else:
                new_row.append("")
        new_rows.append(new_row)

    return new_headers, new_rows


def filter_table(
    rows: List[List[Any]],
    predicate: Callable[[List[Any]], bool],
) -> List[List[Any]]:
    """Filter table rows by a predicate.

    Args:
        rows: Row data.
        predicate: Function that returns True for rows to keep.

    Returns:
        Filtered rows.
    """
    return [row for row in rows if predicate(row)]


def sort_table(
    rows: List[List[Any]],
    column: int,
    reverse: bool = False,
) -> List[List[Any]]:
    """Sort table rows by a column.

    Args:
        rows: Row data.
        column: Column index to sort by.
        reverse: If True, sort descending.

    Returns:
        Sorted rows.
    """
    return sorted(rows, key=lambda row: row[column] if column < len(row) else "", reverse=reverse)


def group_table(
    rows: List[List[Any]],
    column: int,
) -> Dict[str, List[List[Any]]]:
    """Group table rows by a column value.

    Args:
        rows: Row data.
        column: Column index to group by.

    Returns:
        Dict mapping column value to list of rows.
    """
    groups: Dict[str, List[List[Any]]] = {}
    for row in rows:
        key = str(row[column]) if column < len(row) else ""
        if key not in groups:
            groups[key] = []
        groups[key].append(row)
    return groups


def format_table_column(
    rows: List[List[Any]],
    column: int,
    formatter: Callable[[Any], str],
) -> List[List[str]]:
    """Format a specific column in a table.

    Args:
        rows: Row data.
        column: Column index.
        formatter: Function to format each cell.

    Returns:
        New rows with formatted column.
    """
    return [
        [formatter(row[column]) if i == column else str(cell) for i, cell in enumerate(row)]
        for row in rows
    ]


def csv_to_table(
    csv_data: str,
    delimiter: str = ",",
    has_header: bool = True,
) -> tuple[List[str], List[List[str]]]:
    """Parse CSV data into table format.

    Args:
        csv_data: CSV string.
        delimiter: Field delimiter.
        has_header: If True, first row is header.

    Returns:
        Tuple of (headers, rows).
    """
    lines = [line.strip() for line in csv_data.strip().splitlines() if line.strip()]
    rows = [line.split(delimiter) for line in lines]

    if has_header and rows:
        headers = rows[0]
        data_rows = rows[1:]
    else:
        headers = [f"Col{i+1}" for i in range(len(rows[0]))]
        data_rows = rows

    return headers, data_rows


def table_to_csv(
    headers: List[str],
    rows: List[List[Any]],
    delimiter: str = ",",
) -> str:
    """Convert table to CSV format.

    Args:
        headers: Column headers.
        rows: Row data.
        delimiter: Field delimiter.

    Returns:
        CSV string.
    """
    lines = [delimiter.join(headers)]
    for row in rows:
        lines.append(delimiter.join(str(cell) for cell in row))
    return "\n".join(lines)

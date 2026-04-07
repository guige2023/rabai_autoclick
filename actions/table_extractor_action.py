"""Table extractor action for extracting data from HTML tables.

This module provides HTML table parsing with support for
nested tables, header detection, and cell merging.

Example:
    >>> action = TableExtractorAction()
    >>> result = action.execute(html='<table><tr><td>Data</td></tr></table>')
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class TableCell:
    """Represents a table cell."""
    value: str
    row: int
    col: int
    rowspan: int = 1
    colspan: int = 1
    is_header: bool = False


@dataclass
class TableData:
    """Extracted table data."""
    headers: list[str]
    rows: list[list[str]]
    row_count: int = 0
    col_count: int = 0


class TableExtractorAction:
    """HTML table extraction action.

    Extracts structured data from HTML tables with support for
    spanning cells, header detection, and conversion to CSV/JSON.

    Example:
        >>> action = TableExtractorAction()
        >>> result = action.execute(
        ...     html="<table><thead><tr><th>Name</th></tr></thead></table>",
        ...     format="json"
        ... )
    """

    def __init__(self) -> None:
        """Initialize table extractor."""
        self._tables: list[TableData] = []

    def execute(
        self,
        html: str,
        selector: Optional[str] = None,
        format: str = "json",
        detect_headers: bool = True,
        skip_empty: bool = True,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute table extraction.

        Args:
            html: HTML content containing tables.
            selector: CSS selector to target specific table.
            format: Output format ('json', 'csv', 'dict').
            detect_headers: Whether to detect header rows.
            skip_empty: Whether to skip empty cells.
            **kwargs: Additional parameters.

        Returns:
            Extraction result dictionary.

        Raises:
            ValueError: If HTML is empty.
        """
        if not html:
            raise ValueError("HTML content is required")

        result: dict[str, Any] = {"success": True, "format": format}

        # Find tables
        if selector:
            table_htmls = self._find_tables_by_selector(html, selector)
        else:
            table_htmls = self._find_all_tables(html)

        result["table_count"] = len(table_htmls)
        tables_data: list[TableData] = []

        for table_html in table_htmls:
            table_data = self._parse_table(
                table_html,
                detect_headers=detect_headers,
                skip_empty=skip_empty,
            )
            tables_data.append(table_data)

        self._tables = tables_data

        if format == "json":
            result["tables"] = [
                {
                    "headers": t.headers,
                    "rows": t.rows,
                    "row_count": t.row_count,
                    "col_count": t.col_count,
                }
                for t in tables_data
            ]
        elif format == "csv":
            result["csv"] = self._tables_to_csv(tables_data)
        elif format == "dict":
            result["tables"] = [self._to_dict_list(t) for t in tables_data]

        return result

    def _find_all_tables(self, html: str) -> list[str]:
        """Find all table elements in HTML.

        Args:
            html: HTML content.

        Returns:
            List of table HTML strings.
        """
        tables: list[str] = []
        pattern = re.compile(r"<table[^>]*>(.*?)</table>", re.IGNORECASE | re.DOTALL)
        for match in pattern.finditer(html):
            tables.append(match.group(0))
        return tables

    def _find_tables_by_selector(self, html: str, selector: str) -> list[str]:
        """Find tables matching CSS selector.

        Args:
            html: HTML content.
            selector: CSS selector.

        Returns:
            List of matching table HTML strings.
        """
        # Simple selector matching
        tables = self._find_all_tables(html)
        if "class=" in selector:
            class_match = re.search(r"\.([a-zA-Z0-9_-]+)", selector)
            if class_match:
                class_name = class_match.group(1)
                tables = [t for t in tables if f"class=['\"].*\\b{class_name}\\b" in t]
        elif "id=" in selector:
            id_match = re.search(r"#([a-zA-Z0-9_-]+)", selector)
            if id_match:
                id_val = id_match.group(1)
                tables = [t for t in tables if f"id=['\"]{id_val}" in t]
        return tables

    def _parse_table(
        self,
        table_html: str,
        detect_headers: bool,
        skip_empty: bool,
    ) -> TableData:
        """Parse HTML table into structured data.

        Args:
            table_html: Table HTML string.
            detect_headers: Whether to detect headers.
            skip_empty: Whether to skip empty cells.

        Returns:
            TableData object.
        """
        # Extract rows
        rows_html = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.IGNORECASE | re.DOTALL)
        rows: list[list[str]] = []
        headers: list[str] = []

        for row_idx, row_html in enumerate(rows_html):
            cells: list[str] = []

            # Extract cells (td and th)
            cell_pattern = re.compile(
                r"<t[hd][^>]*(?:rowspan=['\"]?(\d+)['\"]?)?(?:colspan=['\"]?(\d+)['\"]?)?[^>]*>(.*?)</t[hd]>",
                re.IGNORECASE | re.DOTALL,
            )

            for cell_match in cell_pattern.finditer(row_html):
                rowspan = int(cell_match.group(1) or 1)
                colspan = int(cell_match.group(2) or 1)
                cell_value = self._clean_cell_text(cell_match.group(3))

                if skip_empty and not cell_value.strip():
                    continue

                # Expand colspan
                for _ in range(colspan):
                    cells.append(cell_value)

            if cells:
                rows.append(cells)

        # Detect headers
        if detect_headers and rows:
            first_row = rows[0]
            # Check if first row looks like headers
            if self._is_header_row(first_row):
                headers = first_row
                rows = rows[1:]

        col_count = max(len(row) for row in rows) if rows else 0

        return TableData(
            headers=headers,
            rows=rows,
            row_count=len(rows),
            col_count=col_count,
        )

    def _clean_cell_text(self, cell_html: str) -> str:
        """Clean cell HTML to extract text.

        Args:
            cell_html: Cell HTML content.

        Returns:
            Cleaned text.
        """
        # Remove nested tags
        text = re.sub(r"<[^>]+>", "", cell_html)
        # Decode entities
        text = text.replace("&nbsp;", " ")
        text = text.replace("&lt;", "<")
        text = text.replace("&gt;", ">")
        text = text.replace("&amp;", "&")
        text = text.replace("&quot;", '"')
        # Normalize whitespace
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def _is_header_row(self, row: list[str]) -> bool:
        """Check if row looks like a header row.

        Args:
            row: Row data.

        Returns:
            True if row appears to be headers.
        """
        if not row:
            return False

        # Check if any cell is all uppercase or short
        short_upper = sum(
            1 for cell in row
            if cell.isupper() or (len(cell) < 20 and cell.title() == cell)
        )

        return short_upper >= len(row) * 0.5

    def _tables_to_csv(self, tables: list[TableData]) -> str:
        """Convert tables to CSV format.

        Args:
            tables: List of TableData objects.

        Returns:
            CSV string.
        """
        import csv
        import io

        output = io.StringIO()
        writer = csv.writer(output)

        for i, table in enumerate(tables):
            if i > 0:
                writer.writerow([])  # Empty row between tables
            if table.headers:
                writer.writerow(table.headers)
            for row in table.rows:
                writer.writerow(row)

        return output.getvalue()

    def _to_dict_list(self, table: TableData) -> list[dict[str, str]]:
        """Convert table to list of dictionaries.

        Args:
            table: TableData object.

        Returns:
            List of row dictionaries.
        """
        if not table.headers:
            # Generate column names
            headers = [f"col{i}" for i in range(table.col_count)]
        else:
            headers = table.headers

        return [dict(zip(headers, row)) for row in table.rows]

    def get_tables(self) -> list[TableData]:
        """Get extracted tables.

        Returns:
            List of TableData objects.
        """
        return self._tables

    def search_in_tables(
        self,
        query: str,
        column: Optional[int] = None,
    ) -> list[tuple[int, int, str]]:
        """Search for text in extracted tables.

        Args:
            query: Search query.
            column: Optional column to search in.

        Returns:
            List of (table_idx, row_idx, value) tuples.
        """
        results: list[tuple[int, int, str]] = []

        for table_idx, table in enumerate(self._tables):
            for row_idx, row in enumerate(table.rows):
                search_cols = [column] if column is not None else range(len(row))
                for col_idx in search_cols:
                    if col_idx < len(row) and query.lower() in row[col_idx].lower():
                        results.append((table_idx, row_idx, row[col_idx]))

        return results

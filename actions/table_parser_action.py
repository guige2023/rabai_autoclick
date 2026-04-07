"""
Table Parser Action Module.

Parses and extracts data from HTML tables, CSV, TSV, and Excel files.
Handles merged cells, header detection, and table-to-records conversion.

Example:
    >>> from table_parser_action import TableParser
    >>> parser = TableParser()
    >>> records = parser.parse_html_table(html_string, headers=True)
    >>> parser.to_csv(records, "/tmp/output.csv")
"""
from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class TableCell:
    """Represents a cell in a parsed table."""
    value: str
    row: int
    col: int
    rowspan: int = 1
    colspan: int = 1
    is_header: bool = False


@dataclass
class ParsedTable:
    """Represents a parsed table with cells and metadata."""
    headers: list[str]
    rows: list[list[str]]
    records: list[dict[str, str]]
    merged_cells: list[tuple[int, int, int, int]] = field(default_factory=list)


class TableParser:
    """Parse HTML tables, CSV, TSV into structured records."""

    def parse_html_table(
        self,
        html: str,
        headers: bool = True,
        header_selector: Optional[str] = None,
    ) -> list[dict[str, str]]:
        """
        Parse HTML table to list of records.

        Args:
            html: HTML string containing table(s)
            headers: Whether first row is headers
            header_selector: CSS selector for header row (e.g., "thead th")

        Returns:
            List of dictionaries with column names as keys
        """
        from html_parser_action import HTMLParserAction, CSSSelector

        parser = HTMLParserAction()
        doc = parser.parse_string(html)

        if header_selector:
            sel = CSSSelector(header_selector)
            header_elems = sel.match_all(doc.root)
            headers_list = [parser.get_text(e).strip() for e in header_elems]
        else:
            table_sel = CSSSelector("table")
            tables = table_sel.match_all(doc.root)
            if not tables:
                return []
            table = tables[0]

            if headers:
                first_row_sel = CSSSelector("tr")
                rows = first_row_sel.match_all(table)
                if rows:
                    header_cells = CSSSelector("th").match_all(rows[0])
                    headers_list = [parser.get_text(h).strip() for h in header_cells]
                    data_rows = rows[1:]
                else:
                    headers_list = []
                    data_rows = []
            else:
                headers_list = [f"col_{i}" for i in range(100)]
                data_rows = CSSSelector("tr").match_all(table)

        records: list[dict[str, str]] = []
        row_idx = 0
        for row_elem in CSSSelector("tr").match_all(table) if header_selector or not headers else data_rows:
            cells = CSSSelector("td").match_all(row_elem)
            if not cells:
                continue
            if row_idx == 0 and headers and not header_selector:
                row_idx += 1
                continue
            record: dict[str, str] = {}
            for col_idx, cell in enumerate(cells):
                header = headers_list[col_idx] if col_idx < len(headers_list) else f"col_{col_idx}"
                record[header] = parser.get_text(cell).strip()
            if record:
                records.append(record)
            row_idx += 1

        return records

    def parse_csv(
        self,
        csv_text: str,
        delimiter: str = ",",
        headers: bool = True,
        skip_rows: int = 0,
    ) -> list[dict[str, str]]:
        """Parse CSV text to list of records."""
        records: list[dict[str, str]] = []
        lines = csv_text.strip().splitlines()
        lines = lines[skip_rows:]
        if not lines:
            return []

        if headers:
            reader = csv.DictReader(lines, delimiter=delimiter)
            for row in reader:
                records.append(dict(row))
        else:
            reader = csv.reader(lines, delimiter=delimiter)
            for row in reader:
                record = {f"col_{i}": val for i, val in enumerate(row)}
                records.append(record)
        return records

    def parse_tsv(self, tsv_text: str, headers: bool = True) -> list[dict[str, str]]:
        """Parse TSV text to list of records."""
        return self.parse_csv(tsv_text, delimiter="\t", headers=headers)

    def parse_excel(self, path: str, sheet_index: int = 0) -> list[dict[str, str]]:
        """Parse Excel file to list of records."""
        try:
            import openpyxl
        except ImportError:
            return []

        records: list[dict[str, str]] = []
        try:
            wb = openpyxl.load_workbook(path, data_only=True)
            ws = wb.worksheets[sheet_index]
            rows = list(ws.iter_rows(values_only=True))
            if not rows:
                return []

            headers = [str(h).strip() if h is not None else f"col_{i}" for i, h in enumerate(rows[0])]
            for row in rows[1:]:
                if all(cell is None for cell in row):
                    continue
                record = {headers[i]: (str(cell).strip() if cell is not None else "") for i, cell in enumerate(row)}
                records.append(record)
        except Exception:
            pass
        return records

    def to_csv(
        self,
        records: list[dict[str, Any]],
        path: str,
        headers: Optional[list[str]] = None,
    ) -> bool:
        """Write records to CSV file."""
        if not records:
            return False
        try:
            if headers is None:
                fieldnames = list(records[0].keys())
            else:
                fieldnames = headers
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for record in records:
                    writer.writerow({k: str(record.get(k, "")) for k in fieldnames})
            return True
        except Exception:
            return False

    def to_tsv(self, records: list[dict[str, Any]], path: str) -> bool:
        """Write records to TSV file."""
        if not records:
            return False
        try:
            fieldnames = list(records[0].keys())
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
                writer.writeheader()
                for record in records:
                    writer.writerow({k: str(record.get(k, "")) for k in fieldnames})
            return True
        except Exception:
            return False

    def transpose(
        self,
        records: list[dict[str, Any]],
        key_column: str = "key",
        value_column: str = "value",
    ) -> list[dict[str, Any]]:
        """Transpose records from column-wise to row-wise format."""
        if not records:
            return []
        result: dict[str, dict[str, Any]] = {}
        for record in records:
            key = str(record.get(key_column, ""))
            if not key:
                continue
            for col, val in record.items():
                if col == key_column:
                    continue
                if col not in result:
                    result[col] = {key_column: col}
                result[col][key] = val
        return list(result.values())

    def merge_tables(
        self,
        tables: list[list[dict[str, Any]]],
        how: str = "vertical",
        on: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Merge multiple tables.

        Args:
            tables: List of record lists to merge
            how: "vertical" (stack), "horizontal" (join), or "union" (SQL union)
            on: Column name to join on (for horizontal)

        Returns:
            Merged records
        """
        if how == "vertical":
            combined: list[dict[str, Any]] = []
            for table in tables:
                combined.extend(table)
            return combined
        elif how == "union":
            seen: set[str] = set()
            result: list[dict[str, Any]] = []
            for table in tables:
                for record in table:
                    key = self._record_key(record)
                    if key not in seen:
                        seen.add(key)
                        result.append(record)
            return result
        elif how == "horizontal" and on:
            result = tables[0]
            for table in tables[1:]:
                index = {r.get(on, ""): r for r in table}
                for i, record in enumerate(result):
                    key = record.get(on, "")
                    if key in index:
                        merged = dict(record)
                        merged.update(index[key])
                        result[i] = merged
            return result
        return tables[0] if tables else []

    def _record_key(self, record: dict[str, Any]) -> str:
        return "|".join(f"{k}={v}" for k, v in sorted(record.items()))

    def filter_records(
        self,
        records: list[dict[str, Any]],
        predicate: Callable[[dict[str, Any]], bool],
    ) -> list[dict[str, Any]]:
        """Filter records by predicate function."""
        return [r for r in records if predicate(r)]

    def sort_records(
        self,
        records: list[dict[str, Any]],
        key: str,
        reverse: bool = False,
    ) -> list[dict[str, Any]]:
        """Sort records by field name."""
        return sorted(records, key=lambda r: r.get(key, ""), reverse=reverse)

    def deduplicate_records(
        self,
        records: list[dict[str, Any]],
        key: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Remove duplicate records."""
        if not records:
            return []
        if key:
            seen: set[str] = set()
            result: list[dict[str, Any]] = []
            for record in records:
                val = str(record.get(key, ""))
                if val not in seen:
                    seen.add(val)
                    result.append(record)
            return result
        else:
            seen: set[str] = set()
            result: list[dict[str, Any]] = []
            for record in records:
                k = self._record_key(record)
                if k not in seen:
                    seen.add(k)
                    result.append(record)
            return result

    def aggregate_column(
        self,
        records: list[dict[str, Any]],
        column: str,
        func: str = "sum",
    ) -> float:
        """Aggregate numeric column (sum, avg, min, max, count)."""
        values = []
        for record in records:
            val = record.get(column)
            if val is not None:
                try:
                    values.append(float(val))
                except (TypeError, ValueError):
                    pass
        if not values:
            return 0.0
        if func == "sum":
            return sum(values)
        elif func == "avg":
            return sum(values) / len(values)
        elif func == "min":
            return min(values)
        elif func == "max":
            return max(values)
        elif func == "count":
            return float(len(values))
        return 0.0


if __name__ == "__main__":
    parser = TableParser()
    html = """
    <table>
        <tr><th>Name</th><th>Age</th></tr>
        <tr><td>Alice</td><td>30</td></tr>
        <tr><td>Bob</td><td>25</td></tr>
    </table>
    """
    records = parser.parse_html_table(html)
    print(f"Parsed {len(records)} records")
    print(records)

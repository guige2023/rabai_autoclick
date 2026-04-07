"""CSV data processor action for tabular data manipulation.

This module provides CSV reading, writing, filtering, sorting,
and transformation with support for large files.

Example:
    >>> action = CSVProcessorAction()
    >>> result = action.execute(operation="read", path="/data/file.csv")
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional


@dataclass
class CSVConfig:
    """Configuration for CSV operations."""
    delimiter: str = ","
    quotechar: str = '"'
    encoding: str = "utf-8"
    skip_rows: int = 0
    has_header: bool = True


class CSVProcessorAction:
    """CSV data processing action with filtering and transformation.

    Provides comprehensive CSV manipulation including reading,
    writing, filtering, sorting, and aggregation.

    Example:
        >>> action = CSVProcessorAction()
        >>> result = action.execute(
        ...     operation="filter",
        ...     path="data.csv",
        ...     where={"status": "active"}
        ... )
    """

    def __init__(self, config: Optional[CSVConfig] = None) -> None:
        """Initialize CSV processor.

        Args:
            config: Optional CSV configuration.
        """
        self.config = config or CSVConfig()
        self._data: list[dict[str, Any]] = []
        self._headers: list[str] = []

    def execute(
        self,
        operation: str,
        path: Optional[str] = None,
        data: Optional[str] = None,
        columns: Optional[list[str]] = None,
        where: Optional[dict[str, Any]] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
        limit: Optional[int] = None,
        offset: int = 0,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Execute CSV operation.

        Args:
            operation: Operation name (read, write, filter, sort, etc.).
            path: File path for read/write.
            data: CSV data string.
            columns: Columns to select.
            where: Filter conditions.
            sort_by: Column to sort by.
            sort_order: Sort direction ('asc' or 'desc').
            limit: Maximum rows to return.
            offset: Row offset for pagination.
            **kwargs: Additional parameters.

        Returns:
            Operation result dictionary.

        Raises:
            ValueError: If operation is invalid or data is missing.
        """
        op = operation.lower()
        result: dict[str, Any] = {"operation": op, "success": True}

        if op == "read":
            if not path and not data:
                raise ValueError("path or data required for 'read'")
            result.update(self._read_csv(path, data))
            self._data = result.get("rows", [])
            self._headers = result.get("headers", [])

        elif op == "write":
            if not path:
                raise ValueError("path required for 'write'")
            result.update(self._write_csv(path, kwargs.get("rows", self._data)))

        elif op == "filter":
            if where:
                result["rows"] = self._filter_data(self._data, where)
            else:
                result["rows"] = self._data
            result["count"] = len(result["rows"])

        elif op == "sort":
            if sort_by:
                result["rows"] = self._sort_data(self._data, sort_by, sort_order)
            else:
                result["rows"] = self._data

        elif op == "select":
            if columns:
                result["rows"] = self._select_columns(self._data, columns)
                result["headers"] = columns
            else:
                result["rows"] = self._data

        elif op == "aggregate":
            group_by = kwargs.get("group_by")
            agg_func = kwargs.get("agg_func", "count")
            result["rows"] = self._aggregate_data(self._data, group_by, agg_func)
            result["count"] = len(result["rows"])

        elif op == "join":
            other_path = kwargs.get("other_path")
            left_key = kwargs.get("left_key", "id")
            right_key = kwargs.get("right_key", "id")
            join_type = kwargs.get("join_type", "inner")
            result["rows"] = self._join_data(self._data, other_path, left_key, right_key, join_type)
            result["count"] = len(result["rows"])

        elif op == "pivot":
            row_key = kwargs.get("row_key")
            col_key = kwargs.get("col_key")
            value_key = kwargs.get("value_key")
            result["rows"] = self._pivot_data(self._data, row_key, col_key, value_key)
            result["count"] = len(result["rows"])

        elif op == "deduplicate":
            result["rows"] = self._deduplicate_data(self._data, kwargs.get("subset"))
            result["removed"] = len(self._data) - len(result["rows"])

        elif op == "stats":
            result.update(self._calculate_stats(self._data))

        else:
            raise ValueError(f"Unknown operation: {operation}")

        # Apply limit and offset
        if "rows" in result and (limit or offset):
            result["rows"] = result["rows"][offset:offset + (limit or len(result["rows"]))]
            result["count"] = len(result["rows"])

        return result

    def _read_csv(self, path: Optional[str], data: Optional[str]) -> dict[str, Any]:
        """Read CSV file or string.

        Args:
            path: File path.
            data: CSV string data.

        Returns:
            Result dictionary with headers and rows.
        """
        rows: list[dict[str, Any]] = []
        headers: list[str] = []

        try:
            if path:
                with open(path, "r", encoding=self.config.encoding) as f:
                    reader = csv.reader(f, delimiter=self.config.delimiter)
                    if self.config.skip_rows:
                        for _ in range(self.config.skip_rows):
                            next(reader)
                    if self.config.has_header:
                        headers = next(reader)
                    for row in reader:
                        if len(row) == len(headers):
                            rows.append(dict(zip(headers, row)))
                        elif headers:
                            rows.append({h: v for h, v in zip(headers, row)})
                        else:
                            rows.append({f"col{i}": v for i, v in enumerate(row)})
            elif data:
                reader = csv.reader(io.StringIO(data), delimiter=self.config.delimiter)
                if self.config.has_header:
                    headers = next(reader)
                for row in reader:
                    if headers and len(row) == len(headers):
                        rows.append(dict(zip(headers, row)))
                    else:
                        rows.append({f"col{i}": v for i, v in enumerate(row)})

        except Exception as e:
            return {"success": False, "error": str(e)}

        return {"headers": headers, "rows": rows, "count": len(rows)}

    def _write_csv(self, path: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
        """Write CSV file.

        Args:
            path: Output path.
            rows: Data rows.

        Returns:
            Result dictionary.
        """
        try:
            if not rows:
                return {"success": True, "written": 0}

            headers = list(rows[0].keys())
            with open(path, "w", encoding=self.config.encoding, newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers, delimiter=self.config.delimiter)
                writer.writeheader()
                writer.writerows(rows)

            return {"written": len(rows), "path": path}

        except Exception as e:
            return {"success": False, "error": str(e)}

    def _filter_data(self, rows: list[dict[str, Any]], conditions: dict[str, Any]) -> list[dict[str, Any]]:
        """Filter rows by conditions.

        Args:
            rows: Data rows.
            conditions: Filter conditions.

        Returns:
            Filtered rows.
        """
        result = []
        for row in rows:
            match = True
            for key, value in conditions.items():
                row_value = row.get(key)
                if isinstance(value, str) and "*" in value:
                    if not self._wildcard_match(str(row_value), value):
                        match = False
                        break
                elif str(row_value) != str(value):
                    match = False
                    break
            if match:
                result.append(row)
        return result

    def _wildcard_match(self, text: str, pattern: str) -> bool:
        """Match text with wildcard pattern.

        Args:
            text: Text to match.
            pattern: Wildcard pattern (* matches any).

        Returns:
            True if matches.
        """
        import re
        regex = "^" + pattern.replace("*", ".*") + "$"
        return bool(re.match(regex, text, re.IGNORECASE))

    def _sort_data(
        self,
        rows: list[dict[str, Any]],
        key: str,
        order: str,
    ) -> list[dict[str, Any]]:
        """Sort rows by key.

        Args:
            rows: Data rows.
            key: Sort key.
            order: Sort order.

        Returns:
            Sorted rows.
        """
        reverse = order.lower() == "desc"
        return sorted(rows, key=lambda x: x.get(key, ""), reverse=reverse)

    def _select_columns(self, rows: list[dict[str, Any]], columns: list[str]) -> list[dict[str, Any]]:
        """Select specific columns.

        Args:
            rows: Data rows.
            columns: Columns to select.

        Returns:
            Filtered rows with only selected columns.
        """
        return [{col: row.get(col) for col in columns} for row in rows]

    def _aggregate_data(
        self,
        rows: list[dict[str, Any]],
        group_by: Optional[str],
        agg_func: str,
    ) -> list[dict[str, Any]]:
        """Aggregate data with grouping.

        Args:
            rows: Data rows.
            group_by: Column to group by.
            agg_func: Aggregation function.

        Returns:
            Aggregated results.
        """
        if not group_by:
            return [{"count": len(rows)}]

        groups: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            key = str(row.get(group_by, ""))
            if key not in groups:
                groups[key] = []
            groups[key].append(row)

        result = []
        for key, group_rows in groups.items():
            entry: dict[str, Any] = {group_by: key}
            if agg_func == "count":
                entry["count"] = len(group_rows)
            elif agg_func == "sum":
                for row in group_rows:
                    for k, v in row.items():
                        if k != group_by and isinstance(v, (int, float)):
                            entry[f"{k}_sum"] = entry.get(f"{k}_sum", 0) + v
            result.append(entry)

        return result

    def _join_data(
        self,
        left_rows: list[dict[str, Any]],
        other_path: Optional[str],
        left_key: str,
        right_key: str,
        join_type: str,
    ) -> list[dict[str, Any]]:
        """Join two CSV datasets.

        Args:
            left_rows: Left data rows.
            other_path: Path to right CSV.
            left_key: Left join key.
            right_key: Right join key.
            join_type: Join type (inner, left, right, outer).

        Returns:
            Joined rows.
        """
        if not other_path:
            return left_rows

        right_result = self._read_csv(other_path, None)
        right_rows = right_result.get("rows", [])

        right_index: dict[str, list[dict[str, Any]]] = {}
        for row in right_rows:
            key = str(row.get(right_key, ""))
            if key not in right_index:
                right_index[key] = []
            right_index[key].append(row)

        result = []
        for left_row in left_rows:
            key = str(left_row.get(left_key, ""))
            right_matches = right_index.get(key, [])
            if right_matches:
                for right_row in right_matches:
                    merged = {**right_row, **left_row}
                    result.append(merged)
            elif join_type == "left":
                result.append(left_row)

        return result

    def _pivot_data(
        self,
        rows: list[dict[str, Any]],
        row_key: Optional[str],
        col_key: str,
        value_key: str,
    ) -> list[dict[str, Any]]:
        """Pivot data from rows to columns.

        Args:
            rows: Data rows.
            row_key: Key for row headers.
            col_key: Key for column headers.
            value_key: Key for values.

        Returns:
            Pivoted data.
        """
        if not row_key:
            return rows

        pivot: dict[str, dict[str, Any]] = {}
        columns: set[str] = set()

        for row in rows:
            rk = str(row.get(row_key, ""))
            ck = str(row.get(col_key, ""))
            val = row.get(value_key, 0)

            if rk not in pivot:
                pivot[rk] = {row_key: rk}
            pivot[rk][ck] = val
            columns.add(ck)

        return list(pivot.values())

    def _deduplicate_data(
        self,
        rows: list[dict[str, Any]],
        subset: Optional[list[str]],
    ) -> list[dict[str, Any]]:
        """Remove duplicate rows.

        Args:
            rows: Data rows.
            subset: Columns to consider for duplicates.

        Returns:
            Deduplicated rows.
        """
        seen: set[tuple] = []
        result = []

        for row in rows:
            if subset:
                key = tuple(row.get(k) for k in subset)
            else:
                key = tuple(row.values())

            if key not in seen:
                seen.add(key)
                result.append(row)

        return result

    def _calculate_stats(self, rows: list[dict[str, Any]]) -> dict[str, Any]:
        """Calculate statistics on data.

        Args:
            rows: Data rows.

        Returns:
            Statistics dictionary.
        """
        stats: dict[str, Any] = {
            "total_rows": len(rows),
            "columns": list(rows[0].keys()) if rows else [],
            "total_columns": len(rows[0]) if rows else 0,
        }

        numeric_cols: dict[str, list[float]] = {}
        for row in rows:
            for key, value in row.items():
                try:
                    num = float(value)
                    if key not in numeric_cols:
                        numeric_cols[key] = []
                    numeric_cols[key].append(num)
                except (ValueError, TypeError):
                    pass

        if numeric_cols:
            stats["numeric_columns"] = {}
            for col, values in numeric_cols.items():
                stats["numeric_columns"][col] = {
                    "min": min(values),
                    "max": max(values),
                    "avg": sum(values) / len(values),
                    "sum": sum(values),
                    "count": len(values),
                }

        return stats

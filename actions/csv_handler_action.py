"""
CSV Handler Action Module.

Provides comprehensive CSV reading, writing, filtering, merging,
and transformation utilities with support for large files.

Example:
    >>> from csv_handler_action import CSVHandler
    >>> handler = CSVHandler()
    >>> records = handler.read("/tmp/data.csv")
    >>> handler.filter(records, {"status": "active"})
    >>> handler.to_csv(records, "/tmp/output.csv")
"""
from __future__ import annotations

import csv
import io
import os
from dataclasses import dataclass
from typing import Any, Callable, Generator, Optional


@dataclass
class CSVConfig:
    """CSV read/write configuration."""
    delimiter: str = ","
    quotechar: str = '"'
    encoding: str = "utf-8"
    skip_rows: int = 0
    headers: Optional[list[str]] = None
    auto_headers: bool = True


class CSVHandler:
    """Handle CSV operations: read, write, filter, merge, transform."""

    def read(
        self,
        path: str,
        config: Optional[CSVConfig] = None,
    ) -> list[dict[str, Any]]:
        """
        Read CSV file to list of dictionaries.

        Args:
            path: CSV file path
            config: CSVConfig with delimiter, encoding, etc.

        Returns:
            List of row dictionaries
        """
        config = config or CSVConfig()
        records: list[dict[str, Any]] = []

        try:
            with open(path, "r", encoding=config.encoding, errors="replace") as f:
                reader = csv.DictReader(
                    f,
                    delimiter=config.delimiter,
                    quotechar=config.quotechar,
                )
                if config.headers:
                    reader.fieldnames = config.headers
                for row in reader:
                    records.append(dict(row))
        except FileNotFoundError:
            return []
        except Exception:
            return []

        return records[config.skip_rows:]

    def read_string(
        self,
        csv_text: str,
        config: Optional[CSVConfig] = None,
    ) -> list[dict[str, Any]]:
        """Parse CSV from string."""
        config = config or CSVConfig()
        records: list[dict[str, Any]] = []

        reader = csv.DictReader(
            io.StringIO(csv_text),
            delimiter=config.delimiter,
            quotechar=config.quotechar,
        )
        if config.headers:
            reader.fieldnames = config.headers
        for row in reader:
            records.append(dict(row))

        return records[config.skip_rows:]

    def write(
        self,
        records: list[dict[str, Any]],
        path: str,
        config: Optional[CSVConfig] = None,
    ) -> bool:
        """
        Write records to CSV file.

        Args:
            records: List of dictionaries
            path: Output file path
            config: CSVConfig with delimiter, etc.

        Returns:
            True on success
        """
        if not records:
            return False

        config = config or CSVConfig()

        try:
            with open(path, "w", encoding=config.encoding, newline="") as f:
                fieldnames = list(records[0].keys())
                writer = csv.DictWriter(
                    f,
                    fieldnames=fieldnames,
                    delimiter=config.delimiter,
                    quotechar=config.quotechar,
                    extrasaction="ignore",
                )
                writer.writeheader()
                writer.writerows(records)
            return True
        except Exception:
            return False

    def write_string(
        self,
        records: list[dict[str, Any]],
        config: Optional[CSVConfig] = None,
    ) -> str:
        """Convert records to CSV string."""
        if not records:
            return ""
        config = config or CSVConfig()
        output = io.StringIO()
        fieldnames = list(records[0].keys())
        writer = csv.DictWriter(
            output,
            fieldnames=fieldnames,
            delimiter=config.delimiter,
            quotechar=config.quotechar,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(records)
        return output.getvalue()

    def filter(
        self,
        records: list[dict[str, Any]],
        conditions: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """
        Filter records by field conditions.

        Args:
            records: Input records
            conditions: Dict of field->value or field->callable

        Returns:
            Filtered records
        """
        result: list[dict[str, Any]] = []
        for record in records:
            match = True
            for field, expected in conditions.items():
                value = record.get(field)
                if callable(expected):
                    if not expected(value):
                        match = False
                        break
                elif value != expected:
                    match = False
                    break
            if match:
                result.append(record)
        return result

    def sort(
        self,
        records: list[dict[str, Any]],
        key: str,
        reverse: bool = False,
    ) -> list[dict[str, Any]]:
        """Sort records by field."""
        return sorted(records, key=lambda r: r.get(key, ""), reverse=reverse)

    def merge(
        self,
        left: list[dict[str, Any]],
        right: list[dict[str, Any]],
        on: str,
        how: str = "inner",
    ) -> list[dict[str, Any]]:
        """
        Merge two CSV record sets.

        Args:
            left: Left records
            right: Right records
            on: Join key field
            how: inner, left, right, full

        Returns:
            Merged records
        """
        right_index: dict[str, list[dict[str, Any]]] = {}
        for r in right:
            k = str(r.get(on, ""))
            if k not in right_index:
                right_index[k] = []
            right_index[k].append(r)

        result: list[dict[str, Any]] = []
        matched_right: set[int] = set()

        for i, l in enumerate(left):
            k = str(l.get(on, ""))
            if k in right_index:
                for r in right_index[k]:
                    result.append({**l, **r})
                    matched_right.add(id(r))
            elif how in ("left", "full"):
                result.append({**l})

        if how in ("right", "full"):
            for r in right:
                if id(r) not in matched_right:
                    result.append({**r})

        return result

    def deduplicate(
        self,
        records: list[dict[str, Any]],
        key: Optional[str] = None,
        keep: str = "first",
    ) -> list[dict[str, Any]]:
        """
        Remove duplicate records.

        Args:
            records: Input records
            key: Field to use for dedup (None = entire row)
            keep: first or last

        Returns:
            Deduplicated records
        """
        if keep == "last":
            records = list(reversed(records))

        seen: set[str] = set()
        result: list[dict[str, Any]] = []

        for record in records:
            if key:
                k = str(record.get(key, ""))
            else:
                k = "|".join(str(v) for v in record.values())
            if k not in seen:
                seen.add(k)
                result.append(record)

        if keep == "last":
            result = list(reversed(result))

        return result

    def aggregate(
        self,
        records: list[dict[str, Any]],
        group_by: str,
        agg_funcs: dict[str, str],
    ) -> list[dict[str, Any]]:
        """
        Aggregate records by grouping field.

        Args:
            records: Input records
            group_by: Field to group by
            agg_funcs: Map of field -> function (sum, avg, count, min, max, first, last)

        Returns:
            Aggregated records
        """
        groups: dict[str, dict[str, list[Any]]] = {}

        for record in records:
            key = str(record.get(group_by, ""))
            if key not in groups:
                groups[key] = {field: [] for field in agg_funcs}
            for field, func in agg_funcs.items():
                val = record.get(field)
                groups[key][field].append(val)

        result: list[dict[str, Any]] = []
        for key, field_groups in groups.items():
            agg_record: dict[str, Any] = {group_by: key}
            for field, func in agg_funcs.items():
                values = [v for v in field_groups[field] if v is not None]
                if not values:
                    agg_record[field] = None
                elif func == "sum":
                    try:
                        agg_record[field] = sum(float(v) for v in values)
                    except (ValueError, TypeError):
                        agg_record[field] = values[0]
                elif func == "avg":
                    try:
                        agg_record[field] = sum(float(v) for v in values) / len(values)
                    except (ValueError, TypeError):
                        agg_record[field] = values[0]
                elif func == "count":
                    agg_record[field] = len(values)
                elif func == "min":
                    try:
                        agg_record[field] = min(float(v) for v in values)
                    except (ValueError, TypeError):
                        agg_record[field] = values[0]
                elif func == "max":
                    try:
                        agg_record[field] = max(float(v) for v in values)
                    except (ValueError, TypeError):
                        agg_record[field] = values[0]
                elif func == "first":
                    agg_record[field] = values[0]
                elif func == "last":
                    agg_record[field] = values[-]
                else:
                    agg_record[field] = values
            result.append(agg_record)
        return result

    def pivot(
        self,
        records: list[dict[str, Any]],
        index: str,
        columns: str,
        values: str,
        agg_func: str = "sum",
    ) -> list[dict[str, Any]]:
        """Pivot records from long to wide format."""
        pivot: dict[str, dict[str, Any]] = {}
        for record in records:
            idx_val = str(record.get(index, ""))
            col_val = str(record.get(columns, ""))
            val = record.get(values)
            if idx_val not in pivot:
                pivot[idx_val] = {index: idx_val}
            existing = pivot[idx_val].get(col_val)
            if existing is None:
                pivot[idx_val][col_val] = val
            else:
                try:
                    if agg_func == "sum":
                        pivot[idx_val][col_val] = float(existing) + float(val)
                    elif agg_func == "count":
                        pivot[idx_val][col_val] = existing + 1
                    elif agg_func == "avg":
                        pivot[idx_val][col_val] = (float(existing) + float(val)) / 2
                except (ValueError, TypeError):
                    pivot[idx_val][col_val] = val
        return list(pivot.values())

    def unpivot(
        self,
        records: list[dict[str, Any]],
        id_cols: list[str],
        value_name: str = "variable",
        label_name: str = "value",
    ) -> list[dict[str, Any]]:
        """Unpivot records from wide to long format."""
        result: list[dict[str, Any]] = []
        for record in records:
            id_vals = {col: record.get(col) for col in id_cols}
            for col, val in record.items():
                if col not in id_cols:
                    new_record = dict(id_vals)
                    new_record[value_name] = col
                    new_record[label_name] = val
                    result.append(new_record)
        return result

    def transpose(
        self,
        records: list[dict[str, Any]],
        header_field: str = "field",
    ) -> list[dict[str, Any]]:
        """Transpose rows to columns."""
        if not records:
            return []
        fields = list(records[0].keys())
        result: list[dict[str, Any]] = []
        for field in fields:
            new_record: dict[str, Any] = {header_field: field}
            for record in records:
                new_record[record.get(header_field, "") or f"row_{records.index(record)}"] = record.get(field)
            result.append(new_record)
        return result

    def sample(
        self,
        records: list[dict[str, Any]],
        n: int = 10,
        random_seed: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """Random sample of records."""
        import random
        if random_seed is not None:
            random.seed(random_seed)
        return random.sample(records, min(n, len(records)))

    def chunk(
        self,
        records: list[dict[str, Any]],
        size: int,
    ) -> Generator[list[dict[str, Any]], None, None]:
        """Yield records in chunks."""
        for i in range(0, len(records), size):
            yield records[i:i + size]

    def stats(
        self,
        records: list[dict[str, Any]],
        field: str,
    ) -> dict[str, float]:
        """Compute statistics for a numeric field."""
        values = []
        for record in records:
            try:
                values.append(float(record.get(field, 0)))
            except (ValueError, TypeError):
                pass
        if not values:
            return {"count": 0}
        return {
            "count": len(values),
            "sum": sum(values),
            "avg": sum(values) / len(values),
            "min": min(values),
            "max": max(values),
        }


if __name__ == "__main__":
    handler = CSVHandler()
    records = [
        {"name": "Alice", "age": 30, "city": "NYC"},
        {"name": "Bob", "age": 25, "city": "LA"},
        {"name": "Alice", "age": 30, "city": "NYC"},
    ]
    csv_str = handler.write_string(records)
    print("CSV output:")
    print(csv_str)
    print("Stats:")
    print(handler.stats(records, "age"))

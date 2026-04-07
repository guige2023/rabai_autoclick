"""CSV and data serialization utilities: read, write, transform, and validate."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from typing import Any, Callable, Generator, TextIO

__all__ = [
    "CSVReader",
    "CSVWriter",
    "CSVConfig",
    "parse_csv",
    "to_csv",
    "transform_csv",
]


@dataclass
class CSVConfig:
    """Configuration for CSV operations."""
    delimiter: str = ","
    quotechar: str = '"'
    lineterminator: str = "\n"
    has_header: bool = True
    encoding: str = "utf-8"
    skip_rows: int = 0


class CSVReader:
    """Streaming CSV reader with type coercion."""

    def __init__(
        self,
        source: str | TextIO,
        config: CSVConfig | None = None,
    ) -> None:
        self._source = source
        self._config = config or CSVConfig()
        self._reader: csv.DictReader | None = None
        self._headers: list[str] = []

    def _ensure_reader(self) -> None:
        if self._reader is not None:
            return
        if isinstance(self._source, str):
            f = open(self._source, newline="", encoding=self._config.encoding)
            self._source = f
        else:
            f = self._source
        if self._config.skip_rows > 0:
            for _ in range(self._config.skip_rows):
                next(f)
        self._reader = csv.DictReader(
            f,
            delimiter=self._config.delimiter,
            quotechar=self._config.quotechar,
            lineterminator=self._config.lineterminator,
        )
        if self._config.has_header:
            self._headers = list(self._reader.fieldnames or [])

    def headers(self) -> list[str]:
        self._ensure_reader()
        return self._headers

    def rows(self) -> Generator[dict[str, str], None, None]:
        self._ensure_reader()
        if self._reader is None:
            return
        for row in self._reader:
            yield dict(row)

    def as_dicts(self) -> list[dict[str, str]]:
        return list(self.rows())

    def as_records(self, types: dict[str, Callable[[str], Any]] | None = None) -> list[dict[str, Any]]:
        """Parse rows with type coercion."""
        records: list[dict[str, Any]] = []
        for row in self.rows():
            record: dict[str, Any] = {}
            for key, value in row.items():
                if types and key in types:
                    try:
                        record[key] = types[key](value)
                    except Exception:
                        record[key] = value
                else:
                    record[key] = value
            records.append(record)
        return records


class CSVWriter:
    """CSV writer with flexible output."""

    def __init__(
        self,
        output: str | TextIO | None = None,
        config: CSVConfig | None = None,
    ) -> None:
        self._output = output
        self._config = config or CSVConfig()
        self._writer: csv.DictWriter | None = None
        self._headers: list[str] = []
        self._file_handle: TextIO | None = None

    def _ensure_writer(self) -> None:
        if self._writer is not None:
            return
        if isinstance(self._output, str):
            self._file_handle = open(self._output, "w", newline="", encoding=self._config.encoding)
            output: TextIO = self._file_handle
        else:
            output = self._output or io.StringIO()
            if self._output is None:
                self._output = output
        self._writer = csv.DictWriter(
            output,
            fieldnames=self._headers,
            delimiter=self._config.delimiter,
            quotechar=self._config.quotechar,
            lineterminator=self._config.lineterminator,
            write_header=self._config.has_header,
        )

    def write_header(self, headers: list[str]) -> None:
        self._headers = headers
        self._ensure_writer()

    def writerow(self, row: dict[str, Any]) -> None:
        self._ensure_writer()
        if self._writer:
            self._writer.writerow({k: row.get(k, "") for k in self._headers})

    def writerows(self, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            self.writerow(row)

    def close(self) -> None:
        if self._file_handle:
            self._file_handle.close()

    @classmethod
    def write_string(
        cls,
        rows: list[dict[str, Any]],
        headers: list[str],
        config: CSVConfig | None = None,
    ) -> str:
        output = io.StringIO()
        writer = CSVWriter(output, config)
        writer.write_header(headers)
        writer.writerows(rows)
        return output.getvalue()


def parse_csv(
    source: str | TextIO,
    config: CSVConfig | None = None,
    types: dict[str, Callable[[str], Any]] | None = None,
) -> list[dict[str, Any]]:
    """Parse CSV from file or string into list of dicts."""
    reader = CSVReader(source, config)
    return reader.as_records(types)


def to_csv(
    rows: list[dict[str, Any]],
    headers: list[str] | None = None,
    output: str | TextIO | None = None,
    config: CSVConfig | None = None,
) -> str | None:
    """Convert list of dicts to CSV."""
    if headers is None and rows:
        headers = list(rows[0].keys())
    if headers is None:
        return ""

    writer = CSVWriter(output, config)
    writer.write_header(headers)
    writer.writerows(rows)
    if isinstance(output, str):
        return None
    if hasattr(output, "getvalue"):
        return output.getvalue()
    return ""


def transform_csv(
    input_source: str | TextIO,
    output_dest: str | TextIO,
    transform_fn: Callable[[dict[str, Any]], dict[str, Any] | None],
    output_headers: list[str],
    config: CSVConfig | None = None,
) -> int:
    """Transform CSV rows one by one. Returns count of transformed rows."""
    reader = CSVReader(input_source, config)
    writer = CSVWriter(output_dest, config)
    writer.write_header(output_headers)
    count = 0
    for row in reader.rows():
        result = transform_fn(dict(row))
        if result is not None:
            writer.writerow({k: result.get(k, "") for k in output_headers})
            count += 1
    writer.close()
    return count

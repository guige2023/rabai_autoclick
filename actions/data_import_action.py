"""Data import action module.

Provides data import from various formats (CSV, JSON, Excel, XML).
Supports streaming import for large files and schema inference.
"""

from __future__ import annotations

import csv
import json
import logging
from typing import Optional, Dict, Any, List, Union, Callable, Iterator
from pathlib import Path
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class ImportOptions:
    """Options for data import."""
    encoding: str = "utf-8"
    delimiter: str = ","
    has_header: bool = True
    skip_rows: int = 0
    max_rows: Optional[int] = None
    column_types: Optional[Dict[str, type]] = None
    null_values: List[str] = field(default_factory=lambda: ["", "NA", "null", "None"])
    sampling_fraction: float = 1.0


from dataclasses import field


class DataImportAction:
    """Data import engine.

    Imports data from various file formats into lists of dicts.

    Example:
        importer = DataImportAction()
        data = importer.from_csv("/tmp/data.csv")
        data = importer.from_jsonl("/tmp/data.jsonl")
    """

    def from_csv(
        self,
        path: Union[str, Path],
        options: Optional[ImportOptions] = None,
    ) -> List[Dict[str, Any]]:
        """Import data from CSV file.

        Args:
            path: Input file path.
            options: Import options.

        Returns:
            List of dicts.
        """
        opts = options or ImportOptions()
        rows = []

        with open(path, "r", encoding=opts.encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=opts.delimiter)

            for i, row in enumerate(reader):
                if opts.skip_rows and i < opts.skip_rows:
                    continue
                if opts.max_rows and len(rows) >= opts.max_rows:
                    break

                cleaned = self._clean_row(row, opts)
                rows.append(cleaned)

        logger.info("Imported %d rows from %s", len(rows), path)
        return rows

    def from_json(
        self,
        path: Union[str, Path],
        options: Optional[ImportOptions] = None,
    ) -> List[Dict[str, Any]]:
        """Import data from JSON file.

        Args:
            path: Input file path.
            options: Import options.

        Returns:
            List of dicts.
        """
        opts = options or ImportOptions()

        with open(path, "r", encoding=opts.encoding) as f:
            data = json.load(f)

        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            rows = [data]
        else:
            rows = [{"value": data}]

        if opts.max_rows:
            rows = rows[:opts.max_rows]

        logger.info("Imported %d rows from %s", len(rows), path)
        return rows

    def from_jsonl(
        self,
        path: Union[str, Path],
        options: Optional[ImportOptions] = None,
    ) -> List[Dict[str, Any]]:
        """Import data from JSON Lines file.

        Args:
            path: Input file path.
            options: Import options.

        Returns:
            List of dicts.
        """
        opts = options or ImportOptions()
        rows = []

        with open(path, "r", encoding=opts.encoding) as f:
            for i, line in enumerate(f):
                if opts.skip_rows and i < opts.skip_rows:
                    continue
                if opts.max_rows and len(rows) >= opts.max_rows:
                    break

                line = line.strip()
                if line:
                    rows.append(json.loads(line))

        logger.info("Imported %d rows from %s", len(rows), path)
        return rows

    def from_tsv(
        self,
        path: Union[str, Path],
        options: Optional[ImportOptions] = None,
    ) -> List[Dict[str, Any]]:
        """Import data from TSV file."""
        opts = (options or ImportOptions())
        opts.delimiter = "\t"
        return self.from_csv(path, opts)

    def stream_csv(
        self,
        path: Union[str, Path],
        options: Optional[ImportOptions] = None,
        chunk_size: int = 1000,
    ) -> Iterator[List[Dict[str, Any]]]:
        """Stream import CSV in chunks.

        Args:
            path: Input file path.
            options: Import options.
            chunk_size: Rows per chunk.

        Yields:
            Chunks of dicts.
        """
        opts = options or ImportOptions()
        chunk = []

        with open(path, "r", encoding=opts.encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=opts.delimiter)

            for i, row in enumerate(reader):
                if opts.skip_rows and i < opts.skip_rows:
                    continue

                cleaned = self._clean_row(row, opts)
                chunk.append(cleaned)

                if len(chunk) >= chunk_size:
                    yield chunk
                    chunk = []

            if chunk:
                yield chunk

    def infer_schema(
        self,
        data: List[Dict[str, Any]],
        sample_size: int = 100,
    ) -> Dict[str, str]:
        """Infer column types from data.

        Args:
            data: List of dicts.
            sample_size: Number of rows to sample.

        Returns:
            Dict mapping field names to type names.
        """
        if not data:
            return {}

        sample = data[:sample_size]
        schema = {}

        for key in data[0].keys():
            type_counts = {"int": 0, "float": 0, "bool": 0, "str": 0}

            for row in sample:
                val = row.get(key)
                if val is None or str(val) in ["", "NA", "null", "None"]:
                    continue

                val_str = str(val).strip().lower()
                if val_str in ["true", "false"]:
                    type_counts["bool"] += 1
                elif self._is_int(val):
                    type_counts["int"] += 1
                elif self._is_float(val):
                    type_counts["float"] += 1
                else:
                    type_counts["str"] += 1

            if type_counts["int"] > len(sample) * 0.8:
                schema[key] = "int"
            elif type_counts["float"] > len(sample) * 0.8:
                schema[key] = "float"
            elif type_counts["bool"] > len(sample) * 0.8:
                schema[key] = "bool"
            else:
                schema[key] = "str"

        return schema

    def _clean_row(self, row: Dict[str, Any], opts: ImportOptions) -> Dict[str, Any]:
        """Clean a row by handling nulls and type conversion."""
        cleaned = {}
        for key, value in row.items():
            if value in opts.null_values:
                cleaned[key] = None
            else:
                cleaned[key] = value

        if opts.column_types:
            for col, col_type in opts.column_types.items():
                if col in cleaned and cleaned[col] is not None:
                    try:
                        if col_type == int:
                            cleaned[col] = int(cleaned[col])
                        elif col_type == float:
                            cleaned[col] = float(cleaned[col])
                        elif col_type == bool:
                            cleaned[col] = str(cleaned[col]).lower() in ["true", "1", "yes"]
                    except (ValueError, TypeError):
                        pass

        return cleaned

    def _is_int(self, val: Any) -> bool:
        """Check if value is an integer."""
        try:
            int(val)
            return "." not in str(val)
        except (ValueError, TypeError):
            return False

    def _is_float(self, val: Any) -> bool:
        """Check if value is a float."""
        try:
            float(val)
            return True
        except (ValueError, TypeError):
            return False

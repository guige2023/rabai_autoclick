"""CSV processing and manipulation utilities.

Handles CSV reading, writing, parsing, validation,
column mapping, and data transformation.
"""

from typing import Any, Optional, Callable, Iterator
import logging
from dataclasses import dataclass, field
from datetime import datetime
from io import StringIO
import csv
import json

logger = logging.getLogger(__name__)


@dataclass
class CSVConfig:
    """Configuration for CSV processing."""
    delimiter: str = ","
    quotechar: str = '"'
    lineterminator: str = "\n"
    encoding: str = "utf-8"
    skipinitialspace: bool = True
    strict: bool = False
    header: bool = True
    skip_rows: int = 0


@dataclass
class CSVRow:
    """Represents a single CSV row."""
    row_number: int
    data: dict
    is_valid: bool = True
    errors: list[str] = field(default_factory=list)


@dataclass
class CSVSchema:
    """Schema definition for CSV validation."""
    columns: list[str]
    required: list[str] = field(default_factory=list)
    types: dict[str, str] = field(default_factory=dict)
    defaults: dict[str, Any] = field(default_factory=dict)
    max_length: dict[str, int] = field(default_factory=dict)


class CSVParseError(Exception):
    """Raised on CSV parsing errors."""
    pass


class CSVValidationError(Exception):
    """Raised on CSV validation errors."""
    pass


class CSVAction:
    """CSV processing utilities."""

    def __init__(self, config: Optional[CSVConfig] = None):
        """Initialize CSV processor with configuration.

        Args:
            config: CSVConfig with processing options
        """
        self.config = config or CSVConfig()

    def parse_string(self, csv_string: str,
                    header: Optional[list[str]] = None) -> list[dict]:
        """Parse CSV from string.

        Args:
            csv_string: CSV content as string
            header: Optional explicit header list

        Returns:
            List of row dicts

        Raises:
            CSVParseError: On parse failure
        """
        try:
            reader = self._get_reader(csv_string, header)
            return list(reader)

        except csv.Error as e:
            raise CSVParseError(f"Parse failed: {e}")

    def parse_file(self, file_path: str,
                   header: Optional[list[str]] = None) -> list[dict]:
        """Parse CSV from file.

        Args:
            file_path: Path to CSV file
            header: Optional explicit header list

        Returns:
            List of row dicts
        """
        try:
            with open(file_path, "r", encoding=self.config.encoding) as f:
                reader = self._get_reader(f, header)
                return list(reader)

        except csv.Error as e:
            raise CSVParseError(f"Parse failed: {e}")
        except IOError as e:
            raise CSVParseError(f"File read failed: {e}")

    def write_string(self, data: list[dict],
                    header: Optional[list[str]] = None,
                    **kwargs) -> str:
        """Write data to CSV string.

        Args:
            data: List of row dicts
            header: Optional explicit header (auto-detect if None)
            **kwargs: Override config options

        Returns:
            CSV string
        """
        if not data:
            return ""

        output = StringIO()

        if header is None:
            header = list(data[0].keys()) if data else []

        config = self._get_config(kwargs)
        writer = csv.DictWriter(
            output,
            fieldnames=header,
            delimiter=config.delimiter,
            quotechar=config.quotechar,
            lineterminator=config.lineterminator,
            skipinitialspace=config.skipinitialspace,
            strict=config.strict,
            quoting=csv.QUOTE_MINIMAL
        )

        if config.header:
            writer.writeheader()

        for row in data:
            writer.writerow(row)

        return output.getvalue()

    def write_file(self, file_path: str,
                   data: list[dict],
                   header: Optional[list[str]] = None,
                   **kwargs) -> bool:
        """Write data to CSV file.

        Args:
            file_path: Output file path
            data: List of row dicts
            header: Optional explicit header
            **kwargs: Override config options

        Returns:
            True if successful
        """
        try:
            content = self.write_string(data, header, **kwargs)

            with open(file_path, "w", encoding=self.config.encoding) as f:
                f.write(content)

            return True

        except Exception as e:
            logger.error(f"Write file failed: {e}")
            return False

    def read_streaming(self, file_path: str,
                      header: Optional[list[str]] = None) -> Iterator[dict]:
        """Stream-parse CSV file row by row.

        Args:
            file_path: Path to CSV file
            header: Optional explicit header list

        Yields:
            Row dicts one at a time
        """
        try:
            with open(file_path, "r", encoding=self.config.encoding) as f:
                reader = self._get_reader(f, header)
                for row in reader:
                    yield row

        except csv.Error as e:
            raise CSVParseError(f"Parse failed: {e}")

    def validate_schema(self, data: list[dict],
                      schema: CSVSchema) -> tuple[list[CSVRow], list[CSVRow]]:
        """Validate CSV data against schema.

        Args:
            data: List of row dicts
            schema: CSVSchema to validate against

        Returns:
            Tuple of (valid_rows, invalid_rows)
        """
        valid_rows = []
        invalid_rows = []

        for i, row in enumerate(data):
            csv_row = CSVRow(row_number=i + 1, data=row)
            errors = []

            for col in schema.required:
                if col not in row or row[col] is None or row[col] == "":
                    errors.append(f"Missing required column: {col}")

            for col, expected_type in schema.types.items():
                if col in row and row[col]:
                    if not self._check_type(row[col], expected_type):
                        errors.append(f"Invalid type for {col}: expected {expected_type}")

            for col, max_len in schema.max_length.items():
                if col in row and row[col] and len(str(row[col])) > max_len:
                    errors.append(f"{col} exceeds max length {max_len}")

            csv_row.is_valid = len(errors) == 0
            csv_row.errors = errors

            if csv_row.is_valid:
                valid_rows.append(csv_row)
            else:
                invalid_rows.append(csv_row)

        return valid_rows, invalid_rows

    def transform_columns(self, data: list[dict],
                         transforms: dict[str, Callable]) -> list[dict]:
        """Apply transformations to columns.

        Args:
            data: List of row dicts
            transforms: Dict mapping column -> transform function

        Returns:
            Transformed data
        """
        result = []

        for row in data:
            new_row = dict(row)
            for col, func in transforms.items():
                if col in new_row:
                    try:
                        new_row[col] = func(new_row[col])
                    except Exception as e:
                        logger.warning(f"Transform failed for {col}: {e}")
            result.append(new_row)

        return result

    def filter_rows(self, data: list[dict],
                   predicate: Callable[[dict], bool]) -> list[dict]:
        """Filter rows by predicate.

        Args:
            data: List of row dicts
            predicate: Function that returns True to keep row

        Returns:
            Filtered data
        """
        return [row for row in data if predicate(row)]

    def select_columns(self, data: list[dict],
                      columns: list[str]) -> list[dict]:
        """Select only specified columns.

        Args:
            data: List of row dicts
            columns: List of columns to keep

        Returns:
            Filtered data with only specified columns
        """
        result = []

        for row in data:
            new_row = {col: row.get(col) for col in columns if col in row}
            result.append(new_row)

        return result

    def rename_columns(self, data: list[dict],
                      mapping: dict[str, str]) -> list[dict]:
        """Rename columns.

        Args:
            data: List of row dicts
            mapping: Dict mapping old_name -> new_name

        Returns:
            Data with renamed columns
        """
        result = []

        for row in data:
            new_row = {}
            for key, value in row.items():
                new_key = mapping.get(key, key)
                new_row[new_key] = value
            result.append(new_row)

        return result

    def sort_rows(self, data: list[dict],
                 key: str, reverse: bool = False) -> list[dict]:
        """Sort rows by column.

        Args:
            data: List of row dicts
            key: Column to sort by
            reverse: Sort descending if True

        Returns:
            Sorted data
        """
        return sorted(data, key=lambda row: row.get(key, ""), reverse=reverse)

    def deduplicate(self, data: list[dict],
                   keys: list[str]) -> list[dict]:
        """Remove duplicate rows based on key columns.

        Args:
            data: List of row dicts
            keys: Columns to check for duplicates

        Returns:
            Deduplicated data (first occurrence kept)
        """
        seen = set()
        result = []

        for row in data:
            key_values = tuple(row.get(k) for k in keys)
            if key_values not in seen:
                seen.add(key_values)
                result.append(row)

        return result

    def merge_csv(self, left_data: list[dict],
                 right_data: list[dict],
                 left_key: str,
                 right_key: str,
                 how: str = "inner") -> list[dict]:
        """Merge two CSV datasets.

        Args:
            left_data: Left dataset
            right_data: Right dataset
            left_key: Join key for left
            right_key: Join key for right
            how: Join type (inner, left, right, outer)

        Returns:
            Merged data
        """
        right_index = {row.get(right_key): row for row in right_data}
        result = []

        left_keys = set(row.get(left_key) for row in left_data)
        right_keys = set(right_index.keys())

        for row in left_data:
            lkey = row.get(left_key)

            if lkey in right_index:
                merged = {**row, **right_index[lkey]}
                result.append(merged)
            elif how in ("left", "outer"):
                result.append(row)

        if how in ("right", "outer"):
            for row in right_data:
                rkey = row.get(right_key)
                if rkey not in left_keys:
                    result.append(row)

        return result

    def aggregate(self, data: list[dict],
                 group_by: list[str],
                 aggregations: dict[str, Callable]) -> list[dict]:
        """Group and aggregate CSV data.

        Args:
            data: List of row dicts
            group_by: Columns to group by
            aggregations: Dict mapping output_col -> aggregation function

        Returns:
            Aggregated data
        """
        groups: dict[tuple, list[dict]] = {}

        for row in data:
            key = tuple(row.get(col) for col in group_by)
            if key not in groups:
                groups[key] = []
            groups[key].append(row)

        result = []

        for key, rows in groups.items():
            aggregated = {col: key[i] for i, col in enumerate(group_by)}

            for col, func in aggregations.items():
                values = [row.get(col) for row in rows if col in row]
                aggregated[col] = func(values)

            result.append(aggregated)

        return result

    def pivot(self, data: list[dict],
             index: str,
             columns: str,
             values: str) -> list[dict]:
        """Pivot CSV data.

        Args:
            data: List of row dicts
            index: Column to use as index
            columns: Column to pivot on
            values: Column with values

        Returns:
            Pivoted data
        """
        pivot_map: dict[tuple, dict] = {}

        for row in data:
            idx_val = row.get(index)
            col_val = row.get(columns)
            val = row.get(values)

            if idx_val not in pivot_map:
                pivot_map[idx_val] = {index: idx_val}

            pivot_map[idx_val][col_val] = val

        return list(pivot_map.values())

    def unpivot(self, data: list[dict],
               id_columns: list[str],
               value_columns: list[str]) -> list[dict]:
        """Unpivot/melt CSV data.

        Args:
            data: List of row dicts
            id_columns: Columns to keep as-is
            value_columns: Columns to unpivot

        Returns:
            Unpivoted data
        """
        result = []

        for row in data:
            for val_col in value_columns:
                new_row = {col: row.get(col) for col in id_columns}
                new_row["column"] = val_col
                new_row["value"] = row.get(val_col)
                result.append(new_row)

        return result

    def to_json(self, data: list[dict],
               output_path: Optional[str] = None,
               indent: int = 2) -> str:
        """Convert CSV data to JSON.

        Args:
            data: List of row dicts
            output_path: Optional path to write JSON file
            indent: JSON indentation

        Returns:
            JSON string
        """
        json_str = json.dumps(data, indent=indent, ensure_ascii=False)

        if output_path:
            with open(output_path, "w", encoding=self.config.encoding) as f:
                f.write(json_str)

        return json_str

    def from_json(self, json_input: str) -> list[dict]:
        """Parse JSON to CSV data format.

        Args:
            json_input: JSON string or file path

        Returns:
            List of row dicts
        """
        if json_input.startswith("{") or json_input.startswith("["):
            return json.loads(json_input)
        else:
            with open(json_input, "r", encoding=self.config.encoding) as f:
                return json.load(f)

    def _get_reader(self, source, header: Optional[list[str]] = None):
        """Create CSV reader with config."""
        config = self.config

        reader_kwargs = {
            "delimiter": config.delimiter,
            "quotechar": config.quotechar,
            "lineterminator": config.lineterminator,
            "skipinitialspace": config.skipinitialspace,
            "strict": config.strict,
        }

        if header is not None:
            reader_kwargs["fieldnames"] = header
            reader_kwargs["header"] = True

        reader = csv.DictReader(source, **reader_kwargs)
        return reader

    def _get_config(self, overrides: dict) -> CSVConfig:
        """Get config with overrides applied."""
        config_dict = {
            "delimiter": self.config.delimiter,
            "quotechar": self.config.quotechar,
            "lineterminator": self.config.lineterminator,
            "encoding": self.config.encoding,
            "skipinitialspace": self.config.skipinitialspace,
            "strict": self.config.strict,
            "header": self.config.header,
        }
        config_dict.update(overrides)
        return CSVConfig(**config_dict)

    def _check_type(self, value: Any, expected: str) -> bool:
        """Check if value matches expected type."""
        if expected == "string":
            return True
        elif expected == "number":
            try:
                float(value)
                return True
            except (ValueError, TypeError):
                return False
        elif expected == "integer":
            try:
                int(value)
                return True
            except (ValueError, TypeError):
                return False
        elif expected == "boolean":
            return str(value).lower() in ("true", "false", "1", "0", "yes", "no")
        elif expected == "date":
            try:
                datetime.fromisoformat(str(value))
                return True
            except ValueError:
                return False
        return True

"""
Data Processing and Transformation Action Module.

Provides data cleaning, normalization, deduplication, aggregation,
schema validation, and format conversion utilities.

Example:
    >>> from data_transform_action import DataTransformer, TransformConfig
    >>> transformer = DataTransformer()
    >>> cleaned = transformer.clean_records(records, remove_nulls=True)
    >>> normalized = transformer.normalize_fields(cleaned, {"name": "title_case"})
"""
from __future__ import annotations

import re
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Optional, TypeVar, Generic


T = TypeVar("T")


@dataclass
class TransformConfig:
    """Configuration for data transformation operations."""
    remove_nulls: bool = False
    trim_strings: bool = True
    lowercase_keys: bool = True
    dedupe: bool = False
    dedupe_key: Optional[str] = None
    sort_by: Optional[str] = None
    reverse_sort: bool = False


@dataclass
class ProcessingStats:
    """Statistics from a transformation operation."""
    input_count: int = 0
    output_count: int = 0
    nulls_removed: int = 0
    duplicates_removed: int = 0
    errors: list[str] = field(default_factory=list)


class DataTransformer:
    """Transform and clean structured data records."""

    def __init__(self):
        self._stats = ProcessingStats()

    def clean_records(
        self,
        records: list[dict[str, Any]],
        config: Optional[TransformConfig] = None,
    ) -> list[dict[str, Any]]:
        """
        Clean a list of records with configurable rules.

        Args:
            records: Input list of dictionaries
            config: TransformConfig with cleaning options

        Returns:
            Cleaned list of dictionaries
        """
        config = config or TransformConfig()
        self._stats = ProcessingStats(input_count=len(records))

        result: list[dict[str, Any]] = []

        for record in records:
            try:
                cleaned = self._clean_record(record, config)
                if cleaned is None:
                    continue
                if config.remove_nulls and self._is_all_null(cleaned):
                    continue
                result.append(cleaned)
            except Exception as e:
                self._stats.errors.append(f"Record error: {e}")

        self._stats.output_count = len(result)
        self._stats.nulls_removed = self._stats.input_count - self._stats.output_count
        return result

    def _clean_record(self, record: dict[str, Any], config: TransformConfig) -> Optional[dict[str, Any]]:
        if record is None:
            return None
        cleaned: dict[str, Any] = {}
        for key, value in record.items():
            new_key = key
            if config.lowercase_keys:
                new_key = key.lower().strip()
            if value is None:
                if not config.remove_nulls:
                    cleaned[new_key] = None
            elif isinstance(value, str) and config.trim_strings:
                value = value.strip()
                if not value:
                    value = None
                    if config.remove_nulls:
                        continue
                cleaned[new_key] = value
            else:
                cleaned[new_key] = value
        return cleaned

    def _is_all_null(self, record: dict[str, Any]) -> bool:
        return all(v is None for v in record.values())

    def deduplicate(
        self,
        records: list[dict[str, Any]],
        key: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """
        Remove duplicate records.

        Args:
            records: Input records
            key: Field name to use for dedup comparison (None = entire record)

        Returns:
            Deduplicated records
        """
        seen: set[str] = set()
        result: list[dict[str, Any]] = []
        for record in records:
            if key:
                val = json.dumps(record.get(key), sort_keys=True, default=str)
            else:
                val = json.dumps(record, sort_keys=True, default=str)
            if val not in seen:
                seen.add(val)
                result.append(record)
        self._stats.duplicates_removed = len(records) - len(result)
        return result

    def normalize_fields(
        self,
        records: list[dict[str, Any]],
        field_rules: dict[str, str],
    ) -> list[dict[str, Any]]:
        """
        Normalize specific fields using named transformations.

        Args:
            records: Input records
            field_rules: Map of field name to normalization type
                         (lower, upper, title_case, strip, int, float, bool)

        Returns:
            Records with normalized fields
        """
        result: list[dict[str, Any]] = []
        for record in records:
            new_record = dict(record)
            for field_name, rule in field_rules.items():
                if field_name not in new_record:
                    continue
                value = new_record[field_name]
                new_record[field_name] = self._apply_normalization(value, rule)
            result.append(new_record)
        return result

    def _apply_normalization(self, value: Any, rule: str) -> Any:
        if value is None:
            return None
        rule = rule.lower().strip()
        if rule == "lower":
            return str(value).lower().strip()
        elif rule == "upper":
            return str(value).upper().strip()
        elif rule == "title_case":
            return str(value).title().strip()
        elif rule == "strip":
            return str(value).strip()
        elif rule == "int":
            try:
                return int(float(value))
            except (TypeError, ValueError):
                return value
        elif rule == "float":
            try:
                return float(value)
            except (TypeError, ValueError):
                return value
        elif rule == "bool":
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes", "on")
            return bool(value)
        elif rule == "slug":
            return self._to_slug(str(value))
        elif rule == "email":
            return self._normalize_email(str(value))
        elif rule == "phone":
            return self._normalize_phone(str(value))
        return value

    def _to_slug(self, text: str) -> str:
        text = text.lower().strip()
        text = re.sub(r"[^\w\s-]", "", text)
        text = re.sub(r"[_\s]+", "-", text)
        return text.strip("-")

    def _normalize_email(self, email: str) -> str:
        email = email.lower().strip()
        return email if "@" in email else ""

    def _normalize_phone(self, phone: str) -> str:
        digits = re.sub(r"\D", "", phone)
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits[0] == "1":
            return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        return phone

    def aggregate(
        self,
        records: list[dict[str, Any]],
        group_by: str,
        agg_field: str,
        agg_func: str = "sum",
    ) -> list[dict[str, Any]]:
        """
        Aggregate records by grouping field.

        Args:
            records: Input records
            group_by: Field to group by
            agg_field: Field to aggregate
            agg_func: Aggregation function (sum, count, avg, min, max, list)

        Returns:
            Aggregated records
        """
        groups: dict[str, list[Any]] = {}
        for record in records:
            key = str(record.get(group_by, ""))
            val = record.get(agg_field)
            if key not in groups:
                groups[key] = []
            groups[key].append(val)

        result: list[dict[str, Any]] = []
        for key, values in groups.items():
            non_null = [v for v in values if v is not None]
            if not non_null:
                continue
            if agg_func == "sum":
                agg_val = sum(non_null)
            elif agg_func == "count":
                agg_val = len(non_null)
            elif agg_func == "avg":
                agg_val = sum(non_null) / len(non_null) if non_null else 0
            elif agg_func == "min":
                agg_val = min(non_null)
            elif agg_func == "max":
                agg_val = max(non_null)
            elif agg_func == "list":
                agg_val = non_null
            else:
                agg_val = non_null
            result.append({group_by: key, agg_field: agg_val})
        return result

    def pivot(
        self,
        records: list[dict[str, Any]],
        index: str,
        columns: str,
        values: str,
        agg_func: str = "first",
    ) -> list[dict[str, Any]]:
        """Pivot records from long to wide format."""
        pivot: dict[str, dict[str, Any]] = {}
        for record in records:
            idx_val = str(record.get(index, ""))
            col_val = str(record.get(columns, ""))
            val = record.get(values)
            if idx_val not in pivot:
                pivot[idx_val] = {index: idx_val}
            if agg_func == "first":
                pivot[idx_val][col_val] = val
            elif agg_func == "count":
                pivot[idx_val][col_val] = pivot[idx_val].get(col_val, 0) + 1
            elif agg_func == "sum" and val is not None:
                pivot[idx_val][col_val] = (pivot[idx_val].get(col_val, 0)) + val
        return list(pivot.values())

    def join_records(
        self,
        left: list[dict[str, Any]],
        right: list[dict[str, Any]],
        left_key: str,
        right_key: str,
        join_type: str = "inner",
    ) -> list[dict[str, Any]]:
        """
        Join two record sets on keys (like SQL join).

        Args:
            left: Left records
            right: Right records
            left_key: Key field in left records
            right_key: Key field in right records
            join_type: inner, left, right, full, cross

        Returns:
            Joined records
        """
        if join_type == "cross":
            return [{**l, **r} for l in left for r in right]

        right_index: dict[str, list[dict[str, Any]]] = {}
        for r in right:
            k = str(r.get(right_key, ""))
            if k not in right_index:
                right_index[k] = []
            right_index[k].append(r)

        result: list[dict[str, Any]] = []
        matched_right: set[int] = set()

        for i, l in enumerate(left):
            k = str(l.get(left_key, ""))
            if k in right_index:
                for r in right_index[k]:
                    result.append({**l, **r})
                    matched_right.add(id(r))
            elif join_type in ("left", "full"):
                result.append({**l})

        if join_type in ("right", "full"):
            for r in right:
                if id(r) not in matched_right:
                    result.append({r})

        return result

    def flatten_record(self, record: dict[str, Any], separator: str = ".") -> dict[str, Any]:
        """Flatten nested records to dot-notation keys."""
        result: dict[str, Any] = {}

        def _flatten(obj: Any, prefix: str = "") -> None:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    new_key = f"{prefix}{separator}{k}" if prefix else k
                    _flatten(v, new_key)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    _flatten(item, f"{prefix}[{i}]")
            else:
                result[prefix] = obj

        _flatten(record)
        return result

    def unflatten_record(self, flat: dict[str, Any], separator: str = ".") -> dict[str, Any]:
        """Unflatten dot-notation keys back to nested structure."""
        result: dict[str, Any] = {}
        for key, value in flat.items():
            parts = re.split(r"[.\[\]]+", key)
            parts = [p for p in parts if p]
            current = result
            for i, part in enumerate(parts[:-1]):
                if part.isdigit():
                    idx = int(part)
                    if i == 0:
                        current = result
                    while len(current) <= idx:
                        current.append({})
                    if isinstance(current, list):
                        current = current[idx]
                    else:
                        current = current.setdefault(idx, {})
                else:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
            last = parts[-1]
            if last.isdigit():
                idx = int(last)
                while len(current) <= idx:
                    current.append(None)
                current[idx] = value
            else:
                current[last] = value
        return result

    def validate_schema(
        self,
        records: list[dict[str, Any]],
        required_fields: list[str],
        field_types: Optional[dict[str, type]] = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
        """
        Validate records against a schema.

        Returns:
            Tuple of (valid_records, errors)
        """
        valid: list[dict[str, Any]] = []
        errors: list[dict[str, str]] = []

        for i, record in enumerate(records):
            record_errors: list[str] = []
            for field in required_fields:
                if field not in record or record[field] is None:
                    record_errors.append(f"Missing required field: {field}")
            if field_types:
                for field, expected_type in field_types.items():
                    if field in record and record[field] is not None:
                        if not isinstance(record[field], expected_type):
                            record_errors.append(
                                f"Field '{field}' has type {type(record[field]).__name__}, "
                                f"expected {expected_type.__name__}"
                            )
            if record_errors:
                errors.append({"index": str(i), "errors": "; ".join(record_errors)})
            else:
                valid.append(record)
        return valid, errors

    def get_stats(self) -> ProcessingStats:
        """Return processing statistics from last operation."""
        return self._stats


if __name__ == "__main__":
    transformer = DataTransformer()
    records = [
        {"name": "  Alice  ", "age": 30, "score": 85},
        {"name": "  bob  ", "age": 25, "score": 90},
        {"name": "  Alice  ", "age": 30, "score": 85},
        {"name": "  carol", "age": None, "score": 70},
    ]
    config = TransformConfig(remove_nulls=True, lowercase_keys=True)
    cleaned = transformer.clean_records(records, config)
    print(f"Cleaned: {len(cleaned)} records")
    print(json.dumps(cleaned, indent=2))

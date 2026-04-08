"""
Data Comparison Action Module.

Provides data comparison capabilities for records,
schemas, and structured data diff.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class DiffType(Enum):
    """Types of differences."""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    UNMODIFIED = "unmodified"


@dataclass
class FieldDiff:
    """Single field difference."""
    field: str
    diff_type: DiffType
    old_value: Any = None
    new_value: Any = None


@dataclass
class RecordDiff:
    """Difference between two records."""
    record_id: Any
    diffs: List[FieldDiff] = field(default_factory=list)
    status: DiffType = DiffType.UNMODIFIED


@dataclass
class ComparisonResult:
    """Result of comparison."""
    total_records: int
    added_count: int
    removed_count: int
    modified_count: int
    unmodified_count: int
    record_diffs: List[RecordDiff] = field(default_factory=list)


class DataComparator:
    """Compares data records."""

    def __init__(
        self,
        id_field: str = "id",
        ignore_fields: Optional[List[str]] = None
    ):
        self.id_field = id_field
        self.ignore_fields = ignore_fields or []

    def _get_record_id(self, record: Dict[str, Any]) -> Any:
        """Get record ID."""
        return record.get(self.id_field)

    def _compare_values(self, old: Any, new: Any) -> bool:
        """Compare two values."""
        return old == new

    def _get_diffs(
        self,
        old_record: Dict[str, Any],
        new_record: Dict[str, Any]
    ) -> List[FieldDiff]:
        """Get field-level differences."""
        diffs = []

        all_fields = set(old_record.keys()) | set(new_record.keys())

        for field in all_fields:
            if field in self.ignore_fields:
                continue

            old_val = old_record.get(field)
            new_val = new_record.get(field)

            if field not in old_record:
                diffs.append(FieldDiff(
                    field=field,
                    diff_type=DiffType.ADDED,
                    new_value=new_val
                ))
            elif field not in new_record:
                diffs.append(FieldDiff(
                    field=field,
                    diff_type=DiffType.REMOVED,
                    old_value=old_val
                ))
            elif not self._compare_values(old_val, new_val):
                diffs.append(FieldDiff(
                    field=field,
                    diff_type=DiffType.MODIFIED,
                    old_value=old_val,
                    new_value=new_val
                ))

        return diffs

    def compare(
        self,
        old_data: List[Dict[str, Any]],
        new_data: List[Dict[str, Any]]
    ) -> ComparisonResult:
        """Compare two datasets."""
        old_records = {self._get_record_id(r): r for r in old_data}
        new_records = {self._get_record_id(r): r for r in new_data}

        old_ids = set(old_records.keys())
        new_ids = set(new_records.keys())

        added_ids = new_ids - old_ids
        removed_ids = old_ids - new_ids
        common_ids = old_ids & new_ids

        record_diffs = []
        added_count = 0
        removed_count = 0
        modified_count = 0
        unmodified_count = 0

        for record_id in added_ids:
            record_diffs.append(RecordDiff(
                record_id=record_id,
                status=DiffType.ADDED
            ))
            added_count += 1

        for record_id in removed_ids:
            record_diffs.append(RecordDiff(
                record_id=record_id,
                status=DiffType.REMOVED
            ))
            removed_count += 1

        for record_id in common_ids:
            old_record = old_records[record_id]
            new_record = new_records[record_id]

            diffs = self._get_diffs(old_record, new_record)

            if diffs:
                record_diffs.append(RecordDiff(
                    record_id=record_id,
                    diffs=diffs,
                    status=DiffType.MODIFIED
                ))
                modified_count += 1
            else:
                unmodified_count += 1

        return ComparisonResult(
            total_records=len(new_data),
            added_count=added_count,
            removed_count=removed_count,
            modified_count=modified_count,
            unmodified_count=unmodified_count,
            record_diffs=record_diffs
        )


class SchemaComparator:
    """Compares data schemas."""

    def compare_schemas(
        self,
        schema1: Dict[str, type],
        schema2: Dict[str, type]
    ) -> List[FieldDiff]:
        """Compare two schemas."""
        diffs = []

        all_fields = set(schema1.keys()) | set(schema2.keys())

        for field in all_fields:
            if field not in schema1:
                diffs.append(FieldDiff(
                    field=field,
                    diff_type=DiffType.ADDED
                ))
            elif field not in schema2:
                diffs.append(FieldDiff(
                    field=field,
                    diff_type=DiffType.REMOVED
                ))
            elif schema1[field] != schema2[field]:
                diffs.append(FieldDiff(
                    field=field,
                    diff_type=DiffType.MODIFIED,
                    old_value=schema1[field],
                    new_value=schema2[field]
                ))

        return diffs


class DeepComparator:
    """Performs deep comparison of nested structures."""

    def compare(
        self,
        old: Any,
        new: Any,
        path: str = ""
    ) -> List[Tuple[str, DiffType, Any, Any]]:
        """Deep compare two structures."""
        diffs = []

        if type(old) != type(new):
            diffs.append((path, DiffType.MODIFIED, old, new))
            return diffs

        if isinstance(old, dict):
            all_keys = set(old.keys()) | set(new.keys())
            for key in all_keys:
                child_path = f"{path}.{key}" if path else key
                if key not in old:
                    diffs.append((child_path, DiffType.ADDED, None, new[key]))
                elif key not in new:
                    diffs.append((child_path, DiffType.REMOVED, old[key], None))
                else:
                    child_diffs = self.compare(old[key], new[key], child_path)
                    diffs.extend(child_diffs)

        elif isinstance(old, list):
            if len(old) != len(new):
                diffs.append((path, DiffType.MODIFIED, old, new))
            else:
                for i, (old_item, new_item) in enumerate(zip(old, new)):
                    child_path = f"{path}[{i}]"
                    child_diffs = self.compare(old_item, new_item, child_path)
                    diffs.extend(child_diffs)

        else:
            if old != new:
                diffs.append((path, DiffType.MODIFIED, old, new))

        return diffs


def main():
    """Demonstrate data comparison."""
    old_data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 3, "name": "Charlie", "age": 35},
    ]

    new_data = [
        {"id": 1, "name": "Alice", "age": 31},
        {"id": 2, "name": "Bob", "age": 25},
        {"id": 4, "name": "Diana", "age": 28},
    ]

    comparator = DataComparator(id_field="id")
    result = comparator.compare(old_data, new_data)

    print(f"Total: {result.total_records}")
    print(f"Added: {result.added_count}")
    print(f"Removed: {result.removed_count}")
    print(f"Modified: {result.modified_count}")
    print(f"Unmodified: {result.unmodified_count}")


if __name__ == "__main__":
    main()

"""
Data Change Detection Module.

Detects changes between dataset versions using various strategies:
content hashing, schema diffing, statistical comparison,
and row-level diffing. Supports CDC (Change Data Capture) patterns.
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Sequence


class ChangeType(Enum):
    """Types of changes detected."""
    INSERTED = "inserted"
    DELETED = "deleted"
    UPDATED = "updated"
    SCHEMA_ADDED = "schema_added"
    SCHEMA_REMOVED = "schema_removed"
    SCHEMA_MODIFIED = "schema_modified"


@dataclass
class Change:
    """Represents a single change."""
    change_type: ChangeType
    identifier: Optional[str] = None
    field: Optional[str] = None
    old_value: Any = None
    new_value: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ChangeSet:
    """Collection of changes between two dataset versions."""
    source_name: str
    version_from: str
    version_to: str
    changes: list[Change] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class SchemaDiff:
    """Schema difference between two versions."""
    added_fields: list[str] = field(default_factory=list)
    removed_fields: list[str] = field(default_factory=list)
    modified_fields: list[str] = field(default_factory=list)
    field_type_changes: dict[str, tuple[str, str]] = field(default_factory=dict)


class ChangeDetector:
    """
    Detects changes between dataset versions.

    Supports row-level diffing, schema diffing, and statistical
    change detection for data quality monitoring.

    Example:
        detector = ChangeDetector()
        detector.load_version_a(old_df, "v1")
        detector.load_version_b(new_df, "v2")
        changeset = detector.detect(key_columns=["id"])
    """

    def __init__(self) -> None:
        self._version_a: Optional[Sequence[dict[str, Any]]] = None
        self._version_b: Optional[Sequence[dict[str, Any]]] = None
        self._schema_a: dict[str, str] = {}
        self._schema_b: dict[str, str] = {}
        self._version_labels: tuple[str, str] = ("v1", "v2")
        self._source_name: str = "dataset"

    def load_version_a(
        self,
        data: Sequence[dict[str, Any]],
        label: str = "v1"
    ) -> None:
        """Load the first version of data."""
        self._version_a = list(data)
        self._schema_a = self._infer_schema(self._version_a)
        self._version_labels = (label, self._version_labels[1])

    def load_version_b(
        self,
        data: Sequence[dict[str, Any]],
        label: str = "v2"
    ) -> None:
        """Load the second version of data."""
        self._version_b = list(data)
        self._schema_b = self._infer_schema(self._version_b)
        self._version_labels = (self._version_labels[0], label)

    def set_source_name(self, name: str) -> None:
        """Set the source/dataset name."""
        self._source_name = name

    def detect(
        self,
        key_columns: Optional[list[str]] = None
    ) -> ChangeSet:
        """
        Detect all changes between loaded versions.

        Args:
            key_columns: Columns that uniquely identify rows

        Returns:
            ChangeSet with all detected changes
        """
        if not self._version_a or not self._version_b:
            raise ValueError("Both versions must be loaded")

        changes: list[Change] = []

        schema_diff = self._diff_schema()
        for field_name in schema_diff.added_fields:
            changes.append(Change(ChangeType.SCHEMA_ADDED, field=field_name))
        for field_name in schema_diff.removed_fields:
            changes.append(Change(ChangeType.SCHEMA_REMOVED, field=field_name))
        for field_name in schema_diff.modified_fields:
            changes.append(Change(ChangeType.SCHEMA_MODIFIED, field=field_name))

        if key_columns:
            row_changes = self._diff_rows_keyed(key_columns)
        else:
            row_changes = self._diff_rows_sequential()

        changes.extend(row_changes)

        stats = self._compute_stats(changes)

        return ChangeSet(
            source_name=self._source_name,
            version_from=self._version_labels[0],
            version_to=self._version_labels[1],
            changes=changes,
            stats=stats
        )

    def _infer_schema(
        self,
        data: Sequence[dict[str, Any]]
    ) -> dict[str, str]:
        """Infer schema from data."""
        if not data:
            return {}
        return {k: self._typeof(v) for k, v in data[0].items()}

    @staticmethod
    def _typeof(value: Any) -> str:
        """Get type name for a value."""
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "boolean"
        if isinstance(value, int):
            return "integer"
        if isinstance(value, float):
            return "float"
        if isinstance(value, str):
            return "string"
        if isinstance(value, (list, tuple)):
            return "array"
        if isinstance(value, dict):
            return "object"
        return "unknown"

    def _diff_schema(self) -> SchemaDiff:
        """Diff schemas between versions."""
        all_fields = set(self._schema_a.keys()) | set(self._schema_b.keys())
        added = [f for f in all_fields if f not in self._schema_a]
        removed = [f for f in all_fields if f not in self._schema_b]

        modified = []
        type_changes: dict[str, tuple[str, str]] = {}

        for field_name in all_fields:
            if field_name in self._schema_a and field_name in self._schema_b:
                type_a = self._schema_a[field_name]
                type_b = self._schema_b[field_name]
                if type_a != type_b:
                    modified.append(field_name)
                    type_changes[field_name] = (type_a, type_b)

        return SchemaDiff(
            added_fields=added,
            removed_fields=removed,
            modified_fields=modified,
            field_type_changes=type_changes
        )

    def _diff_rows_keyed(
        self,
        key_columns: list[str]
    ) -> list[Change]:
        """Diff rows using key columns for matching."""
        changes: list[Change] = []

        index_a = {
            tuple(row.get(k) for k in key_columns): row
            for row in self._version_a
        }
        index_b = {
            tuple(row.get(k) for k in key_columns): row
            for row in self._version_b
        }

        for key, row_a in index_a.items():
            if key not in index_b:
                changes.append(Change(
                    change_type=ChangeType.DELETED,
                    identifier=str(key)
                ))
            else:
                row_b = index_b[key]
                for field_name in set(row_a.keys()) | set(row_b.keys()):
                    val_a = row_a.get(field_name)
                    val_b = row_b.get(field_name)
                    if val_a != val_b:
                        changes.append(Change(
                            change_type=ChangeType.UPDATED,
                            identifier=str(key),
                            field=field_name,
                            old_value=val_a,
                            new_value=val_b
                        ))

        for key, row_b in index_b.items():
            if key not in index_a:
                changes.append(Change(
                    change_type=ChangeType.INSERTED,
                    identifier=str(key)
                ))

        return changes

    def _diff_rows_sequential(self) -> list[Change]:
        """Diff rows sequentially (order-sensitive)."""
        changes: list[Change] = []

        max_len = max(len(self._version_a), len(self._version_b))

        for i in range(max_len):
            if i >= len(self._version_a):
                changes.append(Change(
                    change_type=ChangeType.INSERTED,
                    identifier=f"row_{i}"
                ))
            elif i >= len(self._version_b):
                changes.append(Change(
                    change_type=ChangeType.DELETED,
                    identifier=f"row_{i}"
                ))
            else:
                row_a = self._version_a[i]
                row_b = self._version_b[i]
                for field_name in set(row_a.keys()) | set(row_b.keys()):
                    if row_a.get(field_name) != row_b.get(field_name):
                        changes.append(Change(
                            change_type=ChangeType.UPDATED,
                            identifier=f"row_{i}",
                            field=field_name,
                            old_value=row_a.get(field_name),
                            new_value=row_b.get(field_name)
                        ))

        return changes

    def _compute_stats(self, changes: list[Change]) -> dict[str, int]:
        """Compute change statistics."""
        stats: dict[str, int] = {
            "total": len(changes),
            "inserted": 0,
            "deleted": 0,
            "updated": 0,
            "schema_changes": 0
        }

        for change in changes:
            if change.change_type == ChangeType.INSERTED:
                stats["inserted"] += 1
            elif change.change_type == ChangeType.DELETED:
                stats["deleted"] += 1
            elif change.change_type == ChangeType.UPDATED:
                stats["updated"] += 1
            elif change.change_type in (
                ChangeType.SCHEMA_ADDED,
                ChangeType.SCHEMA_REMOVED,
                ChangeType.SCHEMA_MODIFIED
            ):
                stats["schema_changes"] += 1

        return stats

    def detect_with_hash(
        self,
        hash_column: str = "_row_hash"
    ) -> ChangeSet:
        """
        Detect changes using content hashing.

        Adds a hash column to each row and diffs based on hashes.

        Args:
            hash_column: Name of the hash column

        Returns:
            ChangeSet with hash-based changes
        """
        data_a = self._add_hash(list(self._version_a), hash_column)
        data_b = self._add_hash(list(self._version_b), hash_column)

        self._version_a = data_a
        self._version_b = data_b

        return self.detect(key_columns=[hash_column])

    def _add_hash(
        self,
        data: list[dict[str, Any]],
        hash_column: str
    ) -> list[dict[str, Any]]:
        """Add hash column to data."""
        for row in data:
            content = json.dumps(row, sort_keys=True, default=str)
            row[hash_column] = hashlib.sha256(content.encode()).hexdigest()[:16]
        return data

    def get_changed_fields(self, changeset: ChangeSet) -> list[str]:
        """Get list of fields that changed."""
        fields: set[str] = set()
        for change in changeset.changes:
            if change.field:
                fields.add(change.field)
        return list(fields)

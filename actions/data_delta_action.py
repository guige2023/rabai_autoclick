"""Data Delta Action.

Computes and applies deltas between dataset versions including
change data capture (CDC), diff computation, and patch application.
"""
from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union


class ChangeType(Enum):
    """Type of change detected."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    UNCHANGED = "unchanged"


@dataclass
class FieldDelta:
    """Change to a single field."""
    field_name: str
    old_value: Any = None
    new_value: Any = None
    change_type: ChangeType = ChangeType.UNCHANGED


@dataclass
class RowDelta:
    """Change to a single row."""
    row_key: Any
    change_type: ChangeType
    field_changes: List[FieldDelta] = field(default_factory=list)
    old_row: Optional[Dict[str, Any]] = None
    new_row: Optional[Dict[str, Any]] = None


@dataclass
class DatasetDelta:
    """Complete delta between two dataset versions."""
    source_name: str
    source_version: str
    target_name: str
    target_version: str
    row_deltas: List[RowDelta] = field(default_factory=list)
    inserted_count: int = 0
    updated_count: int = 0
    deleted_count: int = 0
    unchanged_count: int = 0
    computed_at: datetime = field(default_factory=datetime.now)


class DataDeltaAction:
    """Computes and applies deltas between dataset versions."""

    def __init__(
        self,
        key_fields: Optional[List[str]] = None,
        ignore_fields: Optional[List[str]] = None,
        hash_based: bool = False,
    ) -> None:
        self.key_fields = key_fields or ["id"]
        self.ignore_fields = set(ignore_fields or [])
        self.hash_based = hash_based

    def compute_delta(
        self,
        old_data: List[Dict[str, Any]],
        new_data: List[Dict[str, Any]],
        source_name: str = "source",
        target_name: str = "target",
        source_version: str = "v1",
        target_version: str = "v2",
    ) -> DatasetDelta:
        """Compute the delta between two dataset versions."""
        old_index = self._build_index(old_data)
        new_index = self._build_index(new_data)

        all_keys = set(old_index.keys()) | set(new_index.keys())
        row_deltas = []

        for key in all_keys:
            old_row = old_index.get(key)
            new_row = new_index.get(key)

            if old_row is None and new_row is not None:
                row_delta = RowDelta(
                    row_key=key,
                    change_type=ChangeType.INSERT,
                    new_row=new_row,
                )
            elif old_row is not None and new_row is None:
                row_delta = RowDelta(
                    row_key=key,
                    change_type=ChangeType.DELETE,
                    old_row=old_row,
                )
            elif old_row is not None and new_row is not None:
                field_changes = self._compute_field_changes(old_row, new_row)
                if field_changes:
                    row_delta = RowDelta(
                        row_key=key,
                        change_type=ChangeType.UPDATE,
                        field_changes=field_changes,
                        old_row=old_row,
                        new_row=new_row,
                    )
                else:
                    row_delta = RowDelta(
                        row_key=key,
                        change_type=ChangeType.UNCHANGED,
                    )
            else:
                continue

            row_deltas.append(row_delta)

        delta = DatasetDelta(
            source_name=source_name,
            source_version=source_version,
            target_name=target_name,
            target_version=target_version,
            row_deltas=row_deltas,
        )

        delta.inserted_count = sum(1 for r in row_deltas if r.change_type == ChangeType.INSERT)
        delta.updated_count = sum(1 for r in row_deltas if r.change_type == ChangeType.UPDATE)
        delta.deleted_count = sum(1 for r in row_deltas if r.change_type == ChangeType.DELETE)
        delta.unchanged_count = sum(1 for r in row_deltas if r.change_type == ChangeType.UNCHANGED)

        return delta

    def _build_index(
        self,
        data: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """Build an index of rows by key."""
        index = {}
        for row in data:
            key = self._extract_key(row)
            if key is not None:
                index[key] = self._filter_row(row)
        return index

    def _extract_key(self, row: Dict[str, Any]) -> Optional[str]:
        """Extract the key from a row."""
        if self.hash_based:
            sorted_row = json.dumps(row, sort_keys=True, default=str)
            return hashlib.md5(sorted_row.encode()).hexdigest()

        key_parts = []
        for field_name in self.key_fields:
            if field_name in row:
                key_parts.append(str(row[field_name]))
        return ":".join(key_parts) if key_parts else None

    def _filter_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Filter out ignored fields from a row."""
        return {k: v for k, v in row.items() if k not in self.ignore_fields}

    def _compute_field_changes(
        self,
        old_row: Dict[str, Any],
        new_row: Dict[str, Any],
    ) -> List[FieldDelta]:
        """Compute field-level changes between two rows."""
        changes = []
        all_fields = set(old_row.keys()) | set(new_row.keys())

        for field_name in all_fields:
            if field_name in self.ignore_fields:
                continue

            old_value = old_row.get(field_name)
            new_value = new_row.get(field_name)

            if old_value != new_value:
                change_type = ChangeType.UPDATE
            else:
                change_type = ChangeType.UNCHANGED

            changes.append(FieldDelta(
                field_name=field_name,
                old_value=old_value,
                new_value=new_value,
                change_type=change_type,
            ))

        return [c for c in changes if c.change_type == ChangeType.UPDATE]

    def apply_delta(
        self,
        data: List[Dict[str, Any]],
        delta: DatasetDelta,
        on_delete: str = "remove",
    ) -> List[Dict[str, Any]]:
        """Apply a delta to a dataset. Returns new dataset."""
        result = []
        data_index = self._build_index(data)

        for row in data:
            key = self._extract_key(row)
            row_delta = next(
                (d for d in delta.row_deltas if d.row_key == key),
                None,
            )

            if row_delta is None:
                result.append(row)
            elif row_delta.change_type == ChangeType.UNCHANGED:
                result.append(row)
            elif row_delta.change_type == ChangeType.UPDATE:
                if row_delta.new_row is not None:
                    result.append(row_delta.new_row)
                else:
                    updated = dict(row)
                    for field_delta in row_delta.field_changes:
                        updated[field_delta.field_name] = field_delta.new_value
                    result.append(updated)
            elif row_delta.change_type == ChangeType.DELETE:
                if on_delete == "keep":
                    result.append(row)
            elif row_delta.change_type == ChangeType.INSERT:
                if row_delta.new_row is not None:
                    result.append(row_delta.new_row)

        for row_delta in delta.row_deltas:
            if row_delta.change_type == ChangeType.INSERT and row_delta.new_row:
                key = self._extract_key(row_delta.new_row)
                if key not in [self._extract_key(r) for r in result]:
                    result.append(row_delta.new_row)

        return result

    def generate_patch(
        self,
        delta: DatasetDelta,
    ) -> str:
        """Generate a JSON patch representation of the delta."""
        patch = {
            "source": delta.source_name,
            "source_version": delta.source_version,
            "target": delta.target_name,
            "target_version": delta.target_version,
            "computed_at": delta.computed_at.isoformat(),
            "summary": {
                "inserted": delta.inserted_count,
                "updated": delta.updated_count,
                "deleted": delta.deleted_count,
                "unchanged": delta.unchanged_count,
            },
            "changes": [],
        }

        for row_delta in delta.row_deltas:
            if row_delta.change_type == ChangeType.UNCHANGED:
                continue

            change_entry = {
                "key": row_delta.row_key,
                "type": row_delta.change_type.value,
            }

            if row_delta.change_type == ChangeType.UPDATE:
                change_entry["fields"] = [
                    {
                        "name": fd.field_name,
                        "old": fd.old_value,
                        "new": fd.new_value,
                    }
                    for fd in row_delta.field_changes
                ]

            patch["changes"].append(change_entry)

        return json.dumps(patch, indent=2, default=str)

    def summarize(self, delta: DatasetDelta) -> str:
        """Generate a human-readable summary of the delta."""
        lines = [
            f"Delta: {delta.source_name} ({delta.source_version}) -> {delta.target_name} ({delta.target_version})",
            f"Computed at: {delta.computed_at.isoformat()}",
            f"",
            f"Summary:",
            f"  Inserted: {delta.inserted_count}",
            f"  Updated:  {delta.updated_count}",
            f"  Deleted:  {delta.deleted_count}",
            f"  Unchanged: {delta.unchanged_count}",
            f"  Total:    {len(delta.row_deltas)}",
        ]
        return "\n".join(lines)

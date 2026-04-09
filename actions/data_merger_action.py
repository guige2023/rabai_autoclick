"""Data Merger and Join Engine.

This module provides data joining and merging:
- SQL-style joins (inner, left, right, full, cross)
- Multiple key joins
- Conflict resolution
- Denormalization

Example:
    >>> from actions.data_merger_action import DataMerger
    >>> merger = DataMerger()
    >>> result = merger.join(left_df, right_df, left_on="user_id", right_on="id", how="left")
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Callable, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class JoinType(Enum):
    """Join types."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


@dataclass
class MergeResult:
    """Result of a merge operation."""
    success: bool
    merged_records: list[dict[str, Any]]
    join_type: str
    left_key_field: str
    right_key_field: str
    matched_count: int
    unmatched_count: int


class DataMerger:
    """Merges and joins data from multiple sources."""

    def __init__(self) -> None:
        """Initialize the data merger."""
        self._lock = threading.Lock()
        self._stats = {"merges": 0, "records_merged": 0}

    def join(
        self,
        left: list[dict[str, Any]],
        right: list[dict[str, Any]],
        left_on: str,
        right_on: str,
        how: str = "inner",
        suffix_left: str = "_x",
        suffix_right: str = "_y",
    ) -> MergeResult:
        """Join two lists of records.

        Args:
            left: Left records.
            right: Right records.
            left_on: Left key field.
            right_on: Right key field.
            how: Join type (inner, left, right, full, cross).
            suffix_left: Suffix for conflicting left fields.
            suffix_right: Suffix for conflicting right fields.

        Returns:
            MergeResult with merged records.
        """
        self._stats["merges"] += 1

        if how == "cross":
            return self._cross_join(left, right, suffix_left, suffix_right)

        right_index = self._build_index(right, right_on)
        result = []
        matched_count = 0
        unmatched_count = 0

        left_keyed = {r.get(left_on): r for r in left}

        if how in ("inner", "left", "full"):
            for left_rec in left:
                key = left_rec.get(left_on)
                right_rec = right_index.get(key)

                if right_rec:
                    merged = self._merge_records(left_rec, right_rec, left_on, right_on, suffix_left, suffix_right)
                    result.append(merged)
                    matched_count += 1
                else:
                    if how in ("inner", "left"):
                        result.append(self._add_suffix_to_right_fields(dict(left_rec), right[0] if right else {}, suffix_right))
                        unmatched_count += 1

        if how in ("right", "full"):
            matched_left_keys = set()
            for left_rec in left:
                key = left_rec.get(left_on)
                right_rec = right_index.get(key)
                if right_rec:
                    matched_left_keys.add(key)

            for right_rec in right:
                key = right_rec.get(right_on)
                if key in matched_left_keys:
                    continue
                merged = self._merge_records(
                    left_keyed.get(key, {}),
                    right_rec,
                    left_on, right_on,
                    suffix_left, suffix_right
                )
                result.append(merged)
                unmatched_count += 1

        self._stats["records_merged"] += len(result)

        return MergeResult(
            success=True,
            merged_records=result,
            join_type=how,
            left_key_field=left_on,
            right_key_field=right_on,
            matched_count=matched_count,
            unmatched_count=unmatched_count,
        )

    def _build_index(
        self,
        records: list[dict[str, Any]],
        key_field: str,
    ) -> dict[Any, dict[str, Any]]:
        """Build an index on a key field."""
        index = {}
        for r in records:
            key = r.get(key_field)
            if key not in index:
                index[key] = r
        return index

    def _merge_records(
        self,
        left_rec: dict[str, Any],
        right_rec: dict[str, Any],
        left_key: str,
        right_key: str,
        suffix_left: str,
        suffix_right: str,
    ) -> dict[str, Any]:
        """Merge two records, handling key conflicts."""
        result = {}

        all_keys = set(left_rec.keys()) | set(right_rec.keys())

        for key in all_keys:
            left_val = left_rec.get(key)
            right_val = right_rec.get(key)

            if key == right_key:
                result[key] = right_val
            elif key == left_key:
                result[key] = left_val
            elif left_val is not None and right_val is not None:
                result[key + suffix_left] = left_val
                result[key + suffix_right] = right_val
            elif left_val is not None:
                result[key] = left_val
            else:
                result[key] = right_val

        return result

    def _add_suffix_to_right_fields(
        self,
        left_rec: dict[str, Any],
        right_rec: dict[str, Any],
        suffix_right: str,
    ) -> dict[str, Any]:
        """Add right record fields with suffix."""
        result = dict(left_rec)
        for key, val in right_rec.items():
            if key not in left_rec:
                result[key + suffix_right] = val
        return result

    def _cross_join(
        self,
        left: list[dict[str, Any]],
        right: list[dict[str, Any]],
        suffix_left: str,
        suffix_right: str,
    ) -> MergeResult:
        """Create a cross join (Cartesian product)."""
        result = []
        for l in left:
            for r in right:
                merged = self._merge_records(l, r, "", "", suffix_left, suffix_right)
                result.append(merged)

        self._stats["records_merged"] += len(result)

        return MergeResult(
            success=True,
            merged_records=result,
            join_type="cross",
            left_key_field="",
            right_key_field="",
            matched_count=len(result),
            unmatched_count=0,
        )

    def union(
        self,
        *tables: list[dict[str, Any]],
        dedupe: bool = True,
    ) -> list[dict[str, Any]]:
        """Union multiple tables.

        Args:
            *tables: Tables to union.
            dedupe: Remove duplicate records.

        Returns:
            Unioned records.
        """
        result = []
        seen = set() if dedupe else None

        for table in tables:
            for record in table:
                key = tuple(sorted(record.items()))
                if dedupe and key in seen:
                    continue
                result.append(record)
                if seen is not None:
                    seen.add(key)

        return result

    def concatenate(
        self,
        *tables: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Concatenate tables vertically.

        Args:
            *tables: Tables to concatenate.

        Returns:
            Concatenated records.
        """
        result = []
        for table in tables:
            result.extend(table)
        return result

    def get_stats(self) -> dict[str, int]:
        """Get merger statistics."""
        with self._lock:
            return dict(self._stats)

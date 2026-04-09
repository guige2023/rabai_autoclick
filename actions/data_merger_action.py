"""Data merging and joining action."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Sequence


@dataclass
class MergeConfig:
    """Configuration for merge operation."""

    left_key: str
    right_key: str
    how: str = "inner"  # inner, left, right, outer, cross
    suffix_left: str = "_x"
    suffix_right: str = "_y"
    validate: Optional[str] = None  # 1:1, 1:m, m:1, m:m


@dataclass
class MergeResult:
    """Result of merge operation."""

    total_records: int
    merged_records: int
    unmatched_left: int
    unmatched_right: int
    merge_type: str


class DataMergerAction:
    """Merges and joins datasets."""

    def __init__(self):
        """Initialize merger."""
        pass

    def merge(
        self,
        left: Sequence[dict[str, Any]],
        right: Sequence[dict[str, Any]],
        config: MergeConfig,
    ) -> tuple[list[dict[str, Any]], MergeResult]:
        """Merge two datasets.

        Args:
            left: Left dataset.
            right: Right dataset.
            config: Merge configuration.

        Returns:
            Tuple of (merged_records, MergeResult).
        """
        right_index: dict[Any, list[dict[str, Any]]] = {}
        for r in right:
            key_val = r.get(config.right_key)
            if key_val not in right_index:
                right_index[key_val] = []
            right_index[key_val].append(r)

        left_unmatched = []
        right_used = set()
        merged = []

        for record in left:
            key_val = record.get(config.left_key)
            right_matches = right_index.get(key_val, [])

            if not right_matches:
                if config.how in ("left", "outer"):
                    merged.append(self._add_suffix(record.copy(), {}, config, "left"))
                    left_unmatched.append(key_val)
            else:
                for right_record in right_matches:
                    combined = self._merge_records(
                        record.copy(), right_record.copy(), config
                    )
                    merged.append(combined)
                    right_used.add(id(right_record))

        for r in right:
            key_val = r.get(config.right_key)
            if id(r) not in right_used and config.how in ("right", "outer"):
                merged.append(self._add_suffix({}, r.copy(), config, "right"))

        unmatched_left = len([k for k in left_unmatched if k not in right_index])
        unmatched_right = len(right) - len(right_used)

        result = MergeResult(
            total_records=len(left) + len(right),
            merged_records=len(merged),
            unmatched_left=unmatched_left,
            unmatched_right=unmatched_right,
            merge_type=config.how,
        )

        return merged, result

    def _merge_records(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        config: MergeConfig,
    ) -> dict[str, Any]:
        """Merge two records."""
        result = {}

        all_keys = set(left.keys()) | set(right.keys())

        for key in all_keys:
            if key in left and key in right:
                if key == config.left_key:
                    result[key] = left[key]
                else:
                    result[key + config.suffix_left] = left[key]
                    result[key + config.suffix_right] = right[key]
            elif key in left:
                result[key] = left[key]
            else:
                result[key] = right[key]

        return result

    def _add_suffix(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        config: MergeConfig,
        side: str,
    ) -> dict[str, Any]:
        """Add suffixes to unmatched record."""
        result = {}

        for key, value in left.items():
            if key == config.left_key:
                result[key] = value
            else:
                result[key + config.suffix_left] = value

        for key, value in right.items():
            if key == config.right_key:
                result[key] = value
            else:
                result[key + config.suffix_right] = value

        return result

    def concat(
        self,
        datasets: list[Sequence[dict[str, Any]]],
        ignore_index: bool = False,
    ) -> list[dict[str, Any]]:
        """Concatenate multiple datasets.

        Args:
            datasets: List of datasets to concatenate.
            ignore_index: Whether to ignore original indices.

        Returns:
            Concatenated dataset.
        """
        result = []
        for i, ds in enumerate(datasets):
            for record in ds:
                r = record.copy()
                if not ignore_index:
                    r["_source_index"] = i
                result.append(r)
        return result

    def append_distinct(
        self,
        base: Sequence[dict[str, Any]],
        new: Sequence[dict[str, Any]],
        key_fields: list[str],
    ) -> tuple[list[dict[str, Any]], int]:
        """Append new records, excluding duplicates by key.

        Args:
            base: Base dataset.
            new: New records to append.
            key_fields: Fields that uniquely identify a record.

        Returns:
            Tuple of (result, count_added).
        """
        seen_keys: set[tuple] = set()
        for record in base:
            key = tuple(record.get(f) for f in key_fields)
            seen_keys.add(key)

        result = list(base)
        added = 0

        for record in new:
            key = tuple(record.get(f) for f in key_fields)
            if key not in seen_keys:
                result.append(record)
                seen_keys.add(key)
                added += 1

        return result, added

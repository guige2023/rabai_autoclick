"""
Data Merger Action Module.

Merges multiple datasets using join strategies: inner, left, right, full outer.
Supports deduplication after merge and conflict resolution for overlapping fields.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class MergeResult:
    """Result of a merge operation."""
    records: list[dict[str, Any]]
    left_count: int
    right_count: int
    merged_count: int
    unmatched_left: int
    unmatched_right: int


class DataMergerAction(BaseAction):
    """Merge multiple datasets with various join strategies."""

    def __init__(self) -> None:
        super().__init__("data_merger")

    def execute(self, context: dict, params: dict) -> MergeResult:
        """
        Merge two datasets.

        Args:
            context: Execution context
            params: Parameters:
                - left: Left dataset (list of dicts)
                - right: Right dataset (list of dicts)
                - left_key: Join key for left dataset
                - right_key: Join key for right dataset
                - strategy: inner, left, right, full (default: left)
                - suffix_left: Suffix for overlapping fields from left
                - suffix_right: Suffix for overlapping fields from right
                - deduplicate: Remove duplicate rows after merge

        Returns:
            MergeResult with merged records and statistics
        """
        left = params.get("left", [])
        right = params.get("right", [])
        left_key = params.get("left_key", "id")
        right_key = params.get("right_key", "id")
        strategy = params.get("strategy", "left")
        suffix_l = params.get("suffix_left", "_x")
        suffix_r = params.get("suffix_right", "_y")
        deduplicate = params.get("deduplicate", False)

        right_dict: dict[str, dict] = {str(r.get(right_key, "")): r for r in right}

        all_keys = set()
        if left:
            all_keys.update(left[0].keys())
        if right:
            all_keys.update(right[0].keys())
        overlap = [k for k in all_keys if k in (left[0].keys() if left else []) and k in (right[0].keys() if right else [])]
        overlap = [k for k in overlap if k != left_key and k != right_key]

        merged: list[dict[str, Any]] = []
        matched_right_keys: set[str] = set()

        for l_record in left:
            l_key = str(l_record.get(left_key, ""))
            new_record = {k: v for k, v in l_record.items() if k not in overlap}

            if l_key in right_dict:
                r_record = right_dict[l_key]
                matched_right_keys.add(l_key)
                for k in overlap:
                    if k in l_record:
                        new_record[k + suffix_l] = l_record[k]
                    if k in r_record:
                        new_record[k + suffix_r] = r_record[k]
                for k, v in r_record.items():
                    if k == right_key:
                        continue
                    if k in new_record and k not in overlap:
                        new_record[k + suffix_r] = v
                    else:
                        new_record[k] = v
                merged.append(new_record)
            elif strategy in ("left", "full"):
                merged.append(new_record)

        unmatched_left = len(left) - len(matched_right_keys) if strategy == "inner" else 0
        unmatched_right = 0

        if strategy in ("right", "full"):
            for r_record in right:
                r_key = str(r_record.get(right_key, ""))
                if r_key not in matched_right_keys:
                    new_record = {k: v for k, v in r_record.items() if k not in overlap and k != right_key}
                    for k in overlap:
                        if k in r_record:
                            new_record[k + suffix_r] = r_record[k]
                    for ol in overlap:
                        if ol + suffix_l not in new_record:
                            new_record[ol + suffix_l] = None
                    merged.append(new_record)
                    unmatched_right += 1

        if deduplicate:
            seen = set()
            deduped = []
            for rec in merged:
                key = tuple(sorted(str(v) for v in rec.values()))
                if key not in seen:
                    seen.add(key)
                    deduped.append(rec)
            merged = deduped

        return MergeResult(
            records=merged,
            left_count=len(left),
            right_count=len(right),
            merged_count=len(merged),
            unmatched_left=unmatched_left,
            unmatched_right=unmatched_right
        )

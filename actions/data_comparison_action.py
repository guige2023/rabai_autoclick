"""
Data Comparison Action - Compares data from multiple sources.

This module provides data comparison capabilities including
record matching, diff generation, and similarity scoring.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TypeVar


T = TypeVar("T")


@dataclass
class DiffEntry:
    """A single difference entry."""
    record_id: Any
    field: str | None
    left_value: Any
    right_value: Any
    diff_type: str


@dataclass
class ComparisonResult:
    """Result of data comparison."""
    identical: bool
    match_count: int
    mismatch_count: int
    diffs: list[DiffEntry]
    left_only: list[dict[str, Any]] = field(default_factory=list)
    right_only: list[dict[str, Any]] = field(default_factory=list)


class DataComparator:
    """Compares data from different sources."""
    
    def __init__(self, key_field: str = "id") -> None:
        self.key_field = key_field
    
    def compare(
        self,
        left: list[dict[str, Any]],
        right: list[dict[str, Any]],
    ) -> ComparisonResult:
        """Compare two datasets."""
        left_index = {r.get(self.key_field): r for r in left}
        right_index = {r.get(self.key_field): r for r in right}
        
        diffs: list[DiffEntry] = []
        match_count = 0
        mismatch_count = 0
        
        left_keys = set(left_index.keys())
        right_keys = set(right_index.keys())
        
        common_keys = left_keys & right_keys
        
        for key in common_keys:
            l_record = left_index[key]
            r_record = right_index[key]
            
            all_fields = set(l_record.keys()) | set(r_record.keys())
            
            record_diff = False
            for field in all_fields:
                l_val = l_record.get(field)
                r_val = r_record.get(field)
                
                if l_val != r_val:
                    diffs.append(DiffEntry(
                        record_id=key,
                        field=field,
                        left_value=l_val,
                        right_value=r_val,
                        diff_type="modified",
                    ))
                    record_diff = True
            
            if record_diff:
                mismatch_count += 1
            else:
                match_count += 1
        
        left_only = [left_index[k] for k in left_keys - right_keys]
        right_only = [right_index[k] for k in right_keys - left_keys]
        
        return ComparisonResult(
            identical=len(diffs) == 0,
            match_count=match_count,
            mismatch_count=mismatch_count,
            diffs=diffs,
            left_only=left_only,
            right_only=right_only,
        )


class DataComparisonAction:
    """Data comparison action for automation workflows."""
    
    def __init__(self, key_field: str = "id") -> None:
        self.comparator = DataComparator(key_field)
    
    async def compare(
        self,
        left: list[dict[str, Any]],
        right: list[dict[str, Any]],
    ) -> ComparisonResult:
        """Compare two datasets."""
        return self.comparator.compare(left, right)


__all__ = ["DiffEntry", "ComparisonResult", "DataComparator", "DataComparisonAction"]

"""
Data Comparison and Diff Module.

Provides comprehensive data comparison capabilities including
deep equality checks, structural diff, field-level changes,
and similarity scoring for data validation and reconciliation.
"""

from typing import (
    Dict, List, Optional, Any, Union, Callable,
    Tuple, Set, TypeVar, Generic, Sequence
)
from dataclasses import dataclass, field
from enum import Enum, auto
from datetime import datetime
import logging
from collections import Counter

logger = logging.getLogger(__name__)

T = TypeVar("T")


class DiffType(Enum):
    """Type of difference detected."""
    ADDED = auto()
    REMOVED = auto()
    MODIFIED = auto()
    UNCHANGED = auto()
    TYPE_CHANGED = auto()
    MOVED = auto()


@dataclass
class FieldDiff:
    """Represents a difference in a single field."""
    path: str
    diff_type: DiffType
    old_value: Any = None
    new_value: Any = None
    
    def __str__(self) -> str:
        if self.diff_type == DiffType.ADDED:
            return f"+ {self.path}: {self.new_value}"
        elif self.diff_type == DiffType.REMOVED:
            return f"- {self.path}: {self.old_value}"
        elif self.diff_type == DiffType.MODIFIED:
            return f"~ {self.path}: {self.old_value} → {self.new_value}"
        elif self.diff_type == DiffType.TYPE_CHANGED:
            return f"! {self.path}: {type(self.old_value).__name__} → {type(self.new_value).__name__}"
        return f"= {self.path}: {self.old_value}"


@dataclass
class ComparisonResult:
    """Result of data comparison."""
    equal: bool
    diff_type: DiffType
    field_diffs: List[FieldDiff] = field(default_factory=list)
    summary: Dict[str, int] = field(default_factory=dict)
    similarity_score: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __str__(self) -> str:
        lines = [
            f"Equal: {self.equal}",
            f"Type: {self.diff_type.name}",
            f"Similarity: {self.similarity_score:.2%}",
            f"Changes: +{self.summary.get('added', 0)} -{self.summary.get('removed', 0)} ~{self.summary.get('modified', 0)}"
        ]
        if self.field_diffs:
            lines.append("Differences:")
            for diff in self.field_diffs[:10]:
                lines.append(f"  {diff}")
            if len(self.field_diffs) > 10:
                lines.append(f"  ... and {len(self.field_diffs) - 10} more")
        return "\n".join(lines)


class DeepComparator:
    """
    Deep comparison for complex data structures.
    
    Supports nested dictionaries, lists, sets, and custom
    object comparisons with configurable tolerance settings.
    """
    
    def __init__(
        self,
        ignore_keys: Optional[Set[str]] = None,
        float_tolerance: float = 1e-9,
        case_sensitive: bool = True,
        ignore_order: bool = False
    ) -> None:
        self.ignore_keys = ignore_keys or set()
        self.float_tolerance = float_tolerance
        self.case_sensitive = case_sensitive
        self.ignore_order = ignore_order
    
    def compare(
        self,
        old_data: Any,
        new_data: Any,
        path: str = "$"
    ) -> ComparisonResult:
        """
        Compare two data structures.
        
        Args:
            old_data: Original data
            new_data: New data
            path: Current path for tracking
            
        Returns:
            ComparisonResult with detailed differences
        """
        field_diffs = []
        self._compare_values(old_data, new_data, path, field_diffs)
        
        summary = self._summarize_diffs(field_diffs)
        equal = len(field_diffs) == 0
        
        if summary.get("added", 0) + summary.get("removed", 0) + summary.get("modified", 0) > 0:
            diff_type = DiffType.MODIFIED
        else:
            diff_type = DiffType.UNCHANGED
        
        total_fields = self._count_fields(old_data) or self._count_fields(new_data) or 1
        similarity = 1 - (len(field_diffs) / total_fields)
        
        return ComparisonResult(
            equal=equal,
            diff_type=diff_type,
            field_diffs=field_diffs,
            summary=summary,
            similarity_score=max(0, similarity)
        )
    
    def _compare_values(
        self,
        old_val: Any,
        new_val: Any,
        path: str,
        diffs: List[FieldDiff]
    ) -> None:
        """Recursively compare values."""
        # Handle type mismatch
        if type(old_val) != type(new_val):
            # Special case: both are numeric
            if isinstance(old_val, (int, float)) and isinstance(new_val, (int, float)):
                if abs(float(old_val) - float(new_val)) <= self.float_tolerance:
                    return
                diffs.append(FieldDiff(path, DiffType.MODIFIED, old_val, new_val))
                return
            
            diffs.append(FieldDiff(path, DiffType.TYPE_CHANGED, old_val, new_val))
            return
        
        # Handle None
        if old_val is None and new_val is None:
            return
        
        # Handle primitives
        if isinstance(old_val, (str, int, float, bool)):
            if self.case_sensitive or not isinstance(old_val, str):
                if old_val != new_val:
                    diffs.append(FieldDiff(path, DiffType.MODIFIED, old_val, new_val))
            else:
                if old_val.lower() != new_val.lower():
                    diffs.append(FieldDiff(path, DiffType.MODIFIED, old_val, new_val))
            return
        
        # Handle lists
        if isinstance(old_val, list):
            self._compare_lists(old_val, new_val, path, diffs)
            return
        
        # Handle sets
        if isinstance(old_val, set):
            self._compare_sets(old_val, new_val, path, diffs)
            return
        
        # Handle dictionaries
        if isinstance(old_val, dict):
            self._compare_dicts(old_val, new_val, path, diffs)
            return
        
        # Handle other iterables
        if hasattr(old_val, "__iter__"):
            if self.ignore_order:
                self._compare_iterables_unordered(old_val, new_val, path, diffs)
            else:
                self._compare_lists(list(old_val), list(new_val), path, diffs)
            return
        
        # Fallback equality check
        if old_val != new_val:
            diffs.append(FieldDiff(path, DiffType.MODIFIED, old_val, new_val))
    
    def _compare_lists(
        self,
        old_list: List[Any],
        new_list: List[Any],
        path: str,
        diffs: List[FieldDiff]
    ) -> None:
        """Compare two lists."""
        max_len = max(len(old_list), len(new_list))
        
        for i in range(max_len):
            child_path = f"{path}[{i}]"
            
            if i >= len(old_list):
                diffs.append(FieldDiff(child_path, DiffType.ADDED, new_value=new_list[i]))
            elif i >= len(new_list):
                diffs.append(FieldDiff(child_path, DiffType.REMOVED, old_value=old_list[i]))
            else:
                self._compare_values(old_list[i], new_list[i], child_path, diffs)
    
    def _compare_sets(
        self,
        old_set: Set[Any],
        new_set: Set[Any],
        path: str,
        diffs: List[FieldDiff]
    ) -> None:
        """Compare two sets."""
        removed = old_set - new_set
        added = new_set - old_set
        
        for item in removed:
            diffs.append(FieldDiff(path, DiffType.REMOVED, old_value=item))
        for item in added:
            diffs.append(FieldDiff(path, DiffType.ADDED, new_value=item))
    
    def _compare_dicts(
        self,
        old_dict: Dict[str, Any],
        new_dict: Dict[str, Any],
        path: str,
        diffs: List[FieldDiff]
    ) -> None:
        """Compare two dictionaries."""
        all_keys = set(old_dict.keys()) | set(new_dict.keys())
        
        for key in sorted(all_keys):
            if key in self.ignore_keys:
                continue
            
            child_path = f"{path}.{key}"
            
            if key not in old_dict:
                diffs.append(FieldDiff(child_path, DiffType.ADDED, new_value=new_dict[key]))
            elif key not in new_dict:
                diffs.append(FieldDiff(child_path, DiffType.REMOVED, old_value=old_dict[key]))
            else:
                self._compare_values(old_dict[key], new_dict[key], child_path, diffs)
    
    def _compare_iterables_unordered(
        self,
        old_iter: Any,
        new_iter: Any,
        path: str,
        diffs: List[FieldDiff]
    ) -> None:
        """Compare iterables without order."""
        old_items = list(old_iter)
        new_items = list(new_iter)
        
        old_counts = Counter([str(i) for i in old_items])
        new_counts = Counter([str(i) for i in new_items])
        
        all_items = set(old_counts.keys()) | set(new_counts.keys())
        
        for item_str in all_items:
            old_count = old_counts.get(item_str, 0)
            new_count = new_counts.get(item_str, 0)
            
            if old_count < new_count:
                diffs.append(FieldDiff(
                    path, DiffType.ADDED,
                    new_value=f"<{new_count - old_count} x {item_str}>"
                ))
            elif old_count > new_count:
                diffs.append(FieldDiff(
                    path, DiffType.REMOVED,
                    old_value=f"<{old_count - new_count} x {item_str}>"
                ))
    
    def _summarize_diffs(self, diffs: List[FieldDiff]) -> Dict[str, int]:
        """Summarize diffs by type."""
        summary = {"added": 0, "removed": 0, "modified": 0, "type_changed": 0}
        for diff in diffs:
            if diff.diff_type == DiffType.ADDED:
                summary["added"] += 1
            elif diff.diff_type == DiffType.REMOVED:
                summary["removed"] += 1
            elif diff.diff_type == DiffType.MODIFIED:
                summary["modified"] += 1
            elif diff.diff_type == DiffType.TYPE_CHANGED:
                summary["type_changed"] += 1
        return summary
    
    def _count_fields(self, data: Any, count: int = 0) -> int:
        """Count total fields in data structure."""
        if isinstance(data, dict):
            return sum(self._count_fields(v, count) for v in data.values()) + len(data)
        elif isinstance(data, (list, tuple)):
            return sum(self._count_fields(item, count) for item in data)
        return 1


class TableComparator:
    """Compare tabular data (list of dicts)."""
    
    def __init__(
        self,
        key_field: str,
        ignore_fields: Optional[Set[str]] = None
    ) -> None:
        self.key_field = key_field
        self.ignore_fields = ignore_fields or set()
        self.deep_comparator = DeepComparator(ignore_keys=ignore_fields)
    
    def compare_tables(
        self,
        old_rows: List[Dict[str, Any]],
        new_rows: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Compare two tables.
        
        Returns:
            Dictionary with added, removed, modified rows
        """
        old_index = {row[self.key_field]: row for row in old_rows}
        new_index = {row[self.key_field]: row for row in new_rows}
        
        old_keys = set(old_index.keys())
        new_keys = set(new_index.keys())
        
        result = {
            "added": [new_index[k] for k in new_keys - old_keys],
            "removed": [old_index[k] for k in old_keys - new_keys],
            "modified": [],
            "unchanged": []
        }
        
        for key in old_keys & new_keys:
            old_row = old_index[key]
            new_row = new_index[key]
            
            comparison = self.deep_comparator.compare(old_row, new_row)
            
            if comparison.equal:
                result["unchanged"].append(new_row)
            else:
                result["modified"].append({
                    "key": key,
                    "old": old_row,
                    "new": new_row,
                    "diffs": comparison.field_diffs
                })
        
        return result


# Entry point for direct execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    data1 = {
        "name": "Alice",
        "age": 30,
        "address": {"city": "NYC", "zip": "10001"},
        "scores": [85, 90, 78]
    }
    
    data2 = {
        "name": "Alice",
        "age": 31,  # Modified
        "address": {"city": "NYC", "zip": "10002"},  # Modified
        "scores": [85, 88, 78, 92]  # Modified
    }
    
    comparator = DeepComparator()
    result = comparator.compare(data1, data2)
    
    print("=== Data Comparison ===")
    print(result)
    
    print("\n=== Table Comparison ===")
    table1 = [
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "Bob", "value": 200}
    ]
    table2 = [
        {"id": 1, "name": "Alice", "value": 150},
        {"id": 3, "name": "Carol", "value": 300}
    ]
    
    table_comp = TableComparator(key_field="id")
    table_result = table_comp.compare_tables(table1, table2)
    
    print(f"Added: {len(table_result['added'])} rows")
    print(f"Removed: {len(table_result['removed'])} rows")
    print(f"Modified: {len(table_result['modified'])} rows")
    print(f"Unchanged: {len(table_result['unchanged'])} rows")

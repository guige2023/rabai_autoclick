"""Data Comparator Action Module.

Provides data comparison capabilities for comparing datasets,
detecting differences, and generating diff reports.

Example:
    >>> from actions.data.data_comparator_action import DataComparatorAction
    >>> action = DataComparatorAction()
    >>> diff = action.compare(dataset_a, dataset_b)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import threading


class DiffType(Enum):
    """Type of difference."""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    UNCHANGED = "unchanged"


@dataclass
class Diff:
    """Single difference record.
    
    Attributes:
        diff_type: Type of difference
        key: Item key
        old_value: Old value (if modified/removed)
        new_value: New value (if modified/added)
        path: Path to item in nested structure
    """
    diff_type: DiffType
    key: Any
    old_value: Any = None
    new_value: Any = None
    path: str = ""


@dataclass
class DiffReport:
    """Comparison report.
    
    Attributes:
        added: Items added
        removed: Items removed
        modified: Items modified
        unchanged: Items unchanged
        total_changes: Total number of changes
    """
    added: List[Diff]
    removed: List[Diff]
    modified: List[Diff]
    unchanged_count: int
    total_changes: int
    duration: float = 0.0


@dataclass
class ComparatorConfig:
    """Configuration for comparison.
    
    Attributes:
        ignore_order: Ignore order in lists
        deep_compare: Deep nested comparison
        case_sensitive: Case sensitive string comparison
        tolerance: Numeric comparison tolerance
        key_func: Key extraction function
    """
    ignore_order: bool = True
    deep_compare: bool = True
    case_sensitive: bool = True
    tolerance: float = 0.0001
    key_func: Optional[Callable[[Any], Any]] = None


class DataComparatorAction:
    """Data comparator for datasets.
    
    Provides comprehensive comparison with diff generation,
    similarity scoring, and change detection.
    
    Attributes:
        config: Comparator configuration
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[ComparatorConfig] = None,
    ) -> None:
        """Initialize comparator action.
        
        Args:
            config: Comparator configuration
        """
        self.config = config or ComparatorConfig()
        self._lock = threading.Lock()
    
    def compare(
        self,
        data_a: List[Any],
        data_b: List[Any],
    ) -> DiffReport:
        """Compare two datasets.
        
        Args:
            data_a: First dataset
            data_b: Second dataset
        
        Returns:
            DiffReport with differences
        """
        import time
        start = time.time()
        
        if self.config.ignore_order:
            return self._compare_unordered(data_a, data_b, start)
        else:
            return self._compare_ordered(data_a, data_b, start)
    
    def _compare_unordered(
        self,
        data_a: List[Any],
        data_b: List[Any],
        start: float,
    ) -> DiffReport:
        """Compare datasets ignoring order.
        
        Args:
            data_a: First dataset
            data_b: Second dataset
            start: Start time
        
        Returns:
            DiffReport
        """
        key_func = self.config.key_func or (lambda x: x)
        
        keys_a = {key_func(item): item for item in data_a}
        keys_b = {key_func(item): item for item in data_b}
        
        all_keys = set(keys_a.keys()) | set(keys_b.keys())
        
        added: List[Diff] = []
        removed: List[Diff] = []
        modified: List[Diff] = []
        unchanged_count = 0
        
        for key in all_keys:
            in_a = key in keys_a
            in_b = key in keys_b
            
            if in_b and not in_a:
                added.append(Diff(
                    diff_type=DiffType.ADDED,
                    key=key,
                    new_value=keys_b[key],
                ))
            elif in_a and not in_b:
                removed.append(Diff(
                    diff_type=DiffType.REMOVED,
                    key=key,
                    old_value=keys_a[key],
                ))
            else:
                item_a = keys_a[key]
                item_b = keys_b[key]
                
                if self._values_equal(item_a, item_b):
                    unchanged_count += 1
                else:
                    modified.append(Diff(
                        diff_type=DiffType.MODIFIED,
                        key=key,
                        old_value=item_a,
                        new_value=item_b,
                    ))
        
        total_changes = len(added) + len(removed) + len(modified)
        
        return DiffReport(
            added=added,
            removed=removed,
            modified=modified,
            unchanged_count=unchanged_count,
            total_changes=total_changes,
            duration=time.time() - start,
        )
    
    def _compare_ordered(
        self,
        data_a: List[Any],
        data_b: List[Any],
        start: float,
    ) -> DiffReport:
        """Compare datasets preserving order.
        
        Args:
            data_a: First dataset
            data_b: Second dataset
            start: Start time
        
        Returns:
            DiffReport
        """
        added: List[Diff] = []
        removed: List[Diff] = []
        modified: List[Diff] = []
        unchanged_count = 0
        
        max_len = max(len(data_a), len(data_b))
        
        for i in range(max_len):
            in_a = i < len(data_a)
            in_b = i < len(data_b)
            
            if in_a and not in_b:
                removed.append(Diff(
                    diff_type=DiffType.REMOVED,
                    key=i,
                    old_value=data_a[i],
                ))
            elif not in_a and in_b:
                added.append(Diff(
                    diff_type=DiffType.ADDED,
                    key=i,
                    new_value=data_b[i],
                ))
            else:
                if self._values_equal(data_a[i], data_b[i]):
                    unchanged_count += 1
                else:
                    modified.append(Diff(
                        diff_type=DiffType.MODIFIED,
                        key=i,
                        old_value=data_a[i],
                        new_value=data_b[i],
                    ))
        
        total_changes = len(added) + len(removed) + len(modified)
        
        return DiffReport(
            added=added,
            removed=removed,
            modified=modified,
            unchanged_count=unchanged_count,
            total_changes=total_changes,
            duration=time.time() - start,
        )
    
    def _values_equal(
        self,
        value_a: Any,
        value_b: Any,
    ) -> bool:
        """Check if two values are equal.
        
        Args:
            value_a: First value
            value_b: Second value
        
        Returns:
            True if equal
        """
        if not self.config.deep_compare:
            return value_a == value_b
        
        type_a = type(value_a)
        type_b = type(value_b)
        
        if type_a != type_b:
            if isinstance(value_a, (int, float)) and isinstance(value_b, (int, float)):
                return abs(value_a - value_b) <= self.config.tolerance
            return False
        
        if isinstance(value_a, dict):
            return self._dicts_equal(value_a, value_b)
        elif isinstance(value_a, (list, tuple)):
            return self._lists_equal(value_a, value_b)
        elif isinstance(value_a, str):
            if not self.config.case_sensitive:
                return value_a.lower() == value_b.lower()
        
        return value_a == value_b
    
    def _dicts_equal(self, dict_a: dict, dict_b: dict) -> bool:
        """Compare two dictionaries.
        
        Args:
            dict_a: First dictionary
            dict_b: Second dictionary
        
        Returns:
            True if equal
        """
        if set(dict_a.keys()) != set(dict_b.keys()):
            return False
        
        for key in dict_a:
            if not self._values_equal(dict_a[key], dict_b[key]):
                return False
        
        return True
    
    def _lists_equal(self, list_a: list, list_b: list) -> bool:
        """Compare two lists.
        
        Args:
            list_a: First list
            list_b: Second list
        
        Returns:
            True if equal
        """
        if len(list_a) != len(list_b):
            return False
        
        for i in range(len(list_a)):
            if not self._values_equal(list_a[i], list_b[i]):
                return False
        
        return True
    
    def compute_similarity(
        self,
        data_a: List[Any],
        data_b: List[Any],
    ) -> float:
        """Compute similarity score between datasets.
        
        Args:
            data_a: First dataset
            data_b: Second dataset
        
        Returns:
            Similarity score (0-1)
        """
        report = self.compare(data_a, data_b)
        
        total = len(data_a) + len(data_b)
        if total == 0:
            return 1.0
        
        return 1.0 - (report.total_changes / total)
    
    def generate_patch(
        self,
        data_a: List[Any],
        data_b: List[Any],
    ) -> List[Dict[str, Any]]:
        """Generate patch to transform A to B.
        
        Args:
            data_a: Source dataset
            data_b: Target dataset
        
        Returns:
            List of patch operations
        """
        report = self.compare(data_a, data_b)
        
        patch: List[Dict[str, Any]] = []
        
        for diff in report.removed:
            patch.append({
                "op": "remove",
                "key": diff.key,
                "value": diff.old_value,
            })
        
        for diff in report.added:
            patch.append({
                "op": "add",
                "key": diff.key,
                "value": diff.new_value,
            })
        
        for diff in report.modified:
            patch.append({
                "op": "replace",
                "key": diff.key,
                "old": diff.old_value,
                "new": diff.new_value,
            })
        
        return patch

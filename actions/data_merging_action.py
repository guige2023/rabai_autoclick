"""
Data Merging Action Module.

Provides data merging capabilities for combining
multiple datasets with conflict resolution.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)


class MergeStrategy(Enum):
    """Merge strategies."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    UNION = "union"


class ConflictResolution(Enum):
    """Conflict resolution strategies."""
    LEFT_PRIORITY = "left_priority"
    RIGHT_PRIORITY = "right_priority"
    NEWEST = "newest"
    OLDEST = "oldest"
    CONCATENATE = "concatenate"
    CUSTOM = "custom"


@dataclass
class MergeConfig:
    """Merge configuration."""
    strategy: MergeStrategy = MergeStrategy.INNER
    conflict_resolution: ConflictResolution = ConflictResolution.LEFT_PRIORITY
    custom_resolver: Optional[Callable] = None
    key_field: str = "id"


@dataclass
class MergeResult:
    """Result of merge operation."""
    success: bool
    merged_count: int
    conflicts: List[Tuple[Any, Any, str]] = field(default_factory=list)
    error: Optional[str] = None


@dataclass
class Conflict:
    """Merge conflict."""
    key: Any
    field: str
    left_value: Any
    right_value: Any
    resolved_value: Any = None


class DataMerger:
    """Merges multiple datasets."""

    def __init__(self, config: MergeConfig):
        self.config = config
        self.conflicts: List[Conflict] = []

    def _get_key(self, record: Dict[str, Any]) -> Any:
        """Get key from record."""
        return record.get(self.config.key_field)

    def _resolve_conflict(self, conflict: Conflict) -> Any:
        """Resolve a conflict."""
        if self.config.conflict_resolution == ConflictResolution.LEFT_PRIORITY:
            return conflict.left_value

        if self.config.conflict_resolution == ConflictResolution.RIGHT_PRIORITY:
            return conflict.right_value

        if self.config.conflict_resolution == ConflictResolution.NEWEST:
            left_time = conflict.left_value.get("timestamp") if isinstance(conflict.left_value, dict) else None
            right_time = conflict.right_value.get("timestamp") if isinstance(conflict.right_value, dict) else None
            if left_time and right_time:
                return conflict.left_value if left_time > right_time else conflict.right_value
            return conflict.right_value

        if self.config.conflict_resolution == ConflictResolution.CONCATENATE:
            if isinstance(conflict.left_value, list) and isinstance(conflict.right_value, list):
                return conflict.left_value + conflict.right_value
            return [conflict.left_value, conflict.right_value]

        if self.config.conflict_resolution == ConflictResolution.CUSTOM and self.config.custom_resolver:
            return self.config.custom_resolver(conflict.left_value, conflict.right_value)

        return conflict.left_value

    def _merge_records(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Merge two records."""
        merged = left.copy()

        for key, value in right.items():
            if key not in merged:
                merged[key] = value
            else:
                if merged[key] != value:
                    conflict = Conflict(
                        key=self._get_key(left),
                        field=key,
                        left_value=merged[key],
                        right_value=value
                    )
                    conflict.resolved_value = self._resolve_conflict(conflict)
                    self.conflicts.append(conflict)
                    merged[key] = conflict.resolved_value

        return merged

    def merge(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], MergeResult]:
        """Merge two datasets."""
        self.conflicts = []
        merged = []

        left_index = {self._get_key(r): r for r in left_data}
        right_index = {self._get_key(r): r for r in right_data}

        left_keys = set(left_index.keys())
        right_keys = set(right_index.keys())

        try:
            if self.config.strategy in (MergeStrategy.INNER, MergeStrategy.LEFT, MergeStrategy.FULL):
                for key in left_keys:
                    if key in right_keys:
                        merged.append(self._merge_records(left_index[key], right_index[key]))
                    elif self.config.strategy in (MergeStrategy.LEFT, MergeStrategy.FULL):
                        merged.append(left_index[key].copy())

            if self.config.strategy in (MergeStrategy.RIGHT, MergeStrategy.FULL):
                for key in right_keys:
                    if key not in left_keys:
                        merged.append(right_index[key].copy())

            if self.config.strategy == MergeStrategy.UNION:
                for key in left_keys:
                    if key in right_keys:
                        merged.append(self._merge_records(left_index[key], right_index[key]))
                    else:
                        merged.append(left_index[key].copy())

                for key in right_keys:
                    if key not in left_keys:
                        merged.append(right_index[key].copy())

            conflict_tuples = [
                (c.key, c.field, c.resolved_value)
                for c in self.conflicts
            ]

            return merged, MergeResult(
                success=True,
                merged_count=len(merged),
                conflicts=conflict_tuples
            )

        except Exception as e:
            return merged, MergeResult(
                success=False,
                merged_count=len(merged),
                error=str(e)
            )


class MultiWayMerger:
    """Merges multiple datasets."""

    def __init__(self, config: MergeConfig):
        self.config = config
        self.merger = DataMerger(config)

    def merge_all(
        self,
        datasets: List[List[Dict[str, Any]]]
    ) -> Tuple[List[Dict[str, Any]], MergeResult]:
        """Merge multiple datasets."""
        if not datasets:
            return [], MergeResult(success=True, merged_count=0)

        result = datasets[0]
        all_conflicts = []

        for dataset in datasets[1:]:
            result, merge_result = self.merger.merge(result, dataset)
            all_conflicts.extend(self.merger.conflicts)

        return result, MergeResult(
            success=True,
            merged_count=len(result),
            conflicts=[
                (c.key, c.field, c.resolved_value)
                for c in all_conflicts
            ]
        )


def main():
    """Demonstrate data merging."""
    left_data = [
        {"id": 1, "name": "Alice", "age": 30},
        {"id": 2, "name": "Bob", "age": 25},
    ]

    right_data = [
        {"id": 1, "name": "Alice", "age": 31},
        {"id": 3, "name": "Charlie", "age": 35},
    ]

    config = MergeConfig(strategy=MergeStrategy.FULL)
    merger = DataMerger(config)

    merged, result = merger.merge(left_data, right_data)

    print(f"Merged: {len(merged)} records")
    print(f"Conflicts: {len(result.conflicts)}")


if __name__ == "__main__":
    main()

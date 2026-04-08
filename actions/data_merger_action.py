"""Data Merger Action Module.

Provides data merging, joining, and reconciliation
capabilities for combining multiple data sources.
"""

from typing import Any, Dict, List, Optional, Callable, Union, Tuple
from dataclasses import dataclass, field
from enum import Enum


class MergeStrategy(Enum):
    """Strategies for merging records."""
    FIRST_WINS = "first_wins"
    LAST_WINS = "last_wins"
    CONFLICT_RESOLVER = "conflict_resolver"
    MERGE_ALL = "merge_all"


@dataclass
class MergeKey:
    """Defines a key for merging."""
    field: str
    transform: Optional[Callable[[Any], Any]] = None

    def get_value(self, record: Dict[str, Any]) -> Any:
        """Get the key value from a record."""
        value = record.get(self.field)
        if self.transform and value is not None:
            return self.transform(value)
        return value


@dataclass
class MergeResult:
    """Result of a merge operation."""
    merged: List[Dict[str, Any]]
    unmatched_left: List[Dict[str, Any]]
    unmatched_right: List[Dict[str, Any]]
    conflicts: List[Dict[str, Any]]
    merge_stats: Dict[str, int]


class ConflictResolver:
    """Resolves conflicts during merge."""

    def __init__(self, strategy: MergeStrategy = MergeStrategy.LAST_WINS):
        self.strategy = strategy
        self._custom_resolvers: Dict[str, Callable] = {}

    def register_resolver(
        self,
        field: str,
        resolver: Callable[[Any, Any], Any],
    ):
        """Register a custom resolver for a field."""
        self._custom_resolvers[field] = resolver

    def resolve(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any],
        field: str,
    ) -> Tuple[Any, str]:
        """Resolve a conflict for a field."""
        left_value = left.get(field)
        right_value = right.get(field)

        if field in self._custom_resolvers:
            resolved_value = self._custom_resolvers[field](left_value, right_value)
            return resolved_value, "custom"

        if self.strategy == MergeStrategy.FIRST_WINS:
            return left_value, "left"
        elif self.strategy == MergeStrategy.LAST_WINS:
            return right_value, "right"
        elif self.strategy == MergeStrategy.MERGE_ALL:
            if isinstance(left_value, list) and isinstance(right_value, list):
                return list(set(left_value + right_value)), "merged"
            elif isinstance(left_value, dict) and isinstance(right_value, dict):
                merged = left_value.copy()
                merged.update(right_value)
                return merged, "merged"
            return [left_value, right_value], "merged"

        return right_value, "right"


class DataMerger:
    """Merges multiple datasets."""

    def __init__(self, conflict_resolver: Optional[ConflictResolver] = None):
        self.conflict_resolver = conflict_resolver or ConflictResolver()

    def merge_on_keys(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        left_key: MergeKey,
        right_key: MergeKey,
        merge_strategy: MergeStrategy = MergeStrategy.LAST_WINS,
        include_unmatched: bool = True,
    ) -> MergeResult:
        """Merge two datasets on specified keys."""
        right_index: Dict[Any, Dict[str, Any]] = {}
        for record in right:
            key_value = right_key.get_value(record)
            right_index[key_value] = record

        merged: List[Dict[str, Any]] = []
        unmatched_left: List[Dict[str, Any]] = []
        unmatched_right: List[Dict[str, Any]] = []
        conflicts: List[Dict[str, Any]] = []

        for left_record in left:
            left_key_value = left_key.get_value(left_record)

            if left_key_value in right_index:
                right_record = right_index[left_key_value]
                merged_record, record_conflicts = self._merge_records(
                    left_record, right_record, merge_strategy
                )
                merged.append(merged_record)
                conflicts.extend(record_conflicts)
            elif include_unmatched:
                unmatched_left.append(left_record)

        if include_unmatched:
            matched_right_keys = {
                left_key.get_value(r) for r in left
            } & set(right_index.keys())
            unmatched_right = [
                r for r in right
                if right_key.get_value(r) not in matched_right_keys
            ]

        return MergeResult(
            merged=merged,
            unmatched_left=unmatched_left,
            unmatched_right=unmatched_right,
            conflicts=conflicts,
            merge_stats=self._compute_stats(
                left, right, merged, unmatched_left, unmatched_right
            ),
        )

    def _merge_records(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any],
        strategy: MergeStrategy,
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Merge two records."""
        result = left.copy()
        conflicts = []

        all_fields = set(left.keys()) | set(right.keys())

        for field in all_fields:
            left_value = left.get(field)
            right_value = right.get(field)

            if left_value != right_value:
                resolved_value, resolution_type = self.conflict_resolver.resolve(
                    left, right, field
                )
                result[field] = resolved_value

                if resolution_type != "left":
                    conflicts.append({
                        "field": field,
                        "left_value": left_value,
                        "right_value": right_value,
                        "resolved_value": resolved_value,
                        "resolution_type": resolution_type,
                    })
            elif field in right:
                result[field] = right_value

        return result, conflicts

    def _compute_stats(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        merged: List[Dict[str, Any]],
        unmatched_left: List[Dict[str, Any]],
        unmatched_right: List[Dict[str, Any]],
    ) -> Dict[str, int]:
        """Compute merge statistics."""
        return {
            "left_count": len(left),
            "right_count": len(right),
            "merged_count": len(merged),
            "unmatched_left_count": len(unmatched_left),
            "unmatched_right_count": len(unmatched_right),
            "match_rate": len(merged) / len(left) if left else 0,
        }

    def concat(
        self,
        datasets: List[List[Dict[str, Any]]],
        deduplicate: bool = True,
        key_fields: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Concatenate multiple datasets."""
        result = []
        seen: set = set()

        for dataset in datasets:
            for record in dataset:
                if deduplicate and key_fields:
                    key = tuple(record.get(f) for f in key_fields)
                    if key in seen:
                        continue
                    seen.add(key)
                result.append(record)

        return result


class DataJoiner:
    """Performs different types of joins."""

    def __init__(self, merger: Optional[DataMerger] = None):
        self.merger = merger or DataMerger()

    def inner_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        left_key: str,
        right_key: str,
    ) -> List[Dict[str, Any]]:
        """Perform inner join."""
        left_merge_key = MergeKey(field=left_key)
        right_merge_key = MergeKey(field=right_key)

        result = self.merger.merge_on_keys(
            left, right, left_merge_key, right_merge_key, include_unmatched=False
        )
        return result.merged

    def left_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        left_key: str,
        right_key: str,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Perform left join."""
        left_merge_key = MergeKey(field=left_key)
        right_merge_key = MergeKey(field=right_key)

        result = self.merger.merge_on_keys(
            left, right, left_merge_key, right_merge_key, include_unmatched=True
        )
        return result.merged, result.unmatched_left

    def full_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        left_key: str,
        right_key: str,
    ) -> MergeResult:
        """Perform full outer join."""
        left_merge_key = MergeKey(field=left_key)
        right_merge_key = MergeKey(field=right_key)

        return self.merger.merge_on_keys(
            left, right, left_merge_key, right_merge_key, include_unmatched=True
        )


class DataReconciler:
    """Reconciles differences between datasets."""

    def __init__(self):
        self._reconciliation_rules: List[Callable] = []

    def add_rule(self, rule: Callable[[Dict, Dict], Dict]):
        """Add a reconciliation rule."""
        self._reconciliation_rules.append(rule)

    def reconcile(
        self,
        expected: List[Dict[str, Any]],
        actual: List[Dict[str, Any]],
        key_field: str,
    ) -> Dict[str, Any]:
        """Reconcile expected vs actual data."""
        actual_index = {r.get(key_field): r for r in actual}

        missing = []
        extra = []
        modified = []

        expected_keys = {r.get(key_field) for r in expected}
        actual_keys = set(actual_index.keys())

        for record in expected:
            key = record.get(key_field)
            if key not in actual_index:
                missing.append(record)
            else:
                actual_record = actual_index[key]
                differences = self._find_differences(record, actual_record)
                if differences:
                    modified.append({
                        "key": key,
                        "expected": record,
                        "actual": actual_record,
                        "differences": differences,
                    })

        for record in actual:
            key = record.get(key_field)
            if key not in expected_keys:
                extra.append(record)

        return {
            "missing": missing,
            "extra": extra,
            "modified": modified,
            "summary": {
                "total_expected": len(expected),
                "total_actual": len(actual),
                "missing_count": len(missing),
                "extra_count": len(extra),
                "modified_count": len(modified),
                "match_rate": 1 - (len(missing) + len(extra) + len(modified)) / len(expected) if expected else 1,
            },
        }

    def _find_differences(
        self,
        expected: Dict[str, Any],
        actual: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Find differences between two records."""
        differences = []
        all_fields = set(expected.keys()) | set(actual.keys())

        for field in all_fields:
            exp_value = expected.get(field)
            act_value = actual.get(field)

            if exp_value != act_value:
                differences.append({
                    "field": field,
                    "expected": exp_value,
                    "actual": act_value,
                })

        return differences


class DataMergerAction:
    """High-level data merger action."""

    def __init__(
        self,
        merger: Optional[DataMerger] = None,
        joiner: Optional[DataJoiner] = None,
        reconciler: Optional[DataReconciler] = None,
    ):
        self.merger = merger or DataMerger()
        self.joiner = joiner or DataJoiner(self.merger)
        self.reconciler = reconciler or DataReconciler()

    def merge(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        left_key: str,
        right_key: str,
        strategy: str = "last_wins",
    ) -> MergeResult:
        """Merge two datasets."""
        strategy_enum = MergeStrategy(strategy)
        left_merge_key = MergeKey(field=left_key)
        right_merge_key = MergeKey(field=right_key)

        return self.merger.merge_on_keys(
            left, right, left_merge_key, right_merge_key, strategy_enum
        )

    def join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        left_key: str,
        right_key: str,
        join_type: str = "inner",
    ) -> Union[List[Dict[str, Any]], MergeResult]:
        """Join two datasets."""
        if join_type == "inner":
            return self.joiner.inner_join(left, right, left_key, right_key)
        elif join_type == "left":
            return self.joiner.left_join(left, right, left_key, right_key)
        elif join_type == "full":
            return self.joiner.full_join(left, right, left_key, right_key)
        return []

    def reconcile(
        self,
        expected: List[Dict[str, Any]],
        actual: List[Dict[str, Any]],
        key_field: str,
    ) -> Dict[str, Any]:
        """Reconcile datasets."""
        return self.reconciler.reconcile(expected, actual, key_field)

    def concat(
        self,
        datasets: List[List[Dict[str, Any]]],
        deduplicate: bool = True,
    ) -> List[Dict[str, Any]]:
        """Concatenate datasets."""
        return self.merger.concat(datasets, deduplicate)


# Module exports
__all__ = [
    "DataMergerAction",
    "DataMerger",
    "DataJoiner",
    "DataReconciler",
    "ConflictResolver",
    "MergeResult",
    "MergeKey",
    "MergeStrategy",
]

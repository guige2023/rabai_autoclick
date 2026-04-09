"""Data Deduplication Action Module.

Provides intelligent deduplication with support for exact matching,
fuzzy matching, and configurable key-based deduplication strategies.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class DeduplicationStrategy(Enum):
    """Strategy for handling duplicates."""
    KEEP_FIRST = "keep_first"
    KEEP_LAST = "keep_last"
    KEEP_BEST = "keep_best"
    REMOVE_ALL = "remove_all"


class MatchType(Enum):
    """Type of matching for deduplication."""
    EXACT = "exact"
    FUZZY = "fuzzy"
    COMPOUND = "compound"


@dataclass
class DuplicateGroup:
    """A group of duplicate records."""
    key: Any
    records: List[Dict[str, Any]] = field(default_factory=list)
    original_indices: List[int] = field(default_factory=list)


@dataclass
class DeduplicationStats:
    """Statistics for deduplication operation."""
    total_records: int = 0
    duplicate_groups: int = 0
    duplicates_removed: int = 0
    unique_records: int = 0
    dedup_time_ms: float = 0.0


def _levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein distance between two strings."""
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)
    if len(s2) == 0:
        return len(s1)

    prev_row = range(len(s2) + 1)
    for i, c1 in enumerate(s1):
        curr_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = prev_row[j + 1] + 1
            deletions = curr_row[j] + 1
            substitutions = prev_row[j] + (c1 != c2)
            curr_row.append(min(insertions, deletions, substitutions))
        prev_row = curr_row

    return prev_row[-1]


def _get_nested_value(item: Dict[str, Any], field: str) -> Any:
    """Get nested field value using dot notation."""
    parts = field.split(".")
    value = item
    for part in parts:
        if isinstance(value, dict):
            value = value.get(part)
        elif isinstance(value, (list, tuple)):
            try:
                value = value[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
        if value is None:
            return None
    return value


class DataDeduplicationAction(BaseAction):
    """Data Deduplication Action for removing duplicate records.

    Supports exact, fuzzy, and compound key deduplication with
    configurable keep strategies.

    Examples:
        >>> action = DataDeduplicationAction()
        >>> result = action.execute(ctx, {
        ...     "data": [{"id": 1, "name": "Alice"}, {"id": 1, "name": "Alice"}],
        ...     "dedup_keys": ["id"],
        ...     "strategy": "keep_first"
        ... })
    """

    action_type = "data_deduplication"
    display_name = "数据去重"
    description = "精确/模糊/复合键去重，保留策略选择"

    def __init__(self):
        super().__init__()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data deduplication.

        Args:
            context: Execution context.
            params: Dict with keys:
                - data: List of dicts to deduplicate
                - dedup_keys: List of fields to use for deduplication
                - strategy: How to handle duplicates ('keep_first', 'keep_last', 'keep_best')
                - match_type: 'exact' or 'fuzzy' (default: 'exact')
                - fuzzy_threshold: Similarity threshold 0-1 for fuzzy matching
                - best_field: Field to use for 'keep_best' strategy
                - best_order: 'asc' or 'desc' for keep_best

        Returns:
            ActionResult with deduplicated data and statistics.
        """
        import time
        start_time = time.time()

        data = params.get("data", [])
        dedup_keys = params.get("dedup_keys", [])
        strategy_str = params.get("strategy", "keep_first")
        match_type_str = params.get("match_type", "exact")
        fuzzy_threshold = params.get("fuzzy_threshold", 0.85)
        best_field = params.get("best_field")
        best_order = params.get("best_order", "desc")

        if not isinstance(data, list):
            return ActionResult(
                success=False,
                message="'data' parameter must be a list"
            )

        try:
            strategy = DeduplicationStrategy(strategy_str)
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Invalid strategy: {strategy_str}"
            )

        # Deduplicate
        if match_type_str == "fuzzy":
            result, duplicate_groups = self._fuzzy_deduplicate(
                data, dedup_keys, strategy, fuzzy_threshold, best_field, best_order
            )
        else:
            result, duplicate_groups = self._exact_deduplicate(
                data, dedup_keys, strategy, best_field, best_order
            )

        duration_ms = (time.time() - start_time) * 1000
        stats = DeduplicationStats(
            total_records=len(data),
            duplicate_groups=len(duplicate_groups),
            duplicates_removed=len(data) - len(result),
            unique_records=len(result),
            dedup_time_ms=duration_ms,
        )

        return ActionResult(
            success=True,
            message=f"Deduplication: {len(data)} -> {len(result)} unique records "
                    f"({len(data) - len(result)} duplicates removed)",
            data={
                "deduplicated_data": result,
                "total_records": stats.total_records,
                "unique_records": stats.unique_records,
                "duplicates_removed": stats.duplicates_removed,
                "duplicate_groups": len(duplicate_groups),
                "dedup_time_ms": stats.dedup_time_ms,
            }
        )

    def _exact_deduplicate(
        self,
        data: List[Dict[str, Any]],
        keys: List[str],
        strategy: DeduplicationStrategy,
        best_field: Optional[str],
        best_order: str,
    ) -> Tuple[List[Dict[str, Any]], List[DuplicateGroup]]:
        """Perform exact key deduplication."""
        groups: Dict[Tuple, DuplicateGroup] = defaultdict(
            lambda: DuplicateGroup(key=None, records=[], original_indices=[])
        )

        for idx, item in enumerate(data):
            if not isinstance(item, dict):
                continue

            # Build key
            key_values = []
            for key in keys:
                val = _get_nested_value(item, key)
                key_values.append(val if val is not None else "__null__")
            key_tuple = tuple(key_values)

            if key_tuple not in groups:
                groups[key_tuple].key = key_tuple
            groups[key_tuple].records.append(item)
            groups[key_tuple].original_indices.append(idx)

        # Process groups
        result = []
        duplicate_groups = []

        for key_tuple, group in groups.items():
            if len(group.records) == 1:
                result.append(group.records[0])
            else:
                duplicate_groups.append(group)
                selected = self._select_record(
                    group.records, strategy, best_field, best_order
                )
                if selected is not None:
                    result.append(selected)

        return result, duplicate_groups

    def _fuzzy_deduplicate(
        self,
        data: List[Dict[str, Any]],
        keys: List[str],
        strategy: DeduplicationStrategy,
        threshold: float,
        best_field: Optional[str],
        best_order: str,
    ) -> Tuple[List[Dict[str, Any]], List[DuplicateGroup]]:
        """Perform fuzzy deduplication."""
        result = []
        duplicate_groups = []
        processed_indices: Set[int] = set()

        for i, item in enumerate(data):
            if i in processed_indices:
                continue

            current_group = DuplicateGroup(
                key=i, records=[item], original_indices=[i]
            )
            processed_indices.add(i)

            # Compare with remaining items
            key_i = str(_get_nested_value(item, keys[0]) if keys else item) if not keys else str(item)
            if key_i == "None":
                key_i = str(id(item))

            for j in range(i + 1, len(data)):
                if j in processed_indices:
                    continue

                item_j = data[j]
                key_j = str(_get_nested_value(item_j, keys[0]) if keys else item_j) if not keys else str(item_j)
                if key_j == "None":
                    key_j = str(id(item_j))

                # Calculate similarity
                max_len = max(len(key_i), len(key_j))
                if max_len == 0:
                    similarity = 1.0
                else:
                    distance = _levenshtein_distance(key_i.lower(), key_j.lower())
                    similarity = 1 - (distance / max_len)

                if similarity >= threshold:
                    current_group.records.append(item_j)
                    current_group.original_indices.append(j)
                    processed_indices.add(j)

            if len(current_group.records) == 1:
                result.append(current_group.records[0])
            else:
                duplicate_groups.append(current_group)
                selected = self._select_record(
                    current_group.records, strategy, best_field, best_order
                )
                if selected is not None:
                    result.append(selected)

        return result, duplicate_groups

    def _select_record(
        self,
        records: List[Dict[str, Any]],
        strategy: DeduplicationStrategy,
        best_field: Optional[str],
        best_order: str,
    ) -> Optional[Dict[str, Any]]:
        """Select which record to keep from duplicates."""
        if not records:
            return None

        if strategy == DeduplicationStrategy.KEEP_FIRST:
            return records[0]
        elif strategy == DeduplicationStrategy.KEEP_LAST:
            return records[-1]
        elif strategy == DeduplicationStrategy.KEEP_BEST and best_field:
            return self._select_best(records, best_field, best_order)
        elif strategy == DeduplicationStrategy.REMOVE_ALL:
            return None

        return records[0]

    def _select_best(
        self,
        records: List[Dict[str, Any]],
        field: str,
        order: str,
    ) -> Optional[Dict[str, Any]]:
        """Select best record based on a field value."""
        scored = []
        for record in records:
            value = _get_nested_value(record, field)
            try:
                score = float(value)
            except (TypeError, ValueError):
                score = 0
            scored.append((score, record))

        reverse = order == "desc"
        scored.sort(key=lambda x: x[0], reverse=reverse)
        return scored[0][1] if scored else None

    def get_required_params(self) -> List[str]:
        return ["data"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "dedup_keys": [],
            "strategy": "keep_first",
            "match_type": "exact",
            "fuzzy_threshold": 0.85,
            "best_field": None,
            "best_order": "desc",
        }

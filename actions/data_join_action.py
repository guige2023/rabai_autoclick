"""Data Join Action Module.

Provides various join operations including inner, left, right, full outer,
cross joins, and fuzzy matching for data combination.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class JoinType(Enum):
    """Join type enumeration."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL_OUTER = "full_outer"
    LEFT_ANTI = "left_anti"
    RIGHT_ANTI = "right_anti"
    CROSS = "cross"
    FUZZY = "fuzzy"


@dataclass
class JoinSpec:
    """Specification for a join operation."""
    left_key: str
    right_key: str
    join_type: JoinType = JoinType.INNER
    fuzzy_threshold: float = 0.8
    compound_key: bool = False


@dataclass
class JoinStats:
    """Statistics for join operations."""
    left_rows: int = 0
    right_rows: int = 0
    joined_rows: int = 0
    unmatched_left: int = 0
    unmatched_right: int = 0
    join_time_ms: float = 0.0


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


def _fuzzy_match(left_val: Any, right_val: Any, threshold: float) -> bool:
    """Check if two values match with fuzzy matching."""
    if left_val is None or right_val is None:
        return False

    left_str = str(left_val).lower()
    right_str = str(right_val).lower()

    # Exact match
    if left_str == right_str:
        return True

    # Length-based quick check
    len_diff = abs(len(left_str) - len(right_str))
    max_len = max(len(left_str), len(right_str))
    if max_len == 0:
        return False
    if len_diff > max_len * (1 - threshold):
        return False

    # Levenshtein-based similarity
    distance = _levenshtein_distance(left_str, right_str)
    similarity = 1 - (distance / max_len)
    return similarity >= threshold


def _make_hashable(value: Any) -> Any:
    """Convert value to hashable type."""
    if value is None:
        return "__null__"
    if isinstance(value, (list, tuple)):
        return tuple(_make_hashable(v) for v in value)
    if isinstance(value, dict):
        return tuple(sorted((k, _make_hashable(v)) for k, v in value.items()))
    return value


class DataJoinAction(BaseAction):
    """Data Join Action for combining datasets.

    Supports inner, left, right, full outer, anti, cross, and fuzzy joins
    with nested field access and compound keys.

    Examples:
        >>> action = DataJoinAction()
        >>> result = action.execute(ctx, {
        ...     "left_data": [{"id": 1, "name": "Alice"}],
        ...     "right_data": [{"id": 1, "score": 95}],
        ...     "left_key": "id",
        ...     "right_key": "id",
        ...     "join_type": "left"
        ... })
    """

    action_type = "data_join"
    display_name = "数据关联"
    description = "多类型JOIN操作支持、内/外/反连接、模糊匹配"

    def __init__(self):
        super().__init__()
        self._stats = JoinStats()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute data join operation.

        Args:
            context: Execution context.
            params: Dict with keys:
                - left_data: Left dataset (list of dicts)
                - right_data: Right dataset (list of dicts)
                - left_key: Field name for left key
                - right_key: Field name for right key
                - join_type: Type of join (default: 'inner')
                - fuzzy_threshold: Fuzzy match threshold 0-1 (default: 0.8)
                - compound_key: Use compound key (left_key is list)
                - select_fields: Fields to include in output
                - prefix_left: Prefix for left field names
                - prefix_right: Prefix for right field names

        Returns:
            ActionResult with joined data and statistics.
        """
        import time
        start_time = time.time()

        left_data = params.get("left_data", [])
        right_data = params.get("right_data", [])
        left_key = params.get("left_key")
        right_key = params.get("right_key")
        join_type_str = params.get("join_type", "inner")
        fuzzy_threshold = params.get("fuzzy_threshold", 0.8)
        compound_key = params.get("compound_key", False)
        select_fields = params.get("select_fields")
        prefix_left = params.get("prefix_left", "")
        prefix_right = params.get("prefix_right", "")

        try:
            join_type = JoinType(join_type_str)
        except ValueError:
            return ActionResult(
                success=False,
                message=f"Invalid join type: {join_type_str}"
            )

        if not left_data or not right_data:
            return ActionResult(
                success=False,
                message="Both left_data and right_data must be non-empty"
            )

        # Handle compound keys
        if compound_key and isinstance(left_key, list) and isinstance(right_key, list):
            left_keys = left_key
            right_keys = right_key
        else:
            left_keys = [left_key]
            right_keys = [right_key]

        # Execute join
        if join_type == JoinType.CROSS:
            joined = self._cross_join(left_data, right_data, prefix_left, prefix_right)
        elif join_type == JoinType.FUZZY:
            joined = self._fuzzy_join(
                left_data, right_data,
                left_keys, right_keys,
                fuzzy_threshold, prefix_left, prefix_right
            )
        else:
            joined = self._hash_join(
                left_data, right_data,
                left_keys, right_keys,
                join_type, prefix_left, prefix_right
            )

        # Apply field selection
        if select_fields:
            joined = self._select_fields(joined, select_fields)

        duration_ms = (time.time() - start_time) * 1000
        self._stats = JoinStats(
            left_rows=len(left_data),
            right_rows=len(right_data),
            joined_rows=len(joined),
            join_time_ms=duration_ms,
        )

        return ActionResult(
            success=True,
            message=f"Joined {len(left_data)} left x {len(right_data)} right -> {len(joined)} rows",
            data={
                "joined_data": joined,
                "joined_rows": len(joined),
                "left_rows": len(left_data),
                "right_rows": len(right_data),
                "join_time_ms": duration_ms,
                "join_type": join_type.value,
            }
        )

    def _hash_join(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        left_keys: List[str],
        right_keys: List[str],
        join_type: JoinType,
        prefix_left: str,
        prefix_right: str,
    ) -> List[Dict[str, Any]]:
        """Perform hash join."""
        # Build right index
        right_index: Dict[Tuple, List[Dict[str, Any]]] = defaultdict(list)
        for right_row in right_data:
            key_values = []
            for key in right_keys:
                val = _get_nested_value(right_row, key)
                key_values.append(_make_hashable(val))
            key_tuple = tuple(key_values)
            right_index[key_tuple].append(right_row)

        # Match left rows
        matched_right: Set[int] = set()
        joined: List[Dict[str, Any]] = []
        unmatched_left: List[Dict[str, Any]] = []

        for i, left_row in enumerate(left_data):
            key_values = []
            for key in left_keys:
                val = _get_nested_value(left_row, key)
                key_values.append(_make_hashable(val))
            key_tuple = tuple(key_values)

            right_matches = right_index.get(key_tuple, [])
            if right_matches:
                for right_row in right_matches:
                    joined_row = self._merge_rows(
                        left_row, right_row, left_keys, right_keys,
                        prefix_left, prefix_right
                    )
                    joined.append(joined_row)
                    matched_right.add(id(right_row))
            elif join_type in (JoinType.LEFT, JoinType.FULL_OUTER):
                unmatched_left.append(left_row)
                joined_row = self._prefix_row(left_row, prefix_left)
                joined_row.update({f"{prefix_right}{k}": None for k in right_keys})
                joined.append(joined_row)

        # Handle right outer and full outer
        if join_type in (JoinType.RIGHT, JoinType.FULL_OUTER):
            for right_row in right_data:
                if id(right_row) not in matched_right:
                    joined_row = {f"{prefix_left}{k}": None for k in left_keys}
                    joined_row.update(self._prefix_row(right_row, prefix_right))
                    joined.append(joined_row)

        # Anti joins
        if join_type == JoinType.LEFT_ANTI:
            joined = []
            for left_row in left_data:
                key_values = []
                for key in left_keys:
                    val = _get_nested_value(left_row, key)
                    key_values.append(_make_hashable(val))
                key_tuple = tuple(key_values)
                if key_tuple not in right_index:
                    joined.append(self._prefix_row(left_row, prefix_left))

        elif join_type == JoinType.RIGHT_ANTI:
            joined = []
            for right_row in right_data:
                key_values = []
                for key in right_keys:
                    val = _get_nested_value(right_row, key)
                    key_values.append(_make_hashable(val))
                key_tuple = tuple(key_values)
                # Check if key exists in left
                has_match = any(
                    _get_nested_value(lr, left_keys[0]) == key_values[0]
                    for lr in left_data
                )
                if not has_match:
                    joined.append(self._prefix_row(right_row, prefix_right))

        return joined

    def _fuzzy_join(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        left_keys: List[str],
        right_keys: List[str],
        threshold: float,
        prefix_left: str,
        prefix_right: str,
    ) -> List[Dict[str, Any]]:
        """Perform fuzzy join."""
        joined = []
        for left_row in left_data:
            best_match = None
            best_score = 0.0

            for right_row in right_data:
                left_val = _get_nested_value(left_row, left_keys[0])
                right_val = _get_nested_value(right_row, right_keys[0])

                if _fuzzy_match(left_val, right_val, threshold):
                    score = 1 - (_levenshtein_distance(str(left_val).lower(),
                                                         str(right_val).lower()) /
                                  max(len(str(left_val)), len(str(right_val)), 1))
                    if score > best_score:
                        best_score = score
                        best_match = right_row

            if best_match:
                joined.append(self._merge_rows(
                    left_row, best_match, left_keys, right_keys,
                    prefix_left, prefix_right
                ))

        return joined

    def _cross_join(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        prefix_left: str,
        prefix_right: str,
    ) -> List[Dict[str, Any]]:
        """Perform cross join."""
        joined = []
        for left_row in left_data:
            for right_row in right_data:
                joined_row = self._merge_rows(
                    left_row, right_row, [], [],
                    prefix_left, prefix_right
                )
                joined.append(joined_row)
        return joined

    def _merge_rows(
        self,
        left_row: Dict[str, Any],
        right_row: Dict[str, Any],
        left_keys: List[str],
        right_keys: List[str],
        prefix_left: str,
        prefix_right: str,
    ) -> Dict[str, Any]:
        """Merge two rows handling key overlap."""
        result = {}
        left_keys_set = set(left_keys)

        # Add left row with prefix
        for k, v in left_row.items():
            if k not in left_keys_set:
                result[f"{prefix_left}{k}"] = v
            else:
                result[k] = v

        # Add right row with prefix
        for k, v in right_row.items():
            result[f"{prefix_right}{k}"] = v

        return result

    def _prefix_row(self, row: Dict[str, Any], prefix: str) -> Dict[str, Any]:
        """Add prefix to all keys in a row."""
        if not prefix:
            return dict(row)
        return {f"{prefix}{k}": v for k, v in row.items()}

    def _select_fields(
        self, data: List[Dict[str, Any]], fields: List[str]
    ) -> List[Dict[str, Any]]:
        """Select specific fields from data."""
        return [{k: row.get(k) for k in fields if k in row} for row in data]

    def get_stats(self) -> Dict[str, Any]:
        """Get join statistics."""
        return {
            "left_rows": self._stats.left_rows,
            "right_rows": self._stats.right_rows,
            "joined_rows": self._stats.joined_rows,
            "join_time_ms": self._stats.join_time_ms,
        }

    def get_required_params(self) -> List[str]:
        return ["left_data", "right_data", "left_key", "right_key"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "join_type": "inner",
            "fuzzy_threshold": 0.8,
            "compound_key": False,
            "select_fields": None,
            "prefix_left": "",
            "prefix_right": "",
        }

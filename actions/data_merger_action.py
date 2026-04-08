"""
Data Merger Action Module.

Merges multiple datasets with configurable join keys,
 conflict resolution, and deduplication strategies.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class MergeStrategy(Enum):
    """Strategy for merging datasets."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    OUTER = "outer"
    CROSS = "cross"


class ConflictResolution(Enum):
    """How to resolve conflicting values."""
    LEFT_WINS = "left_wins"
    RIGHT_WINS = "right_wins"
    PREFER_LEFT = "prefer_left"
    PREFER_RIGHT = "prefer_right"
    CONCATENATE = "concatenate"
    CUSTOM = "custom"


@dataclass
class MergeConfig:
    """Configuration for merge operations."""
    strategy: MergeStrategy = MergeStrategy.LEFT
    conflict_resolution: ConflictResolution = ConflictResolution.LEFT_WINS
    dedupe: bool = True
    case_sensitive: bool = True


class DataMergerAction:
    """
    Dataset merging with multiple strategies.

    Merges datasets using various join strategies with
    conflict resolution and deduplication.

    Example:
        merger = DataMergerAction()
        result = merger.merge(
            left_dataset,
            right_dataset,
            left_on="id",
            right_on="user_id",
            strategy=MergeStrategy.LEFT,
        )
    """

    def __init__(
        self,
        config: Optional[MergeConfig] = None,
    ) -> None:
        self.config = config or MergeConfig()

    def merge(
        self,
        left: list[dict[str, Any]],
        right: list[dict[str, Any]],
        left_on: str,
        right_on: Optional[str] = None,
        config: Optional[MergeConfig] = None,
    ) -> list[dict[str, Any]]:
        """Merge two datasets."""
        config = config or self.config
        right_on = right_on or left_on

        if config.strategy == MergeStrategy.CROSS:
            return self._cross_join(left, right)

        right_index = self._build_index(right, right_on)
        result: list[dict[str, Any]] = []
        used_right: set[int] = set()

        for idx, left_row in enumerate(left):
            right_key = left_row.get(left_on)
            right_matches = self._find_matches(right_index, right_key, config.case_sensitive)

            if not right_matches:
                if config.strategy in (MergeStrategy.LEFT, MergeStrategy.OUTER):
                    result.append(dict(left_row))
            else:
                for right_idx in right_matches:
                    used_right.add(right_idx)
                    merged = self._merge_rows(
                        left_row,
                        right[right_idx],
                        config.conflict_resolution,
                    )
                    result.append(merged)

        if config.strategy in (MergeStrategy.RIGHT, MergeStrategy.OUTER):
            for idx, right_row in enumerate(right):
                if idx not in used_right:
                    result.append(dict(right_row))

        if config.dedupe:
            result = self._deduplicate(result)

        return result

    def _build_index(
        self,
        data: list[dict[str, Any]],
        key_field: str,
    ) -> dict[Any, list[tuple[int, dict[str, Any]]]]:
        """Build lookup index for join key."""
        index: dict[Any, list[tuple[int, dict[str, Any]]]] = {}

        for idx, row in enumerate(data):
            key = row.get(key_field)
            if key is not None:
                if key not in index:
                    index[key] = []
                index[key].append((idx, row))

        return index

    def _find_matches(
        self,
        index: dict[Any, list[tuple[int, dict[str, Any]]]],
        key: Any,
        case_sensitive: bool,
    ) -> list[int]:
        """Find matching indices for a key."""
        if not case_sensitive and isinstance(key, str):
            key_lower = key.lower()
            for k in index:
                if isinstance(k, str) and k.lower() == key_lower:
                    return [idx for idx, _ in index[k]]
            return []

        return [idx for idx, _ in index.get(key, [])]

    def _cross_join(
        self,
        left: list[dict[str, Any]],
        right: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Create cross join of two datasets."""
        result: list[dict[str, Any]] = []

        for l in left:
            for r in right:
                merged = dict(l)
                merged.update({f"right_{k}": v for k, v in r.items()})
                result.append(merged)

        return result

    def _merge_rows(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        conflict_resolution: ConflictResolution,
    ) -> dict[str, Any]:
        """Merge two rows with conflict resolution."""
        result: dict[str, Any] = {}
        all_keys = set(left.keys()) | set(right.keys())

        for key in all_keys:
            left_val = left.get(key)
            right_val = right.get(key)

            if key not in left:
                result[key] = right_val
            elif key not in right:
                result[key] = left_val
            elif left_val == right_val:
                result[key] = left_val
            else:
                result[key] = self._resolve_conflict(
                    left_val,
                    right_val,
                    conflict_resolution,
                )

        return result

    def _resolve_conflict(
        self,
        left: Any,
        right: Any,
        resolution: ConflictResolution,
    ) -> Any:
        """Resolve value conflict between left and right."""
        if resolution == ConflictResolution.LEFT_WINS:
            return left
        elif resolution == ConflictResolution.RIGHT_WINS:
            return right
        elif resolution == ConflictResolution.PREFER_LEFT:
            return left if left is not None else right
        elif resolution == ConflictResolution.PREFER_RIGHT:
            return right if right is not None else left
        elif resolution == ConflictResolution.CONCATENATE:
            return f"{left}|{right}"

        return left

    def _deduplicate(
        self,
        data: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Remove duplicate rows."""
        seen: set[str] = set()
        result: list[dict[str, Any]] = []

        for row in data:
            key = self._row_key(row)
            if key not in seen:
                seen.add(key)
                result.append(row)

        return result

    def _row_key(self, row: dict[str, Any]) -> str:
        """Generate deduplication key for a row."""
        items = []
        for k in sorted(row.keys()):
            items.append(f"{k}={row[k]}")
        return "|".join(items)

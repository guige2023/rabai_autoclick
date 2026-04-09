"""
Data Join Action Module.

Provides join operations for combining datasets based on keys,
including inner, left, right, full outer, and cross joins.

Author: RabAi Team
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

T = TypeVar("T")


class JoinType(Enum):
    """Types of join operations."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL_OUTER = "full_outer"
    CROSS = "cross"


@dataclass
class JoinConfig:
    """Configuration for join operation."""
    left_key: str
    right_key: str
    join_type: JoinType = JoinType.INNER
    left_select: Optional[List[str]] = None
    right_select: Optional[List[str]] = None
    left_prefix: Optional[str] = None
    right_prefix: Optional[str] = None


@dataclass
class JoinResult:
    """Result of a join operation."""
    data: List[Dict[str, Any]]
    matched_count: int
    unmatched_left_count: int
    unmatched_right_count: int
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataJoiner:
    """Main join engine."""

    def __init__(self) -> None:
        pass

    def _get_key_value(self, record: Dict[str, Any], key: str) -> Any:
        """Get value from record using key (supports dot notation)."""
        keys = key.split(".")
        value = record
        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return None
        return value

    def _select_fields(
        self,
        record: Dict[str, Any],
        fields: Optional[List[str]],
        prefix: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Select specific fields from record."""
        if fields is None:
            result = record.copy()
        else:
            result = {f: record.get(f) for f in fields if f in record}

        if prefix:
            result = {f"{prefix}{k}": v for k, v in result.items()}

        return result

    def _build_index(
        self,
        data: List[Dict[str, Any]],
        key: str,
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """Build index on key for faster lookups."""
        index: Dict[Any, List[Dict[str, Any]]] = {}
        for record in data:
            key_value = self._get_key_value(record, key)
            if key_value not in index:
                index[key_value] = []
            index[key_value].append(record)
        return index

    def join(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        config: JoinConfig,
    ) -> JoinResult:
        """Perform join operation."""
        if config.join_type == JoinType.CROSS:
            return self._cross_join(left_data, right_data)

        # Build index on right data for faster lookups
        right_index = self._build_index(right_data, config.right_key)

        result: List[Dict[str, Any]] = []
        matched_left: set = set()
        matched_right: set = set()

        # Process left records
        for i, left_record in enumerate(left_data):
            left_key_val = self._get_key_value(left_record, config.left_key)
            right_records = right_index.get(left_key_val, [])

            if right_records:
                for right_record in right_records:
                    joined = self._merge_records(
                        left_record,
                        right_record,
                        config,
                    )
                    result.append(joined)
                    matched_left.add(i)
                    matched_right.add(id(right_record))
            elif config.join_type in (JoinType.LEFT, JoinType.FULL_OUTER):
                # Left join with no match
                left_selected = self._select_fields(
                    left_record,
                    config.left_select,
                    config.left_prefix,
                )
                right_selected = self._select_fields(
                    {},
                    config.right_select,
                    config.right_prefix,
                )
                result.append({**left_selected, **right_selected})

        # Process right records for right/full outer joins
        if config.join_type in (JoinType.RIGHT, JoinType.FULL_OUTER):
            for i, right_record in enumerate(right_data):
                if id(right_record) not in matched_right:
                    right_selected = self._select_fields(
                        right_record,
                        config.right_select,
                        config.right_prefix,
                    )
                    left_selected = self._select_fields(
                        {},
                        config.left_select,
                        config.left_prefix,
                    )
                    result.append({**left_selected, **right_selected})

        unmatched_left = len(left_data) - len(matched_left)
        unmatched_right = len(right_data) - len(matched_right)

        return JoinResult(
            data=result,
            matched_count=len(matched_left),
            unmatched_left_count=unmatched_left,
            unmatched_right_count=unmatched_right,
            metadata={
                "join_type": config.join_type.value,
                "left_count": len(left_data),
                "right_count": len(right_data),
            },
        )

    def _merge_records(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any],
        config: JoinConfig,
    ) -> Dict[str, Any]:
        """Merge two records based on config."""
        left_selected = self._select_fields(
            left,
            config.left_select,
            config.left_prefix,
        )
        right_selected = self._select_fields(
            right,
            config.right_select,
            config.right_prefix,
        )
        return {**left_selected, **right_selected}

    def _cross_join(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
    ) -> JoinResult:
        """Perform cross join."""
        result = []
        for left in left_data:
            for right in right_data:
                result.append({**left, **right})

        return JoinResult(
            data=result,
            matched_count=len(left_data) * len(right_data),
            unmatched_left_count=0,
            unmatched_right_count=0,
            metadata={
                "join_type": "cross",
                "left_count": len(left_data),
                "right_count": len(right_data),
            },
        )

    def inner_join(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        left_key: str,
        right_key: str,
    ) -> JoinResult:
        """Perform inner join."""
        config = JoinConfig(left_key=left_key, right_key=right_key, join_type=JoinType.INNER)
        return self.join(left_data, right_data, config)

    def left_join(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        left_key: str,
        right_key: str,
    ) -> JoinResult:
        """Perform left join."""
        config = JoinConfig(left_key=left_key, right_key=right_key, join_type=JoinType.LEFT)
        return self.join(left_data, right_data, config)

    def right_join(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        left_key: str,
        right_key: str,
    ) -> JoinResult:
        """Perform right join."""
        config = JoinConfig(left_key=left_key, right_key=right_key, join_type=JoinType.RIGHT)
        return self.join(left_data, right_data, config)

    def full_outer_join(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        left_key: str,
        right_key: str,
    ) -> JoinResult:
        """Perform full outer join."""
        config = JoinConfig(left_key=left_key, right_key=right_key, join_type=JoinType.FULL_OUTER)
        return self.join(left_data, right_data, config)


class MultiWayJoiner:
    """Join multiple datasets in sequence."""

    def __init__(self) -> None:
        self.joiner = DataJoiner()

    def join_multiple(
        self,
        datasets: List[Tuple[List[Dict[str, Any]], str, str]],
        join_type: JoinType = JoinType.INNER,
    ) -> List[Dict[str, Any]]:
        """Join multiple datasets in sequence."""
        if not datasets:
            return []
        if len(datasets) == 1:
            return datasets[0][0]

        result = datasets[0][0]
        for i in range(1, len(datasets)):
            left_key = f"_key_{i-1}"
            right_key = datasets[i][1]

            # Add key column to current result
            for record in result:
                record[left_key] = record.get(left_key, record.get("id", ""))

            right_data = datasets[i][0]

            config = JoinConfig(
                left_key=left_key,
                right_key=right_key,
                join_type=join_type,
            )
            join_result = self.joiner.join(result, right_data, config)
            result = join_result.data

        return result

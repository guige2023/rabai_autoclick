"""
Data Joiner Action Module.

Provides SQL-style join operations for data sources
including inner, left, right, full, and cross joins.

Author: rabai_autoclick team
"""

import logging
from typing import (
    Optional, Dict, Any, List, Callable, Union,
    Tuple, TypeVar
)
from dataclasses import dataclass, field
from enum import Enum
from itertools import product

logger = logging.getLogger(__name__)

T = TypeVar("T")


class JoinType(Enum):
    """Join types supported."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"
    LEFT_ANTI = "left_anti"
    RIGHT_ANTI = "right_anti"


@dataclass
class JoinConfig:
    """Configuration for join operations."""
    join_type: JoinType = JoinType.INNER
    left_key: Union[str, Callable] = "id"
    right_key: Union[str, Callable] = "id"
    left_suffix: str = ""
    right_suffix: str = "_right"
    how: Optional[str] = None
    validate: bool = True


@dataclass
class DataSource:
    """Represents a data source for joining."""
    data: List[Dict[str, Any]]
    alias: Optional[str] = None
    key_field: Optional[str] = None

    @property
    def size(self) -> int:
        """Get number of records."""
        return len(self.data)


class DataJoinerAction:
    """
    SQL-Style Join Operations.

    Provides various join types for combining data from
    multiple sources with support for complex key functions.

    Example:
        >>> joiner = DataJoinerAction()
        >>> left = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        >>> right = [{"id": 1, "value": 100}, {"id": 3, "value": 300}]
        >>> result = joiner.join(left, right, JoinType.LEFT, "id")
    """

    def __init__(self, config: Optional[JoinConfig] = None):
        self.config = config or JoinConfig()

    def _get_key_value(
        self,
        record: Dict[str, Any],
        key: Union[str, Callable],
    ) -> Any:
        """
        Get key value from a record.

        Args:
            record: Data record
            key: Key field name or key function

        Returns:
            Key value
        """
        if callable(key):
            return key(record)
        return record.get(key)

    def _create_index(
        self,
        data: List[Dict[str, Any]],
        key: Union[str, Callable],
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """
        Create an index on key field for efficient lookup.

        Args:
            data: Data records
            key: Key field or function

        Returns:
            Dictionary mapping key values to records
        """
        index: Dict[Any, List[Dict[str, Any]]] = {}

        for record in data:
            key_value = self._get_key_value(record, key)
            if key_value not in index:
                index[key_value] = []
            index[key_value].append(record)

        return index

    def join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        join_type: JoinType = JoinType.INNER,
        left_key: Union[str, Callable] = "id",
        right_key: Union[str, Callable] = "id",
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        Join two data sources.

        Args:
            left: Left data source
            right: Right data source
            join_type: Type of join
            left_key: Key field/function for left
            right_key: Key field/function for right
            **kwargs: Additional configuration

        Returns:
            Joined records
        """
        if not left or not right:
            return self._handle_empty_join(left, right, join_type)

        if join_type == JoinType.CROSS:
            return self._cross_join(left, right, **kwargs)

        left_index = self._create_index(left, left_key)
        right_index = self._create_index(right, right_key)

        left_keys = set(left_index.keys())
        right_keys = set(right_index.keys())

        result: List[Dict[str, Any]] = []

        if join_type == JoinType.INNER:
            common_keys = left_keys & right_keys
            for key in common_keys:
                result.extend(
                    self._merge_records(l, r, **kwargs)
                    for l in left_index[key]
                    for r in right_index[key]
                )

        elif join_type == JoinType.LEFT:
            for record in left:
                key = self._get_key_value(record, left_key)
                if key in right_index:
                    result.extend(
                        self._merge_records(record, r, **kwargs)
                        for r in right_index[key]
                    )
                else:
                    result.append(self._pad_record(record, len(right[0]) if right else 0, is_left=True, **kwargs))

        elif join_type == JoinType.RIGHT:
            for record in right:
                key = self._get_key_value(record, right_key)
                if key in left_index:
                    result.extend(
                        self._merge_records(l, record, **kwargs)
                        for l in left_index[key]
                    )
                else:
                    result.append(self._pad_record(record, len(left[0]) if left else 0, is_left=False, **kwargs))

        elif join_type == JoinType.FULL:
            matched_keys = left_keys & right_keys

            for key in matched_keys:
                result.extend(
                    self._merge_records(l, r, **kwargs)
                    for l in left_index[key]
                    for r in right_index[key]
                )

            for key in left_keys - right_keys:
                for record in left_index[key]:
                    result.append(
                        self._pad_record(record, 0, is_left=True, **kwargs)
                    )

            for key in right_keys - left_keys:
                for record in right_index[key]:
                    result.append(
                        self._pad_record(record, 0, is_left=False, **kwargs)
                    )

        elif join_type == JoinType.LEFT_ANTI:
            for record in left:
                key = self._get_key_value(record, left_key)
                if key not in right_keys:
                    result.append(record)

        elif join_type == JoinType.RIGHT_ANTI:
            for record in right:
                key = self._get_key_value(record, right_key)
                if key not in left_keys:
                    result.append(record)

        return result

    def _cross_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """Execute cross join."""
        result = []
        for l_record, r_record in product(left, right):
            result.append(self._merge_records(l_record, r_record, **kwargs))
        return result

    def _merge_records(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any],
        left_suffix: str = "",
        right_suffix: str = "_right",
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Merge two records into one.

        Args:
            left: Left record
            right: Right record
            left_suffix: Suffix for left-conflicting keys
            right_suffix: Suffix for right-conflicting keys

        Returns:
            Merged record
        """
        result = {}

        for key, value in left.items():
            if key in right and left_suffix or right_suffix:
                result[f"{key}{left_suffix}"] = value
            else:
                result[key] = value

        for key, value in right.items():
            if key in left:
                result[f"{key}{right_suffix}"] = value
            else:
                result[key] = value

        return result

    def _pad_record(
        self,
        record: Dict[str, Any],
        padding_count: int,
        is_left: bool,
        right_suffix: str = "_right",
        **kwargs,
    ) -> Dict[str, Any]:
        """Pad record with null values."""
        result = record.copy()
        for i in range(padding_count):
            result[f"_padding_{i}{right_suffix}"] = None
        return result

    def _handle_empty_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        join_type: JoinType,
    ) -> List[Dict[str, Any]]:
        """Handle joins with empty data sources."""
        if join_type in (JoinType.INNER, JoinType.LEFT_ANTI, JoinType.RIGHT_ANTI):
            return []
        if join_type == JoinType.LEFT:
            return left.copy()
        if join_type == JoinType.RIGHT:
            return right.copy()
        if join_type == JoinType.FULL:
            return left.copy() + right.copy()
        if join_type == JoinType.CROSS:
            return []
        return []

    def multi_join(
        self,
        sources: List[Tuple[List[Dict[str, Any]], JoinType, str, str]],
    ) -> List[Dict[str, Any]]:
        """
        Perform multiple joins in sequence.

        Args:
            sources: List of (data, join_type, left_key, right_key) tuples

        Returns:
            Final joined result
        """
        if not sources:
            return []

        result = sources[0][0]

        for left_data, join_type, left_key, right_key in sources[1:]:
            result = self.join(
                result,
                left_data,
                join_type=join_type,
                left_key=left_key,
                right_key=right_key,
            )

        return result

    def broadcast_join(
        self,
        small: List[Dict[str, Any]],
        large: List[Dict[str, Any]],
        key: Union[str, Callable] = "id",
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        Optimize join by broadcasting small dataset.

        Args:
            small: Small data source (broadcasted)
            large: Large data source
            key: Join key
            **kwargs: Additional join config

        Returns:
            Join result
        """
        return self.join(
            small if len(small) <= len(large) else large,
            large if len(small) <= len(large) else small,
            left_key=key,
            right_key=key,
            **kwargs,
        )

    def hash_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        key: Union[str, Callable] = "id",
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        Perform hash join (alias for standard join).

        Args:
            left: Left data source
            right: Right data source
            key: Join key
            **kwargs: Additional configuration

        Returns:
            Join result
        """
        return self.join(left, right, left_key=key, right_key=key, **kwargs)

    def merge_asof(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        left_time: str = "timestamp",
        right_time: str = "timestamp",
        direction: str = "nearest",
    ) -> List[Dict[str, Any]]:
        """
        Perform asof merge (time-based join).

        Args:
            left: Left data source
            right: Right data source
            left_time: Time field in left
            right_time: Time field in right
            direction: Merge direction (nearest, forward, backward)

        Returns:
            Merged result
        """
        import bisect

        result = []
        right_times = sorted([r[right_time] for r in right])

        for record in left:
            lt = record.get(left_time)

            if not right_times:
                result.append({**record, **dict.fromkeys(right[0].keys(), None) if right else {}})
                continue

            idx = bisect.bisect_left(right_times, lt)

            if direction == "nearest":
                candidates = []
                if idx < len(right_times):
                    candidates.append((abs(right_times[idx] - lt), idx))
                if idx > 0:
                    candidates.append((abs(right_times[idx - 1] - lt), idx - 1))
                if candidates:
                    _, best_idx = min(candidates)
                else:
                    best_idx = idx if idx < len(right_times) else idx - 1

            elif direction == "forward":
                best_idx = idx if idx < len(right_times) else len(right_times) - 1

            elif direction == "backward":
                best_idx = idx - 1 if idx > 0 else 0

            else:
                best_idx = idx if idx < len(right_times) else len(right_times) - 1

            if 0 <= best_idx < len(right):
                result.append({**record, **right[best_idx]})
            else:
                result.append({**record})

        return result

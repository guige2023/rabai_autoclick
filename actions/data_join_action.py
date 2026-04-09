"""Data join action for combining datasets.

Performs SQL-style joins between datasets with support
for inner, outer, left, and right join operations.
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class JoinType(Enum):
    """Types of join operations."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


@dataclass
class JoinConfig:
    """Configuration for join operations."""
    join_type: JoinType = JoinType.INNER
    left_key: str = "id"
    right_key: str = "id"
    left_prefix: Optional[str] = None
    right_prefix: Optional[str] = None
    null_value: Any = None


@dataclass
class JoinResult:
    """Result of a join operation."""
    data: list[dict]
    left_count: int
    right_count: int
    joined_count: int
    processing_time_ms: float


class DataJoinAction:
    """Join multiple datasets together.

    Example:
        >>> joiner = DataJoinAction()
        >>> result = joiner.join(left_data, right_data, left_key="id", right_key="user_id")
    """

    def __init__(self) -> None:
        self._default_config = JoinConfig()

    def join(
        self,
        left_data: list[dict],
        right_data: list[dict],
        left_key: str = "id",
        right_key: str = "id",
        join_type: JoinType = JoinType.INNER,
        left_prefix: Optional[str] = None,
        right_prefix: Optional[str] = None,
    ) -> JoinResult:
        """Join two datasets.

        Args:
            left_data: Left dataset.
            right_data: Right dataset.
            left_key: Key field in left dataset.
            right_key: Key field in right dataset.
            join_type: Type of join.
            left_prefix: Prefix for left dataset fields.
            right_prefix: Prefix for right dataset fields.

        Returns:
            Join result with merged data.
        """
        import time
        start_time = time.time()

        if not left_data or not right_data:
            if join_type == JoinType.LEFT:
                result = left_data.copy()
            elif join_type == JoinType.RIGHT:
                result = right_data.copy()
            else:
                result = []
        else:
            if join_type == JoinType.INNER:
                result = self._inner_join(
                    left_data, right_data, left_key, right_key,
                    left_prefix, right_prefix
                )
            elif join_type == JoinType.LEFT:
                result = self._left_join(
                    left_data, right_data, left_key, right_key,
                    left_prefix, right_prefix
                )
            elif join_type == JoinType.RIGHT:
                result = self._right_join(
                    left_data, right_data, left_key, right_key,
                    left_prefix, right_prefix
                )
            elif join_type == JoinType.FULL:
                result = self._full_join(
                    left_data, right_data, left_key, right_key,
                    left_prefix, right_prefix
                )
            elif join_type == JoinType.CROSS:
                result = self._cross_join(
                    left_data, right_data, left_prefix, right_prefix
                )
            else:
                result = []

        return JoinResult(
            data=result,
            left_count=len(left_data),
            right_count=len(right_data),
            joined_count=len(result),
            processing_time_ms=(time.time() - start_time) * 1000,
        )

    def _inner_join(
        self,
        left_data: list[dict],
        right_data: list[dict],
        left_key: str,
        right_key: str,
        left_prefix: Optional[str],
        right_prefix: Optional[str],
    ) -> list[dict]:
        """Perform inner join.

        Args:
            left_data: Left dataset.
            right_data: Right dataset.
            left_key: Left key field.
            right_key: Right key field.
            left_prefix: Left prefix.
            right_prefix: Right prefix.

        Returns:
            Joined records.
        """
        right_index = {row[right_key]: row for row in right_data}
        result = []

        for left_row in left_data:
            key = left_row.get(left_key)
            if key in right_index:
                merged = self._merge_rows(
                    left_row, right_index[key],
                    left_prefix, right_prefix, left_key, right_key
                )
                result.append(merged)

        return result

    def _left_join(
        self,
        left_data: list[dict],
        right_data: list[dict],
        left_key: str,
        right_key: str,
        left_prefix: Optional[str],
        right_prefix: Optional[str],
    ) -> list[dict]:
        """Perform left join.

        Args:
            left_data: Left dataset.
            right_data: Right dataset.
            left_key: Left key field.
            right_key: Right key field.
            left_prefix: Left prefix.
            right_prefix: Right prefix.

        Returns:
            Joined records.
        """
        right_index = {row[right_key]: row for row in right_data}
        result = []

        for left_row in left_data:
            key = left_row.get(left_key)
            right_row = right_index.get(key, {})
            merged = self._merge_rows(
                left_row, right_row,
                left_prefix, right_prefix, left_key, right_key
            )
            result.append(merged)

        return result

    def _right_join(
        self,
        left_data: list[dict],
        right_data: list[dict],
        left_key: str,
        right_key: str,
        left_prefix: Optional[str],
        right_prefix: Optional[str],
    ) -> list[dict]:
        """Perform right join.

        Args:
            left_data: Left dataset.
            right_data: Right dataset.
            left_key: Left key field.
            right_key: Right key field.
            left_prefix: Left prefix.
            right_prefix: Right prefix.

        Returns:
            Joined records.
        """
        left_index = {row.get(left_key): row for row in left_data}
        result = []

        for right_row in right_data:
            key = right_row.get(right_key)
            left_row = left_index.get(key, {})
            merged = self._merge_rows(
                left_row, right_row,
                left_prefix, right_prefix, left_key, right_key
            )
            result.append(merged)

        return result

    def _full_join(
        self,
        left_data: list[dict],
        right_data: list[dict],
        left_key: str,
        right_key: str,
        left_prefix: Optional[str],
        right_prefix: Optional[str],
    ) -> list[dict]:
        """Perform full outer join.

        Args:
            left_data: Left dataset.
            right_data: Right dataset.
            left_key: Left key field.
            right_key: Right key field.
            left_prefix: Left prefix.
            right_prefix: Right prefix.

        Returns:
            Joined records.
        """
        right_index = {row[right_key]: row for row in right_data}
        left_matched: set = set()
        result = []

        for left_row in left_data:
            key = left_row.get(left_key)
            right_row = right_index.get(key, {})
            merged = self._merge_rows(
                left_row, right_row,
                left_prefix, right_prefix, left_key, right_key
            )
            result.append(merged)
            if key in right_index:
                left_matched.add(key)

        for right_row in right_data:
            key = right_row.get(right_key)
            if key not in left_matched:
                merged = self._merge_rows(
                    {}, right_row,
                    left_prefix, right_prefix, left_key, right_key
                )
                result.append(merged)

        return result

    def _cross_join(
        self,
        left_data: list[dict],
        right_data: list[dict],
        left_prefix: Optional[str],
        right_prefix: Optional[str],
    ) -> list[dict]:
        """Perform cross join.

        Args:
            left_data: Left dataset.
            right_data: Right dataset.
            left_prefix: Left prefix.
            right_prefix: Right prefix.

        Returns:
            Joined records.
        """
        result = []
        for left_row in left_data:
            for right_row in right_data:
                merged = self._merge_rows(
                    left_row, right_row,
                    left_prefix, right_prefix, None, None
                )
                result.append(merged)
        return result

    def _merge_rows(
        self,
        left_row: dict,
        right_row: dict,
        left_prefix: Optional[str],
        right_prefix: Optional[str],
        left_key: Optional[str],
        right_key: Optional[str],
    ) -> dict:
        """Merge two rows with optional prefixes.

        Args:
            left_row: Left row.
            right_row: Right row.
            left_prefix: Prefix for left fields.
            right_prefix: Prefix for right fields.
            left_key: Left key field to exclude from prefix.
            right_key: Right key field to exclude from prefix.

        Returns:
            Merged row.
        """
        result = {}

        for key, value in left_row.items():
            if left_key and key == left_key:
                result[key] = value
            elif left_prefix:
                result[f"{left_prefix}_{key}"] = value
            else:
                result[key] = value

        for key, value in right_row.items():
            if right_key and key == right_key:
                if key not in result:
                    result[key] = value
            elif right_prefix:
                result[f"{right_prefix}_{key}"] = value
            elif key not in result:
                result[key] = value

        return result

    def multi_join(
        self,
        datasets: list[list[dict]],
        keys: list[str],
        join_type: JoinType = JoinType.INNER,
    ) -> list[dict]:
        """Join multiple datasets sequentially.

        Args:
            datasets: List of datasets to join.
            keys: List of key pairs for each join.
            join_type: Type of join to use.

        Returns:
            Final joined dataset.
        """
        if not datasets:
            return []

        if len(datasets) == 1:
            return datasets[0]

        result = datasets[0]

        for i in range(1, len(datasets)):
            left_key = keys[i - 1][0]
            right_key = keys[i - 1][1]
            join_result = self.join(
                result, datasets[i],
                left_key, right_key, join_type
            )
            result = join_result.data

        return result

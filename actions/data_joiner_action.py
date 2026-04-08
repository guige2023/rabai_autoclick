"""Data Joiner Action Module.

Provides SQL-style joins (inner, left, right, full, cross)
for data records with key matching and conflict resolution.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import logging

logger = logging.getLogger(__name__)


class JoinType(Enum):
    """Join type."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


@dataclass
class JoinConfig:
    """Join configuration."""
    join_type: JoinType
    left_key: Union[str, Callable]
    right_key: Union[str, Callable]
    left_fields: Optional[List[str]] = None
    right_fields: Optional[List[str]] = None
    field_mapping: Optional[Dict[str, str]] = None
    conflict_resolver: Optional[Callable[[Any, Any], Any]] = None


class DataJoinerAction:
    """Data joiner with multiple join strategies.

    Example:
        joiner = DataJoinerAction()

        result = joiner.join(
            left_data=[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}],
            right_data=[{"user_id": 1, "email": "alice@example.com"}, {"user_id": 3, "email": "charlie@example.com"}],
            config=JoinConfig(
                join_type=JoinType.LEFT,
                left_key="id",
                right_key="user_id",
                field_mapping={"id": "id", "name": "name", "email": "email"}
            )
        )
    """

    def __init__(self) -> None:
        self._index_cache: Dict[str, Dict] = {}

    def join(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        config: JoinConfig,
    ) -> List[Dict[str, Any]]:
        """Join two datasets.

        Args:
            left_data: Left dataset
            right_data: Right dataset
            config: Join configuration

        Returns:
            Joined records
        """
        if config.join_type == JoinType.CROSS:
            return self._cross_join(left_data, right_data, config)

        right_index = self._build_index(right_data, config.right_key)

        if config.join_type == JoinType.INNER:
            return self._inner_join(left_data, right_index, config)
        elif config.join_type == JoinType.LEFT:
            return self._left_join(left_data, right_index, config)
        elif config.join_type == JoinType.RIGHT:
            return self._right_join(left_data, right_index, right_data, config)
        elif config.join_type == JoinType.FULL:
            return self._full_join(left_data, right_index, right_data, config)

        return []

    def _build_index(
        self,
        data: List[Dict[str, Any]],
        key: Union[str, Callable],
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """Build lookup index for right dataset."""
        index: Dict[Any, List[Dict[str, Any]]] = {}

        for record in data:
            key_value = self._extract_key(record, key)
            if key_value not in index:
                index[key_value] = []
            index[key_value].append(record)

        return index

    def _extract_key(
        self,
        record: Dict[str, Any],
        key: Union[str, Callable],
    ) -> Any:
        """Extract key value from record."""
        if callable(key):
            return key(record)
        return record.get(key)

    def _inner_join(
        self,
        left_data: List[Dict[str, Any]],
        right_index: Dict,
        config: JoinConfig,
    ) -> List[Dict[str, Any]]:
        """Perform inner join."""
        results: List[Dict[str, Any]] = []

        for left_record in left_data:
            key_value = self._extract_key(left_record, config.left_key)
            right_matches = right_index.get(key_value, [])

            for right_record in right_matches:
                joined = self._merge_records(
                    left_record, right_record, config
                )
                results.append(joined)

        return results

    def _left_join(
        self,
        left_data: List[Dict[str, Any]],
        right_index: Dict,
        config: JoinConfig,
    ) -> List[Dict[str, Any]]:
        """Perform left join."""
        results: List[Dict[str, Any]] = []

        for left_record in left_data:
            key_value = self._extract_key(left_record, config.left_key)
            right_matches = right_index.get(key_value, [])

            if right_matches:
                for right_record in right_matches:
                    joined = self._merge_records(left_record, right_record, config)
                    results.append(joined)
            else:
                results.append(self._pad_right_record(left_record, config))

        return results

    def _right_join(
        self,
        left_data: List[Dict[str, Any]],
        right_index: Dict,
        right_data: List[Dict[str, Any]],
        config: JoinConfig,
    ) -> List[Dict[str, Any]]:
        """Perform right join."""
        results: List[Dict[str, Any]] = []
        left_index = self._build_index(left_data, config.left_key)
        used_left_keys: set = set()

        for right_record in right_data:
            key_value = self._extract_key(right_record, config.right_key)
            left_matches = left_index.get(key_value, [])

            if left_matches:
                for left_record in left_matches:
                    joined = self._merge_records(left_record, right_record, config)
                    results.append(joined)
                    used_left_keys.add(key_value)
            else:
                results.append(self._pad_left_record(right_record, config))

        return results

    def _full_join(
        self,
        left_data: List[Dict[str, Any]],
        right_index: Dict,
        right_data: List[Dict[str, Any]],
        config: JoinConfig,
    ) -> List[Dict[str, Any]]:
        """Perform full outer join."""
        left_index = self._build_index(left_data, config.left_key)
        results: List[Dict[str, Any]] = []
        matched_right_keys: set = set()

        for left_record in left_data:
            key_value = self._extract_key(left_record, config.left_key)
            right_matches = right_index.get(key_value, [])

            if right_matches:
                for right_record in right_matches:
                    joined = self._merge_records(left_record, right_record, config)
                    results.append(joined)
                    matched_right_keys.add(key_value)
            else:
                results.append(self._pad_right_record(left_record, config))

        for right_record in right_data:
            key_value = self._extract_key(right_record, config.right_key)
            if key_value not in matched_right_keys:
                results.append(self._pad_left_record(right_record, config))

        return results

    def _cross_join(
        self,
        left_data: List[Dict[str, Any]],
        right_data: List[Dict[str, Any]],
        config: JoinConfig,
    ) -> List[Dict[str, Any]]:
        """Perform cross join."""
        results: List[Dict[str, Any]] = []

        for left_record in left_data:
            for right_record in right_data:
                joined = self._merge_records(left_record, right_record, config)
                results.append(joined)

        return results

    def _merge_records(
        self,
        left_record: Dict[str, Any],
        right_record: Dict[str, Any],
        config: JoinConfig,
    ) -> Dict[str, Any]:
        """Merge two records according to config."""
        result: Dict[str, Any] = {}

        if config.field_mapping:
            for left_field, right_field in config.field_mapping.items():
                if left_field in left_record:
                    result[left_field] = left_record[left_field]
                if right_field in right_record:
                    result[right_field] = right_record[right_field]
        else:
            result.update(left_record)
            for key, value in right_record.items():
                if key not in result:
                    result[key] = value
                elif config.conflict_resolver:
                    result[key] = config.conflict_resolver(result[key], value)

        return result

    def _pad_right_record(
        self,
        left_record: Dict[str, Any],
        config: JoinConfig,
    ) -> Dict[str, Any]:
        """Pad left record with None for right side."""
        result = dict(left_record)
        if config.right_fields:
            for field_name in config.right_fields:
                if field_name not in result:
                    result[field_name] = None
        return result

    def _pad_left_record(
        self,
        right_record: Dict[str, Any],
        config: JoinConfig,
    ) -> Dict[str, Any]:
        """Pad right record with None for left side."""
        result: Dict[str, Any] = {}
        if config.left_fields:
            for field_name in config.left_fields:
                result[field_name] = None
        result.update(right_record)
        return result

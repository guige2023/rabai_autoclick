"""
Data Joiner Action Module.

Provides SQL-style joins with support for multiple join types,
cross references, and async execution.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar, Optional
from collections import defaultdict

T = TypeVar("T")


class JoinType(Enum):
    """Join types."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"
    SEMI = "semi"
    ANTI = "anti"


@dataclass
class JoinConfig:
    """Join configuration."""
    join_type: JoinType = JoinType.INNER
    left_key: str = "id"
    right_key: str = "id"
    left_alias: Optional[str] = None
    right_alias: Optional[str] = None
    condition: Optional[Callable[[dict, dict], bool]] = None


@dataclass
class JoinResult:
    """Join operation result."""
    rows: list[dict] = field(default_factory=list)
    left_unmatched: list[dict] = field(default_factory=list)
    right_unmatched: list[dict] = field(default_factory=list)
    join_type: JoinType = JoinType.INNER
    left_count: int = 0
    right_count: int = 0
    matched_count: int = 0


class HashJoin:
    """Hash join implementation."""

    def __init__(self, config: JoinConfig):
        self.config = config

    def _get_key_value(self, record: dict, key: str) -> Any:
        """Get key value from record."""
        parts = key.split(".")
        value = record
        for part in parts:
            if isinstance(value, dict):
                value = value.get(part)
            else:
                return None
        return value

    def _build_hash_table(
        self,
        records: list[dict],
        key: str
    ) -> dict[Any, list[dict]]:
        """Build hash table for joining."""
        hash_table = defaultdict(list)
        for record in records:
            key_value = self._get_key_value(record, key)
            hash_table[key_value].append(record)
        return hash_table

    def inner_join(
        self,
        left: list[dict],
        right: list[dict]
    ) -> JoinResult:
        """Perform inner join."""
        right_hash = self._build_hash_table(right, self.config.right_key)
        result_rows = []
        left_unmatched = []

        for left_record in left:
            key_value = self._get_key_value(left_record, self.config.left_key)
            right_matches = right_hash.get(key_value, [])

            if not right_matches:
                left_unmatched.append(left_record)
                continue

            for right_record in right_matches:
                if self.config.condition and not self.config.condition(left_record, right_record):
                    continue

                merged = {**left_record, **right_record}
                result_rows.append(merged)

        return JoinResult(
            rows=result_rows,
            left_unmatched=left_unmatched,
            right_unmatched=[],
            join_type=JoinType.INNER,
            left_count=len(left),
            right_count=len(right),
            matched_count=len(result_rows)
        )

    def left_join(
        self,
        left: list[dict],
        right: list[dict]
    ) -> JoinResult:
        """Perform left join."""
        right_hash = self._build_hash_table(right, self.config.right_key)
        result_rows = []
        left_unmatched = []

        for left_record in left:
            key_value = self._get_key_value(left_record, self.config.left_key)
            right_matches = right_hash.get(key_value, [])

            if not right_matches:
                merged = {**left_record}
                for right_field in right[0].keys() if right else []:
                    if right_field not in merged:
                        merged[right_field] = None
                result_rows.append(merged)
                left_unmatched.append(left_record)
                continue

            for right_record in right_matches:
                if self.config.condition and not self.config.condition(left_record, right_record):
                    continue
                merged = {**left_record, **right_record}
                result_rows.append(merged)

        return JoinResult(
            rows=result_rows,
            left_unmatched=left_unmatched,
            right_unmatched=[],
            join_type=JoinType.LEFT,
            left_count=len(left),
            right_count=len(right),
            matched_count=len(result_rows)
        )

    def right_join(
        self,
        left: list[dict],
        right: list[dict]
    ) -> JoinResult:
        """Perform right join."""
        left_hash = self._build_hash_table(left, self.config.left_key)
        result_rows = []
        right_unmatched = []

        for right_record in right:
            key_value = self._get_key_value(right_record, self.config.right_key)
            left_matches = left_hash.get(key_value, [])

            if not left_matches:
                merged = {**right_record}
                for left_field in left[0].keys() if left else []:
                    if left_field not in merged:
                        merged[left_field] = None
                result_rows.append(merged)
                right_unmatched.append(right_record)
                continue

            for left_record in left_matches:
                if self.config.condition and not self.config.condition(left_record, right_record):
                    continue
                merged = {**left_record, **right_record}
                result_rows.append(merged)

        return JoinResult(
            rows=result_rows,
            left_unmatched=[],
            right_unmatched=right_unmatched,
            join_type=JoinType.RIGHT,
            left_count=len(left),
            right_count=len(right),
            matched_count=len(result_rows)
        )

    def full_join(
        self,
        left: list[dict],
        right: list[dict]
    ) -> JoinResult:
        """Perform full outer join."""
        left_hash = self._build_hash_table(left, self.config.left_key)
        right_hash = self._build_hash_table(right, self.config.right_key)

        left_keys = set(self._get_key_value(r, self.config.left_key) for r in left)
        right_keys = set(self._get_key_value(r, self.config.right_key) for r in right)
        all_keys = left_keys | right_keys

        result_rows = []
        left_unmatched = []
        right_unmatched = []
        matched_left_keys = set()
        matched_right_keys = set()

        for key in all_keys:
            left_matches = left_hash.get(key, [])
            right_matches = right_hash.get(key, [])

            if left_matches and right_matches:
                for lr in left_matches:
                    for rr in right_matches:
                        if self.config.condition and not self.config.condition(lr, rr):
                            continue
                        result_rows.append({**lr, **rr})
                        matched_left_keys.add(key)
                        matched_right_keys.add(key)
            elif left_matches:
                for lr in left_matches:
                    merged = {**lr}
                    for rf in right[0].keys() if right else []:
                        if rf not in merged:
                            merged[rf] = None
                    result_rows.append(merged)
                    left_unmatched.append(lr)
            elif right_matches:
                for rr in right_matches:
                    merged = {**rr}
                    for lf in left[0].keys() if left else []:
                        if lf not in merged:
                            merged[lf] = None
                    result_rows.append(merged)
                    right_unmatched.append(rr)

        return JoinResult(
            rows=result_rows,
            left_unmatched=left_unmatched,
            right_unmatched=right_unmatched,
            join_type=JoinType.FULL,
            left_count=len(left),
            right_count=len(right),
            matched_count=len(result_rows)
        )


class DataJoiner:
    """Data joiner with multiple join types."""

    def __init__(self, config: Optional[JoinConfig] = None):
        self.config = config or JoinConfig()
        self._hash_join = HashJoin(self.config)

    def join(
        self,
        left: list[dict],
        right: list[dict],
        join_type: Optional[JoinType] = None
    ) -> JoinResult:
        """Perform join."""
        jt = join_type or self.config.join_type

        if jt == JoinType.INNER:
            return self._hash_join.inner_join(left, right)
        elif jt == JoinType.LEFT:
            return self._hash_join.left_join(left, right)
        elif jt == JoinType.RIGHT:
            return self._hash_join.right_join(left, right)
        elif jt == JoinType.FULL:
            return self._hash_join.full_join(left, right)
        elif jt == JoinType.CROSS:
            return self._cross_join(left, right)
        elif jt == JoinType.SEMI:
            return self._semi_join(left, right)
        elif jt == JoinType.ANTI:
            return self._anti_join(left, right)

        return self._hash_join.inner_join(left, right)

    def _cross_join(
        self,
        left: list[dict],
        right: list[dict]
    ) -> JoinResult:
        """Perform cross join."""
        results = []
        for l in left:
            for r in right:
                results.append({**l, **r})
        return JoinResult(
            rows=results,
            join_type=JoinType.CROSS,
            left_count=len(left),
            right_count=len(right),
            matched_count=len(results)
        )

    def _semi_join(
        self,
        left: list[dict],
        right: list[dict]
    ) -> JoinResult:
        """Perform semi join."""
        right_hash = self._hash_join._build_hash_table(right, self.config.right_key)
        results = []
        unmatched = []

        for record in left:
            key = self._hash_join._get_key_value(record, self.config.left_key)
            if key in right_hash:
                results.append(record)
            else:
                unmatched.append(record)

        return JoinResult(
            rows=results,
            left_unmatched=unmatched,
            join_type=JoinType.SEMI,
            left_count=len(left),
            right_count=len(right),
            matched_count=len(results)
        )

    def _anti_join(
        self,
        left: list[dict],
        right: list[dict]
    ) -> JoinResult:
        """Perform anti join."""
        right_hash = self._hash_join._build_hash_table(right, self.config.right_key)
        results = []
        matched = []

        for record in left:
            key = self._hash_join._get_key_value(record, self.config.left_key)
            if key not in right_hash:
                results.append(record)
            else:
                matched.append(record)

        return JoinResult(
            rows=results,
            left_unmatched=matched,
            join_type=JoinType.ANTI,
            left_count=len(left),
            right_count=len(right),
            matched_count=len(results)
        )


class DataJoinerAction:
    """
    SQL-style data joins.

    Example:
        joiner = DataJoinerAction(
            join_type=JoinType.LEFT,
            left_key="user_id",
            right_key="id"
        )

        result = joiner.join(users, orders)
        print(f"Matched: {result.matched_count}")
    """

    def __init__(
        self,
        join_type: JoinType = JoinType.INNER,
        left_key: str = "id",
        right_key: str = "id",
        condition: Optional[Callable[[dict, dict], bool]] = None
    ):
        config = JoinConfig(
            join_type=join_type,
            left_key=left_key,
            right_key=right_key,
            condition=condition
        )
        self._joiner = DataJoiner(config)

    def join(
        self,
        left: list[dict],
        right: list[dict],
        join_type: Optional[JoinType] = None
    ) -> JoinResult:
        """Join datasets."""
        return self._joiner.join(left, right, join_type)

    async def join_async(
        self,
        left: list[dict],
        right: list[dict],
        join_type: Optional[JoinType] = None
    ) -> JoinResult:
        """Join datasets asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            lambda: self._joiner.join(left, right, join_type)
        )

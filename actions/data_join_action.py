"""Data Join Action Module.

Provides data joining operations with support for multiple join types,
key extraction, conflict resolution, and null handling.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class JoinType(Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    OUTER = "outer"
    CROSS = "cross"
    ANTI = "anti"
    SEMI = "semi"


@dataclass
class JoinConfig:
    join_type: JoinType = JoinType.INNER
    left_key: str = "id"
    right_key: str = "id"
    key_extractor: Optional[Callable[[Dict[str, Any]], Any]] = None
    null_handling: str = "skip"
    conflict_resolver: Optional[Callable[[Any, Any], Any]] = None
    suffix_left: str = "_x"
    suffix_right: str = "_y"


@dataclass
class JoinStats:
    total_left: int
    total_right: int
    matched: int
    unmatched_left: int
    unmatched_right: int
    duration_ms: float


class DataJoiner:
    def __init__(self, config: Optional[JoinConfig] = None):
        self.config = config or JoinConfig()
        self._stats: Optional[JoinStats] = None

    def join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        import time
        start = time.time()

        if self.config.key_extractor:
            left_indexed = {self.config.key_extractor(r): r for r in left}
            right_indexed = {self.config.key_extractor(r): r for r in right}
        else:
            left_indexed = {r.get(self.config.left_key): r for r in left}
            right_indexed = {r.get(self.config.right_key): r for r in right}

        results = []
        matched_left: Set[Any] = set()
        matched_right: Set[Any] = set()

        if self.config.join_type == JoinType.CROSS:
            for l in left:
                for r in right:
                    merged = dict(l)
                    merged.update(r)
                    results.append(merged)
        elif self.config.join_type == JoinType.INNER:
            for key, l_record in left_indexed.items():
                if key is None and self.config.null_handling == "skip":
                    continue
                r_record = right_indexed.get(key)
                if r_record is not None:
                    matched_left.add(key)
                    matched_right.add(key)
                    results.append(self._merge_records(l_record, r_record))
        elif self.config.join_type == JoinType.LEFT:
            for key, l_record in left_indexed.items():
                if key is None and self.config.null_handling == "skip":
                    continue
                r_record = right_indexed.get(key)
                matched_left.add(key)
                if r_record is not None:
                    matched_right.add(key)
                    results.append(self._merge_records(l_record, r_record))
                else:
                    results.append(self._add_suffix(l_record, self.config.suffix_right, right))
        elif self.config.join_type == JoinType.RIGHT:
            for key, r_record in right_indexed.items():
                if key is None and self.config.null_handling == "skip":
                    continue
                l_record = left_indexed.get(key)
                matched_right.add(key)
                if l_record is not None:
                    matched_left.add(key)
                    results.append(self._merge_records(l_record, r_record))
                else:
                    results.append(self._add_suffix(r_record, self.config.suffix_left, left))
        elif self.config.join_type == JoinType.OUTER:
            all_keys = set(left_indexed.keys()) | set(right_indexed.keys())
            for key in all_keys:
                if key is None and self.config.null_handling == "skip":
                    continue
                l_record = left_indexed.get(key)
                r_record = right_indexed.get(key)
                if l_record is not None:
                    matched_left.add(key)
                if r_record is not None:
                    matched_right.add(key)
                if l_record and r_record:
                    results.append(self._merge_records(l_record, r_record))
                elif l_record:
                    results.append(self._add_suffix(l_record, self.config.suffix_right, right))
                elif r_record:
                    results.append(self._add_suffix(r_record, self.config.suffix_left, left))
        elif self.config.join_type == JoinType.ANTI:
            for key, l_record in left_indexed.items():
                if key not in right_indexed:
                    results.append(l_record)
        elif self.config.join_type == JoinType.SEMI:
            for key, l_record in left_indexed.items():
                if key in right_indexed:
                    results.append(l_record)

        duration_ms = (time.time() - start) * 1000
        self._stats = JoinStats(
            total_left=len(left),
            total_right=len(right),
            matched=len(matched_left),
            unmatched_left=len(left) - len(matched_left),
            unmatched_right=len(right) - len(matched_right),
            duration_ms=duration_ms,
        )

        return results

    def _merge_records(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any],
    ) -> Dict[str, Any]:
        merged = {}
        all_keys = set(left.keys()) | set(right.keys())

        for key in all_keys:
            l_val = left.get(key)
            r_val = right.get(key)

            if key in left and key in right:
                if l_val == r_val:
                    merged[key] = l_val
                elif self.config.conflict_resolver:
                    merged[key] = self.config.conflict_resolver(l_val, r_val)
                else:
                    merged[f"{key}{self.config.suffix_left}"] = l_val
                    merged[f"{key}{self.config.suffix_right}"] = r_val
            elif key in left:
                merged[key] = l_val
            else:
                merged[key] = r_val

        return merged

    def _add_suffix(
        self,
        record: Dict[str, Any],
        suffix: str,
        other: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        result = {}
        other_keys = set()
        if other:
            other_keys = set(other[0].keys()) if other else set()

        for key, val in record.items():
            if key in other_keys:
                result[f"{key}{suffix}"] = val
            else:
                result[key] = val

        return result

    def get_stats(self) -> Optional[JoinStats]:
        return self._stats


def join_lists(
    left: List[Dict[str, Any]],
    right: List[Dict[str, Any]],
    join_type: JoinType = JoinType.INNER,
    key: str = "id",
) -> List[Dict[str, Any]]:
    config = JoinConfig(join_type=join_type, left_key=key, right_key=key)
    joiner = DataJoiner(config)
    return joiner.join(left, right)


def union_data(
    datasets: List[List[Dict[str, Any]]],
    dedup: bool = True,
) -> List[Dict[str, Any]]:
    result = []
    seen = set() if dedup else None

    for dataset in datasets:
        for record in dataset:
            record_tuple = tuple(sorted(record.items()))
            if dedup:
                if record_tuple in seen:
                    continue
                seen.add(record_tuple)
            result.append(record)

    return result


def intersect_data(
    datasets: List[List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    if not datasets:
        return []

    result = None
    for dataset in datasets:
        record_set = set(tuple(sorted(r.items())) for r in dataset)
        if result is None:
            result = record_set
        else:
            result &= record_set

    return [dict(r) for r in (result or set())]

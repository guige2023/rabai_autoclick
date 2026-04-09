"""Data Join Action Module.

Performs SQL-style joins (inner, left, right, full outer, cross) on
in-memory data collections with key extraction and conflict resolution.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


@dataclass
class JoinConfig:
    join_type: str = "inner"
    key_field: str = "id"
    left_key_field: Optional[str] = None
    right_key_field: Optional[str] = None
    conflict_resolution: str = "left_wins"
    dedupe: bool = True


class DataJoinAction:
    """SQL-style join operations for in-memory data collections."""

    JOIN_TYPES = {"inner", "left", "right", "full", "cross"}

    def __init__(self, config: Optional[JoinConfig] = None) -> None:
        self._config = config or JoinConfig()

    def join(
        self,
        left: List[Dict],
        right: List[Dict],
        config: Optional[JoinConfig] = None,
    ) -> List[Dict]:
        cfg = config or self._config
        if cfg.join_type not in self.JOIN_TYPES:
            raise ValueError(f"Unknown join type: {cfg.join_type}")
        if cfg.join_type == "inner":
            return self._inner_join(left, right, cfg)
        elif cfg.join_type == "left":
            return self._left_join(left, right, cfg)
        elif cfg.join_type == "right":
            return self._right_join(left, right, cfg)
        elif cfg.join_type == "full":
            return self._full_join(left, right, cfg)
        elif cfg.join_type == "cross":
            return self._cross_join(left, right, cfg)
        return []

    def _inner_join(
        self,
        left: List[Dict],
        right: List[Dict],
        cfg: JoinConfig,
    ) -> List[Dict]:
        lkf = cfg.left_key_field or cfg.key_field
        rkf = cfg.right_key_field or cfg.key_field
        right_index = self._build_index(right, rkf)
        results = []
        for lrow in left:
            lkey = lrow.get(lkf)
            rrow = right_index.get(lkey)
            if rrow:
                results.append(self._merge_rows(lrow, rrow, cfg))
        return results

    def _left_join(
        self,
        left: List[Dict],
        right: List[Dict],
        cfg: JoinConfig,
    ) -> List[Dict]:
        lkf = cfg.left_key_field or cfg.key_field
        rkf = cfg.right_key_field or cfg.key_field
        right_index = self._build_index(right, rkf)
        results = []
        for lrow in left:
            lkey = lrow.get(lkf)
            rrow = right_index.get(lkey)
            if rrow:
                results.append(self._merge_rows(lrow, rrow, cfg))
            else:
                results.append(self._merge_rows(lrow, {}, cfg, missing_right=True))
        return results

    def _right_join(
        self,
        left: List[Dict],
        right: List[Dict],
        cfg: JoinConfig,
    ) -> List[Dict]:
        lkf = cfg.left_key_field or cfg.key_field
        rkf = cfg.right_key_field or cfg.key_field
        left_index = self._build_index(left, lkf)
        results = []
        for rrow in right:
            rkey = rrow.get(rkf)
            lrow = left_index.get(rkey)
            if lrow:
                results.append(self._merge_rows(lrow, rrow, cfg))
            else:
                results.append(self._merge_rows({}, rrow, cfg, missing_left=True))
        return results

    def _full_join(
        self,
        left: List[Dict],
        right: List[Dict],
        cfg: JoinConfig,
    ) -> List[Dict]:
        lkf = cfg.left_key_field or cfg.key_field
        rkf = cfg.right_key_field or cfg.key_field
        right_index = self._build_index(right, rkf)
        left_index = self._build_index(left, lkf)
        all_keys = set(left_index.keys()) | set(right_index.keys())
        results = []
        for key in all_keys:
            lrow = left_index.get(key, {})
            rrow = right_index.get(key, {})
            if lrow and rrow:
                results.append(self._merge_rows(lrow, rrow, cfg))
            elif lrow:
                results.append(self._merge_rows(lrow, {}, cfg, missing_right=True))
            else:
                results.append(self._merge_rows({}, rrow, cfg, missing_left=True))
        return results

    def _cross_join(
        self,
        left: List[Dict],
        right: List[Dict],
        cfg: JoinConfig,
    ) -> List[Dict]:
        results = []
        for lrow in left:
            for rrow in right:
                results.append(self._merge_rows(lrow, rrow, cfg))
        return results

    def _build_index(
        self,
        data: List[Dict],
        key_field: str,
    ) -> Dict[Any, Dict]:
        index: Dict[Any, Dict] = {}
        for row in data:
            key = row.get(key_field)
            if key is not None:
                if key not in index:
                    index[key] = row
        return index

    def _merge_rows(
        self,
        left: Dict,
        right: Dict,
        cfg: JoinConfig,
        missing_left: bool = False,
        missing_right: bool = False,
    ) -> Dict:
        left_prefix = cfg.left_key_field or "left_"
        right_prefix = cfg.right_key_field or "right_"
        result: Dict[str, Any] = {}
        if missing_left:
            result[f"{left_prefix}_null"] = True
        if missing_right:
            result[f"{right_prefix}_null"] = True
        for k, v in left.items():
            result[k] = v
        for k, v in right.items():
            if k in result and cfg.conflict_resolution == "left_wins":
                continue
            elif k in result and cfg.conflict_resolution == "right_wins":
                result[k] = v
            elif k in result:
                result[f"{right_prefix}{k}"] = v
            else:
                result[k] = v
        if cfg.dedupe:
            seen: Set[str] = set()
            deduped: Dict[str, Any] = {}
            for k, v in result.items():
                if k not in seen:
                    deduped[k] = v
                    seen.add(k)
            return deduped
        return result

    def multi_join(
        self,
        tables: List[Tuple[List[Dict], JoinConfig]],
    ) -> List[Dict]:
        if not tables:
            return []
        result = tables[0][0]
        for data, config in tables[1:]:
            result = self.join(result, data, config)
        return result

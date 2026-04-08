"""Data join action module.

Provides SQL-style join operations for lists of dicts.
Supports inner, left, right, full, and cross joins with deduplication.
"""

from __future__ import annotations

import logging
from typing import Optional, Dict, Any, List, Callable
from collections import defaultdict

logger = logging.getLogger(__name__)


class JoinType(Enum):
    """Type of join operation."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"


from enum import Enum


class DataJoinAction:
    """Data join engine.

    Provides SQL-style join operations for list-of-dict data structures.

    Example:
        left = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        right = [{"id": 1, "dept": "A"}, {"id": 3, "dept": "C"}]
        result = DataJoinAction().join(left, right, "id", JoinType.LEFT)
    """

    def join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        key: str,
        join_type: JoinType = JoinType.INNER,
        left_key: Optional[str] = None,
        right_key: Optional[str] = None,
        prefix_left: str = "",
        prefix_right: str = "",
        how: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Join two lists of dicts on a key field.

        Args:
            left: Left dataset.
            right: Right dataset.
            key: Join key (field name in both datasets).
            join_type: Type of join.
            left_key: Override key name in left dataset.
            right_key: Override key name in right dataset.
            prefix_left: Prefix for left field names.
            prefix_right: Prefix for right field names.
            how: Alias for join_type (inner, left, right, full, cross).

        Returns:
            Joined list of dicts.
        """
        if how:
            join_type = JoinType(how)

        lk = left_key or key
        rk = right_key or key

        if join_type == JoinType.CROSS:
            return self._cross_join(left, right, prefix_left, prefix_right)
        elif join_type == JoinType.INNER:
            return self._inner_join(left, right, lk, rk, prefix_left, prefix_right)
        elif join_type == JoinType.LEFT:
            return self._left_join(left, right, lk, rk, prefix_left, prefix_right)
        elif join_type == JoinType.RIGHT:
            return self._right_join(left, right, lk, rk, prefix_left, prefix_right)
        elif join_type == JoinType.FULL:
            return self._full_join(left, right, lk, rk, prefix_left, prefix_right)

        return []

    def _build_index(
        self,
        data: List[Dict[str, Any]],
        key: str,
    ) -> Dict[Any, List[Dict[str, Any]]]:
        """Build a lookup index for a dataset."""
        index: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
        for item in data:
            val = item.get(key)
            if val is not None:
                index[val].append(item)
        return index

    def _inner_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        lk: str,
        rk: str,
        pl: str,
        pr: str,
    ) -> List[Dict[str, Any]]:
        """Inner join: only matching keys from both sides."""
        right_index = self._build_index(right, rk)
        result = []

        for l_item in left:
            l_val = l_item.get(lk)
            if l_val is None:
                continue
            r_items = right_index.get(l_val, [])
            for r_item in r_items:
                merged = self._merge_items(l_item, r_item, pl, pr, lk)
                result.append(merged)

        return result

    def _left_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        lk: str,
        rk: str,
        pl: str,
        pr: str,
    ) -> List[Dict[str, Any]]:
        """Left join: all left rows, matching right rows."""
        right_index = self._build_index(right, rk)
        result = []

        for l_item in left:
            l_val = l_item.get(lk)
            r_items = right_index.get(l_val, []) if l_val is not None else []
            if r_items:
                for r_item in r_items:
                    result.append(self._merge_items(l_item, r_item, pl, pr, lk))
            else:
                result.append(self._prefix_item(dict(l_item), pl))

        return result

    def _right_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        lk: str,
        rk: str,
        pl: str,
        pr: str,
    ) -> List[Dict[str, Any]]:
        """Right join: all right rows, matching left rows."""
        left_index = self._build_index(left, lk)
        result = []

        for r_item in right:
            r_val = r_item.get(rk)
            l_items = left_index.get(r_val, []) if r_val is not None else []
            if l_items:
                for l_item in l_items:
                    result.append(self._merge_items(l_item, r_item, pl, pr, lk))
            else:
                result.append(self._prefix_item(dict(r_item), pr))

        return result

    def _full_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        lk: str,
        rk: str,
        pl: str,
        pr: str,
    ) -> List[Dict[str, Any]]:
        """Full join: all rows from both sides."""
        right_index = self._build_index(right, rk)
        left_index = self._build_index(left, lk)
        result = []
        used_right = set()

        for l_item in left:
            l_val = l_item.get(lk)
            r_items = right_index.get(l_val, []) if l_val is not None else []
            if r_items:
                for r_item in r_items:
                    result.append(self._merge_items(l_item, r_item, pl, pr, lk))
                    used_right.add(id(r_item))
            else:
                result.append(self._prefix_item(dict(l_item), pl))

        for r_item in right:
            if id(r_item) not in used_right:
                result.append(self._prefix_item(dict(r_item), pr))

        return result

    def _cross_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        pl: str,
        pr: str,
    ) -> List[Dict[str, Any]]:
        """Cross join: Cartesian product of both sides."""
        result = []
        for l_item in left:
            for r_item in right:
                result.append(self._merge_items(l_item, r_item, pl, pr))
        return result

    def join_multiple(
        self,
        datasets: List[tuple],
        keys: List[str],
        join_type: JoinType = JoinType.INNER,
    ) -> List[Dict[str, Any]]:
        """Join multiple datasets sequentially.

        Args:
            datasets: List of (data, key) tuples.
            keys: Join keys for each step.
            join_type: Type of join.

        Returns:
            Final joined dataset.
        """
        if not datasets:
            return []

        result = datasets[0][0]
        for i in range(1, len(datasets)):
            result = self.join(result, datasets[i][0], keys[i - 1], join_type=join_type)

        return result

    def semi_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        key: str,
    ) -> List[Dict[str, Any]]:
        """Semi join: left rows that have a match in right.

        Args:
            left: Left dataset.
            right: Right dataset.
            key: Join key.

        Returns:
            Filtered left dataset.
        """
        right_keys = {item.get(key) for item in right if item.get(key) is not None}
        return [item for item in left if item.get(key) in right_keys]

    def anti_join(
        self,
        left: List[Dict[str, Any]],
        right: List[Dict[str, Any]],
        key: str,
    ) -> List[Dict[str, Any]]:
        """Anti join: left rows that have NO match in right.

        Args:
            left: Left dataset.
            right: Right dataset.
            key: Join key.

        Returns:
            Filtered left dataset.
        """
        right_keys = {item.get(key) for item in right if item.get(key) is not None}
        return [item for item in left if item.get(key) not in right_keys]

    def _merge_items(
        self,
        left: Dict[str, Any],
        right: Dict[str, Any],
        pl: str,
        pr: str,
        overlap_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Merge two dicts with optional prefixing."""
        result = {}

        for k, v in left.items():
            result[f"{pl}{k}"] = v

        for k, v in right.items():
            prefixed = f"{pr}{k}" if pr else k
            if k != overlap_key or overlap_key not in result:
                result[prefixed] = v

        return result

    def _prefix_item(self, item: Dict[str, Any], prefix: str) -> Dict[str, Any]:
        """Add prefix to all keys in an item."""
        if not prefix:
            return item
        return {f"{prefix}{k}": v for k, v in item.items()}

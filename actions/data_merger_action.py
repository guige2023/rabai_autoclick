"""
Data merger action for combining multiple data sources.

Provides SQL-style joins and data reconciliation.
"""

from typing import Any, Dict, List, Optional, Tuple, Union
from collections import defaultdict


class DataMergerAction:
    """Data merging with join operations."""

    def __init__(self) -> None:
        """Initialize data merger."""
        pass

    def execute(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        Execute merge operation.

        Args:
            params: Dictionary containing:
                - operation: 'join', 'union', 'intersect', 'difference'
                - left: Left dataset
                - right: Right dataset
                - join_key: Key(s) to join on
                - join_type: 'inner', 'left', 'right', 'full', 'cross'

        Returns:
            Dictionary with merged result
        """
        operation = params.get("operation", "join")

        if operation == "join":
            return self._join(params)
        elif operation == "union":
            return self._union(params)
        elif operation == "intersect":
            return self._intersect(params)
        elif operation == "difference":
            return self._difference(params)
        elif operation == "cross":
            return self._cross_join(params)
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

    def _join(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute join operation."""
        left = params.get("left", [])
        right = params.get("right", [])
        join_key = params.get("join_key", "")
        join_type = params.get("join_type", "inner")
        suffix_left = params.get("suffix_left", "_l")
        suffix_right = params.get("suffix_right", "_r")

        if not left or not right:
            return {"success": False, "error": "Both left and right datasets required"}

        if not join_key:
            return {"success": False, "error": "join_key is required"}

        if isinstance(join_key, str):
            join_key = [join_key]

        left_index = self._build_index(left, join_key)
        right_index = self._build_index(right, join_key)

        result = []
        matched_right = set()

        for left_row in left:
            left_key = tuple(left_row.get(k) for k in join_key)
            right_rows = left_index.get(left_key, [])

            if right_rows:
                matched_right.add(left_key)
                for right_row in right_rows:
                    merged = self._merge_rows(
                        left_row, right_row, join_key, suffix_left, suffix_right
                    )
                    result.append(merged)
            elif join_type in ("left", "full"):
                merged = {**left_row}
                for key in right[0].keys():
                    if key not in join_key:
                        merged[f"{key}{suffix_right}"] = None
                result.append(merged)

        if join_type in ("right", "full"):
            for right_row in right:
                right_key = tuple(right_row.get(k) for k in join_key)
                if right_key not in matched_right:
                    merged = {**{k: None for k in left[0].keys() if k not in join_key}, **right_row}
                    for key in left[0].keys():
                        if key not in join_key:
                            merged[f"{key}{suffix_left}"] = None
                    result.append(merged)

        return {
            "success": True,
            "result": result,
            "row_count": len(result),
            "join_type": join_type,
            "join_key": join_key,
        }

    def _build_index(
        self, data: List[Dict], keys: List[str]
    ) -> Dict[Tuple, List[Dict]]:
        """Build index for join operation."""
        index = defaultdict(list)
        for row in data:
            key = tuple(row.get(k) for k in keys)
            index[key].append(row)
        return dict(index)

    def _merge_rows(
        self,
        left_row: Dict,
        right_row: Dict,
        join_keys: List[str],
        suffix_left: str,
        suffix_right: str,
    ) -> Dict[str, Any]:
        """Merge two rows handling duplicate keys."""
        result = {}

        for key, value in left_row.items():
            if key in join_keys:
                result[key] = value
            else:
                result[f"{key}{suffix_left}"] = value

        for key, value in right_row.items():
            if key in join_keys:
                continue
            base_key = f"{key}{suffix_right}"
            if base_key in result:
                result[key] = value
            else:
                result[base_key] = value

        return result

    def _union(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute union operation (combining rows)."""
        left = params.get("left", [])
        right = params.get("right", [])
        deduplicate = params.get("deduplicate", True)

        result = left + right

        if deduplicate:
            seen = set()
            unique_result = []
            for row in result:
                row_key = tuple(sorted(row.items()))
                if row_key not in seen:
                    seen.add(row_key)
                    unique_result.append(row)
            result = unique_result

        return {"success": True, "result": result, "row_count": len(result)}

    def _intersect(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute intersection operation (common rows)."""
        left = params.get("left", [])
        right = params.get("right", [])

        left_tuples = {tuple(sorted(row.items())) for row in left}
        right_tuples = {tuple(sorted(row.items())) for row in right}

        common_tuples = left_tuples & right_tuples

        result = [dict(t) for t in common_tuples]

        return {"success": True, "result": result, "row_count": len(result)}

    def _difference(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute set difference (rows in left but not in right)."""
        left = params.get("left", [])
        right = params.get("right", [])

        right_tuples = {tuple(sorted(row.items())) for row in right}

        result = []
        for row in left:
            row_tuple = tuple(sorted(row.items()))
            if row_tuple not in right_tuples:
                result.append(row)

        return {"success": True, "result": result, "row_count": len(result)}

    def _cross_join(self, params: dict[str, Any]) -> dict[str, Any]:
        """Execute cross join (Cartesian product)."""
        left = params.get("left", [])
        right = params.get("right", [])

        result = []
        for l_row in left:
            for r_row in right:
                merged = {**l_row, **r_row}
                result.append(merged)

        return {"success": True, "result": result, "row_count": len(result)}

"""Joiner action module for RabAI AutoClick.

Provides data joining and merging operations.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Union

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class JoinerAction(BaseAction):
    """Data joining and merging operations.
    
    Supports inner, left, right, full outer joins, cross joins,
    and lookup operations between datasets.
    """
    action_type = "joiner"
    display_name = "数据关联"
    description = "多表关联：INNER/LEFT/RIGHT/FULL JOIN"
    
    def __init__(self) -> None:
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute join operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - command: 'inner', 'left', 'right', 'full', 'cross', 'lookup'
                - left_data: Left dataset (list of dicts)
                - right_data: Right dataset (list of dicts)
                - left_on: Field name for left key
                - right_on: Field name for right key (defaults to left_on)
                - how: Alias for command (inner/left/right/full/cross)
                - suffix_left: Suffix for left-only fields
                - suffix_right: Suffix for right-only fields
        
        Returns:
            ActionResult with joined data.
        """
        command = params.get('command') or params.get('how', 'inner')
        left_data = params.get('left_data', [])
        right_data = params.get('right_data', [])
        left_on = params.get('left_on')
        right_on = params.get('right_on', left_on)
        suffix_left = params.get('suffix_left', '_x')
        suffix_right = params.get('suffix_right', '_y')
        
        if not isinstance(left_data, list) or not isinstance(right_data, list):
            return ActionResult(success=False, message="left_data and right_data must be lists")
        
        if command == 'inner':
            return self._inner_join(left_data, right_data, left_on, right_on, suffix_left, suffix_right)
        if command == 'left':
            return self._left_join(left_data, right_data, left_on, right_on, suffix_left, suffix_right)
        if command == 'right':
            return self._right_join(left_data, right_data, left_on, right_on, suffix_left, suffix_right)
        if command == 'full':
            return self._full_join(left_data, right_data, left_on, right_on, suffix_left, suffix_right)
        if command == 'cross':
            return self._cross_join(left_data, right_data, params.get('limit'))
        if command == 'lookup':
            return self._lookup(left_data, right_data, left_on, right_on)
        
        return ActionResult(success=False, message=f"Unknown command: {command}")
    
    def _inner_join(self, left: List[Dict], right: List[Dict], lk: str, rk: str, sl: str, sr: str) -> ActionResult:
        """Inner join - only matching keys from both sides."""
        right_index: Dict[Any, List[Dict]] = {}
        for r in right:
            key = r.get(rk)
            if key not in right_index:
                right_index[key] = []
            right_index[key].append(r)
        
        results = []
        for lrow in left:
            lkey = lrow.get(lk)
            if lkey in right_index:
                for rrow in right_index[lkey]:
                    merged = self._merge_rows(lrow, rrow, lk, rk, sl, sr, False)
                    results.append(merged)
        
        return ActionResult(
            success=True,
            message=f"Inner join: {len(results)} rows from {len(left)}x{len(right)}",
            data={'results': results, 'row_count': len(results)}
        )
    
    def _left_join(self, left: List[Dict], right: List[Dict], lk: str, rk: str, sl: str, sr: str) -> ActionResult:
        """Left join - all left rows, matching right rows."""
        right_index: Dict[Any, List[Dict]] = {}
        for r in right:
            key = r.get(rk)
            if key not in right_index:
                right_index[key] = []
            right_index[key].append(r)
        
        results = []
        for lrow in left:
            lkey = lrow.get(lk)
            if lkey in right_index:
                for rrow in right_index[lkey]:
                    results.append(self._merge_rows(lrow, rrow, lk, rk, sl, sr, False))
            else:
                results.append(self._add_suffix_to_row(lrow, right, sl, True))
        
        return ActionResult(
            success=True,
            message=f"Left join: {len(results)} rows",
            data={'results': results, 'row_count': len(results)}
        )
    
    def _right_join(self, left: List[Dict], right: List[Dict], lk: str, rk: str, sl: str, sr: str) -> ActionResult:
        """Right join - all right rows, matching left rows."""
        left_index: Dict[Any, List[Dict]] = {}
        for l in left:
            key = l.get(lk)
            if key not in left_index:
                left_index[key] = []
            left_index[key].append(l)
        
        results = []
        for rrow in right:
            rkey = rrow.get(rk)
            if rkey in left_index:
                for lrow in left_index[rkey]:
                    results.append(self._merge_rows(lrow, rrow, lk, rk, sl, sr, False))
            else:
                results.append(self._add_suffix_to_row(lrow, left, sr, False))
        
        return ActionResult(
            success=True,
            message=f"Right join: {len(results)} rows",
            data={'results': results, 'row_count': len(results)}
        )
    
    def _full_join(self, left: List[Dict], right: List[Dict], lk: str, rk: str, sl: str, sr: str) -> ActionResult:
        """Full outer join - all rows from both sides."""
        left_index: Dict[Any, List[Dict]] = {}
        for l in left:
            key = l.get(lk)
            if key not in left_index:
                left_index[key] = []
            left_index[key].append(l)
        
        right_index: Dict[Any, List[Dict]] = {}
        for r in right:
            key = r.get(rk)
            if key not in right_index:
                right_index[key] = []
            right_index[key].append(r)
        
        all_keys = set(left_index.keys()) | set(right_index.keys())
        results = []
        for key in all_keys:
            lrows = left_index.get(key, [None])
            rrows = right_index.get(key, [None])
            for lrow in lrows:
                for rrow in rrows:
                    if lrow and rrow:
                        results.append(self._merge_rows(lrow, rrow, lk, rk, sl, sr, False))
                    elif lrow:
                        results.append(self._add_suffix_to_row(lrow, right, sl, True))
                    elif rrow:
                        results.append(self._add_suffix_to_row(rrow, left, sr, False))
        
        return ActionResult(
            success=True,
            message=f"Full join: {len(results)} rows",
            data={'results': results, 'row_count': len(results)}
        )
    
    def _cross_join(self, left: List[Dict], right: List[Dict], limit: Optional[int]) -> ActionResult:
        """Cartesian product of both datasets."""
        limit = limit or 10000
        results = []
        for lrow in left:
            for rrow in right:
                merged = {**lrow, **rrow}
                results.append(merged)
                if len(results) >= limit:
                    return ActionResult(
                        success=True,
                        message=f"Cross join: {len(results)} rows (limited from {len(left)*len(right)})",
                        data={'results': results, 'row_count': len(results), 'limited': True}
                    )
        return ActionResult(
            success=True,
            message=f"Cross join: {len(results)} rows",
            data={'results': results, 'row_count': len(results)}
        )
    
    def _lookup(self, primary: List[Dict], lookup: List[Dict], pk: str, lk: str) -> ActionResult:
        """Add lookup fields to primary dataset (like VLOOKUP)."""
        lookup_index: Dict[Any, Dict] = {}
        for lr in lookup:
            lookup_index[lr.get(lk)] = lr
        
        results = []
        for prow in primary:
            key = prow.get(pk)
            result = {**prow}
            if key in lookup_index:
                lrow = lookup_index[key]
                for lk_field, lv in lrow.items():
                    if lk_field != lk:
                        result[f'{lk_field}_lookup'] = lv
            results.append(result)
        
        return ActionResult(
            success=True,
            message=f"Lookup: {len(results)} rows enriched",
            data={'results': results, 'row_count': len(results)}
        )
    
    def _merge_rows(self, lrow: Dict, rrow: Dict, lk: str, rk: str, sl: str, sr: str, is_left: bool) -> Dict:
        """Merge two rows, handling duplicate key fields."""
        merged = {}
        all_keys = set(lrow.keys()) | set(rrow.keys())
        for key in all_keys:
            if key == rk and key not in lrow:
                merged[key] = rrow.get(key)
            elif key == lk and key not in rrow:
                merged[key] = lrow.get(key)
            elif key in lrow and key in rrow and lrow[key] != rrow[key]:
                if is_left:
                    merged[f'{key}{sl}'] = lrow[key]
                    merged[f'{key}{sr}'] = rrow[key]
                else:
                    merged[key] = lrow[key]
            elif key in lrow:
                merged[key] = lrow[key]
            elif key in rrow:
                merged[key] = rrow[key]
        return merged
    
    def _add_suffix_to_row(self, row: Dict, other: List[Dict], suffix: str, is_left: bool) -> Dict:
        """Add suffix to fields from non-matching side."""
        result = {}
        other_fields = set()
        if other:
            other_fields = set(other[0].keys())
        for key, val in row.items():
            result[key] = val
        return result

"""Data Join Action.

Joins multiple datasets using various join types (inner, left, right, full, cross)
with key matching, multiple key support, and anti-join operations.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataJoinAction(BaseAction):
    """Join multiple datasets with various join types.
    
    Supports inner, left, right, full outer, cross, and anti joins
    with multiple key fields and conflict resolution.
    """
    action_type = "data_join"
    display_name = "数据关联"
    description = "关联多个数据集，支持内联/外联/交叉等多种关联类型"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Join multiple datasets.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data_a: First dataset.
                - data_b: Second dataset.
                - join_type: 'inner', 'left', 'right', 'full', 'cross', 'anti_a', 'anti_b'.
                - keys_a: Key field(s) for dataset A.
                - keys_b: Key field(s) for dataset B.
                - select_a: Fields to select from A (default: all).
                - select_b: Fields to select from B (default: all).
                - suffix_a: Suffix for duplicate fields from A.
                - suffix_b: Suffix for duplicate fields from B.
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with joined data.
        """
        try:
            data_a = params.get('data_a') or context.get_variable(params.get('use_var_a', 'data_a'))
            data_b = params.get('data_b') or context.get_variable(params.get('use_var_b', 'data_b'))
            join_type = params.get('join_type', 'inner').lower()
            keys_a = params.get('keys_a')
            keys_b = params.get('keys_b')
            select_a = params.get('select_a')
            select_b = params.get('select_b')
            suffix_a = params.get('suffix_a', '_a')
            suffix_b = params.get('suffix_b', '_b')
            save_to_var = params.get('save_to_var', 'joined_data')

            if not data_a or not data_b:
                return ActionResult(success=False, message="Both data_a and data_b are required")

            if join_type != 'cross' and (not keys_a or not keys_b):
                return ActionResult(success=False, message="keys_a and keys_b are required for non-cross join")

            if isinstance(keys_a, str):
                keys_a = [keys_a]
            if isinstance(keys_b, str):
                keys_b = [keys_b]

            # Build indices
            if join_type != 'cross':
                index_a = self._build_index(data_a, keys_a)
                index_b = self._build_index(data_b, keys_b)
            else:
                index_a = None
                index_b = None

            # Perform join
            if join_type == 'inner':
                result = self._inner_join(data_a, data_b, index_a, index_b, keys_a, keys_b, 
                                         select_a, select_b, suffix_a, suffix_b)
            elif join_type == 'left':
                result = self._left_join(data_a, data_b, index_a, index_b, keys_a, keys_b,
                                       select_a, select_b, suffix_a, suffix_b)
            elif join_type == 'right':
                result = self._right_join(data_a, data_b, index_a, index_b, keys_a, keys_b,
                                        select_a, select_b, suffix_a, suffix_b)
            elif join_type == 'full' or join_type == 'outer':
                result = self._full_join(data_a, data_b, index_a, index_b, keys_a, keys_b,
                                       select_a, select_b, suffix_a, suffix_b)
            elif join_type == 'cross':
                result = self._cross_join(data_a, data_b, select_a, select_b)
            elif join_type == 'anti_a':
                result = self._anti_join_a(data_a, index_a, index_b, select_a)
            elif join_type == 'anti_b':
                result = self._anti_join_b(data_b, index_a, index_b, select_b)
            else:
                return ActionResult(success=False, message=f"Unknown join type: {join_type}")

            summary = {
                'join_type': join_type,
                'total_a': len(data_a),
                'total_b': len(data_b),
                'joined_count': len(result)
            }

            context.set_variable(save_to_var, result)
            return ActionResult(success=True, data=summary,
                             message=f"{join_type} join: {len(result)} rows")

        except Exception as e:
            return ActionResult(success=False, message=f"Join error: {e}")

    def _build_index(self, data: List[Dict], keys: List[str]) -> Dict[Tuple, List]:
        """Build index for join keys."""
        index = {}
        for item in data:
            if isinstance(item, dict):
                key = tuple(item.get(k) for k in keys)
                if key not in index:
                    index[key] = []
                index[key].append(item)
        return index

    def _get_key(self, item: Dict, keys: List[str]) -> Tuple:
        """Get key tuple from item."""
        return tuple(item.get(k) for k in keys)

    def _inner_join(self, data_a, data_b, index_a, index_b, keys_a, keys_b,
                   select_a, select_b, suffix_a, suffix_b) -> List[Dict]:
        """Inner join - only matching rows from both."""
        result = []
        common_keys = set(index_a.keys()) & set(index_b.keys())
        
        for key in common_keys:
            for item_a in index_a[key]:
                for item_b in index_b[key]:
                    merged = self._merge_items(item_a, item_b, keys_a, keys_b, 
                                             select_a, select_b, suffix_a, suffix_b)
                    result.append(merged)
        return result

    def _left_join(self, data_a, data_b, index_a, index_b, keys_a, keys_b,
                  select_a, select_b, suffix_a, suffix_b) -> List[Dict]:
        """Left join - all from A, matching from B."""
        result = []
        for item_a in data_a:
            key = self._get_key(item_a, keys_a)
            if key in index_b:
                for item_b in index_b[key]:
                    merged = self._merge_items(item_a, item_b, keys_a, keys_b,
                                             select_a, select_b, suffix_a, suffix_b)
                    result.append(merged)
            else:
                merged = self._select_fields(item_a, select_a)
                result.append(merged)
        return result

    def _right_join(self, data_a, data_b, index_a, index_b, keys_a, keys_b,
                   select_a, select_b, suffix_a, suffix_b) -> List[Dict]:
        """Right join - all from B, matching from A."""
        result = []
        for item_b in data_b:
            key = self._get_key(item_b, keys_b)
            if key in index_a:
                for item_a in index_a[key]:
                    merged = self._merge_items(item_a, item_b, keys_a, keys_b,
                                             select_a, select_b, suffix_a, suffix_b)
                    result.append(merged)
            else:
                merged = self._select_fields(item_b, select_b)
                result.append(merged)
        return result

    def _full_join(self, data_a, data_b, index_a, index_b, keys_a, keys_b,
                  select_a, select_b, suffix_a, suffix_b) -> List[Dict]:
        """Full outer join - all from both."""
        result = []
        seen = set()

        # All from A
        for item_a in data_a:
            key = self._get_key(item_a, keys_a)
            if key in index_b:
                for item_b in index_b[key]:
                    merged = self._merge_items(item_a, item_b, keys_a, keys_b,
                                             select_a, select_b, suffix_a, suffix_b)
                    result.append(merged)
                    seen.add((key, id(item_b)))
            else:
                merged = self._select_fields(item_a, select_a)
                result.append(merged)

        # Remaining from B
        for item_b in data_b:
            key = self._get_key(item_b, keys_b)
            if key not in index_a:
                merged = self._select_fields(item_b, select_b)
                result.append(merged)

        return result

    def _cross_join(self, data_a, data_b, select_a, select_b) -> List[Dict]:
        """Cross join - Cartesian product."""
        result = []
        for item_a in data_a:
            for item_b in data_b:
                merged = {}
                merged.update(self._select_fields(item_a, select_a))
                merged.update(self._select_fields(item_b, select_b))
                result.append(merged)
        return result

    def _anti_join_a(self, data_a, index_a, index_b, select_a) -> List[Dict]:
        """Anti join A - rows in A not in B."""
        result = []
        for item_a in data_a:
            key = self._get_key(item_a, list(index_a.keys())[0] if index_a else [])
            if key not in index_b:
                merged = self._select_fields(item_a, select_a)
                result.append(merged)
        return result

    def _anti_join_b(self, data_b, index_a, index_b, select_b) -> List[Dict]:
        """Anti join B - rows in B not in A."""
        result = []
        for item_b in data_b:
            key = self._get_key(item_b, list(index_b.keys())[0] if index_b else [])
            if key not in index_a:
                merged = self._select_fields(item_b, select_b)
                result.append(merged)
        return result

    def _merge_items(self, item_a, item_b, keys_a, keys_b, 
                    select_a, select_b, suffix_a, suffix_b) -> Dict:
        """Merge two items handling field conflicts."""
        merged = {}
        
        # Add fields from A
        a_fields = self._select_fields(item_a, select_a) if select_a else item_a
        if isinstance(a_fields, dict):
            for k, v in a_fields.items():
                if k in merged:
                    merged[k + suffix_a] = v
                else:
                    merged[k] = v

        # Add fields from B
        b_fields = self._select_fields(item_b, select_b) if select_b else item_b
        if isinstance(b_fields, dict):
            for k, v in b_fields.items():
                if k in merged:
                    merged[k + suffix_b] = v
                else:
                    merged[k] = v

        return merged

    def _select_fields(self, item: Dict, fields: Optional[List[str]]) -> Dict:
        """Select specific fields from item."""
        if not fields:
            return item.copy() if isinstance(item, dict) else item
        return {k: v for k, v in item.items() if k in fields} if isinstance(item, dict) else item

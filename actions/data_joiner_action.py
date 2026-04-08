"""Data joiner action module for RabAI AutoClick.

Provides data joining capabilities with support for inner, left,
right, outer joins and cross joins.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Set, Tuple
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class JoinType(Enum):
    """Join types."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    OUTER = "outer"
    CROSS = "cross"


from enum import Enum


class DataJoinerAction(BaseAction):
    """Data joiner action for combining data sources.
    
    Supports inner, left, right, outer, and cross joins with
    configurable key fields.
    """
    action_type = "data_joiner"
    display_name = "数据连接"
    description = "多数据源关联Join"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute join operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                left: Left data source
                right: Right data source
                left_key: Key field for left source
                right_key: Key field for right source
                join_type: Type of join (inner, left, right, outer, cross)
                how: Alias for join_type
                select_fields: Fields to include in output.
        
        Returns:
            ActionResult with joined data.
        """
        left = params.get('left', [])
        right = params.get('right', [])
        left_key = params.get('left_key')
        right_key = params.get('right_key')
        join_type = params.get('join_type') or params.get('how', 'inner')
        select_fields = params.get('select_fields')
        
        if not isinstance(left, list) or not isinstance(right, list):
            return ActionResult(success=False, message="Both sources must be lists")
        
        if join_type == 'cross':
            return self._cross_join(left, right, select_fields)
        
        if not left_key or not right_key:
            return ActionResult(success=False, message="left_key and right_key required")
        
        return self._join(
            left, right,
            left_key, right_key,
            join_type,
            select_fields
        )
    
    def _join(
        self,
        left: List[Dict],
        right: List[Dict],
        left_key: str,
        right_key: str,
        join_type: str,
        select_fields: Optional[List[str]]
    ) -> ActionResult:
        """Perform join operation."""
        join_type = join_type.lower()
        
        right_index: Dict[Any, List[Dict]] = defaultdict(list)
        for item in right:
            key = item.get(right_key)
            if key is not None:
                right_index[key].append(item)
        
        matched_right: Set[int] = set()
        results = []
        
        for i, left_item in enumerate(left):
            left_val = left_item.get(left_key)
            right_matches = right_index.get(left_val, [])
            
            if right_matches:
                matched_right.update(id(r) for r in right_matches)
                for right_item in right_matches:
                    merged = self._merge_items(left_item, right_item, select_fields)
                    results.append(merged)
            elif join_type in ('left', 'outer'):
                merged = self._merge_items(left_item, {}, select_fields)
                results.append(merged)
        
        if join_type in ('right', 'outer'):
            for right_item in right:
                if id(right_item) not in matched_right:
                    merged = self._merge_items({}, right_item, select_fields)
                    results.append(merged)
        
        return ActionResult(
            success=True,
            message=f"{join_type.title()} join: {len(results)} results",
            data={
                'results': results,
                'count': len(results),
                'join_type': join_type
            }
        )
    
    def _cross_join(
        self,
        left: List[Dict],
        right: List[Dict],
        select_fields: Optional[List[str]]
    ) -> ActionResult:
        """Perform cross join."""
        if len(left) * len(right) > 100000:
            return ActionResult(
                success=False,
                message=f"Cross join would produce {len(left) * len(right)} results (limit: 100000)"
            )
        
        results = []
        for left_item in left:
            for right_item in right:
                merged = self._merge_items(left_item, right_item, select_fields)
                results.append(merged)
        
        return ActionResult(
            success=True,
            message=f"Cross join: {len(results)} results",
            data={
                'results': results,
                'count': len(results),
                'join_type': 'cross'
            }
        )
    
    def _merge_items(
        self,
        left_item: Dict,
        right_item: Dict,
        select_fields: Optional[List[str]]
    ) -> Dict:
        """Merge two items, handling key conflicts."""
        result = {}
        
        for key, value in left_item.items():
            if select_fields is None or key in select_fields:
                result[key] = value
        
        for key, value in right_item.items():
            if key in left_item:
                result[f"{key}_y"] = value
            elif select_fields is None or key in select_fields:
                result[key] = value
        
        return result

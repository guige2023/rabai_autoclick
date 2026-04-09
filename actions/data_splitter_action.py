"""Data Splitter Action Module.

Provides data splitting and partitioning capabilities.
"""

import math
import traceback
import sys
import os
from typing import Any, Dict, List, Optional, Tuple, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataSplitterAction(BaseAction):
    """Split data into partitions.
    
    Supports various splitting strategies: equal, by-key, range, and custom.
    """
    action_type = "data_splitter"
    display_name = "数据分割"
    description = "支持多种策略的数据分割"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data splitting.
        
        Args:
            context: Execution context.
            params: Dict with keys: data, strategy, num_partitions, options.
        
        Returns:
            ActionResult with split data.
        """
        data = params.get('data', [])
        strategy = params.get('strategy', 'equal')
        num_partitions = params.get('num_partitions', 2)
        options = params.get('options', {})
        
        if not data:
            return ActionResult(
                success=False,
                data=None,
                error="No data to split"
            )
        
        try:
            if strategy == 'equal':
                splits = self._split_equal(data, num_partitions)
            elif strategy == 'by_key':
                splits = self._split_by_key(data, options)
            elif strategy == 'by_range':
                splits = self._split_by_range(data, options)
            elif strategy == 'by_size':
                splits = self._split_by_size(data, options)
            elif strategy == 'custom':
                splits = self._split_custom(data, options)
            else:
                return ActionResult(
                    success=False,
                    data=None,
                    error=f"Unknown strategy: {strategy}"
                )
            
            return ActionResult(
                success=True,
                data={
                    'splits': splits,
                    'num_partitions': len(splits),
                    'strategy': strategy
                },
                error=None
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Split failed: {str(e)}"
            )
    
    def _split_equal(self, data: List, num_partitions: int) -> List[List]:
        """Split data into equal partitions."""
        if num_partitions <= 0:
            num_partitions = 1
        
        partition_size = math.ceil(len(data) / num_partitions)
        splits = []
        
        for i in range(0, len(data), partition_size):
            splits.append(data[i:i + partition_size])
        
        return splits
    
    def _split_by_key(self, data: List, options: Dict) -> Dict[str, List]:
        """Split data by key field."""
        key_field = options.get('key_field', 'category')
        
        splits = {}
        for item in data:
            if isinstance(item, dict):
                key = str(item.get(key_field, 'unknown'))
            else:
                key = 'default'
            
            if key not in splits:
                splits[key] = []
            splits[key].append(item)
        
        return splits
    
    def _split_by_range(self, data: List, options: Dict) -> List[List]:
        """Split data by numeric range."""
        value_field = options.get('value_field', 'value')
        ranges = options.get('ranges', [(0, 50), (50, 100), (100, float('inf'))])
        
        splits = [[] for _ in ranges]
        
        for item in data:
            if isinstance(item, dict):
                value = item.get(value_field, 0)
            else:
                value = item if isinstance(item, (int, float)) else 0
            
            for i, (min_val, max_val) in enumerate(ranges):
                if min_val <= value < max_val:
                    splits[i].append(item)
                    break
        
        return splits
    
    def _split_by_size(self, data: List, options: Dict) -> List[List]:
        """Split data by chunk size."""
        chunk_size = options.get('chunk_size', 100)
        
        if chunk_size <= 0:
            chunk_size = 1
        
        splits = []
        for i in range(0, len(data), chunk_size):
            splits.append(data[i:i + chunk_size])
        
        return splits
    
    def _split_custom(self, data: List, options: Dict) -> List[List]:
        """Split using custom function."""
        split_fn_str = options.get('split_function', '')
        
        if not split_fn_str:
            return [data]
        
        # Placeholder for custom function
        # In practice, would use eval or a proper function parser
        return [data]


class DataJoinerAction(BaseAction):
    """Join multiple data sources.
    
    Supports inner, left, right, and full outer joins.
    """
    action_type = "data_joiner"
    display_name: "数据连接"
    description = "支持多种连接类型的数据合并"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data join.
        
        Args:
            context: Execution context.
            params: Dict with keys: left, right, join_key, join_type.
        
        Returns:
            ActionResult with joined data.
        """
        left = params.get('left', [])
        right = params.get('right', [])
        join_key = params.get('join_key', 'id')
        join_type = params.get('join_type', 'inner')
        
        if not left or not right:
            return ActionResult(
                success=False,
                data=None,
                error="Both left and right data required"
            )
        
        try:
            if join_type == 'inner':
                result = self._inner_join(left, right, join_key)
            elif join_type == 'left':
                result = self._left_join(left, right, join_key)
            elif join_type == 'right':
                result = self._right_join(left, right, join_key)
            elif join_type == 'full':
                result = self._full_outer_join(left, right, join_key)
            else:
                return ActionResult(
                    success=False,
                    data=None,
                    error=f"Unknown join type: {join_type}"
                )
            
            return ActionResult(
                success=True,
                data={
                    'joined': result,
                    'count': len(result),
                    'join_type': join_type
                },
                error=None
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                data=None,
                error=f"Join failed: {str(e)}"
            )
    
    def _build_index(self, data: List, key: str) -> Dict:
        """Build index for join key."""
        index = {}
        for item in data:
            if isinstance(item, dict):
                key_value = item.get(key)
                if key_value not in index:
                    index[key_value] = []
                index[key_value].append(item)
        return index
    
    def _inner_join(self, left: List, right: List, key: str) -> List[Dict]:
        """Inner join."""
        right_index = self._build_index(right, key)
        result = []
        
        for item in left:
            if isinstance(item, dict):
                key_value = item.get(key)
                if key_value in right_index:
                    for right_item in right_index[key_value]:
                        merged = {**item, **right_item}
                        result.append(merged)
        
        return result
    
    def _left_join(self, left: List, right: List, key: str) -> List[Dict]:
        """Left outer join."""
        right_index = self._build_index(right, key)
        result = []
        
        for item in left:
            if isinstance(item, dict):
                key_value = item.get(key)
                if key_value in right_index:
                    for right_item in right_index[key_value]:
                        merged = {**item, **right_item}
                        result.append(merged)
                else:
                    result.append(item.copy())
        
        return result
    
    def _right_join(self, left: List, right: List, key: str) -> List[Dict]:
        """Right outer join."""
        left_index = self._build_index(left, key)
        result = []
        
        for item in right:
            if isinstance(item, dict):
                key_value = item.get(key)
                if key_value in left_index:
                    for left_item in left_index[key_value]:
                        merged = {**left_item, **item}
                        result.append(merged)
                else:
                    result.append(item.copy())
        
        return result
    
    def _full_outer_join(self, left: List, right: List, key: str) -> List[Dict]:
        """Full outer join."""
        left_index = self._build_index(left, key)
        right_index = self._build_index(right, key)
        result = []
        seen_keys = set()
        
        # Left matches
        for item in left:
            if isinstance(item, dict):
                key_value = item.get(key)
                seen_keys.add(key_value)
                if key_value in right_index:
                    for right_item in right_index[key_value]:
                        merged = {**item, **right_item}
                        result.append(merged)
                else:
                    result.append(item.copy())
        
        # Right-only
        for item in right:
            if isinstance(item, dict):
                key_value = item.get(key)
                if key_value not in seen_keys:
                    result.append(item.copy())
        
        return result


class DataMergerAction(BaseAction):
    """Merge multiple data collections.
    
    Combines data from various sources with conflict resolution.
    """
    action_type = "data_merger"
    display_name = "数据合并"
    description = "合并多个数据源并解决冲突"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data merge.
        
        Args:
            context: Execution context.
            params: Dict with keys: sources, merge_strategy, conflict_resolution.
        
        Returns:
            ActionResult with merged data.
        """
        sources = params.get('sources', [])
        merge_strategy = params.get('merge_strategy', 'union')
        conflict_resolution = params.get('conflict_resolution', 'last_wins')
        
        if not sources:
            return ActionResult(
                success=False,
                data=None,
                error="No sources provided"
            )
        
        if merge_strategy == 'union':
            result = self._merge_union(sources, conflict_resolution)
        elif merge_strategy == 'intersection':
            result = self._merge_intersection(sources)
        elif merge_strategy == 'difference':
            result = self._merge_difference(sources)
        else:
            return ActionResult(
                success=False,
                data=None,
                error=f"Unknown strategy: {merge_strategy}"
            )
        
        return ActionResult(
            success=True,
            data={
                'merged': result,
                'count': len(result)
            },
            error=None
        )
    
    def _merge_union(self, sources: List, conflict_resolution: str) -> List[Dict]:
        """Union merge with conflict resolution."""
        merged = {}
        
        for source in sources:
            for item in source:
                if not isinstance(item, dict):
                    continue
                
                key_field = item.get('id', str(id(item)))
                
                if key_field not in merged:
                    merged[key_field] = item.copy()
                else:
                    if conflict_resolution == 'first_wins':
                        continue
                    elif conflict_resolution == 'last_wins':
                        merged[key_field] = item.copy()
                    elif conflict_resolution == 'merge':
                        merged[key_field].update(item)
        
        return list(merged.values())
    
    def _merge_intersection(self, sources: List) -> List[Dict]:
        """Intersection merge - items in all sources."""
        if not sources:
            return []
        
        sets = []
        for source in sources:
            keys = set()
            for item in source:
                if isinstance(item, dict) and 'id' in item:
                    keys.add(item['id'])
            sets.append(keys)
        
        common_keys = sets[0].intersection(*sets[1:])
        
        result = []
        for item in sources[0]:
            if isinstance(item, dict) and item.get('id') in common_keys:
                result.append(item)
        
        return result
    
    def _merge_difference(self, sources: List) -> List[Dict]:
        """Difference merge - items in first but not others."""
        if len(sources) < 2:
            return sources[0] if sources else []
        
        other_keys = set()
        for item in sources[1]:
            if isinstance(item, dict) and 'id' in item:
                other_keys.add(item['id'])
        
        result = []
        for item in sources[0]:
            if isinstance(item, dict) and item.get('id') not in other_keys:
                result.append(item)
        
        return result


def register_actions():
    """Register all Data Splitter actions."""
    return [
        DataSplitterAction,
        DataJoinerAction,
        DataMergerAction,
    ]

"""Data comparator action module for RabAI AutoClick.

Provides data comparison capabilities for finding differences
between datasets, records, and structured data.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from difflib import SequenceMatcher

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class DiffResult:
    """Result of a diff operation."""
    added: List[Any]
    removed: List[Any]
    modified: List[Tuple[Any, Any]]
    unchanged: List[Any]


class DataComparatorAction(BaseAction):
    """Data comparator action for comparing data sources.
    
    Supports comparing lists, dicts, and structured data with
    configurable key fields and similarity thresholds.
    """
    action_type = "data_comparator"
    display_name = "数据比较"
    description = "数据集差异比较"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute data comparison.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: compare|diff|match|similarity
                left: Left data source
                right: Right data source
                key_field: Field to use as key for matching
                ignore_fields: Fields to ignore in comparison
                threshold: Similarity threshold (0-1).
        
        Returns:
            ActionResult with comparison results.
        """
        operation = params.get('operation', 'compare')
        
        if operation == 'compare':
            return self._compare(params)
        elif operation == 'diff':
            return self._diff_lists(params)
        elif operation == 'match':
            return self._match(params)
        elif operation == 'similarity':
            return self._similarity(params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _compare(self, params: Dict[str, Any]) -> ActionResult:
        """Compare two data sources."""
        left = params.get('left', [])
        right = params.get('right', [])
        key_field = params.get('key_field')
        ignore_fields = params.get('ignore_fields', [])
        
        if isinstance(left, list) and isinstance(right, list):
            if key_field:
                return self._compare_list_of_dicts(left, right, key_field, ignore_fields)
            else:
                return self._diff_lists({
                    **params,
                    'left': left,
                    'right': right
                })
        
        if isinstance(left, dict) and isinstance(right, dict):
            return self._compare_dicts(left, right, ignore_fields)
        
        if left == right:
            return ActionResult(
                success=True,
                message="Data sources are equal",
                data={'equal': True}
            )
        else:
            return ActionResult(
                success=True,
                message="Data sources differ",
                data={'equal': False, 'left': left, 'right': right}
            )
    
    def _compare_list_of_dicts(
        self,
        left: List[Dict],
        right: List[Dict],
        key_field: str,
        ignore_fields: List[str]
    ) -> ActionResult:
        """Compare two lists of dictionaries by key field."""
        left_keys = {item.get(key_field) for item in left if item.get(key_field) is not None}
        right_keys = {item.get(key_field) for item in right if item.get(key_field) is not None}
        
        added_keys = right_keys - left_keys
        removed_keys = left_keys - right_keys
        common_keys = left_keys & right_keys
        
        left_by_key = {item.get(key_field): item for item in left}
        right_by_key = {item.get(key_field): item for item in right}
        
        added = [right_by_key[k] for k in added_keys]
        removed = [left_by_key[k] for k in removed_keys]
        modified = []
        unchanged = []
        
        for key in common_keys:
            left_item = left_by_key[key]
            right_item = right_by_key[key]
            
            if self._items_differ(left_item, right_item, ignore_fields):
                modified.append((left_item, right_item))
            else:
                unchanged.append(left_item)
        
        return ActionResult(
            success=True,
            message=f"Comparison: {len(added)} added, {len(removed)} removed, {len(modified)} modified, {len(unchanged)} unchanged",
            data={
                'added': added,
                'added_count': len(added),
                'removed': removed,
                'removed_count': len(removed),
                'modified': [{'before': m[0], 'after': m[1]} for m in modified],
                'modified_count': len(modified),
                'unchanged': unchanged,
                'unchanged_count': len(unchanged)
            }
        )
    
    def _items_differ(
        self,
        left: Dict,
        right: Dict,
        ignore_fields: List[str]
    ) -> bool:
        """Check if two items differ."""
        all_keys = set(left.keys()) | set(right.keys())
        
        for key in all_keys:
            if key in ignore_fields:
                continue
            
            if left.get(key) != right.get(key):
                return True
        
        return False
    
    def _compare_dicts(
        self,
        left: Dict,
        right: Dict,
        ignore_fields: List[str]
    ) -> ActionResult:
        """Compare two dictionaries."""
        all_keys = set(left.keys()) | set(right.keys())
        
        added = []
        removed = []
        modified = []
        unchanged = []
        
        for key in sorted(all_keys):
            if key in ignore_fields:
                continue
            
            left_val = left.get(key)
            right_val = right.get(key)
            
            if key not in left:
                added.append({'field': key, 'value': right_val})
            elif key not in right:
                removed.append({'field': key, 'value': left_val})
            elif left_val != right_val:
                modified.append({'field': key, 'before': left_val, 'after': right_val})
            else:
                unchanged.append({'field': key, 'value': left_val})
        
        return ActionResult(
            success=True,
            message=f"Dict comparison: {len(added)} added, {len(removed)} removed, {len(modified)} modified",
            data={
                'added': added,
                'removed': removed,
                'modified': modified,
                'unchanged': unchanged
            }
        )
    
    def _diff_lists(self, params: Dict[str, Any]) -> ActionResult:
        """Diff two lists."""
        left = params.get('left', [])
        right = params.get('right', [])
        
        left_set = set(left)
        right_set = set(right)
        
        added = list(right_set - left_set)
        removed = list(left_set - right_set)
        common = list(left_set & right_set)
        
        return ActionResult(
            success=True,
            message=f"List diff: {len(added)} added, {len(removed)} removed, {len(common)} common",
            data={
                'added': added,
                'added_count': len(added),
                'removed': removed,
                'removed_count': len(removed),
                'common': common,
                'common_count': len(common)
            }
        )
    
    def _match(self, params: Dict[str, Any]) -> ActionResult:
        """Match items between two sources using similarity."""
        left = params.get('left', [])
        right = params.get('right', [])
        threshold = params.get('threshold', 0.8)
        key_field = params.get('key_field')
        
        if not left or not right:
            return ActionResult(success=False, message="Both sources must be non-empty")
        
        matches = []
        unmatched_left = []
        unmatched_right = []
        
        if key_field:
            left_items = {item.get(key_field): item for item in left}
            right_items = {item.get(key_field): item for item in right}
            
            left_keys = set(left_items.keys())
            right_keys = set(right_items.keys())
            
            for key in left_keys & right_keys:
                matches.append({
                    'left': left_items[key],
                    'right': right_items[key],
                    'similarity': 1.0,
                    'key': key
                })
            
            unmatched_left = [left_items[k] for k in left_keys - right_keys]
            unmatched_right = [right_items[k] for k in right_keys - left_keys]
        else:
            right_remaining = list(right)
            
            for left_item in left:
                best_match = None
                best_score = 0
                
                for i, right_item in enumerate(right_remaining):
                    score = self._calculate_similarity(left_item, right_item)
                    
                    if score > best_score:
                        best_score = score
                        best_match = (i, right_item)
                
                if best_match and best_score >= threshold:
                    matches.append({
                        'left': left_item,
                        'right': best_match[1],
                        'similarity': best_score
                    })
                    del right_remaining[best_match[0]]
                else:
                    unmatched_left.append(left_item)
            
            unmatched_right = right_remaining
        
        return ActionResult(
            success=True,
            message=f"Matched {len(matches)} pairs, {len(unmatched_left)} unmatched left, {len(unmatched_right)} unmatched right",
            data={
                'matches': matches,
                'match_count': len(matches),
                'unmatched_left': unmatched_left,
                'unmatched_left_count': len(unmatched_left),
                'unmatched_right': unmatched_right,
                'unmatched_right_count': len(unmatched_right)
            }
        )
    
    def _similarity(self, params: Dict[str, Any]) -> ActionResult:
        """Calculate similarity between two values."""
        left = params.get('left')
        right = params.get('right')
        
        if left is None or right is None:
            return ActionResult(success=False, message="Both values required")
        
        score = self._calculate_similarity(left, right)
        
        return ActionResult(
            success=True,
            message=f"Similarity: {score:.2%}",
            data={
                'left': left,
                'right': right,
                'similarity': round(score, 4)
            }
        )
    
    def _calculate_similarity(self, left: Any, right: Any) -> float:
        """Calculate similarity score between two values."""
        if left == right:
            return 1.0
        
        if isinstance(left, str) and isinstance(right, str):
            return SequenceMatcher(None, left, right).ratio()
        
        if isinstance(left, dict) and isinstance(right, dict):
            all_keys = set(left.keys()) | set(right.keys())
            if not all_keys:
                return 1.0
            
            matches = 0
            for key in all_keys:
                if left.get(key) == right.get(key):
                    matches += 1
            
            return matches / len(all_keys)
        
        if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
            max_len = max(len(left), len(right))
            if max_len == 0:
                return 1.0
            
            matches = sum(1 for i in range(min(len(left), len(right))) if left[i] == right[i])
            return matches / max_len
        
        return 0.0

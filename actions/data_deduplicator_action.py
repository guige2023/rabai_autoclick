"""Data deduplicator action module for RabAI AutoClick.

Provides data deduplication with support for exact and
fuzzy matching based on specified fields.
"""

import sys
import os
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataDeduplicatorAction(BaseAction):
    """Data deduplicator action for removing duplicate records.
    
    Supports exact deduplication by key fields and fuzzy
    deduplication with similarity thresholds.
    """
    action_type = "data_deduplicator"
    display_name = "数据去重"
    description = "数据去重与重复检测"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute deduplication.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                data: Data to deduplicate
                key_fields: Fields to use for deduplication
                method: 'exact' or 'fuzzy'
                threshold: Similarity threshold for fuzzy (0-1)
                keep: 'first', 'last', or 'none'.
        
        Returns:
            ActionResult with deduplicated data.
        """
        data = params.get('data', [])
        key_fields = params.get('key_fields')
        method = params.get('method', 'exact')
        threshold = params.get('threshold', 0.9)
        keep = params.get('keep', 'first')
        
        if not data:
            return ActionResult(success=False, message="No data provided")
        
        if method == 'exact':
            return self._exact_deduplicate(data, key_fields, keep)
        elif method == 'fuzzy':
            return self._fuzzy_deduplicate(data, key_fields, threshold, keep)
        else:
            return ActionResult(success=False, message=f"Unknown method: {method}")
    
    def _exact_deduplicate(
        self,
        data: List[Any],
        key_fields: Optional[List[str]],
        keep: str
    ) -> ActionResult:
        """Exact deduplication by key fields."""
        if not key_fields:
            seen: Set[Tuple] = set()
            result = []
            duplicates = 0
            
            for item in data:
                if isinstance(item, dict):
                    key = tuple(item.get(f) for f in key_fields)
                else:
                    key = (item,)
                
                if key not in seen:
                    seen.add(key)
                    result.append(item)
                else:
                    duplicates += 1
                    if keep == 'none':
                        pass
                    elif keep == 'last':
                        result[-1] = item
        else:
            result = list(data)
            duplicates = 0
        
        return ActionResult(
            success=True,
            message=f"Deduplicated: {len(data)} -> {len(result)} ({duplicates} removed)",
            data={
                'items': result,
                'count': len(result),
                'original_count': len(data),
                'duplicates_removed': duplicates,
                'method': 'exact'
            }
        )
    
    def _fuzzy_deduplicate(
        self,
        data: List[Any],
        key_fields: Optional[List[str]],
        threshold: float,
        keep: str
    ) -> ActionResult:
        """Fuzzy deduplication with similarity matching."""
        if not key_fields:
            return ActionResult(success=False, message="key_fields required for fuzzy deduplication")
        
        result = []
        duplicates = 0
        
        for item in data:
            if not isinstance(item, dict):
                result.append(item)
                continue
            
            is_duplicate = False
            
            for existing in result:
                if not isinstance(existing, dict):
                    continue
                
                similarity = self._calculate_similarity(item, existing, key_fields)
                
                if similarity >= threshold:
                    is_duplicate = True
                    duplicates += 1
                    
                    if keep == 'last':
                        idx = result.index(existing)
                        result[idx] = item
                    
                    break
            
            if not is_duplicate:
                result.append(item)
        
        return ActionResult(
            success=True,
            message=f"Fuzzy deduplicated: {len(data)} -> {len(result)} ({duplicates} removed)",
            data={
                'items': result,
                'count': len(result),
                'original_count': len(data),
                'duplicates_removed': duplicates,
                'threshold': threshold,
                'method': 'fuzzy'
            }
        )
    
    def _calculate_similarity(
        self,
        item1: Dict,
        item2: Dict,
        key_fields: List[str]
    ) -> float:
        """Calculate similarity between two items."""
        if not key_fields:
            return 0.0
        
        matches = 0
        total = len(key_fields)
        
        for field in key_fields:
            val1 = item1.get(field)
            val2 = item2.get(field)
            
            if val1 == val2:
                matches += 1
            elif val1 is not None and val2 is not None:
                if isinstance(val1, str) and isinstance(val2, str):
                    if self._string_similarity(val1, val2) >= 0.8:
                        matches += 0.5
        
        return matches / total if total > 0 else 0.0
    
    def _string_similarity(self, s1: str, s2: str) -> float:
        """Calculate string similarity using simple ratio."""
        if s1 == s2:
            return 1.0
        
        longer = s1 if len(s1) >= len(s2) else s2
        shorter = s2 if len(s1) >= len(s2) else s1
        
        if len(longer) == 0:
            return 1.0
        
        matches = sum(1 for c1, c2 in zip(longer, shorter) if c1 == c2)
        
        return matches / len(longer)

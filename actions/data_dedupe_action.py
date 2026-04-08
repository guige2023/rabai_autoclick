"""Data Dedupe Action.

Deduplicates data based on key fields with configurable strategies
(exact match, fuzzy match, similarity threshold) and merge policies.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Callable
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataDedupeAction(BaseAction):
    """Deduplicate data with various matching strategies.
    
    Supports exact match, fuzzy match, and similarity-based
    deduplication with configurable merge policies.
    """
    action_type = "data_dedupe"
    display_name = "数据去重"
    description = "数据去重，支持精确匹配、模糊匹配和相似度阈值"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Deduplicate data.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data: List of records to deduplicate.
                - key_fields: Field(s) to use for deduplication.
                - strategy: 'exact', 'fuzzy', 'similarity' (default: exact).
                - similarity_threshold: 0-1 threshold for fuzzy matching.
                - merge_policy: 'keep_first', 'keep_last', 'merge', 'custom'.
                - custom_merge_fn: Lambda for custom merge logic.
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with deduplicated data.
        """
        try:
            data = params.get('data')
            key_fields = params.get('key_fields')
            strategy = params.get('strategy', 'exact').lower()
            similarity_threshold = params.get('similarity_threshold', 0.8)
            merge_policy = params.get('merge_policy', 'keep_first')
            custom_merge_fn = params.get('custom_merge_fn')
            save_to_var = params.get('save_to_var', 'deduped_data')

            if data is None:
                data = context.get_variable(params.get('use_var', 'input_data'))

            if not data:
                return ActionResult(success=False, message="No data provided")

            if not isinstance(data, list):
                return ActionResult(success=False, message="Data must be a list")

            if key_fields is None:
                return ActionResult(success=False, message="key_fields is required")

            if isinstance(key_fields, str):
                key_fields = [key_fields]

            if strategy == 'exact':
                result = self._exact_dedupe(data, key_fields, merge_policy, custom_merge_fn)
            elif strategy == 'fuzzy':
                result = self._fuzzy_dedupe(data, key_fields, similarity_threshold, merge_policy)
            elif strategy == 'similarity':
                result = self._similarity_dedupe(data, key_fields, similarity_threshold, merge_policy)
            else:
                return ActionResult(success=False, message=f"Unknown strategy: {strategy}")

            summary = {
                'original_count': len(data),
                'deduped_count': len(result),
                'removed_count': len(data) - len(result),
                'strategy': strategy,
                'key_fields': key_fields
            }

            context.set_variable(save_to_var, result)
            return ActionResult(success=True, data=result, message=f"Deduped: {len(result)}/{len(data)} kept")

        except Exception as e:
            return ActionResult(success=False, message=f"Dedupe error: {e}")

    def _exact_dedupe(self, data: List[Dict], key_fields: List[str],
                    merge_policy: str, custom_merge_fn: Optional[str]) -> List[Dict]:
        """Exact match deduplication."""
        seen = {}
        result = []

        for item in data:
            key = tuple(item.get(f) for f in key_fields)
            key_str = str(key)

            if key_str not in seen:
                seen[key_str] = len(result)
                result.append(item.copy())
            else:
                # Handle merge
                existing = result[seen[key_str]]
                result[seen[key_str]] = self._merge_items(existing, item, merge_policy, custom_merge_fn)

        return result

    def _fuzzy_dedupe(self, data: List[Dict], key_fields: List[str],
                      threshold: float, merge_policy: str) -> List[Dict]:
        """Fuzzy deduplication based on string similarity."""
        result = []
        
        for item in data:
            key_value = ' '.join(str(item.get(f, '')) for f in key_fields)
            is_duplicate = False

            for existing in result:
                existing_key = ' '.join(str(existing.get(f, '')) for f in key_fields)
                similarity = self._string_similarity(key_value, existing_key)

                if similarity >= threshold:
                    idx = result.index(existing)
                    result[idx] = self._merge_items(existing, item, merge_policy, None)
                    is_duplicate = True
                    break

            if not is_duplicate:
                result.append(item.copy())

        return result

    def _similarity_dedupe(self, data: List[Dict], key_fields: List[str],
                          threshold: float, merge_policy: str) -> List[Dict]:
        """Similarity-based deduplication using multiple fields."""
        result = []
        
        for item in data:
            is_duplicate = False
            key_value = [str(item.get(f, '')) for f in key_fields]

            for i, existing in enumerate(result):
                existing_key = [str(existing.get(f, '')) for f in key_fields]
                similarity = self._jaccard_similarity(set(' '.join(key_value).split()),
                                                      set(' '.join(existing_key).split()))

                if similarity >= threshold:
                    result[i] = self._merge_items(existing, item, merge_policy, None)
                    is_duplicate = True
                    break

            if not is_duplicate:
                result.append(item.copy())

        return result

    def _merge_items(self, existing: Dict, new: Dict, policy: str, 
                    custom_fn: Optional[str]) -> Dict:
        """Merge two items based on policy."""
        if policy == 'keep_first':
            return existing
        elif policy == 'keep_last':
            return new
        elif policy == 'merge':
            merged = existing.copy()
            for k, v in new.items():
                if k not in merged:
                    merged[k] = v
                elif merged[k] != v:
                    # Handle conflict - keep existing or newest
                    if isinstance(merged[k], (int, float)) and isinstance(v, (int, float)):
                        merged[k] = merged[k] + v
                    else:
                        merged[k] = merged[k] if len(str(merged[k])) >= len(str(v)) else v
            return merged
        elif policy == 'custom' and custom_fn:
            try:
                return eval(custom_fn)(existing, new)
            except Exception:
                return existing
        return existing

    def _string_similarity(self, s1: str, s2: str) -> float:
        """Calculate string similarity (Levenshtein-based)."""
        if s1 == s2:
            return 1.0
        if not s1 or not s2:
            return 0.0

        len1, len2 = len(s1), len(s2)
        if len1 > len2:
            s1, s2 = s2, s1
            len1, len2 = len2, len1

        current_row = range(len1 + 1)
        for i in range(1, len2 + 1):
            previous_row, current_row = current_row, [i] + [0] * len1
            for j in range(1, len1 + 1):
                add = previous_row[j] + 1
                delete = current_row[j - 1] + 1
                change = previous_row[j - 1]
                if s1[j - 1] != s2[i - 1]:
                    change += 1
                current_row[j] = min(add, delete, change)

        distance = current_row[len1]
        max_len = max(len1, len2)
        return 1.0 - (distance / max_len)

    def _jaccard_similarity(self, set1: set, set2: set) -> float:
        """Calculate Jaccard similarity between two sets."""
        if not set1 or not set2:
            return 0.0
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        return intersection / union if union > 0 else 0.0

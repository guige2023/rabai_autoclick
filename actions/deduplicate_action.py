"""Deduplicate action module for RabAI AutoClick.

Provides actions for removing duplicate data from lists,
dictionaries, and datasets based on various strategies.
"""

import hashlib
import json
import sys
import os
from typing import Any, Dict, List, Optional, Union, Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DeduplicateListAction(BaseAction):
    """Remove duplicates from a list.
    
    Supports preserving order and handling various data types.
    """
    action_type = "deduplicate_list"
    display_name = "列表去重"
    description = "列表元素去重"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Remove duplicate items.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, preserve_order,
                   key_func, keep.
        
        Returns:
            ActionResult with deduplicated list.
        """
        items = params.get('items', [])
        preserve_order = params.get('preserve_order', True)
        key_func = params.get('key_func', None)
        keep = params.get('keep', 'first')

        if not items:
            return ActionResult(success=False, message="items list is required")

        try:
            seen = set()
            result = []
            duplicates = []

            for item in items:
                if key_func:
                    key = key_func(item) if callable(key_func) else self._get_key(item, key_func)
                else:
                    key = self._hash_item(item)

                if key in seen:
                    duplicates.append(item)
                    if keep == 'last':
                        result = [r for r in result if self._get_comparison_key(r, key_func) != key]
                        result.append(item)
                else:
                    seen.add(key)
                    result.append(item)

            if not preserve_order and keep == 'first':
                result = list(seen)

            return ActionResult(
                success=True,
                message=f"Removed {len(duplicates)} duplicates",
                data={
                    'items': result,
                    'original_count': len(items),
                    'deduplicated_count': len(result),
                    'duplicates_removed': len(items) - len(result)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Deduplication failed: {str(e)}")

    def _hash_item(self, item: Any) -> str:
        """Generate hash for item."""
        try:
            s = json.dumps(item, sort_keys=True, default=str)
            return hashlib.md5(s.encode('utf-8')).hexdigest()
        except:
            return str(id(item))

    def _get_key(self, item: Any, key_func: str) -> Any:
        """Get key from item using function name."""
        if callable(key_func):
            return key_func(item)
        if isinstance(item, dict) and key_func in item:
            return item[key_func]
        return item

    def _get_comparison_key(self, item: Any, key_func: Any) -> Any:
        """Get comparison key for item."""
        if key_func and callable(key_func):
            return key_func(item)
        if isinstance(item, dict) and isinstance(key_func, str) and key_func in item:
            return item[key_func]
        return self._hash_item(item)


class DeduplicateDictAction(BaseAction):
    """Remove duplicate dictionaries from list.
    
    Deduplicates based on specified fields.
    """
    action_type = "deduplicate_dict"
    display_name = "字典去重"
    description = "字典列表去重"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Remove duplicate dicts.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, fields, preserve_order,
                   strategy.
        
        Returns:
            ActionResult with deduplicated list.
        """
        items = params.get('items', [])
        fields = params.get('fields', [])
        preserve_order = params.get('preserve_order', True)
        strategy = params.get('strategy', 'first')

        if not items:
            return ActionResult(success=False, message="items list is required")
        if not fields:
            return ActionResult(success=False, message="fields list is required")

        try:
            seen = {}
            result = []
            duplicates = []

            for item in items:
                if not isinstance(item, dict):
                    continue

                key_values = tuple(item.get(f) for f in fields)
                
                if key_values in seen:
                    duplicates.append(item)
                    if strategy == 'last':
                        result_idx = seen[key_values]
                        result[result_idx] = item
                else:
                    seen[key_values] = len(result)
                    result.append(item)

            return ActionResult(
                success=True,
                message=f"Removed {len(duplicates)} duplicates",
                data={
                    'items': result,
                    'original_count': len(items),
                    'deduplicated_count': len(result),
                    'duplicates_removed': len(items) - len(result)
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Deduplication failed: {str(e)}")


class DeduplicateFuzzyAction(BaseAction):
    """Fuzzy deduplication based on similarity.
    
    Finds and removes near-duplicate items.
    """
    action_type = "deduplicate_fuzzy"
    display_name = "模糊去重"
    description = "模糊相似度去重"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Fuzzy deduplicate items.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, threshold, key_func,
                   similarity_func.
        
        Returns:
            ActionResult with deduplicated list.
        """
        items = params.get('items', [])
        threshold = params.get('threshold', 0.8)
        key_func = params.get('key_func', None)
        similarity_func = params.get('similarity_func', 'levenshtein')

        if not items:
            return ActionResult(success=False, message="items list is required")

        try:
            result = []
            removed = 0

            for item in items:
                key = self._get_item_key(item, key_func)
                is_duplicate = False

                for existing in result:
                    existing_key = self._get_item_key(existing, key_func)
                    similarity = self._calculate_similarity(key, existing_key, similarity_func)
                    
                    if similarity >= threshold:
                        is_duplicate = True
                        removed += 1
                        break

                if not is_duplicate:
                    result.append(item)

            return ActionResult(
                success=True,
                message=f"Removed {removed} fuzzy duplicates",
                data={
                    'items': result,
                    'original_count': len(items),
                    'deduplicated_count': len(result),
                    'threshold': threshold
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Fuzzy deduplication failed: {str(e)}")

    def _get_item_key(self, item: Any, key_func: Any) -> str:
        """Get string key from item."""
        if key_func and callable(key_func):
            return str(key_func(item))
        if isinstance(item, dict):
            return json.dumps(item, sort_keys=True, default=str)
        return str(item)

    def _calculate_similarity(self, s1: str, s2: str, method: str) -> float:
        """Calculate string similarity."""
        if s1 == s2:
            return 1.0
        if not s1 or not s2:
            return 0.0

        if method == 'levenshtein':
            distance = self._levenshtein_distance(s1, s2)
            max_len = max(len(s1), len(s2))
            return 1 - (distance / max_len)
        
        elif method == 'jaccard':
            set1 = set(s1.lower())
            set2 = set(s2.lower())
            intersection = len(set1 & set2)
            union = len(set1 | set2)
            return intersection / union if union > 0 else 0.0
        
        elif method == 'cosine':
            words1 = set(s1.lower().split())
            words2 = set(s2.lower().split())
            intersection = len(words1 & words2)
            norm1 = len(words1) ** 0.5
            norm2 = len(words2) ** 0.5
            return intersection / (norm1 * norm2) if norm1 and norm2 else 0.0

        return 0.0

    def _levenshtein_distance(self, s1: str, s2: str) -> int:
        """Calculate Levenshtein distance."""
        if len(s1) < len(s2):
            return self._levenshtein_distance(s2, s1)
        
        if len(s2) == 0:
            return len(s1)

        previous_row = range(len(s2) + 1)
        for i, c1 in enumerate(s1):
            current_row = [i + 1]
            for j, c2 in enumerate(s2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]


class DeduplicateByHashAction(BaseAction):
    """Deduplicate based on content hash.
    
    Uses cryptographic hashing for exact and near-exact deduplication.
    """
    action_type = "deduplicate_hash"
    display_name = "哈希去重"
    description = "内容哈希去重"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Hash-based deduplication.
        
        Args:
            context: Execution context.
            params: Dict with keys: items, hash_algorithm,
                   include_fields, output_hash.
        
        Returns:
            ActionResult with deduplicated items and hashes.
        """
        items = params.get('items', [])
        hash_algorithm = params.get('hash_algorithm', 'md5')
        include_fields = params.get('include_fields', None)
        output_hash = params.get('output_hash', False)

        if not items:
            return ActionResult(success=False, message="items list is required")

        try:
            seen_hashes = set()
            result = []
            hash_map = {}

            for item in items:
                if include_fields and isinstance(item, dict):
                    hash_input = {k: item[k] for k in include_fields if k in item}
                else:
                    hash_input = item

                content = json.dumps(hash_input, sort_keys=True, default=str)
                
                if hash_algorithm == 'md5':
                    hash_value = hashlib.md5(content.encode('utf-8')).hexdigest()
                elif hash_algorithm == 'sha1':
                    hash_value = hashlib.sha1(content.encode('utf-8')).hexdigest()
                elif hash_algorithm == 'sha256':
                    hash_value = hashlib.sha256(content.encode('utf-8')).hexdigest()
                else:
                    hash_value = hashlib.md5(content.encode('utf-8')).hexdigest()

                if hash_value not in seen_hashes:
                    seen_hashes.add(hash_value)
                    result.append(item)
                    if output_hash:
                        hash_map[hash_value] = item

            return ActionResult(
                success=True,
                message=f"Removed {len(items) - len(result)} duplicates",
                data={
                    'items': result,
                    'original_count': len(items),
                    'deduplicated_count': len(result),
                    'duplicates_removed': len(items) - len(result),
                    'hashes': hash_map if output_hash else None,
                    'algorithm': hash_algorithm
                }
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Hash deduplication failed: {str(e)}")

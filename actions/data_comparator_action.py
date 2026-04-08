"""Data comparator action module for RabAI AutoClick.

Provides data comparison with field-level diff,
similarity scoring, change detection, and merge strategies.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
from deepdiff import DeepDiff
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataComparatorAction(BaseAction):
    """Compare data records and detect differences.
    
    Supports field-level diff, similarity scoring,
    change detection, and automatic merge strategies.
    """
    action_type = "data_comparator"
    display_name = "数据比较"
    description = "数据比较和差异检测"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute comparison operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (compare, find_changes,
                   similarity, merge), records/config.
        
        Returns:
            ActionResult with comparison results.
        """
        action = params.get('action', 'compare')
        
        if action == 'compare':
            return self._compare_records(params)
        elif action == 'find_changes':
            return self._find_changes(params)
        elif action == 'similarity':
            return self._calculate_similarity(params)
        elif action == 'merge':
            return self._merge_records(params)
        elif action == 'compare_batch':
            return self._compare_batch(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _compare_records(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compare two records and return differences."""
        record1 = params.get('record1', {})
        record2 = params.get('record2', {})
        
        if not isinstance(record1, dict) or not isinstance(record2, dict):
            return ActionResult(
                success=False,
                message="Both record1 and record2 must be dictionaries"
            )
        
        ignore_fields = params.get('ignore_fields', [])
        ignore_order = params.get('ignore_order', False)
        
        try:
            diff = DeepDiff(
                record1, record2,
                ignore_order=ignore_order,
                exclude_paths=[f"root['{f}']" for f in ignore_fields]
            )
            
            added = diff.get('dictionary_item_added', [])
            removed = diff.get('dictionary_item_removed', [])
            changed = diff.get('values_changed', {})
            type_changes = diff.get('type_changes', {})
            
            return ActionResult(
                success=True,
                message=f"Found {len(changed)} changes",
                data={
                    'identical': len(diff) == 0,
                    'added': [str(a) for a in added],
                    'removed': [str(r) for r in removed],
                    'changed': {k: {'old': v.get('old_value'), 'new': v.get('new_value')} 
                               for k, v in changed.items()},
                    'type_changes': {k: {'old_type': v.get('old_type'), 'new_type': v.get('new_type')}
                                    for k, v in type_changes.items()},
                    'diff': diff.to_dict()
                }
            )
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Comparison failed: {e}"
            )
    
    def _find_changes(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Find changes between old and new record versions."""
        old_record = params.get('old_record', {})
        new_record = params.get('new_record', {})
        
        if not isinstance(old_record, dict) or not isinstance(new_record, dict):
            return ActionResult(
                success=False,
                message="Both old_record and new_record must be dictionaries"
            )
        
        changes = []
        
        all_keys = set(old_record.keys()) | set(new_record.keys())
        
        for key in all_keys:
            old_val = old_record.get(key)
            new_val = new_record.get(key)
            
            if old_val != new_val:
                changes.append({
                    'field': key,
                    'old_value': old_val,
                    'new_value': new_val,
                    'change_type': 'modified'
                })
            elif key in new_record and key not in old_record:
                changes.append({
                    'field': key,
                    'old_value': None,
                    'new_value': new_val,
                    'change_type': 'added'
                })
            elif key in old_record and key not in new_record:
                changes.append({
                    'field': key,
                    'old_value': old_val,
                    'new_value': None,
                    'change_type': 'removed'
                })
        
        return ActionResult(
            success=True,
            message=f"Found {len(changes)} changes",
            data={
                'changes': changes,
                'change_count': len(changes)
            }
        )
    
    def _calculate_similarity(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Calculate similarity between two records."""
        record1 = params.get('record1', {})
        record2 = params.get('record2', {})
        
        if not isinstance(record1, dict) or not isinstance(record2, dict):
            return ActionResult(
                success=False,
                message="Both record1 and record2 must be dictionaries"
            )
        
        algorithm = params.get('algorithm', 'jaccard')
        fields = params.get('fields')
        
        if fields:
            r1 = {k: record1.get(k) for k in fields if k in record1}
            r2 = {k: record2.get(k) for k in fields if k in record2}
        else:
            r1 = record1
            r2 = record2
        
        if algorithm == 'jaccard':
            similarity = self._jaccard_similarity(r1, r2)
        elif algorithm == 'cosine':
            similarity = self._cosine_similarity(r1, r2)
        elif algorithm == 'exact':
            similarity = 1.0 if r1 == r2 else 0.0
        else:
            similarity = self._jaccard_similarity(r1, r2)
        
        return ActionResult(
            success=True,
            message=f"Similarity: {similarity:.2%}",
            data={
                'similarity': similarity,
                'algorithm': algorithm
            }
        )
    
    def _jaccard_similarity(
        self,
        dict1: Dict[str, Any],
        dict2: Dict[str, Any]
    ) -> float:
        """Calculate Jaccard similarity between two dictionaries."""
        keys1 = set(dict1.keys())
        keys2 = set(dict2.keys())
        
        if len(keys1 | keys2) == 0:
            return 1.0
        
        intersection = len(keys1 & keys2)
        union = len(keys1 | keys2)
        
        if union == 0:
            return 1.0
        
        key_similarity = intersection / union
        
        matching_values = sum(
            1 for k in keys1 & keys2 if dict1[k] == dict2[k]
        )
        value_similarity = matching_values / len(keys1 & keys2) if keys1 & keys2 else 0
        
        return (key_similarity + value_similarity) / 2
    
    def _cosine_similarity(
        self,
        dict1: Dict[str, Any],
        dict2: Dict[str, Any]
    ) -> float:
        """Calculate cosine similarity between two dictionaries."""
        all_keys = set(dict1.keys()) | set(dict2.keys())
        
        if not all_keys:
            return 1.0
        
        vec1 = [self._normalize_value(dict1.get(k)) for k in all_keys]
        vec2 = [self._normalize_value(dict2.get(k)) for k in all_keys]
        
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        magnitude1 = sum(a * a for a in vec1) ** 0.5
        magnitude2 = sum(b * b for b in vec2) ** 0.5
        
        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0
        
        return dot_product / (magnitude1 * magnitude2)
    
    def _normalize_value(self, value: Any) -> float:
        """Normalize a value to float for similarity calculation."""
        if value is None:
            return 0.0
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            return 1.0 if value else 0.0
        return 0.0
    
    def _merge_records(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Merge multiple records with conflict resolution."""
        records = params.get('records', [])
        if not records:
            return ActionResult(success=False, message="No records provided")
        
        strategy = params.get('strategy', 'last_wins')
        conflict_field = params.get('conflict_field', 'updated_at')
        
        if len(records) == 1:
            return ActionResult(
                success=True,
                message="Single record, returned as-is",
                data={'merged': records[0]}
            )
        
        merged = records[0].copy()
        
        for record in records[1:]:
            for key, value in record.items():
                if key not in merged:
                    merged[key] = value
                else:
                    if strategy == 'last_wins':
                        merged[key] = value
                    elif strategy == 'first_wins':
                        pass
                    elif strategy == 'conflict':
                        if merged[key] != value:
                            merged[key] = {'conflict': [merged[key], value]}
                    elif strategy == 'keep_max':
                        if isinstance(value, (int, float)) and isinstance(merged[key], (int, float)):
                            merged[key] = max(merged[key], value)
        
        return ActionResult(
            success=True,
            message=f"Merged {len(records)} records",
            data={
                'merged': merged,
                'strategy': strategy
            }
        )
    
    def _compare_batch(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Compare records in batch mode."""
        baseline = params.get('baseline', {})
        records = params.get('records', [])
        
        if not records:
            return ActionResult(success=False, message="No records provided")
        
        results = []
        identical_count = 0
        changed_count = 0
        
        for record in records:
            diff = DeepDiff(baseline, record, ignore_order=False)
            
            is_identical = len(diff) == 0
            
            if is_identical:
                identical_count += 1
            else:
                changed_count += 1
            
            results.append({
                'identical': is_identical,
                'changes': diff.to_dict() if diff else {}
            })
        
        return ActionResult(
            success=True,
            message=f"Compared {len(records)} records",
            data={
                'total': len(records),
                'identical': identical_count,
                'changed': changed_count,
                'results': results
            }
        )

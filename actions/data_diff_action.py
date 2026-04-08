"""Data Diff Action.

Compares two datasets and reports differences with detailed change tracking,
semantic versioning of changes, and patch generation.
"""

import sys
import os
from typing import Any, Dict, List, Optional, Set, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataDiffAction(BaseAction):
    """Compare datasets and report differences.
    
    Detects added, removed, and modified records with detailed
    change tracking and patch generation.
    """
    action_type = "data_diff"
    display_name = "数据对比"
    description = "对比两个数据集，报告差异，支持变更追踪和补丁生成"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Compare two datasets.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - data_a: First dataset (list of records).
                - data_b: Second dataset (list of records).
                - key_field: Field to use as unique key for comparison.
                - compare_fields: Fields to compare (default: all).
                - ignore_fields: Fields to ignore in comparison.
                - detect_moves: Detect records that moved position.
                - generate_patch: Generate patch for applying changes.
                - save_to_var: Variable name for result.
        
        Returns:
            ActionResult with diff results.
        """
        try:
            data_a = params.get('data_a') or context.get_variable(params.get('use_var_a', 'data_a'))
            data_b = params.get('data_b') or context.get_variable(params.get('use_var_b', 'data_b'))
            key_field = params.get('key_field')
            compare_fields = params.get('compare_fields')
            ignore_fields = set(params.get('ignore_fields', []))
            detect_moves = params.get('detect_moves', False)
            generate_patch = params.get('generate_patch', False)
            save_to_var = params.get('save_to_var', 'diff_result')

            if not data_a or not data_b:
                return ActionResult(success=False, message="Both data_a and data_b are required")

            if not isinstance(data_a, list) or not isinstance(data_b, list):
                return ActionResult(success=False, message="Both datasets must be lists")

            if not key_field:
                return ActionResult(success=False, message="key_field is required")

            # Determine fields to compare
            all_fields = set()
            for item in data_a + data_b:
                if isinstance(item, dict):
                    all_fields.update(item.keys())
            
            if compare_fields:
                compare_fields = set(compare_fields) - ignore_fields
            else:
                compare_fields = all_fields - ignore_fields - {key_field}

            # Build index for both datasets
            index_a = {item.get(key_field): item for item in data_a if isinstance(item, dict)}
            index_b = {item.get(key_field): item for isinstance(item, dict) for item in data_b}

            keys_a = set(index_a.keys())
            keys_b = set(index_b.keys())

            # Find differences
            added_keys = keys_b - keys_a
            removed_keys = keys_a - keys_b
            common_keys = keys_a & keys_b

            added = [index_b[k] for k in added_keys]
            removed = [index_a[k] for k in removed_keys]
            
            modified = []
            for key in common_keys:
                item_a = index_a[key]
                item_b = index_b[key]
                diff_fields = self._compare_items(item_a, item_b, compare_fields)
                if diff_fields:
                    modified.append({
                        'key': key,
                        'before': item_a,
                        'after': item_b,
                        'changes': diff_fields
                    })

            # Detect moves
            moves = []
            if detect_moves and not modified:
                moves = self._detect_moves(data_a, data_b, key_field, compare_fields)

            # Generate patch if requested
            patch = None
            if generate_patch:
                patch = self._generate_patch(added, removed, modified)

            result = {
                'summary': {
                    'total_a': len(data_a),
                    'total_b': len(data_b),
                    'added_count': len(added),
                    'removed_count': len(removed),
                    'modified_count': len(modified),
                    'moves_count': len(moves),
                    'unchanged_count': len(common_keys) - len(modified)
                },
                'added': added,
                'removed': removed,
                'modified': modified,
                'moves': moves,
                'patch': patch
            }

            context.set_variable(save_to_var, result)
            return ActionResult(success=True, data=result,
                             message=f"Diff: +{len(added)} -{len(removed)} ~{len(modified)}")

        except Exception as e:
            return ActionResult(success=False, message=f"Diff error: {e}")

    def _compare_items(self, item_a: Dict, item_b: Dict, 
                     fields: Set[str]) -> Dict[str, Dict]:
        """Compare two items and return changed fields."""
        changes = {}
        
        for field in fields:
            val_a = item_a.get(field)
            val_b = item_b.get(field)
            
            if val_a != val_b:
                changes[field] = {
                    'before': val_a,
                    'after': val_b,
                    'change_type': self._classify_change(val_a, val_b)
                }
        
        return changes

    def _classify_change(self, before: Any, after: Any) -> str:
        """Classify the type of change."""
        if before is None or before == '':
            return 'added'
        if after is None or after == '':
            return 'removed'
        if isinstance(before, (int, float)) and isinstance(after, (int, float)):
            if after > before:
                return 'increased'
            elif after < before:
                return 'decreased'
        return 'changed'

    def _detect_moves(self, data_a: List, data_b: List, 
                    key_field: str, compare_fields: Set[str]) -> List[Dict]:
        """Detect records that changed significantly (potential moves)."""
        # Build signature index for data_a
        sig_index = {}
        for item in data_a:
            if isinstance(item, dict):
                key = item.get(key_field)
                sig = self._make_signature(item, compare_fields)
                sig_index[key] = sig

        moves = []
        for item in data_b:
            if isinstance(item, dict):
                key = item.get(key_field)
                if key not in sig_index:
                    sig = self._make_signature(item, compare_fields)
                    for old_key, old_sig in sig_index.items():
                        if self._signature_similarity(sig, old_sig) > 0.7:
                            moves.append({
                                'from': old_key,
                                'to': key,
                                'similarity': self._signature_similarity(sig, old_sig)
                            })
                            break

        return moves

    def _make_signature(self, item: Dict, fields: Set[str]) -> Set[str]:
        """Make a signature set for an item."""
        sig = set()
        for field in fields:
            val = item.get(field)
            if val is not None:
                sig.add(f"{field}={val}")
        return sig

    def _signature_similarity(self, sig1: Set[str], sig2: Set[str]) -> float:
        """Calculate similarity between two signatures."""
        if not sig1 or not sig2:
            return 0.0
        intersection = len(sig1 & sig2)
        union = len(sig1 | sig2)
        return intersection / union if union > 0 else 0.0

    def _generate_patch(self, added: List, removed: List, 
                       modified: List) -> Dict[str, Any]:
        """Generate a patch that can be applied to transform data_a to data_b."""
        patch = {
            'version': '1.0',
            'operations': []
        }

        for item in removed:
            patch['operations'].append({
                'op': 'remove',
                'key': item.get(params.get('key_field', 'id')),
                'data': item
            })

        for item in added:
            patch['operations'].append({
                'op': 'add',
                'data': item
            })

        for change in modified:
            patch['operations'].append({
                'op': 'modify',
                'key': change['key'],
                'before': change['before'],
                'after': change['after'],
                'changes': change['changes']
            })

        return patch

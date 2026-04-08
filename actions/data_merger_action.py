"""Data merger action module for RabAI AutoClick.

Provides data merging with multiple strategies,
conflict resolution, and join operations.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DataMergerAction(BaseAction):
    """Merge data from multiple sources with conflict resolution.
    
    Supports left/right/inner/outer joins, union, concatenation,
    and custom merge strategies.
    """
    action_type = "data_merger"
    display_name = "数据合并"
    description = "多数据源合并，支持各种连接策略"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute merge operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: operation (join, union, concat,
                   lookup_merge), config.
        
        Returns:
            ActionResult with merged data.
        """
        operation = params.get('operation', 'join')
        
        if operation == 'join':
            return self._join_records(params)
        elif operation == 'union':
            return self._union_records(params)
        elif operation == 'concat':
            return self._concat_records(params)
        elif operation == 'lookup_merge':
            return self._lookup_merge(params)
        elif operation == 'hierarchical_merge':
            return self._hierarchical_merge(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _join_records(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Join two record sets."""
        left_records = params.get('left_records', [])
        right_records = params.get('right_records', [])
        
        if not left_records or not right_records:
            return ActionResult(success=False, message="Both left and right records required")
        
        join_key = params.get('join_key', 'id')
        join_type = params.get('join_type', 'inner')
        suffix_left = params.get('suffix_left', '_left')
        suffix_right = params.get('suffix_right', '_right')
        
        right_index = {rec.get(join_key): rec for rec in right_records if join_key in rec}
        
        joined = []
        
        for left_rec in left_records:
            key = left_rec.get(join_key)
            right_rec = right_index.get(key)
            
            if right_rec:
                merged = {}
                for k, v in left_rec.items():
                    merged[k + suffix_left if k in right_rec else k] = v
                for k, v in right_rec.items():
                    if k not in left_rec:
                        merged[k] = v
                    else:
                        merged[k + suffix_right] = v
                joined.append(merged)
            elif join_type in ('left', 'outer'):
                merged = left_rec.copy()
                for k in right_rec.keys() if right_rec else []:
                    merged[k + suffix_right if k in left_rec else k] = None
                joined.append(merged)
        
        if join_type == 'outer':
            for right_rec in right_records:
                key = right_rec.get(join_key)
                if not any(rec.get(join_key + suffix_left if join_key in left_records[0] else join_key) == key 
                          for rec in joined):
                    merged = {}
                    for k in left_records[0].keys():
                        merged[k + suffix_left] = None
                    for k, v in right_rec.items():
                        merged[k + suffix_right if k in left_records[0] else k] = v
                    joined.append(merged)
        
        return ActionResult(
            success=True,
            message=f"Joined {len(joined)} records ({join_type} join)",
            data={
                'joined': joined,
                'count': len(joined)
            }
        )
    
    def _union_records(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Union multiple record sets."""
        record_sets = params.get('record_sets', [])
        if not record_sets:
            return ActionResult(success=False, message="No record sets provided")
        
        if not isinstance(record_sets, list) or len(record_sets) < 2:
            return ActionResult(success=False, message="At least 2 record sets required")
        
        dedupe = params.get('dedupe', True)
        dedupe_key = params.get('dedupe_key')
        
        all_records = []
        for records in record_sets:
            if isinstance(records, list):
                all_records.extend(records)
        
        if dedupe:
            if dedupe_key:
                seen = set()
                unique = []
                for record in all_records:
                    key = record.get(dedupe_key)
                    if key not in seen:
                        seen.add(key)
                        unique.append(record)
                all_records = unique
            else:
                seen = set()
                unique = []
                for record in all_records:
                    try:
                        key = json.dumps(record, sort_keys=True, default=str)
                        if key not in seen:
                            seen.add(key)
                            unique.append(record)
                    except Exception:
                        unique.append(record)
                all_records = unique
        
        return ActionResult(
            success=True,
            message=f"Union of {len(all_records)} records",
            data={
                'records': all_records,
                'count': len(all_records)
            }
        )
    
    def _concat_records(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Concatenate records horizontally (side by side)."""
        record_sets = params.get('record_sets', [])
        if not record_sets or len(record_sets) < 2:
            return ActionResult(success=False, message="At least 2 record sets required")
        
        prefix = params.get('prefix', 'set')
        fillna = params.get('fillna', None)
        
        max_len = max(len(records) for records in record_sets)
        
        result = []
        
        for i in range(max_len):
            combined = {}
            for set_idx, records in enumerate(record_sets):
                set_prefix = f"{prefix}_{set_idx}_" if prefix else ""
                if i < len(records):
                    record = records[i]
                    if isinstance(record, dict):
                        for k, v in record.items():
                            combined[set_prefix + k] = v
                    else:
                        combined[set_prefix + f"value"] = record
                elif fillna is not None:
                    if i < len(records):
                        record = records[i]
                        if isinstance(record, dict):
                            for k in record.keys():
                                combined[set_prefix + k] = fillna
            result.append(combined)
        
        return ActionResult(
            success=True,
            message=f"Concatenated {len(result)} records",
            data={
                'records': result,
                'count': len(result)
            }
        )
    
    def _lookup_merge(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Merge records using lookup tables."""
        records = params.get('records', [])
        if not records:
            return ActionResult(success=False, message="No records provided")
        
        lookups = params.get('lookups', {})
        if not lookups:
            return ActionResult(success=False, message="No lookups provided")
        
        for lookup_name, lookup_config in lookups.items():
            lookup_key = lookup_config.get('key')
            lookup_data = lookup_config.get('data', {})
            target_field = lookup_config.get('target_field', lookup_name)
            default = lookup_config.get('default')
            
            if not lookup_key or not lookup_data:
                continue
            
            for record in records:
                key_value = record.get(lookup_key)
                record[target_field] = lookup_data.get(key_value, default)
        
        return ActionResult(
            success=True,
            message=f"Lookup merge completed for {len(records)} records",
            data={
                'records': records,
                'count': len(records)
            }
        )
    
    def _hierarchical_merge(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Merge records with hierarchical (nested) structure."""
        records = params.get('records', [])
        if not records:
            return ActionResult(success=False, message="No records provided")
        
        merge_strategy = params.get('merge_strategy', 'deep')
        conflict_resolution = params.get('conflict_resolution', 'last_wins')
        
        if len(records) == 1:
            return ActionResult(
                success=True,
                message="Single record, returned as-is",
                data={'merged': records[0]}
            )
        
        result = records[0].copy() if isinstance(records[0], dict) else {}
        
        for record in records[1:]:
            if not isinstance(record, dict):
                continue
            result = self._deep_merge(result, record, conflict_resolution)
        
        return ActionResult(
            success=True,
            message=f"Hierarchical merge of {len(records)} records",
            data={
                'merged': result
            }
        )
    
    def _deep_merge(
        self,
        base: Dict[str, Any],
        overlay: Dict[str, Any],
        conflict_resolution: str
    ) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = base.copy()
        
        for key, value in overlay.items():
            if key in result:
                if isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = self._deep_merge(result[key], value, conflict_resolution)
                else:
                    if conflict_resolution == 'first_wins':
                        pass
                    elif conflict_resolution == 'last_wins':
                        result[key] = value
                    elif conflict_resolution == 'keep_list':
                        if isinstance(result[key], list) and isinstance(value, list):
                            result[key] = result[key] + value
                        else:
                            result[key] = [result[key], value]
                    else:
                        result[key] = value
            else:
                result[key] = value
        
        return result

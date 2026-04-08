"""
Data Join Action Module.

Joins data from multiple sources including SQL-like joins,
lookup joins, and fuzzy joins with conflict resolution.

Author: RabAi Team
"""

from __future__ import annotations

import sys
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class JoinType(Enum):
    """Types of data joins."""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"
    LOOKUP = "lookup"
    FUZZY = "fuzzy"


class ConflictResolution(Enum):
    """Conflict resolution strategies."""
    LEFT_WINS = "left_wins"
    RIGHT_WINS = "right_wins"
    MERGE = "merge"
    CONFLICT_ERROR = "conflict_error"


@dataclass
class JoinConfig:
    """Configuration for join operations."""
    join_type: JoinType = JoinType.INNER
    left_key: str = ""
    right_key: str = ""
    left_prefix: str = ""
    right_prefix: str = "_right"
    conflict_resolution: ConflictResolution = ConflictResolution.LEFT_WINS
    fuzzy_threshold: float = 0.8
    case_sensitive: bool = True


@dataclass
class JoinResult:
    """Result of a join operation."""
    matched: int
    unmatched_left: int
    unmatched_right: int
    output: List[Dict[str, Any]]


class DataJoinAction(BaseAction):
    """Data join action.
    
    Joins data from multiple sources using various join strategies
    with configurable key matching and conflict resolution.
    """
    action_type = "data_join"
    display_name = "数据关联"
    description = "多数据源关联合并"
    
    def __init__(self):
        super().__init__()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Perform data join operation.
        
        Args:
            context: The execution context.
            params: Dictionary containing:
                - operation: join/lookup/batch_join
                - left_data: Left dataset
                - right_data: Right dataset
                - join_type: Type of join (inner/left/right/full/cross/lookup/fuzzy)
                - left_key: Key field from left dataset
                - right_key: Key field from right dataset
                - left_prefix: Prefix for left field names
                - right_prefix: Prefix for right field names
                - conflict_resolution: How to handle conflicts
                - fuzzy_threshold: Threshold for fuzzy matching
                
        Returns:
            ActionResult with joined data.
        """
        start_time = time.time()
        
        operation = params.get("operation", "join")
        
        try:
            if operation == "join":
                result = self._join(params, start_time)
            elif operation == "lookup":
                result = self._lookup(params, start_time)
            elif operation == "batch_join":
                result = self._batch_join(params, start_time)
            elif operation == "concatenate":
                result = self._concatenate(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
            
            return result
            
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Join failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _join(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Perform a join operation."""
        left_data = params.get("left_data", [])
        right_data = params.get("right_data", [])
        join_type_str = params.get("join_type", "inner")
        left_key = params.get("left_key", "")
        right_key = params.get("right_key", left_key)
        left_prefix = params.get("left_prefix", "")
        right_prefix = params.get("right_prefix", "_right")
        conflict_str = params.get("conflict_resolution", "left_wins")
        fuzzy_threshold = params.get("fuzzy_threshold", 0.8)
        
        try:
            join_type = JoinType(join_type_str)
        except ValueError:
            join_type = JoinType.INNER
        
        try:
            conflict = ConflictResolution(conflict_str)
        except ValueError:
            conflict = ConflictResolution.LEFT_WINS
        
        if not left_data or not right_data:
            return ActionResult(
                success=False,
                message="Both left and right data required",
                duration=time.time() - start_time
            )
        
        if join_type == JoinType.CROSS:
            return self._cross_join(left_data, right_data, left_prefix, right_prefix, start_time)
        
        if join_type == JoinType.FUZZY:
            return self._fuzzy_join(left_data, right_data, left_key, right_key, fuzzy_threshold,
                                   left_prefix, right_prefix, start_time)
        
        if not left_key or not right_key:
            return ActionResult(
                success=False,
                message="Join keys required for non-cross joins",
                duration=time.time() - start_time
            )
        
        right_index = self._build_index(right_data, right_key)
        
        matched_count = 0
        unmatched_left_count = 0
        unmatched_right_keys = set(range(len(right_data)))
        output = []
        
        for left_record in left_data:
            left_val = left_record.get(left_key)
            if left_val is None:
                continue
            
            right_record = right_index.get(left_val if isinstance(left_val, str) else str(left_val))
            
            if right_record is not None:
                unmatched_right_keys.discard(right_record["_idx"])
                joined = self._merge_records(
                    left_record, right_record["record"], left_key, right_key,
                    left_prefix, right_prefix, conflict
                )
                output.append(joined)
                matched_count += 1
            elif join_type in (JoinType.LEFT, JoinType.FULL):
                left_only = {f: v for f, v in left_record.items()}
                for k in self._get_right_fields(right_data[0], right_key):
                    left_only[f"{right_prefix}{k}"] = None
                output.append(left_only)
                unmatched_left_count += 1
            else:
                unmatched_left_count += 1
        
        if join_type in (JoinType.RIGHT, JoinType.FULL):
            for idx in unmatched_right_keys:
                right_record = right_data[idx]
                left_vals = {f: None for f in left_data[0].keys() if f != left_key} if left_data else {}
                if left_prefix and left_vals:
                    left_vals = {f"{left_prefix}{k}": v for k, v in left_vals.items()}
                joined = self._merge_records(
                    left_vals if isinstance(left_vals, dict) else {left_key: None},
                    right_record, left_key, right_key,
                    left_prefix, right_prefix, conflict
                )
                output.append(joined)
        
        return ActionResult(
            success=True,
            message=f"Join complete: {matched_count} matched",
            data={
                "matched": matched_count,
                "unmatched_left": unmatched_left_count,
                "unmatched_right": len(unmatched_right_keys),
                "output": output,
                "join_type": join_type.value
            },
            duration=time.time() - start_time
        )
    
    def _cross_join(
        self, left_data: List, right_data: List,
        left_prefix: str, right_prefix: str, start_time: float
    ) -> ActionResult:
        """Perform cross join (Cartesian product)."""
        output = []
        
        for left_record in left_data:
            for right_record in right_data:
                merged = {}
                for k, v in left_record.items():
                    merged[f"{left_prefix}{k}" if left_prefix else k] = v
                for k, v in right_record.items():
                    merged[f"{right_prefix}{k}" if right_prefix else k] = v
                output.append(merged)
        
        return ActionResult(
            success=True,
            message=f"Cross join complete: {len(output)} rows",
            data={
                "matched": len(output),
                "output": output,
                "join_type": "cross"
            },
            duration=time.time() - start_time
        )
    
    def _fuzzy_join(
        self, left_data: List, right_data: List, left_key: str, right_key: str,
        threshold: float, left_prefix: str, right_prefix: str, start_time: float
    ) -> ActionResult:
        """Perform fuzzy join using string similarity."""
        from difflib import SequenceMatcher
        
        output = []
        matched_count = 0
        
        right_vals = [(i, str(r.get(right_key, ""))) for i, r in enumerate(right_data)]
        
        for left_record in left_data:
            left_val = str(left_record.get(left_key, ""))
            best_score = 0
            best_idx = -1
            
            for idx, right_val in right_vals:
                score = SequenceMatcher(None, left_val.lower(), right_val.lower()).ratio()
                if score > best_score:
                    best_score = score
                    best_idx = idx
            
            if best_score >= threshold and best_idx >= 0:
                joined = self._merge_records(
                    left_record, right_data[best_idx],
                    left_key, right_key, left_prefix, right_prefix,
                    ConflictResolution.LEFT_WINS
                )
                output.append(joined)
                matched_count += 1
        
        return ActionResult(
            success=True,
            message=f"Fuzzy join complete: {matched_count} matched (threshold={threshold})",
            data={
                "matched": matched_count,
                "output": output,
                "join_type": "fuzzy"
            },
            duration=time.time() - start_time
        )
    
    def _lookup(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Perform a lookup (single record join)."""
        data = params.get("data", [])
        lookup_table = params.get("lookup_table", [])
        key_field = params.get("key_field", "")
        lookup_key = params.get("lookup_key", key_field)
        
        if not data or not lookup_table or not key_field:
            return ActionResult(
                success=False,
                message="Missing required parameters for lookup",
                duration=time.time() - start_time
            )
        
        lookup_index = self._build_index(lookup_table, lookup_key)
        
        output = []
        found_count = 0
        
        for record in data:
            key = record.get(key_field)
            if key is not None:
                key_str = key if isinstance(key, str) else str(key)
                if key_str in lookup_index:
                    merged = dict(record)
                    merged["_lookup"] = lookup_index[key_str]["record"]
                    output.append(merged)
                    found_count += 1
                else:
                    output.append(record)
            else:
                output.append(record)
        
        return ActionResult(
            success=True,
            message=f"Lookup complete: {found_count}/{len(data)} found",
            data={
                "found": found_count,
                "not_found": len(data) - found_count,
                "output": output
            },
            duration=time.time() - start_time
        )
    
    def _batch_join(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Join multiple datasets at once."""
        datasets = params.get("datasets", [])
        primary_key = params.get("primary_key", "")
        join_type_str = params.get("join_type", "inner")
        
        if len(datasets) < 2:
            return ActionResult(
                success=False,
                message="Need at least 2 datasets for batch join",
                duration=time.time() - start_time
            )
        
        result = datasets[0]
        
        for i, dataset in enumerate(datasets[1:], 1):
            join_params = {
                "left_data": result,
                "right_data": dataset,
                "join_type": join_type_str,
                "left_key": primary_key,
                "right_key": primary_key,
                "right_prefix": f"_ds{i}"
            }
            join_result = self._join(join_params, start_time)
            if join_result.success:
                result = join_result.data["output"]
        
        return ActionResult(
            success=True,
            message=f"Batch join complete: {len(result)} rows",
            data={"output": result, "count": len(result), "datasets": len(datasets)},
            duration=time.time() - start_time
        )
    
    def _concatenate(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Concatenate multiple datasets (union)."""
        datasets = params.get("datasets", [])
        deduplicate = params.get("deduplicate", False)
        key_field = params.get("key_field")
        
        if not datasets:
            return ActionResult(
                success=False,
                message="No datasets provided",
                duration=time.time() - start_time
            )
        
        output = []
        for dataset in datasets:
            output.extend(dataset)
        
        original_count = len(output)
        
        if deduplicate and key_field:
            seen = set()
            unique_output = []
            for record in output:
                key = record.get(key_field)
                if key not in seen:
                    seen.add(key)
                    unique_output.append(record)
            output = unique_output
        
        return ActionResult(
            success=True,
            message=f"Concatenated {len(datasets)} datasets: {len(output)} rows",
            data={
                "output": output,
                "count": len(output),
                "original_count": original_count,
                "deduplicated": original_count - len(output) if deduplicate else 0
            },
            duration=time.time() - start_time
        )
    
    def _build_index(self, data: List[Dict], key_field: str) -> Dict[str, Dict]:
        """Build an index on key field for efficient lookup."""
        index = {}
        for i, record in enumerate(data):
            key = record.get(key_field)
            if key is not None:
                key_str = key if isinstance(key, str) else str(key)
                index[key_str] = {"record": record, "_idx": i}
        return index
    
    def _get_right_fields(self, record: Dict, exclude_key: str) -> List[str]:
        """Get field names excluding key."""
        return [k for k in record.keys() if k != exclude_key]
    
    def _merge_records(
        self, left: Dict, right: Dict, left_key: str, right_key: str,
        left_prefix: str, right_prefix: str, conflict: ConflictResolution
    ) -> Dict[str, Any]:
        """Merge two records with conflict resolution."""
        merged = {}
        
        for k, v in left.items():
            if k == left_key:
                merged[k] = v
            else:
                merged[f"{left_prefix}{k}" if left_prefix else k] = v
        
        for k, v in right.items():
            if k == right_key:
                continue
            
            field_name = f"{right_prefix}{k}" if right_prefix else k
            prefixed_left_key = f"{left_prefix}{k}" if left_prefix else k
            
            if field_name in merged:
                if conflict == ConflictResolution.LEFT_WINS:
                    pass
                elif conflict == ConflictResolution.RIGHT_WINS:
                    merged[field_name] = v
                elif conflict == ConflictResolution.MERGE:
                    if merged[field_name] != v:
                        merged[field_name] = [merged[field_name], v]
            else:
                merged[field_name] = v
        
        return merged
    
    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, str]:
        """Validate join parameters."""
        return True, ""
    
    def get_required_params(self) -> List[str]:
        """Return required parameters."""
        return []

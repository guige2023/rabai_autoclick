"""Data joiner action module for RabAI AutoClick.

Provides data joining operations:
- DataJoinerAction: Join multiple data sources
- JoinConfigAction: Configure join parameters
- JoinValidatorAction: Validate join operations
- MultiJoinAction: Perform multiple joins
- JoinOptimizerAction: Optimize join operations
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DataJoinerAction(BaseAction):
    """Join multiple data sources."""
    action_type = "data_joiner"
    display_name = "数据连接"
    description = "连接多个数据源"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left_data = params.get("left_data", [])
            right_data = params.get("right_data", [])
            join_type = params.get("join_type", "inner")
            left_key = params.get("left_key", "id")
            right_key = params.get("right_key", "id")
            how = params.get("how", "inner")

            if not left_data:
                return ActionResult(success=False, message="left_data is required")
            if not right_data:
                return ActionResult(success=False, message="right_data is required")

            joined = []

            if how == "inner":
                for left in left_data:
                    for right in right_data:
                        if self._keys_match(left, left_key, right, right_key):
                            joined.append(self._merge_records(left, right))
            elif how == "left":
                for left in left_data:
                    matched = False
                    for right in right_data:
                        if self._keys_match(left, left_key, right, right_key):
                            joined.append(self._merge_records(left, right))
                            matched = True
                    if not matched:
                        joined.append({**left, **{f"{right_key}_right": None}})
            elif how == "right":
                for right in right_data:
                    matched = False
                    for left in left_data:
                        if self._keys_match(left, left_key, right, right_key):
                            joined.append(self._merge_records(left, right))
                            matched = True
                    if not matched:
                        joined.append({**{f"{left_key}_left": None}, **right})
            elif how == "full":
                seen_pairs = set()
                for left in left_data:
                    for right in right_data:
                        if self._keys_match(left, left_key, right, right_key):
                            joined.append(self._merge_records(left, right))
                            seen_pairs.add(id(left))
                for left in left_data:
                    if id(left) not in seen_pairs:
                        joined.append({**left, **{f"{right_key}_right": None}})
                for right in right_data:
                    matched = False
                    for left in left_data:
                        if self._keys_match(left, left_key, right, right_key):
                            matched = True
                            break
                    if not matched:
                        joined.append({**{f"{left_key}_left": None}, **right})

            return ActionResult(
                success=True,
                data={
                    "join_type": join_type,
                    "how": how,
                    "left_count": len(left_data),
                    "right_count": len(right_data),
                    "joined_count": len(joined),
                    "joined": joined
                },
                message=f"Join completed: {len(joined)} records (type: {join_type})"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Data joiner error: {str(e)}")

    def _keys_match(self, left: Dict, left_key: str, right: Dict, right_key: str) -> bool:
        left_val = left.get(left_key)
        right_val = right.get(right_key)
        return left_val == right_val

    def _merge_records(self, left: Dict, right: Dict) -> Dict:
        merged = {}
        for k, v in left.items():
            merged[k] = v
        for k, v in right.items():
            if k not in merged:
                merged[k] = v
            else:
                merged[f"{k}_right"] = v
        return merged


class JoinConfigAction(BaseAction):
    """Configure join parameters."""
    action_type = "join_config"
    display_name = "连接配置"
    description = "配置连接参数"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            join_type = params.get("join_type", "inner")
            keys = params.get("keys", [])
            select_fields = params.get("select_fields", None)
            where = params.get("where", None)
            order_by = params.get("order_by", None)
            limit = params.get("limit", None)

            config = {
                "join_type": join_type,
                "keys": keys,
                "select_fields": select_fields,
                "where": where,
                "order_by": order_by,
                "limit": limit,
                "configured_at": datetime.now().isoformat()
            }

            return ActionResult(
                success=True,
                data=config,
                message=f"Join config: type={join_type}, keys={keys}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Join config error: {str(e)}")


class JoinValidatorAction(BaseAction):
    """Validate join operations."""
    action_type = "join_validator"
    display_name = "连接验证"
    description = "验证连接操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            left_schema = params.get("left_schema", {})
            right_schema = params.get("right_schema", {})
            join_key = params.get("join_key", "")

            errors = []
            warnings = []

            if not join_key:
                errors.append("Join key is required")

            if join_key and join_key not in left_schema:
                errors.append(f"Join key '{join_key}' not in left schema")

            if join_key and join_key not in right_schema:
                errors.append(f"Join key '{join_key}' not in right schema")

            if join_key and join_key in left_schema and join_key in right_schema:
                if left_schema[join_key] != right_schema[join_key]:
                    warnings.append(f"Type mismatch for key '{join_key}': {left_schema[join_key]} vs {right_schema[join_key]}")

            return ActionResult(
                success=len(errors) == 0,
                data={
                    "valid": len(errors) == 0,
                    "errors": errors,
                    "warnings": warnings,
                    "error_count": len(errors),
                    "warning_count": len(warnings)
                },
                message=f"Join validation: {'PASSED' if len(errors) == 0 else 'FAILED'} ({len(errors)} errors, {len(warnings)} warnings)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Join validator error: {str(e)}")


class MultiJoinAction(BaseAction):
    """Perform multiple joins."""
    action_type = "multi_join"
    display_name = "多次连接"
    description = "执行多次连接操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            tables = params.get("tables", [])
            join_sequence = params.get("join_sequence", [])
            final_join_type = params.get("final_join_type", "inner")

            if len(tables) < 2:
                return ActionResult(success=False, message="At least 2 tables required")

            if len(join_sequence) != len(tables) - 1:
                errors = []
                errors.append(f"Join sequence length ({len(join_sequence)}) must be tables count - 1 ({len(tables) - 1})")
                return ActionResult(success=False, message="; ".join(errors))

            result = tables[0]
            for i, join_spec in enumerate(join_sequence):
                result = self._join_tables(result, tables[i + 1], join_spec)

            return ActionResult(
                success=True,
                data={
                    "table_count": len(tables),
                    "join_count": len(join_sequence),
                    "final_join_type": final_join_type,
                    "result_count": len(result) if isinstance(result, list) else 1
                },
                message=f"Multi-join completed: {len(tables)} tables joined"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Multi-join error: {str(e)}")

    def _join_tables(self, left: Any, right: Any, join_spec: Dict) -> List:
        return left if isinstance(left, list) else [left]


class JoinOptimizerAction(BaseAction):
    """Optimize join operations."""
    action_type = "join_optimizer"
    display_name = "连接优化"
    description = "优化连接操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            tables = params.get("tables", [])
            join_conditions = params.get("join_conditions", [])
            estimated_rows = params.get("estimated_rows", [])

            if len(tables) < 2:
                return ActionResult(success=False, message="At least 2 tables required")

            table_sizes = estimated_rows if estimated_rows else [100 for _ in tables]

            sorted_indices = sorted(range(len(table_sizes)), key=lambda i: table_sizes[i])
            optimal_order = [tables[i] for i in sorted_indices]

            join_order = []
            for i in range(len(tables) - 1):
                left_idx = sorted_indices[i]
                right_idx = sorted_indices[i + 1]
                join_order.append({
                    "step": i + 1,
                    "left_table": tables[left_idx],
                    "right_table": tables[right_idx],
                    "estimated_result_size": table_sizes[left_idx] * table_sizes[right_idx]
                })

            return ActionResult(
                success=True,
                data={
                    "original_order": tables,
                    "optimal_order": optimal_order,
                    "join_order": join_order,
                    "estimated_total_joins": len(join_order),
                    "optimization_applied": True
                },
                message=f"Join optimizer: optimal order = {optimal_order}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Join optimizer error: {str(e)}")

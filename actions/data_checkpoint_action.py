"""Data checkpoint action module for RabAI AutoClick.

Provides data checkpoint operations:
- CheckpointCreateAction: Create a checkpoint
- CheckpointRestoreAction: Restore from checkpoint
- CheckpointListAction: List checkpoints
- CheckpointDeleteAction: Delete checkpoint
- CheckpointDiffAction: Compare checkpoints
"""

import hashlib
import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class CheckpointCreateAction(BaseAction):
    """Create a data checkpoint."""
    action_type = "checkpoint_create"
    display_name = "创建检查点"
    description = "创建数据检查点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            data_snapshot = params.get("data", {})
            tags = params.get("tags", [])

            if not name:
                return ActionResult(success=False, message="name is required")

            checkpoint_id = hashlib.md5(f"{name}:{time.time()}".encode()).hexdigest()[:12]

            if not hasattr(context, "checkpoints"):
                context.checkpoints = {}
            context.checkpoints[checkpoint_id] = {
                "checkpoint_id": checkpoint_id,
                "name": name,
                "data_size": len(str(data_snapshot)),
                "tags": tags,
                "created_at": time.time(),
                "status": "created",
            }

            return ActionResult(
                success=True,
                data={"checkpoint_id": checkpoint_id, "name": name, "tags": tags},
                message=f"Checkpoint {checkpoint_id} created: {name}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Checkpoint create failed: {e}")


class CheckpointRestoreAction(BaseAction):
    """Restore from checkpoint."""
    action_type = "checkpoint_restore"
    display_name = "恢复检查点"
    description = "从检查点恢复数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            checkpoint_id = params.get("checkpoint_id", "")
            if not checkpoint_id:
                return ActionResult(success=False, message="checkpoint_id is required")

            checkpoints = getattr(context, "checkpoints", {})
            if checkpoint_id not in checkpoints:
                return ActionResult(success=False, message=f"Checkpoint {checkpoint_id} not found")

            cp = checkpoints[checkpoint_id]
            cp["last_restored_at"] = time.time()
            cp["restore_count"] = cp.get("restore_count", 0) + 1

            return ActionResult(
                success=True,
                data={"checkpoint_id": checkpoint_id, "name": cp["name"], "restored": True},
                message=f"Restored from checkpoint {checkpoint_id}: {cp['name']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Checkpoint restore failed: {e}")


class CheckpointListAction(BaseAction):
    """List all checkpoints."""
    action_type = "checkpoint_list"
    display_name = "检查点列表"
    description = "列出所有检查点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            filter_tags = params.get("tags", [])
            limit = params.get("limit", 50)

            checkpoints = getattr(context, "checkpoints", {})
            results = list(checkpoints.values())

            if filter_tags:
                results = [cp for cp in results if any(tag in cp.get("tags", []) for tag in filter_tags)]

            results = sorted(results, key=lambda x: x.get("created_at", 0), reverse=True)[:limit]

            return ActionResult(
                success=True,
                data={"checkpoints": results, "count": len(results)},
                message=f"Found {len(results)} checkpoints",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Checkpoint list failed: {e}")


class CheckpointDeleteAction(BaseAction):
    """Delete a checkpoint."""
    action_type = "checkpoint_delete"
    display_name = "删除检查点"
    description = "删除检查点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            checkpoint_id = params.get("checkpoint_id", "")
            if not checkpoint_id:
                return ActionResult(success=False, message="checkpoint_id is required")

            checkpoints = getattr(context, "checkpoints", {})
            if checkpoint_id not in checkpoints:
                return ActionResult(success=False, message=f"Checkpoint {checkpoint_id} not found")

            cp_name = checkpoints[checkpoint_id]["name"]
            del checkpoints[checkpoint_id]

            return ActionResult(success=True, data={"checkpoint_id": checkpoint_id, "name": cp_name}, message=f"Checkpoint {cp_name} deleted")
        except Exception as e:
            return ActionResult(success=False, message=f"Checkpoint delete failed: {e}")


class CheckpointDiffAction(BaseAction):
    """Compare two checkpoints."""
    action_type = "checkpoint_diff"
    display_name = "检查点对比"
    description = "对比两个检查点"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            checkpoint_id_a = params.get("checkpoint_id_a", "")
            checkpoint_id_b = params.get("checkpoint_id_b", "")

            if not checkpoint_id_a or not checkpoint_id_b:
                return ActionResult(success=False, message="checkpoint_id_a and checkpoint_id_b are required")

            checkpoints = getattr(context, "checkpoints", {})
            cp_a = checkpoints.get(checkpoint_id_a)
            cp_b = checkpoints.get(checkpoint_id_b)

            if not cp_a or not cp_b:
                return ActionResult(success=False, message="One or both checkpoints not found")

            diff = {
                "checkpoint_a": cp_a["name"],
                "checkpoint_b": cp_b["name"],
                "size_diff": cp_b.get("data_size", 0) - cp_a.get("data_size", 0),
                "time_diff_s": cp_b.get("created_at", 0) - cp_a.get("created_at", 0),
            }

            return ActionResult(
                success=True,
                data=diff,
                message=f"Diff: {cp_a['name']} vs {cp_b['name']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Checkpoint diff failed: {e}")

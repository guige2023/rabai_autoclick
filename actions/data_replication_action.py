"""Data replication action module for RabAI AutoClick.

Provides data replication operations:
- ReplicationSetupAction: Setup replication
- ReplicationSyncAction: Synchronize data
- ReplicationStatusAction: Check replication status
- ReplicationPauseAction: Pause replication
- ReplicationResumeAction: Resume replication
- ReplicationDeleteAction: Remove replication
"""

import time
import uuid
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ReplicationSetupAction(BaseAction):
    """Setup data replication."""
    action_type = "replication_setup"
    display_name = "配置复制"
    description = "配置数据复制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            source = params.get("source", "")
            target = params.get("target", "")
            mode = params.get("mode", "async")
            conflict_resolution = params.get("conflict_resolution", "source_wins")

            if not source or not target:
                return ActionResult(success=False, message="source and target are required")

            replication_id = str(uuid.uuid4())[:8]

            if not hasattr(context, "replications"):
                context.replications = {}
            context.replications[replication_id] = {
                "replication_id": replication_id,
                "source": source,
                "target": target,
                "mode": mode,
                "conflict_resolution": conflict_resolution,
                "status": "active",
                "created_at": time.time(),
                "last_sync": None,
                "bytes_synced": 0,
            }

            return ActionResult(
                success=True,
                data={"replication_id": replication_id, "source": source, "target": target, "mode": mode},
                message=f"Replication {replication_id}: {source} -> {target}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Replication setup failed: {e}")


class ReplicationSyncAction(BaseAction):
    """Synchronize replicated data."""
    action_type = "replication_sync"
    display_name = "同步复制"
    description = "同步复制数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            replication_id = params.get("replication_id", "")
            full_sync = params.get("full_sync", False)

            if not replication_id:
                return ActionResult(success=False, message="replication_id is required")

            replications = getattr(context, "replications", {})
            if replication_id not in replications:
                return ActionResult(success=False, message=f"Replication {replication_id} not found")

            rep = replications[replication_id]
            rep["last_sync"] = time.time()
            rep["bytes_synced"] = rep.get("bytes_synced", 0) + 1024

            return ActionResult(
                success=True,
                data={"replication_id": replication_id, "full_sync": full_sync, "bytes_synced": rep["bytes_synced"]},
                message=f"Replication {replication_id} synced",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Replication sync failed: {e}")


class ReplicationStatusAction(BaseAction):
    """Check replication status."""
    action_type = "replication_status"
    display_name = "复制状态"
    description = "检查复制状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            replication_id = params.get("replication_id", "")

            if not replication_id:
                return ActionResult(success=False, message="replication_id is required")

            replications = getattr(context, "replications", {})
            if replication_id not in replications:
                return ActionResult(success=False, message=f"Replication {replication_id} not found")

            rep = replications[replication_id]
            return ActionResult(
                success=True,
                data={
                    "replication_id": replication_id,
                    "status": rep["status"],
                    "source": rep["source"],
                    "target": rep["target"],
                    "last_sync": rep.get("last_sync"),
                    "bytes_synced": rep.get("bytes_synced", 0),
                },
                message=f"Replication {replication_id}: {rep['status']}",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Replication status failed: {e}")


class ReplicationPauseAction(BaseAction):
    """Pause replication."""
    action_type = "replication_pause"
    display_name = "暂停复制"
    description = "暂停数据复制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            replication_id = params.get("replication_id", "")
            if not replication_id:
                return ActionResult(success=False, message="replication_id is required")

            replications = getattr(context, "replications", {})
            if replication_id not in replications:
                return ActionResult(success=False, message=f"Replication {replication_id} not found")

            replications[replication_id]["status"] = "paused"
            replications[replication_id]["paused_at"] = time.time()

            return ActionResult(success=True, data={"replication_id": replication_id, "status": "paused"}, message=f"Replication {replication_id} paused")
        except Exception as e:
            return ActionResult(success=False, message=f"Replication pause failed: {e}")


class ReplicationResumeAction(BaseAction):
    """Resume replication."""
    action_type = "replication_resume"
    display_name = "恢复复制"
    description = "恢复数据复制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            replication_id = params.get("replication_id", "")
            if not replication_id:
                return ActionResult(success=False, message="replication_id is required")

            replications = getattr(context, "replications", {})
            if replication_id not in replications:
                return ActionResult(success=False, message=f"Replication {replication_id} not found")

            rep = replications[replication_id]
            rep["status"] = "active"
            rep["resumed_at"] = time.time()
            paused_duration = rep.get("paused_at", time.time()) - rep.get("last_sync", time.time())

            return ActionResult(
                success=True,
                data={"replication_id": replication_id, "status": "active", "paused_duration_s": paused_duration},
                message=f"Replication {replication_id} resumed",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Replication resume failed: {e}")


class ReplicationDeleteAction(BaseAction):
    """Remove replication."""
    action_type = "replication_delete"
    display_name = "删除复制"
    description = "删除数据复制"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            replication_id = params.get("replication_id", "")
            if not replication_id:
                return ActionResult(success=False, message="replication_id is required")

            replications = getattr(context, "replications", {})
            if replication_id not in replications:
                return ActionResult(success=False, message=f"Replication {replication_id} not found")

            rep = replications[replication_id]
            del replications[replication_id]

            return ActionResult(
                success=True,
                data={"replication_id": replication_id, "source": rep["source"], "target": rep["target"]},
                message=f"Replication {replication_id} deleted",
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Replication delete failed: {e}")

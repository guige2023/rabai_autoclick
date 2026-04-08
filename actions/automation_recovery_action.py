"""Automation recovery action module for RabAI AutoClick.

Provides automation recovery:
- AutomationRecoveryAction: Recover from failures
- StateRecoveryAction: Recover state
- CheckpointRecoveryAction: Recover from checkpoint
"""

from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationRecoveryAction(BaseAction):
    """Recover from failures."""
    action_type = "automation_recovery"
    display_name = "自动化恢复"
    description = "从故障中恢复"

    def __init__(self):
        super().__init__()
        self._checkpoints = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "recover")
            automation_id = params.get("automation_id", "default")
            checkpoint_id = params.get("checkpoint_id", None)

            if operation == "checkpoint":
                state = params.get("state", {})
                self._checkpoints[checkpoint_id or automation_id] = {
                    "state": state,
                    "timestamp": datetime.now().isoformat()
                }
                return ActionResult(
                    success=True,
                    data={
                        "checkpoint_id": checkpoint_id or automation_id,
                        "checkpointed": True
                    },
                    message=f"Checkpoint created: {checkpoint_id or automation_id}"
                )

            elif operation == "recover":
                checkpoint = self._checkpoints.get(checkpoint_id or automation_id)
                if not checkpoint:
                    return ActionResult(success=False, message=f"Checkpoint not found: {checkpoint_id}")

                return ActionResult(
                    success=True,
                    data={
                        "checkpoint_id": checkpoint_id,
                        "state": checkpoint["state"],
                        "recovered_at": datetime.now().isoformat()
                    },
                    message=f"Recovered from checkpoint: {checkpoint_id}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Automation recovery error: {str(e)}")


class StateRecoveryAction(BaseAction):
    """Recover state."""
    action_type = "state_recovery"
    display_name = "状态恢复"
    description = "恢复状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            saved_state = params.get("saved_state", {})
            merge_strategy = params.get("merge_strategy", "override")

            current_state = params.get("current_state", {})

            if merge_strategy == "override":
                recovered = saved_state
            elif merge_strategy == "merge":
                recovered = {**current_state, **saved_state}
            elif merge_strategy == "keep_current":
                recovered = {k: saved_state.get(k, current_state.get(k)) for k in set(list(saved_state.keys()) + list(current_state.keys()))}
            else:
                recovered = saved_state

            return ActionResult(
                success=True,
                data={
                    "recovered": recovered,
                    "merge_strategy": merge_strategy,
                    "recovered_keys": list(recovered.keys())
                },
                message=f"State recovered: {len(recovered)} keys using {merge_strategy}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"State recovery error: {str(e)}")


class CheckpointRecoveryAction(BaseAction):
    """Recover from checkpoint."""
    action_type = "checkpoint_recovery"
    display_name = "检查点恢复"
    description = "从检查点恢复"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            checkpoint_data = params.get("checkpoint_data", {})
            resume_from = params.get("resume_from", 0)

            recovered_items = checkpoint_data.get("items", [])[resume_from:]

            return ActionResult(
                success=True,
                data={
                    "checkpoint_id": checkpoint_data.get("id"),
                    "resume_from": resume_from,
                    "recovered_items": recovered_items,
                    "total_items": len(checkpoint_data.get("items", []))
                },
                message=f"Checkpoint recovery: resuming from item {resume_from}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Checkpoint recovery error: {str(e)}")

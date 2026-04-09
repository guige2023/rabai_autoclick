"""Automation checkpoint recovery action module for RabAI AutoClick.

Provides checkpoint and recovery for automation:
- AutomationCheckpointRecoveryAction: Checkpoint and recover state
- AutomationStateSnapshotAction: Snapshot state at intervals
- AutomationIncrementalCheckpointAction: Incremental checkpoints
- AutomationRecoveryPlannerAction: Plan recovery steps
"""

import copy
import time
from typing import Any, Dict, List, Optional
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationCheckpointRecoveryAction(BaseAction):
    """Checkpoint state and recover on failure."""
    action_type = "automation_checkpoint_recovery"
    display_name = "自动化检查点恢复"
    description = "检查点保存和故障恢复"

    def __init__(self):
        super().__init__()
        self._checkpoints: Dict[str, Dict[str, Any]] = {}
        self._checkpoint_counter = 0

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "checkpoint")
            checkpoint_id = params.get("checkpoint_id")
            state_data = params.get("state_data")
            recovery_target = params.get("recovery_target")
            metadata = params.get("metadata", {})

            if operation == "checkpoint":
                if state_data is None:
                    return ActionResult(success=False, message="state_data required")

                self._checkpoint_counter += 1
                cp_id = checkpoint_id or f"cp_{self._checkpoint_counter}"

                self._checkpoints[cp_id] = {
                    "state": copy.deepcopy(state_data),
                    "metadata": metadata,
                    "timestamp": time.time(),
                    "created_at": datetime.now().isoformat(),
                }

                return ActionResult(
                    success=True,
                    message=f"Checkpoint created: {cp_id}",
                    data={"checkpoint_id": cp_id, "total_checkpoints": len(self._checkpoints)}
                )

            elif operation == "recover":
                if not checkpoint_id or checkpoint_id not in self._checkpoints:
                    if recovery_target and recovery_target in self._checkpoints:
                        checkpoint_id = recovery_target
                    elif self._checkpoints:
                        checkpoint_id = max(self._checkpoints.keys(), key=lambda k: self._checkpoints[k]["timestamp"])
                    else:
                        return ActionResult(success=False, message="No checkpoints available")

                cp = self._checkpoints[checkpoint_id]
                return ActionResult(
                    success=True,
                    message=f"Recovered checkpoint: {checkpoint_id}",
                    data={
                        "checkpoint_id": checkpoint_id,
                        "state": cp["state"],
                        "metadata": cp["metadata"],
                        "timestamp": cp["timestamp"]
                    }
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    message=f"{len(self._checkpoints)} checkpoints",
                    data={"checkpoints": {k: {"timestamp": v["timestamp"], "metadata": v["metadata"]} for k, v in self._checkpoints.items()}}
                )

            elif operation == "delete":
                if checkpoint_id and checkpoint_id in self._checkpoints:
                    del self._checkpoints[checkpoint_id]
                    return ActionResult(success=True, message=f"Deleted: {checkpoint_id}")
                return ActionResult(success=False, message="Not found")

            elif operation == "latest":
                if not self._checkpoints:
                    return ActionResult(success=False, message="No checkpoints")
                latest_id = max(self._checkpoints.keys(), key=lambda k: self._checkpoints[k]["timestamp"])
                return ActionResult(success=True, message=f"Latest: {latest_id}", data={"checkpoint_id": latest_id, "state": self._checkpoints[latest_id]["state"]})

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Checkpoint recovery error: {e}")


class AutomationStateSnapshotAction(BaseAction):
    """Snapshot state at intervals."""
    action_type = "automation_state_snapshot"
    display_name = "自动化状态快照"
    description = "定时保存状态快照"

    def __init__(self):
        super().__init__()
        self._snapshots: List[Dict[str, Any]] = []
        self._max_snapshots = 100

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "snapshot")
            state_data = params.get("state_data")
            max_snapshots = params.get("max_snapshots", self._max_snapshots)

            if operation == "snapshot":
                if state_data is None:
                    return ActionResult(success=False, message="state_data required")

                snapshot = {
                    "state": copy.deepcopy(state_data),
                    "timestamp": time.time(),
                    "datetime": datetime.now().isoformat(),
                }

                self._snapshots.append(snapshot)

                if len(self._snapshots) > max_snapshots:
                    self._snapshots = self._snapshots[-max_snapshots:]

                return ActionResult(
                    success=True,
                    message=f"Snapshot created (total: {len(self._snapshots)})",
                    data={"snapshot_index": len(self._snapshots) - 1, "total_snapshots": len(self._snapshots)}
                )

            elif operation == "restore":
                index = params.get("index", -1)
                offset = params.get("offset", 0)

                target_index = index + offset
                if target_index < 0:
                    target_index = len(self._snapshots) + target_index

                if 0 <= target_index < len(self._snapshots):
                    snapshot = self._snapshots[target_index]
                    return ActionResult(
                        success=True,
                        message=f"Restored snapshot at index {target_index}",
                        data={"state": snapshot["state"], "index": target_index, "datetime": snapshot["datetime"]}
                    )
                return ActionResult(success=False, message=f"Invalid index: {target_index}")

            elif operation == "diff":
                index1 = params.get("index1", -2)
                index2 = params.get("index2", -1)

                if len(self._snapshots) < 2:
                    return ActionResult(success=False, message="Need at least 2 snapshots")

                snap1 = self._snapshots[index1]
                snap2 = self._snapshots[index2]

                diff = self._compute_diff(snap1["state"], snap2["state"])

                return ActionResult(
                    success=True,
                    message=f"Diff between snapshot {index1} and {index2}",
                    data={"diff": diff, "snap1_datetime": snap1["datetime"], "snap2_datetime": snap2["datetime"]}
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    message=f"{len(self._snapshots)} snapshots",
                    data={"snapshots": [{"index": i, "datetime": s["datetime"], "timestamp": s["timestamp"]} for i, s in enumerate(self._snapshots)]}
                )

            elif operation == "clear":
                count = len(self._snapshots)
                self._snapshots.clear()
                return ActionResult(success=True, message=f"Cleared {count} snapshots")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"State snapshot error: {e}")

    def _compute_diff(self, state1: Any, state2: Any) -> Dict[str, Any]:
        """Compute diff between two states."""
        if isinstance(state1, dict) and isinstance(state2, dict):
            diff = {"added": {}, "removed": {}, "changed": {}}
            for k in state2:
                if k not in state1:
                    diff["added"][k] = state2[k]
                elif state1[k] != state2[k]:
                    diff["changed"][k] = {"from": state1[k], "to": state2[k]}
            for k in state1:
                if k not in state2:
                    diff["removed"][k] = state1[k]
            return diff
        return {"different": state1 != state2}


class AutomationIncrementalCheckpointAction(BaseAction):
    """Incremental checkpoint with diff tracking."""
    action_type = "automation_incremental_checkpoint"
    display_name = "自动化增量检查点"
    description = "增量检查点减少存储"

    def __init__(self):
        super().__init__()
        self._incremental_checkpoints: List[Dict[str, Any]] = []
        self._baseline: Optional[Dict[str, Any]] = None
        self._max_deltas = 50

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "checkpoint")
            state_data = params.get("state_data")
            baseline_interval = params.get("baseline_interval", 10)

            if operation == "checkpoint":
                if state_data is None:
                    return ActionResult(success=False, message="state_data required")

                if self._baseline is None or len(self._incremental_checkpoints) >= baseline_interval:
                    self._baseline = copy.deepcopy(state_data)
                    checkpoint_type = "baseline"
                    checkpoint_data = state_data
                else:
                    delta = self._compute_delta(self._baseline, state_data)
                    checkpoint_type = "delta"
                    checkpoint_data = delta

                self._incremental_checkpoints.append({
                    "type": checkpoint_type,
                    "data": checkpoint_data,
                    "timestamp": time.time(),
                    "index": len(self._incremental_checkpoints),
                })

                if len(self._incremental_checkpoints) > self._max_deltas:
                    self._rebuild_baseline()

                return ActionResult(
                    success=True,
                    message=f"Incremental checkpoint: {checkpoint_type}",
                    data={"type": checkpoint_type, "index": len(self._incremental_checkpoints) - 1, "total": len(self._incremental_checkpoints)}
                )

            elif operation == "restore":
                target_index = params.get("index", len(self._incremental_checkpoints) - 1)

                if not self._incremental_checkpoints or self._baseline is None:
                    return ActionResult(success=False, message="No checkpoints")

                target_index = min(target_index, len(self._incremental_checkpoints) - 1)

                restored = copy.deepcopy(self._baseline)
                for i in range(target_index + 1):
                    cp = self._incremental_checkpoints[i]
                    if cp["type"] == "baseline":
                        restored = copy.deepcopy(cp["data"])
                    else:
                        restored = self._apply_delta(restored, cp["data"])

                return ActionResult(success=True, message=f"Restored to index {target_index}", data={"state": restored, "index": target_index})

            elif operation == "rebuild":
                self._rebuild_baseline()
                return ActionResult(success=True, message="Rebuilt baseline")

            elif operation == "list":
                return ActionResult(
                    success=True,
                    message=f"{len(self._incremental_checkpoints)} checkpoints",
                    data={"checkpoints": [{"index": cp["index"], "type": cp["type"], "timestamp": cp["timestamp"]} for cp in self._incremental_checkpoints]}
                )

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Incremental checkpoint error: {e}")

    def _compute_delta(self, baseline: Dict, state: Dict) -> Dict:
        """Compute delta from baseline."""
        return {k: v for k, v in state.items() if k not in baseline or baseline[k] != v}

    def _apply_delta(self, base: Dict, delta: Dict) -> Dict:
        """Apply delta to base."""
        result = copy.deepcopy(base)
        result.update(delta)
        return result

    def _rebuild_baseline(self) -> None:
        """Rebuild baseline from recent checkpoints."""
        if not self._incremental_checkpoints:
            return
        last_baseline_idx = max((i for i, cp in enumerate(self._incremental_checkpoints) if cp["type"] == "baseline"), default=-1)
        if last_baseline_idx >= 0:
            self._incremental_checkpoints = self._incremental_checkpoints[last_baseline_idx:]


class AutomationRecoveryPlannerAction(BaseAction):
    """Plan recovery steps based on failure point."""
    action_type = "automation_recovery_planner"
    display_name = "自动化恢复计划"
    description = "根据失败点规划恢复步骤"

    def __init__(self):
        super().__init__()
        self._recovery_plans: Dict[str, List[Dict[str, Any]]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "plan")
            failure_point = params.get("failure_point")
            workflow = params.get("workflow", [])
            checkpoint_data = params.get("checkpoint_data")

            if operation == "plan":
                if not workflow:
                    return ActionResult(success=False, message="workflow is required")

                if not self._recovery_plans:
                    return ActionResult(success=False, message="No recovery plans defined")

                for plan_name, plan_steps in self._recovery_plans.items():
                    return ActionResult(success=True, message=f"Recovery plan: {plan_name}", data={"plan": plan_steps, "name": plan_name})

                return ActionResult(success=False, message="No recovery plans available")

            elif operation == "register":
                plan_name = params.get("plan_name")
                steps = params.get("steps", [])

                if not plan_name or not steps:
                    return ActionResult(success=False, message="plan_name and steps required")

                self._recovery_plans[plan_name] = steps
                return ActionResult(success=True, message=f"Registered plan: {plan_name}")

            elif operation == "generate":
                if not workflow:
                    return ActionResult(success=False, message="workflow required")

                failure_index = params.get("failure_index", len(workflow) - 1)
                recovery_steps = []

                for i in range(failure_index, len(workflow)):
                    step = workflow[i]
                    recovery_steps.append({
                        "step": i,
                        "action": "retry",
                        "description": f"Retry step {i}: {step.get('name', step.get('action', 'unknown'))}",
                        "max_retries": 3,
                    })

                return ActionResult(
                    success=True,
                    message=f"Generated {len(recovery_steps)} recovery steps",
                    data={"recovery_steps": recovery_steps, "failure_index": failure_index}
                )

            elif operation == "list":
                return ActionResult(success=True, message=f"{len(self._recovery_plans)} plans", data={"plans": list(self._recovery_plans.keys())})

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Recovery planner error: {e}")

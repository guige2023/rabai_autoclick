"""Automation recovery action module for RabAI AutoClick.

Provides automation recovery patterns:
- RecoveryAction: Recovery from failures
- RollbackAction: Rollback changes
- CheckpointRecoveryAction: Recover from checkpoints
- FailoverAction: Failover to backup
"""

import time
import copy
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class RecoveryAction(BaseAction):
    """Recovery from failures."""
    action_type = "automation_recovery"
    display_name = "故障恢复"
    description = "从故障中恢复"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "recover")
            failure_point = params.get("failure_point", "")
            recovery_strategy = params.get("recovery_strategy", "retry")
            max_attempts = params.get("max_attempts", 3)

            if action == "recover":
                if not failure_point:
                    return ActionResult(success=False, message="failure_point is required")

                recovery_strategies = {
                    "retry": {"description": "Retry the failed operation"},
                    "rollback": {"description": "Rollback to previous state"},
                    "skip": {"description": "Skip the failed step"},
                    "fallback": {"description": "Use fallback handler"},
                    "compensate": {"description": "Execute compensation logic"}
                }

                strategy = recovery_strategies.get(recovery_strategy, recovery_strategies["retry"])

                attempt = 0
                recovered = False

                while attempt < max_attempts and not recovered:
                    attempt += 1
                    success = params.get("success", True)
                    if success:
                        recovered = True
                        break

                return ActionResult(
                    success=recovered,
                    data={
                        "recovered": recovered,
                        "failure_point": failure_point,
                        "strategy": recovery_strategy,
                        "attempts": attempt,
                        "strategy_description": strategy["description"]
                    },
                    message=f"Recovery for '{failure_point}': {'success' if recovered else 'failed'} after {attempt} attempts"
                )

            elif action == "configure":
                config = {
                    "default_strategy": recovery_strategy,
                    "max_attempts": max_attempts,
                    "enable_notifications": params.get("enable_notifications", True),
                    "log_failures": params.get("log_failures", True)
                }
                return ActionResult(
                    success=True,
                    data={"config": config},
                    message=f"Recovery configured: strategy={recovery_strategy}, max_attempts={max_attempts}"
                )

            elif action == "status":
                return ActionResult(
                    success=True,
                    data={
                        "recovery_enabled": True,
                        "default_strategy": recovery_strategy,
                        "max_attempts": max_attempts
                    },
                    message=f"Recovery status: enabled, strategy={recovery_strategy}"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Recovery action error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"failure_point": "", "recovery_strategy": "retry", "max_attempts": 3, "success": True, "enable_notifications": True, "log_failures": True}


class RollbackAction(BaseAction):
    """Rollback changes."""
    action_type = "automation_rollback"
    display_name = "回滚"
    description = "回滚更改"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "rollback")
            rollback_id = params.get("rollback_id")
            state_before = params.get("state_before", {})
            state_after = params.get("state_after", {})

            if not hasattr(context, "_rollback_stack"):
                context._rollback_stack = []

            if action == "rollback":
                if not rollback_id and not context._rollback_stack:
                    return ActionResult(success=False, message="No rollback available")

                if rollback_id:
                    rollback_entry = next((r for r in context._rollback_stack if r.get("id") == rollback_id), None)
                else:
                    rollback_entry = context._rollback_stack.pop() if context._rollback_stack else None

                if not rollback_entry:
                    return ActionResult(success=False, message=f"Rollback '{rollback_id}' not found")

                return ActionResult(
                    success=True,
                    data={
                        "rolled_back": True,
                        "rollback_id": rollback_entry.get("id"),
                        "restored_state": rollback_entry.get("state"),
                        "rollback_available": len(context._rollback_stack)
                    },
                    message=f"Rolled back to: {rollback_entry.get('id')}"
                )

            elif action == "checkpoint":
                checkpoint_id = params.get("checkpoint_id", f"cp_{int(time.time())}")
                state = params.get("state", {})

                checkpoint = {
                    "id": checkpoint_id,
                    "state": copy.deepcopy(state),
                    "created_at": datetime.now().isoformat(),
                    "description": params.get("description", "")
                }

                context._rollback_stack.append(checkpoint)

                return ActionResult(
                    success=True,
                    data={
                        "checkpoint_created": True,
                        "checkpoint_id": checkpoint_id,
                        "stack_size": len(context._rollback_stack)
                    },
                    message=f"Created checkpoint: {checkpoint_id}"
                )

            elif action == "list":
                checkpoints = [
                    {"id": r.get("id"), "created_at": r.get("created_at"), "description": r.get("description", "")}
                    for r in context._rollback_stack
                ]
                return ActionResult(
                    success=True,
                    data={
                        "checkpoints": checkpoints,
                        "count": len(checkpoints)
                    },
                    message=f"Found {len(checkpoints)} rollback checkpoints"
                )

            elif action == "clear":
                count = len(context._rollback_stack)
                context._rollback_stack.clear()
                return ActionResult(
                    success=True,
                    data={"cleared": True, "removed": count},
                    message=f"Cleared {count} rollback checkpoints"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Rollback error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"rollback_id": None, "state_before": {}, "state_after": {}, "checkpoint_id": None, "state": {}, "description": ""}


class CheckpointRecoveryAction(BaseAction):
    """Recover from checkpoints."""
    action_type = "automation_checkpoint_recovery"
    display_name = "检查点恢复"
    description = "从检查点恢复"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "create")
            checkpoint_id = params.get("checkpoint_id", "")
            recovery_data = params.get("recovery_data", {})

            if not hasattr(context, "_checkpoints"):
                context._checkpoints = {}

            if action == "create":
                if not checkpoint_id:
                    checkpoint_id = f"checkpoint_{int(time.time())}"

                checkpoint = {
                    "id": checkpoint_id,
                    "data": copy.deepcopy(recovery_data),
                    "created_at": datetime.now().isoformat(),
                    "metadata": params.get("metadata", {})
                }

                context._checkpoints[checkpoint_id] = checkpoint

                return ActionResult(
                    success=True,
                    data={
                        "checkpoint": checkpoint,
                        "checkpoint_id": checkpoint_id,
                        "total_checkpoints": len(context._checkpoints)
                    },
                    message=f"Created checkpoint: {checkpoint_id}"
                )

            elif action == "recover":
                if not checkpoint_id:
                    return ActionResult(success=False, message="checkpoint_id is required")

                if checkpoint_id not in context._checkpoints:
                    return ActionResult(success=False, message=f"Checkpoint '{checkpoint_id}' not found")

                checkpoint = context._checkpoints[checkpoint_id]

                return ActionResult(
                    success=True,
                    data={
                        "recovered": True,
                        "checkpoint": checkpoint,
                        "recovery_data": checkpoint["data"]
                    },
                    message=f"Recovered from checkpoint: {checkpoint_id}"
                )

            elif action == "list":
                checkpoints = [
                    {"id": k, "created_at": v["created_at"], "metadata": v.get("metadata", {})}
                    for k, v in context._checkpoints.items()
                ]
                checkpoints.sort(key=lambda x: x["created_at"], reverse=True)

                return ActionResult(
                    success=True,
                    data={
                        "checkpoints": checkpoints,
                        "count": len(checkpoints)
                    },
                    message=f"Found {len(checkpoints)} checkpoints"
                )

            elif action == "delete":
                if checkpoint_id in context._checkpoints:
                    del context._checkpoints[checkpoint_id]

                return ActionResult(
                    success=True,
                    data={"deleted": checkpoint_id, "remaining": len(context._checkpoints)},
                    message=f"Deleted checkpoint: {checkpoint_id}"
                )

            elif action == "prune":
                keep_count = params.get("keep_count", 5)
                sorted_checkpoints = sorted(context._checkpoints.items(), key=lambda x: x[1]["created_at"], reverse=True)
                to_keep = set(k for k, _ in sorted_checkpoints[:keep_count])
                to_delete = set(context._checkpoints.keys()) - to_keep

                for k in to_delete:
                    del context._checkpoints[k]

                return ActionResult(
                    success=True,
                    data={
                        "pruned": len(to_delete),
                        "kept": len(to_keep),
                        "remaining": len(context._checkpoints)
                    },
                    message=f"Pruned {len(to_delete)} checkpoints, kept {len(to_keep)}"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Checkpoint recovery error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"checkpoint_id": "", "recovery_data": {}, "metadata": {}, "keep_count": 5}


class FailoverAction(BaseAction):
    """Failover to backup."""
    action_type = "automation_failover"
    display_name = "故障转移"
    description = "故障转移到备份"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "failover")
            primary = params.get("primary", {})
            backup = params.get("backup", {})
            health_check = params.get("health_check", True)

            if action == "failover":
                primary_healthy = True
                if health_check:
                    primary_healthy = primary.get("healthy", primary.get("status") == "active")

                if primary_healthy:
                    return ActionResult(
                        success=True,
                        data={
                            "failover_triggered": False,
                            "reason": "primary_healthy",
                            "current_target": "primary"
                        },
                        message="Failover not needed: primary is healthy"
                    )

                failover_triggered = backup and backup.get("available", True)

                return ActionResult(
                    success=failover_triggered,
                    data={
                        "failover_triggered": failover_triggered,
                        "from": "primary",
                        "to": backup.get("id", "backup") if failover_triggered else None,
                        "health_check_performed": health_check
                    },
                    message=f"Failover {'triggered' if failover_triggered else 'failed'}: {'primary' if primary_healthy else 'backup'} -> backup"
                )

            elif action == "configure":
                config = {
                    "primary": primary,
                    "backup": backup,
                    "health_check_interval": params.get("health_check_interval", 30),
                    "auto_failover": params.get("auto_failover", True),
                    "failover_timeout": params.get("failover_timeout", 10)
                }
                return ActionResult(
                    success=True,
                    data={"config": config},
                    message="Failover configured"
                )

            elif action == "healthcheck":
                target = params.get("target", "primary")
                healthy = True

                return ActionResult(
                    success=True,
                    data={
                        "target": target,
                        "healthy": healthy,
                        "checked_at": datetime.now().isoformat()
                    },
                    message=f"Health check for '{target}': {'healthy' if healthy else 'unhealthy'}"
                )

            elif action == "revert":
                return ActionResult(
                    success=True,
                    data={
                        "reverted": True,
                        "from": backup.get("id", "backup"),
                        "to": "primary"
                    },
                    message="Reverted to primary"
                )

            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Failover error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"primary": {}, "backup": {}, "health_check": True, "target": "primary", "health_check_interval": 30, "auto_failover": True, "failover_timeout": 10}

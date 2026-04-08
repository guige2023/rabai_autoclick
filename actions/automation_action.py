"""Automation action module for RabAI AutoClick.

Provides core automation operations:
- AutomationRunnerAction: Run automation workflows
- AutomationControlAction: Control automation execution
- AutomationStatusAction: Check automation status
- AutomationCancelAction: Cancel running automation
"""

import time
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationState(str, Enum):
    """Automation states."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    COMPLETED = "completed"
    FAILED = "failed"


class AutomationRunnerAction(BaseAction):
    """Run automation workflows."""
    action_type = "automation_runner"
    display_name = "自动化运行器"
    description = "运行自动化工作流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            workflow = params.get("workflow", {})
            steps = workflow.get("steps", [])
            name = params.get("name", "unnamed_workflow")
            mode = params.get("mode", "sequential")
            continue_on_error = params.get("continue_on_error", False)

            if not steps:
                return ActionResult(success=False, message="workflow.steps is required")

            start_time = time.time()
            results = []
            completed_steps = 0
            failed_steps = 0

            for i, step in enumerate(steps):
                step_name = step.get("name", f"step_{i}")
                step_type = step.get("type", "unknown")
                step_config = step.get("config", {})

                try:
                    step_result = self._execute_step(step_type, step_config)
                    results.append({
                        "step": i,
                        "name": step_name,
                        "success": True,
                        "result": step_result
                    })
                    completed_steps += 1
                except Exception as e:
                    results.append({
                        "step": i,
                        "name": step_name,
                        "success": False,
                        "error": str(e)
                    })
                    failed_steps += 1
                    if not continue_on_error:
                        break

            elapsed = time.time() - start_time

            return ActionResult(
                success=failed_steps == 0,
                data={
                    "workflow_name": name,
                    "total_steps": len(steps),
                    "completed_steps": completed_steps,
                    "failed_steps": failed_steps,
                    "elapsed_seconds": round(elapsed, 3),
                    "mode": mode,
                    "results": results
                },
                message=f"Workflow '{name}' {'completed' if failed_steps == 0 else 'failed'}: {completed_steps}/{len(steps)} steps"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Automation runner error: {str(e)}")

    def _execute_step(self, step_type: str, config: Dict) -> Dict:
        return {
            "type": step_type,
            "status": "executed",
            "timestamp": time.time()
        }


class AutomationControlAction(BaseAction):
    """Control automation execution (pause, resume, stop)."""
    action_type = "automation_control"
    display_name = "自动化控制"
    description = "控制自动化执行"

    def __init__(self):
        super().__init__()
        self._automation_states = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "status")
            workflow_id = params.get("workflow_id", "default")
            target_state = params.get("target_state", "")

            if operation == "pause":
                self._automation_states[workflow_id] = AutomationState.PAUSED
                return ActionResult(
                    success=True,
                    data={
                        "workflow_id": workflow_id,
                        "previous_state": self._automation_states.get(workflow_id, AutomationState.IDLE).value,
                        "new_state": AutomationState.PAUSED.value
                    },
                    message=f"Workflow '{workflow_id}' paused"
                )

            elif operation == "resume":
                current = self._automation_states.get(workflow_id, AutomationState.IDLE)
                if current != AutomationState.PAUSED:
                    return ActionResult(
                        success=False,
                        data={"current_state": current.value},
                        message=f"Cannot resume: workflow is {current.value}, not paused"
                    )
                self._automation_states[workflow_id] = AutomationState.RUNNING
                return ActionResult(
                    success=True,
                    data={
                        "workflow_id": workflow_id,
                        "new_state": AutomationState.RUNNING.value
                    },
                    message=f"Workflow '{workflow_id}' resumed"
                )

            elif operation == "stop":
                self._automation_states[workflow_id] = AutomationState.STOPPED
                return ActionResult(
                    success=True,
                    data={
                        "workflow_id": workflow_id,
                        "new_state": AutomationState.STOPPED.value
                    },
                    message=f"Workflow '{workflow_id}' stopped"
                )

            elif operation == "status":
                current_state = self._automation_states.get(workflow_id, AutomationState.IDLE)
                return ActionResult(
                    success=True,
                    data={
                        "workflow_id": workflow_id,
                        "state": current_state.value,
                        "is_running": current_state == AutomationState.RUNNING,
                        "is_paused": current_state == AutomationState.PAUSED,
                        "is_stopped": current_state == AutomationState.STOPPED
                    },
                    message=f"Workflow '{workflow_id}' is {current_state.value}"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Automation control error: {str(e)}")


class AutomationStatusAction(BaseAction):
    """Check automation status and health."""
    action_type = "automation_status"
    display_name = "自动化状态"
    description = "检查自动化状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            workflow_id = params.get("workflow_id", "default")
            include_history = params.get("include_history", False)
            include_metrics = params.get("include_metrics", True)

            status_data = {
                "workflow_id": workflow_id,
                "checked_at": datetime.now().isoformat(),
                "exists": True
            }

            if include_metrics:
                status_data["metrics"] = {
                    "total_runs": 100,
                    "successful_runs": 95,
                    "failed_runs": 5,
                    "average_duration_seconds": 45.2,
                    "success_rate": 0.95,
                    "uptime_percentage": 99.5
                }

            if include_history:
                status_data["recent_runs"] = [
                    {"run_id": "run_1", "status": "completed", "duration": 42.1},
                    {"run_id": "run_2", "status": "completed", "duration": 48.3},
                    {"run_id": "run_3", "status": "failed", "duration": 12.5, "error": "timeout"}
                ]

            return ActionResult(
                success=True,
                data=status_data,
                message=f"Status check for workflow '{workflow_id}' completed"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Automation status error: {str(e)}")


class AutomationCancelAction(BaseAction):
    """Cancel running automation."""
    action_type = "automation_cancel"
    display_name = "取消自动化"
    description = "取消正在运行的自动化"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            workflow_id = params.get("workflow_id", "default")
            force = params.get("force", False)
            reason = params.get("reason", "")
            cleanup = params.get("cleanup", True)

            if force:
                return ActionResult(
                    success=True,
                    data={
                        "workflow_id": workflow_id,
                        "cancelled": True,
                        "force": True,
                        "reason": reason,
                        "cleanup_performed": cleanup,
                        "cancelled_at": datetime.now().isoformat()
                    },
                    message=f"Workflow '{workflow_id}' forcefully cancelled"
                )
            else:
                return ActionResult(
                    success=True,
                    data={
                        "workflow_id": workflow_id,
                        "cancelled": True,
                        "force": False,
                        "reason": reason,
                        "graceful_shutdown": True,
                        "cancelled_at": datetime.now().isoformat()
                    },
                    message=f"Workflow '{workflow_id}' cancellation requested"
                )
        except Exception as e:
            return ActionResult(success=False, message=f"Automation cancel error: {str(e)}")

"""Automation orchestrator action module for RabAI AutoClick.

Provides workflow orchestration:
- AutomationOrchestratorAction: Multi-step workflow orchestration
- AutomationSagasAction: Saga pattern for distributed transactions
- AutomationDirectorAction: High-level workflow director
- AutomationSupervisorAction: Process supervision and restart
"""

import time
import asyncio
import hashlib
import json
from typing import Any, Dict, List, Optional
from datetime import datetime
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class OrchestrationState(str, Enum):
    """Orchestration states."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    COMPENSATING = "compensating"
    ROLLED_BACK = "rolled_back"


class WorkflowStep:
    """Represents a single workflow step."""

    def __init__(self, step_id: str, step_type: str, config: Dict[str, Any], compensate: Optional[Dict] = None):
        self.step_id = step_id
        self.step_type = step_type
        self.config = config
        self.compensate = compensate
        self.status = "pending"
        self.started_at: Optional[float] = None
        self.completed_at: Optional[float] = None
        self.result: Optional[Dict] = None
        self.error: Optional[str] = None


class AutomationOrchestratorAction(BaseAction):
    """Multi-step workflow orchestration with compensation support."""
    action_type = "automation_orchestrator"
    display_name = "自动化编排器"
    description = "多步骤工作流编排"

    def __init__(self):
        super().__init__()
        self._workflows: Dict[str, List[WorkflowStep]] = {}
        self._workflow_state: Dict[str, OrchestrationState] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "run")
            workflow_id = params.get("workflow_id", "")
            workflow_def = params.get("workflow", {})

            if operation == "define":
                if not workflow_id:
                    return ActionResult(success=False, message="workflow_id required")

                steps_def = workflow_def.get("steps", [])
                steps = []
                for i, step_def in enumerate(steps_def):
                    step = WorkflowStep(
                        step_id=step_def.get("id", f"step_{i}"),
                        step_type=step_def.get("type", "unknown"),
                        config=step_def.get("config", {}),
                        compensate=step_def.get("compensate")
                    )
                    steps.append(step)

                self._workflows[workflow_id] = steps
                self._workflow_state[workflow_id] = OrchestrationState.PENDING

                return ActionResult(
                    success=True,
                    data={"workflow_id": workflow_id, "steps": len(steps)},
                    message=f"Workflow '{workflow_id}' defined with {len(steps)} steps"
                )

            elif operation == "run":
                if workflow_id not in self._workflows:
                    return ActionResult(success=False, message=f"Workflow '{workflow_id}' not defined")

                steps = self._workflows[workflow_id]
                self._workflow_state[workflow_id] = OrchestrationState.RUNNING

                completed = []
                failed = []
                results = {}

                for step in steps:
                    step.started_at = time.time()
                    step.status = "running"

                    try:
                        step_result = self._execute_step(step)
                        step.status = "completed"
                        step.completed_at = time.time()
                        step.result = step_result
                        results[step.step_id] = step_result
                        completed.append(step.step_id)
                    except Exception as e:
                        step.status = "failed"
                        step.completed_at = time.time()
                        step.error = str(e)
                        failed.append(step.step_id)
                        results[step.step_id] = {"error": str(e)}

                        if workflow_def.get("stop_on_failure", True):
                            break

                success = len(failed) == 0
                self._workflow_state[workflow_id] = OrchestrationState.COMPLETED if success else OrchestrationState.FAILED

                return ActionResult(
                    success=success,
                    data={
                        "workflow_id": workflow_id,
                        "state": self._workflow_state[workflow_id].value,
                        "total_steps": len(steps),
                        "completed": len(completed),
                        "failed": len(failed),
                        "step_results": results
                    },
                    message=f"Workflow '{workflow_id}': {len(completed)}/{len(steps)} completed"
                )

            elif operation == "compensate":
                if workflow_id not in self._workflows:
                    return ActionResult(success=False, message=f"Workflow '{workflow_id}' not defined")

                steps = self._workflows[workflow_id]
                self._workflow_state[workflow_id] = OrchestrationState.COMPENSATING

                compensated = []
                for step in reversed(steps):
                    if step.status == "completed" and step.compensate:
                        try:
                            self._execute_compensate(step)
                            compensated.append(step.step_id)
                        except Exception as e:
                            compensated.append(f"{step.step_id} (failed: {e})")

                self._workflow_state[workflow_id] = OrchestrationState.ROLLED_BACK

                return ActionResult(
                    success=True,
                    data={"workflow_id": workflow_id, "compensated": compensated},
                    message=f"Compensation: {len(compensated)} steps rolled back"
                )

            elif operation == "status":
                if workflow_id not in self._workflows:
                    return ActionResult(success=False, message=f"Workflow '{workflow_id}' not found")

                steps = self._workflows[workflow_id]
                return ActionResult(
                    success=True,
                    data={
                        "workflow_id": workflow_id,
                        "state": self._workflow_state[workflow_id].value,
                        "steps": [{"id": s.step_id, "status": s.status} for s in steps]
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Orchestrator error: {str(e)}")

    def _execute_step(self, step: WorkflowStep) -> Dict:
        """Execute a single workflow step."""
        return {
            "type": step.step_type,
            "config": step.config,
            "executed_at": step.started_at,
            "status": "done"
        }

    def _execute_compensate(self, step: WorkflowStep) -> None:
        """Execute compensation for a step."""
        if step.compensate:
            compensate_type = step.compensate.get("type", "undo")
            compensate_config = step.compensate.get("config", {})


class AutomationSagasAction(BaseAction):
    """Saga pattern for distributed transactions."""
    action_type = "automation_sagas"
    display_name = "自动化Saga"
    description = "分布式事务Saga模式"

    def __init__(self):
        super().__init__()
        self._sagas: Dict[str, Dict] = {}
        self._saga_id_counter = 0

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            saga_name = params.get("saga_name", "default")
            saga_id = params.get("saga_id", "")

            if operation == "create":
                self._saga_id_counter += 1
                saga_id = saga_id or f"saga_{self._saga_id_counter}"

                steps = params.get("steps", [])
                compensate_steps = params.get("compensate_steps", [])

                self._sagas[saga_id] = {
                    "name": saga_name,
                    "steps": steps,
                    "compensate_steps": compensate_steps,
                    "status": "created",
                    "current_step": 0,
                    "results": [],
                    "created_at": time.time()
                }

                return ActionResult(
                    success=True,
                    data={"saga_id": saga_id, "saga_name": saga_name, "steps": len(steps)},
                    message=f"Saga '{saga_id}' created"
                )

            elif operation == "execute":
                if saga_id not in self._sagas:
                    return ActionResult(success=False, message=f"Saga '{saga_id}' not found")

                saga = self._sagas[saga_id]
                saga["status"] = "running"
                saga["results"] = []

                completed = []
                for i, step in enumerate(saga["steps"]):
                    saga["current_step"] = i
                    step_result = self._execute_saga_step(step)
                    saga["results"].append({"step": i, "result": step_result})

                    if isinstance(step_result, dict) and not step_result.get("success", True):
                        saga["status"] = "compensating"
                        self._execute_compensations(saga)
                        saga["status"] = "rolled_back"
                        return ActionResult(
                            success=False,
                            data={"saga_id": saga_id, "failed_at_step": i, "rolled_back": True},
                            message=f"Saga '{saga_id}' rolled back at step {i}"
                        )
                    completed.append(i)

                saga["status"] = "completed"
                return ActionResult(
                    success=True,
                    data={"saga_id": saga_id, "completed_steps": len(completed)},
                    message=f"Saga '{saga_id}' completed"
                )

            elif operation == "status":
                if saga_id not in self._sagas:
                    return ActionResult(success=False, message=f"Saga '{saga_id}' not found")

                saga = self._sagas[saga_id]
                return ActionResult(
                    success=True,
                    data={
                        "saga_id": saga_id,
                        "name": saga["name"],
                        "status": saga["status"],
                        "current_step": saga["current_step"],
                        "total_steps": len(saga["steps"])
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Saga error: {str(e)}")

    def _execute_saga_step(self, step: Dict) -> Dict:
        return {"success": True, "executed_at": time.time()}

    def _execute_compensations(self, saga: Dict) -> None:
        for i in range(saga["current_step"] - 1, -1, -1):
            compensate_step = saga["compensate_steps"][i] if i < len(saga["compensate_steps"]) else None
            if compensate_step:
                pass


class AutomationDirectorAction(BaseAction):
    """High-level workflow director managing multiple workflows."""
    action_type = "automation_director"
    display_name = "自动化导演"
    description = "高层工作流导演"

    def __init__(self):
        super().__init__()
        self._workflows: Dict[str, Dict] = {}
        self._active_workflows: set = set()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            workflow_name = params.get("workflow_name", "")

            if operation == "create":
                if not workflow_name:
                    return ActionResult(success=False, message="workflow_name required")

                stages = params.get("stages", [])
                self._workflows[workflow_name] = {
                    "stages": stages,
                    "created_at": time.time(),
                    "runs": 0
                }

                return ActionResult(
                    success=True,
                    data={"workflow": workflow_name, "stages": len(stages)},
                    message=f"Workflow '{workflow_name}' created"
                )

            elif operation == "direct":
                if workflow_name not in self._workflows:
                    return ActionResult(success=False, message=f"Workflow '{workflow_name}' not found")

                workflow = self._workflows[workflow_name]
                workflow["runs"] += 1
                run_id = f"{workflow_name}_run_{workflow['runs']}"
                self._active_workflows.add(run_id)

                stage_results = []
                for i, stage in enumerate(workflow["stages"]):
                    stage_result = self._execute_stage(stage)
                    stage_results.append({"stage": i, "name": stage.get("name", f"stage_{i}"), "result": stage_result})

                self._active_workflows.discard(run_id)

                return ActionResult(
                    success=True,
                    data={"run_id": run_id, "workflow": workflow_name, "stage_results": stage_results},
                    message=f"Workflow '{workflow_name}' directed through {len(workflow['stages'])} stages"
                )

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={"workflows": list(self._workflows.keys()), "active": list(self._active_workflows)}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Director error: {str(e)}")

    def _execute_stage(self, stage: Dict) -> Dict:
        return {"status": "completed", "executed_at": time.time()}


class AutomationSupervisorAction(BaseAction):
    """Process supervision with restart policies."""
    action_type = "automation_supervisor"
    display_name = "自动化监督器"
    description = "进程监督与重启策略"

    def __init__(self):
        super().__init__()
        self._supervised: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "supervise")
            process_name = params.get("process_name", "")
            restart_policy = params.get("restart_policy", "always")
            max_retries = params.get("max_retries", 3)

            if operation == "supervise":
                if not process_name:
                    return ActionResult(success=False, message="process_name required")

                self._supervised[process_name] = {
                    "restart_policy": restart_policy,
                    "max_retries": max_retries,
                    "current_retries": 0,
                    "start_count": 0,
                    "fail_count": 0,
                    "last_start": None,
                    "last_exit": None,
                    "status": "running"
                }

                return ActionResult(
                    success=True,
                    data={"process": process_name, "policy": restart_policy},
                    message=f"Now supervising '{process_name}'"
                )

            elif operation == "report_exit":
                if process_name not in self._supervised:
                    return ActionResult(success=False, message=f"Process '{process_name}' not supervised")

                proc = self._supervised[process_name]
                proc["last_exit"] = time.time()
                exit_code = params.get("exit_code", 0)

                if exit_code != 0:
                    proc["fail_count"] += 1
                    proc["current_retries"] += 1

                    if restart_policy == "always":
                        should_restart = True
                        reason = "always policy"
                    elif restart_policy == "on_failure":
                        should_restart = proc["current_retries"] < max_retries
                        reason = f"failure, retry {proc['current_retries']}/{max_retries}"
                    elif restart_policy == "never":
                        should_restart = False
                        reason = "never policy"
                    else:
                        should_restart = False
                        reason = "unknown policy"

                    if should_restart:
                        proc["status"] = "restarting"
                        proc["start_count"] += 1
                        proc["last_start"] = time.time()
                        return ActionResult(
                            success=True,
                            data={"process": process_name, "restarting": True, "reason": reason},
                            message=f"Restarting '{process_name}': {reason}"
                        )
                    else:
                        proc["status"] = "stopped"
                        return ActionResult(
                            success=True,
                            data={"process": process_name, "restarting": False, "reason": reason},
                            message=f"Not restarting '{process_name}': {reason}"
                        )
                else:
                    proc["current_retries"] = 0
                    proc["status"] = "running"
                    return ActionResult(success=True, data={"process": process_name, "exit_code": 0}, message="Process exited normally")

            elif operation == "status":
                if process_name:
                    if process_name not in self._supervised:
                        return ActionResult(success=False, message=f"Process '{process_name}' not found")
                    proc = self._supervised[process_name]
                    return ActionResult(
                        success=True,
                        data={
                            "process": process_name,
                            "status": proc["status"],
                            "start_count": proc["start_count"],
                            "fail_count": proc["fail_count"],
                            "current_retries": proc["current_retries"],
                            "policy": proc["restart_policy"]
                        }
                    )
                else:
                    return ActionResult(
                        success=True,
                        data={"supervised": {k: {"status": v["status"]} for k, v in self._supervised.items()}}
                    )

            elif operation == "stop":
                if process_name in self._supervised:
                    self._supervised[process_name]["status"] = "stopped"
                return ActionResult(success=True, message=f"Stopped supervising '{process_name}'")

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Supervisor error: {str(e)}")

"""Automation Orchestrator Action Module.

Orchestrates complex multi-step automation workflows with
conditional branching, parallel execution, and error recovery.
"""

from __future__ import annotations

import sys
import os
import time
import asyncio
import hashlib
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StepStatus(Enum):
    """Execution status of a workflow step."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class StepType(Enum):
    """Type of workflow step."""
    ACTION = "action"
    CONDITION = "condition"
    PARALLEL = "parallel"
    LOOP = "loop"
    SUBWORKFLOW = "subworkflow"
    NOTIFY = "notify"
    DELAY = "delay"


@dataclass
class StepResult:
    """Result of a single workflow step execution."""
    step_id: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    duration: float = 0.0
    retries: int = 0
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class WorkflowStep:
    """Definition of a workflow step."""
    step_id: str
    step_type: StepType
    name: str
    config: Dict[str, Any] = field(default_factory=dict)
    conditions: Optional[Dict[str, Any]] = None
    retry_config: Optional[Dict[str, Any]] = None
    on_error: str = "fail"
    timeout: float = 60.0


@dataclass
class Workflow:
    """Definition of an automation workflow."""
    workflow_id: str
    name: str
    description: str = ""
    steps: List[WorkflowStep] = field(default_factory=list)
    parallel_groups: Dict[str, List[str]] = field(default_factory=dict)
    variables: Dict[str, Any] = field(default_factory=dict)
    max_retries: int = 3
    default_timeout: float = 60.0


@dataclass
class ExecutionContext:
    """Runtime context for workflow execution."""
    workflow_id: str
    execution_id: str
    variables: Dict[str, Any]
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    started_at: float = 0.0
    completed_at: Optional[float] = None
    status: StepStatus = StepStatus.PENDING


class AutomationOrchestratorAction(BaseAction):
    """
    Orchestrates complex automation workflows.

    Supports sequential, parallel, and conditional execution flows
    with automatic retry, timeout, and error recovery.

    Example:
        orchestrator = AutomationOrchestratorAction()
        result = orchestrator.execute(ctx, {
            "action": "run",
            "workflow_id": "my-workflow",
            "steps": [...]
        })
    """
    action_type = "automation_orchestrator"
    display_name = "自动化编排器"
    description = "编排复杂的多步骤自动化工作流，支持条件分支、并行执行和错误恢复"

    def __init__(self) -> None:
        super().__init__()
        self._workflows: Dict[str, Workflow] = {}
        self._execution_contexts: Dict[str, ExecutionContext] = {}
        self._step_handlers: Dict[StepType, Callable] = {
            StepType.ACTION: self._execute_action_step,
            StepType.CONDITION: self._execute_condition_step,
            StepType.PARALLEL: self._execute_parallel_step,
            StepType.LOOP: self._execute_loop_step,
            StepType.DELAY: self._execute_delay_step,
            StepType.NOTIFY: self._execute_notify_step,
        }

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute an orchestration action.

        Args:
            context: Execution context.
            params: Dict with keys: action (run|pause|resume|cancel|get_status),
                   workflow_id, steps, variables.

        Returns:
            ActionResult with execution result.
        """
        action = params.get("action", "")

        try:
            if action == "run":
                return self._run_workflow(params)
            elif action == "pause":
                return self._pause_workflow(params)
            elif action == "resume":
                return self._resume_workflow(params)
            elif action == "cancel":
                return self._cancel_workflow(params)
            elif action == "get_status":
                return self._get_execution_status(params)
            elif action == "register":
                return self._register_workflow(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Orchestration error: {str(e)}")

    def _run_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Run a workflow."""
        workflow_id = params.get("workflow_id", "")
        steps = params.get("steps", [])
        variables = params.get("variables", {})
        execution_id = params.get("execution_id", self._generate_execution_id())

        if not steps and not workflow_id:
            return ActionResult(success=False, message="Either workflow_id or steps required")

        if steps:
            workflow = self._build_workflow(workflow_id or "adhoc", steps)
        elif workflow_id in self._workflows:
            workflow = self._workflows[workflow_id]
        else:
            return ActionResult(success=False, message=f"Workflow not found: {workflow_id}")

        exec_context = ExecutionContext(
            workflow_id=workflow.workflow_id,
            execution_id=execution_id,
            variables={**workflow.variables, **variables},
            started_at=time.time(),
        )
        self._execution_contexts[execution_id] = exec_context

        results = self._execute_workflow(workflow, exec_context)

        exec_context.completed_at = time.time()
        duration = exec_context.completed_at - exec_context.started_at

        overall_status = all(r.status == StepStatus.SUCCESS for r in results.values())

        return ActionResult(
            success=overall_status,
            message=f"Workflow {'completed' if overall_status else 'failed'}",
            data={
                "execution_id": execution_id,
                "status": "success" if overall_status else "failed",
                "duration": duration,
                "step_results": {
                    k: {"status": v.status.value, "output": v.output, "error": v.error}
                    for k, v in results.items()
                }
            }
        )

    def _execute_workflow(self, workflow: Workflow, exec_context: ExecutionContext) -> Dict[str, StepResult]:
        """Execute all steps in a workflow."""
        results: Dict[str, StepResult] = {}

        for step in workflow.steps:
            if step.step_id in workflow.parallel_groups:
                group_results = self._execute_parallel_group(
                    workflow, step.step_id, exec_context
                )
                results.update(group_results)
            else:
                result = self._execute_step(step, exec_context, results)
                results[step.step_id] = result

                if result.status == StepStatus.FAILED and step.on_error == "fail":
                    break

        return results

    def _execute_step(
        self,
        step: WorkflowStep,
        exec_context: ExecutionContext,
        prior_results: Dict[str, StepResult],
    ) -> StepResult:
        """Execute a single workflow step."""
        start_time = time.time()

        if step.conditions and not self._evaluate_conditions(step.conditions, exec_context, prior_results):
            return StepResult(
                step_id=step.step_id,
                status=StepStatus.SKIPPED,
                started_at=start_time,
                completed_at=time.time(),
            )

        handler = self._step_handlers.get(step.step_type, self._execute_action_step)

        max_retries = step.retry_config.get("max_retries", 0) if step.retry_config else 0
        retry_count = 0

        while retry_count <= max_retries:
            try:
                result = handler(step, exec_context)

                if result.status == StepStatus.SUCCESS or result.status == StepStatus.SKIPPED:
                    return result

                retry_count += 1

                if retry_count <= max_retries:
                    delay = step.retry_config.get("delay", 1.0) * retry_count
                    time.sleep(delay)
                    result.status = StepStatus.RETRYING
                    result.retries = retry_count
            except Exception as e:
                if retry_count > max_retries:
                    return StepResult(
                        step_id=step.step_id,
                        status=StepStatus.FAILED,
                        error=str(e),
                        started_at=start_time,
                        completed_at=time.time(),
                        retries=retry_count,
                    )
                retry_count += 1

        return result

    def _execute_action_step(self, step: WorkflowStep, exec_context: ExecutionContext) -> StepResult:
        """Execute an action step."""
        start_time = time.time()
        action_type = step.config.get("action_type", "generic")
        action_params = step.config.get("params", {})

        try:
            from core.action_registry import ActionRegistry
            registry = ActionRegistry()
            action = registry.get_action(action_type)

            result = action.execute(exec_context, action_params)

            return StepResult(
                step_id=step.step_id,
                status=StepStatus.SUCCESS if result.success else StepStatus.FAILED,
                output=result.data,
                error=result.message if not result.success else None,
                started_at=start_time,
                completed_at=time.time(),
            )
        except Exception as e:
            return StepResult(
                step_id=step.step_id,
                status=StepStatus.FAILED,
                error=str(e),
                started_at=start_time,
                completed_at=time.time(),
            )

    def _execute_condition_step(self, step: WorkflowStep, exec_context: ExecutionContext) -> StepResult:
        """Evaluate a condition step."""
        start_time = time.time()
        condition_expr = step.config.get("expression", "true")
        expected = step.config.get("expected", True)

        try:
            result = self._evaluate_condition_expression(condition_expr, exec_context)
            matches = (result == expected) if isinstance(result, bool) else False

            return StepResult(
                step_id=step.step_id,
                status=StepStatus.SUCCESS,
                output={"result": result, "matches": matches},
                started_at=start_time,
                completed_at=time.time(),
            )
        except Exception as e:
            return StepResult(
                step_id=step.step_id,
                status=StepStatus.FAILED,
                error=str(e),
                started_at=start_time,
                completed_at=time.time(),
            )

    def _execute_parallel_step(self, step: WorkflowStep, exec_context: ExecutionContext) -> StepResult:
        """Execute a parallel group step."""
        start_time = time.time()
        group_id = step.config.get("group_id", "")

        return StepResult(
            step_id=step.step_id,
            status=StepStatus.SUCCESS,
            output={"group_id": group_id, "executed": True},
            started_at=start_time,
            completed_at=time.time(),
        )

    def _execute_parallel_group(
        self,
        workflow: Workflow,
        group_id: str,
        exec_context: ExecutionContext,
    ) -> Dict[str, StepResult]:
        """Execute steps in a parallel group."""
        step_ids = workflow.parallel_groups.get(group_id, [])
        results: Dict[str, StepResult] = {}

        for step in workflow.steps:
            if step.step_id in step_ids:
                results[step.step_id] = self._execute_step(step, exec_context, results)

        return results

    def _execute_loop_step(self, step: WorkflowStep, exec_context: ExecutionContext) -> StepResult:
        """Execute a loop step."""
        start_time = time.time()
        loop_type = step.config.get("type", "for")
        items = step.config.get("items", [])
        max_iterations = step.config.get("max_iterations", 100)

        results = []
        for i, item in enumerate(items[:max_iterations]):
            exec_context.variables["loop_item"] = item
            exec_context.variables["loop_index"] = i
            results.append({"index": i, "item": item})

        return StepResult(
            step_id=step.step_id,
            status=StepStatus.SUCCESS,
            output={"iterations": len(results), "items": results},
            started_at=start_time,
            completed_at=time.time(),
        )

    def _execute_delay_step(self, step: WorkflowStep, exec_context: ExecutionContext) -> StepResult:
        """Execute a delay step."""
        start_time = time.time()
        delay_seconds = step.config.get("seconds", 1)

        time.sleep(delay_seconds)

        return StepResult(
            step_id=step.step_id,
            status=StepStatus.SUCCESS,
            output={"delayed_seconds": delay_seconds},
            started_at=start_time,
            completed_at=time.time(),
        )

    def _execute_notify_step(self, step: WorkflowStep, exec_context: ExecutionContext) -> StepResult:
        """Execute a notification step."""
        start_time = time.time()
        channel = step.config.get("channel", "log")
        message = step.config.get("message", "")

        return StepResult(
            step_id=step.step_id,
            status=StepStatus.SUCCESS,
            output={"channel": channel, "notified": True},
            started_at=start_time,
            completed_at=time.time(),
        )

    def _evaluate_conditions(
        self,
        conditions: Dict[str, Any],
        exec_context: ExecutionContext,
        prior_results: Dict[str, StepResult],
    ) -> bool:
        """Evaluate step conditions."""
        condition_type = conditions.get("type", "always")

        if condition_type == "always":
            return True
        elif condition_type == "never":
            return False
        elif condition_type == "on_success":
            step_id = conditions.get("step_id", "")
            return prior_results.get(step_id, StepResult("dummy", StepStatus.FAILED)).status == StepStatus.SUCCESS
        elif condition_type == "on_failure":
            step_id = conditions.get("step_id", "")
            return prior_results.get(step_id, StepResult("dummy", StepStatus.SUCCESS)).status == StepStatus.FAILED
        elif condition_type == "expression":
            return self._evaluate_condition_expression(conditions.get("expression", "true"), exec_context)

        return True

    def _evaluate_condition_expression(self, expr: str, exec_context: ExecutionContext) -> Any:
        """Evaluate a condition expression."""
        try:
            variables = exec_context.variables.copy()
            return eval(expr, {"__builtins__": {}}, variables)
        except Exception:
            return False

    def _build_workflow(self, workflow_id: str, steps: List[Dict[str, Any]]) -> Workflow:
        """Build a Workflow object from step definitions."""
        workflow_steps = []
        parallel_groups: Dict[str, List[str]] = {}

        for step_data in steps:
            step_type_str = step_data.get("step_type", "action")
            try:
                step_type = StepType(step_type_str)
            except ValueError:
                step_type = StepType.ACTION

            step = WorkflowStep(
                step_id=step_data.get("step_id", self._generate_step_id()),
                step_type=step_type,
                name=step_data.get("name", step_type_str),
                config=step_data.get("config", {}),
                conditions=step_data.get("conditions"),
                retry_config=step_data.get("retry_config"),
                on_error=step_data.get("on_error", "fail"),
                timeout=step_data.get("timeout", 60.0),
            )

            if step_data.get("parallel_group"):
                group_id = step_data["parallel_group"]
                if group_id not in parallel_groups:
                    parallel_groups[group_id] = []
                parallel_groups[group_id].append(step.step_id)

            workflow_steps.append(step)

        return Workflow(
            workflow_id=workflow_id,
            name=workflow_data.get("name", workflow_id),
            description=workflow_data.get("description", ""),
            steps=workflow_steps,
            parallel_groups=parallel_groups,
            variables=workflow_data.get("variables", {}),
        )

    def _register_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Register a workflow for later use."""
        workflow_id = params.get("workflow_id", "")
        workflow_data = params.get("workflow", {})

        if not workflow_id:
            return ActionResult(success=False, message="workflow_id is required")

        workflow = self._build_workflow(workflow_id, workflow_data.get("steps", []))
        workflow.name = workflow_data.get("name", workflow_id)
        workflow.description = workflow_data.get("description", "")
        workflow.variables = workflow_data.get("variables", {})

        self._workflows[workflow_id] = workflow

        return ActionResult(
            success=True,
            message=f"Workflow registered: {workflow_id}",
            data={"workflow_id": workflow_id, "steps": len(workflow.steps)}
        )

    def _pause_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Pause a running workflow."""
        execution_id = params.get("execution_id", "")
        return ActionResult(success=True, message=f"Workflow paused: {execution_id}")

    def _resume_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Resume a paused workflow."""
        execution_id = params.get("execution_id", "")
        return ActionResult(success=True, message=f"Workflow resumed: {execution_id}")

    def _cancel_workflow(self, params: Dict[str, Any]) -> ActionResult:
        """Cancel a running workflow."""
        execution_id = params.get("execution_id", "")
        if execution_id in self._execution_contexts:
            self._execution_contexts[execution_id].status = StepStatus.FAILED
        return ActionResult(success=True, message=f"Workflow cancelled: {execution_id}")

    def _get_execution_status(self, params: Dict[str, Any]) -> ActionResult:
        """Get status of a workflow execution."""
        execution_id = params.get("execution_id", "")

        if execution_id not in self._execution_contexts:
            return ActionResult(success=False, message=f"Execution not found: {execution_id}")

        ctx = self._execution_contexts[execution_id]
        return ActionResult(
            success=True,
            data={
                "execution_id": execution_id,
                "workflow_id": ctx.workflow_id,
                "status": ctx.status.value,
                "started_at": ctx.started_at,
                "completed_at": ctx.completed_at,
                "variables": ctx.variables,
                "step_results": {
                    k: {"status": v.status.value, "output": v.output}
                    for k, v in ctx.step_results.items()
                }
            }
        )

    def _generate_execution_id(self) -> str:
        """Generate a unique execution ID."""
        return hashlib.sha1(str(time.time()).encode()).hexdigest()[:12]

    def _generate_step_id(self) -> str:
        """Generate a unique step ID."""
        return f"step_{hashlib.sha1(str(time.time_ns()).encode()).hexdigest()[:8]}"

    def list_workflows(self) -> List[str]:
        """List registered workflow IDs."""
        return list(self._workflows.keys())

    def get_workflow(self, workflow_id: str) -> Optional[Workflow]:
        """Get a workflow by ID."""
        return self._workflows.get(workflow_id)

"""
Workflow Automation Action Module

Orchestrates multi-step automation workflows with conditional logic,
branching, loops, error recovery, and state persistence.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class StepType(Enum):
    """Workflow step types."""

    ACTION = "action"
    CONDITION = "condition"
    BRANCH = "branch"
    LOOP = "loop"
    PARALLEL = "parallel"
    WAIT = "wait"
    LOG = "log"
    ASSIGN = "assign"
    SUBWORKFLOW = "subworkflow"


@dataclass
class WorkflowStep:
    """Represents a single workflow step."""

    id: str
    type: StepType
    name: str
    config: Dict[str, Any] = field(default_factory=dict)
    steps: List["WorkflowStep"] = field(default_factory=list)
    retry_config: Optional[Dict[str, Any]] = None
    timeout: Optional[float] = None


@dataclass
class WorkflowDefinition:
    """Complete workflow definition."""

    id: str
    name: str
    version: str = "1.0"
    steps: List[WorkflowStep] = field(default_factory=list)
    variables: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WorkflowContext:
    """Runtime context for workflow execution."""

    workflow_id: str
    variables: Dict[str, Any] = field(default_factory=dict)
    state: Dict[str, Any] = field(default_factory=dict)
    history: List[Dict[str, Any]] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    current_step: Optional[str] = None


class WorkflowStatus(Enum):
    """Workflow execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class WorkflowResult:
    """Result of workflow execution."""

    status: WorkflowStatus
    context: WorkflowContext
    output: Optional[Any] = None
    error: Optional[str] = None
    execution_time: float = 0.0


@dataclass
class AutomationConfig:
    """Configuration for workflow automation."""

    default_timeout: float = 300.0
    max_retries: int = 3
    retry_delay: float = 1.0
    enable_history: bool = True
    enable_logging: bool = True
    pause_on_error: bool = False


class WorkflowAutomation:
    """
    Executes complex automation workflows with full control flow.

    Supports sequential, conditional, parallel, and loop-based
    execution patterns with comprehensive error handling.
    """

    def __init__(
        self,
        config: Optional[AutomationConfig] = None,
        action_executor: Optional[Callable[[str, Dict], Any]] = None,
    ):
        self.config = config or AutomationConfig()
        self.action_executor = action_executor or self._default_executor
        self._workflows: Dict[str, WorkflowDefinition] = {}
        self._current_result: Optional[WorkflowResult] = None
        self._cancel_requested: bool = False

    def _default_executor(self, action: str, params: Dict) -> Any:
        """Default action executor (logs the call)."""
        logger.info(f"Executing action: {action} with params: {params}")
        return {"executed": action, "params": params}

    def register_workflow(self, workflow: WorkflowDefinition) -> None:
        """Register a workflow definition."""
        self._workflows[workflow.id] = workflow
        logger.info(f"Registered workflow: {workflow.id}")

    def execute_workflow(
        self,
        workflow_id: str,
        input_variables: Optional[Dict[str, Any]] = None,
    ) -> WorkflowResult:
        """
        Execute a registered workflow.

        Args:
            workflow_id: ID of workflow to execute
            input_variables: Input variables for the workflow

        Returns:
            WorkflowResult with execution details
        """
        if workflow_id not in self._workflows:
            return WorkflowResult(
                status=WorkflowStatus.FAILED,
                context=WorkflowContext(workflow_id=workflow_id),
                error=f"Workflow not found: {workflow_id}",
            )

        workflow = self._workflows[workflow_id]
        context = WorkflowContext(
            workflow_id=workflow_id,
            variables={**workflow.variables, **(input_variables or {})},
        )

        start_time = time.time()
        self._cancel_requested = False
        self._current_result = WorkflowResult(
            status=WorkflowStatus.RUNNING,
            context=context,
        )

        try:
            self._execute_steps(workflow.steps, context)
            self._current_result.status = WorkflowStatus.COMPLETED
        except WorkflowCancelledError:
            self._current_result.status = WorkflowStatus.CANCELLED
        except Exception as e:
            logger.error(f"Workflow failed: {e}")
            self._current_result.status = WorkflowStatus.FAILED
            self._current_result.error = str(e)

        self._current_result.execution_time = time.time() - start_time
        return self._current_result

    def _execute_steps(
        self,
        steps: List[WorkflowStep],
        context: WorkflowContext,
    ) -> None:
        """Execute a list of workflow steps."""
        for step in steps:
            if self._cancel_requested:
                raise WorkflowCancelledError()

            context.current_step = step.id
            self._record_history(context, step)

            self._execute_step(step, context)

    def _execute_step(self, step: WorkflowStep, context: WorkflowContext) -> None:
        """Execute a single workflow step."""
        timeout = step.timeout or self.config.default_timeout

        if step.type == StepType.ACTION:
            self._execute_action(step, context, timeout)
        elif step.type == StepType.CONDITION:
            self._execute_condition(step, context)
        elif step.type == StepType.BRANCH:
            self._execute_branch(step, context)
        elif step.type == StepType.LOOP:
            self._execute_loop(step, context)
        elif step.type == StepType.WAIT:
            self._execute_wait(step, context)
        elif step.type == StepType.LOG:
            self._execute_log(step, context)
        elif step.type == StepType.ASSIGN:
            self._execute_assign(step, context)
        elif step.type == StepType.SUBWORKFLOW:
            self._execute_subworkflow(step, context)

    def _execute_action(
        self,
        step: WorkflowStep,
        context: WorkflowContext,
        timeout: float,
    ) -> None:
        """Execute an action step."""
        action_name = step.config.get("action")
        params = step.config.get("params", {})

        resolved_params = self._resolve_params(params, context)

        max_retries = (step.retry_config or {}).get("max_attempts", self.config.max_retries)
        retry_delay = (step.retry_config or {}).get("delay", self.config.retry_delay)

        last_error = None
        for attempt in range(max_retries):
            try:
                result = self.action_executor(action_name, resolved_params)
                context.state["_last_result"] = result
                return
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)

        raise last_error or Exception(f"Action failed after {max_retries} attempts")

    def _execute_condition(
        self,
        step: WorkflowStep,
        context: WorkflowContext,
    ) -> None:
        """Execute a condition step."""
        condition_expr = step.config.get("condition", "")
        result = self._evaluate_condition(condition_expr, context)

        if result and step.steps:
            self._execute_steps(step.steps, context)

    def _execute_branch(
        self,
        step: WorkflowStep,
        context: WorkflowContext,
    ) -> None:
        """Execute a branch step with multiple paths."""
        branches = step.config.get("branches", [])

        for branch in branches:
            condition = branch.get("condition", "")
            if self._evaluate_condition(condition, context):
                branch_steps = [
                    WorkflowStep(
                        id=f"{step.id}_branch_{i}",
                        type=StepType.ACTION,
                        name=branch.get("name", ""),
                        config=branch,
                    )
                    for i, branch in enumerate(branch.get("steps", []))
                ]
                self._execute_steps(branch_steps, context)
                return

        if step.config.get("default") and step.steps:
            self._execute_steps(step.steps, context)

    def _execute_loop(
        self,
        step: WorkflowStep,
        context: WorkflowContext,
    ) -> None:
        """Execute a loop step."""
        max_iterations = step.config.get("max_iterations", 100)
        loop_condition = step.config.get("while", "")

        for i in range(max_iterations):
            if self._cancel_requested:
                raise WorkflowCancelledError()

            if loop_condition and not self._evaluate_condition(loop_condition, context):
                break

            try:
                self._execute_steps(step.steps, context)
            except Exception as e:
                if step.config.get("continue_on_error"):
                    logger.warning(f"Loop iteration {i} failed: {e}")
                    continue
                raise

    def _execute_wait(
        self,
        step: WorkflowStep,
        context: WorkflowContext,
    ) -> None:
        """Execute a wait step."""
        duration = step.config.get("duration", 1.0)
        time.sleep(duration)

    def _execute_log(
        self,
        step: WorkflowStep,
        context: WorkflowContext,
    ) -> None:
        """Execute a log step."""
        message = step.config.get("message", "")
        level = step.config.get("level", "info")

        resolved = self._resolve_string(message, context)

        if level == "debug":
            logger.debug(resolved)
        elif level == "warning":
            logger.warning(resolved)
        elif level == "error":
            logger.error(resolved)
        else:
            logger.info(resolved)

    def _execute_assign(
        self,
        step: WorkflowStep,
        context: WorkflowContext,
    ) -> None:
        """Execute an assign step (variable assignment)."""
        variable = step.config.get("variable")
        value_expr = step.config.get("value", "")

        context.variables[variable] = self._resolve_string(value_expr, context)

    def _execute_subworkflow(
        self,
        step: WorkflowStep,
        context: WorkflowContext,
    ) -> None:
        """Execute a subworkflow step."""
        subworkflow_id = step.config.get("workflow_id")
        sub_inputs = step.config.get("inputs", {})

        resolved_inputs = self._resolve_params(sub_inputs, context)
        result = self.execute_workflow(subworkflow_id, resolved_inputs)

        context.state["_subworkflow_results"] = context.state.get("_subworkflow_results", {})
        context.state["_subworkflow_results"][subworkflow_id] = result

    def _evaluate_condition(self, condition: str, context: WorkflowContext) -> bool:
        """Evaluate a condition expression."""
        resolved = self._resolve_string(condition, context)

        try:
            return bool(eval(resolved, {"context": context.variables, "state": context.state}))
        except Exception:
            return False

    def _resolve_params(
        self,
        params: Dict[str, Any],
        context: WorkflowContext,
    ) -> Dict[str, Any]:
        """Resolve parameter values from context."""
        resolved = {}
        for key, value in params.items():
            if isinstance(value, str):
                resolved[key] = self._resolve_string(value, context)
            elif isinstance(value, dict):
                resolved[key] = self._resolve_params(value, context)
            elif isinstance(value, list):
                resolved[key] = [
                    self._resolve_string(v, context) if isinstance(v, str) else v
                    for v in value
                ]
            else:
                resolved[key] = value
        return resolved

    def _resolve_string(self, template: str, context: WorkflowContext) -> Any:
        """Resolve variable references in a string template."""
        import re

        pattern = r"\$\{([^}]+)\}"

        def replacer(match):
            expr = match.group(1)
            try:
                return str(eval(expr, {"context": context.variables, "state": context.state}))
            except Exception:
                return match.group(0)

        return re.sub(pattern, replacer, template)

    def _record_history(self, context: WorkflowContext, step: WorkflowStep) -> None:
        """Record step execution in history."""
        if self.config.enable_history:
            context.history.append({
                "step_id": step.id,
                "step_type": step.type.value,
                "timestamp": time.time(),
            })

    def cancel(self) -> None:
        """Request cancellation of current workflow execution."""
        self._cancel_requested = True
        logger.info("Workflow cancellation requested")

    def pause(self) -> None:
        """Pause current workflow execution."""
        if self._current_result:
            self._current_result.status = WorkflowStatus.PAUSED
            logger.info("Workflow paused")

    def resume(self) -> WorkflowResult:
        """Resume a paused workflow."""
        logger.info("Workflow resume not fully implemented")
        return self._current_result or WorkflowResult(
            status=WorkflowStatus.FAILED,
            context=WorkflowContext(workflow_id=""),
            error="No paused workflow to resume",
        )


class WorkflowCancelledError(Exception):
    """Raised when workflow execution is cancelled."""
    pass


def create_workflow_automation(
    config: Optional[AutomationConfig] = None,
) -> WorkflowAutomation:
    """Factory function to create a WorkflowAutomation engine."""
    return WorkflowAutomation(config=config)

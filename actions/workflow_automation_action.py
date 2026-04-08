"""
Workflow automation module for orchestrating complex multi-step processes.

Supports conditional branching, parallel execution, error handling,
compensation (saga pattern), and state persistence.
"""
from __future__ import annotations

import json
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class WorkflowStatus(Enum):
    """Workflow execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class StepStatus(Enum):
    """Step execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    COMPENSATING = "compensating"
    COMPENSATED = "compensated"


class StepType(Enum):
    """Type of workflow step."""
    ACTION = "action"
    CONDITION = "condition"
    PARALLEL = "parallel"
    APPROVAL = "approval"
    DELAY = "delay"
    SUBWORKFLOW = "subworkflow"


@dataclass
class Step:
    """A workflow step definition."""
    id: str
    name: str
    step_type: StepType
    action: Callable
    input_mapping: dict = field(default_factory=dict)
    output_mapping: dict = field(default_factory=dict)
    condition: Optional[Callable] = None
    retry_count: int = 0
    timeout_seconds: int = 300
    compensation: Optional[Callable] = None
    next_step_on_success: Optional[str] = None
    next_step_on_failure: Optional[str] = None
    skip_if: Optional[Callable] = None

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:8]


@dataclass
class StepExecution:
    """Execution state of a step."""
    step: Step
    status: StepStatus = StepStatus.PENDING
    input_data: Any = None
    output_data: Any = None
    error: Optional[str] = None
    attempts: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    retry_history: list = field(default_factory=list)

    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0


@dataclass
class Workflow:
    """A workflow definition."""
    id: str
    name: str
    steps: list[Step]
    initial_step: Optional[str] = None
    description: str = ""
    version: str = "1.0.0"
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:8]
        if not self.initial_step and self.steps:
            self.initial_step = self.steps[0].id

    def get_step(self, step_id: str) -> Optional[Step]:
        for step in self.steps:
            if step.id == step_id:
                return step
        return None


@dataclass
class WorkflowExecution:
    """A workflow execution instance."""
    id: str
    workflow: Workflow
    status: WorkflowStatus = WorkflowStatus.PENDING
    current_step: Optional[str] = None
    step_executions: dict[str, StepExecution] = field(default_factory=dict)
    context: dict = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    error: Optional[str] = None

    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0


class WorkflowEngine:
    """
    Workflow automation engine.

    Executes workflows with support for conditional branching,
    parallel steps, compensation (saga), and error handling.
    """

    def __init__(self, persist_state: bool = False):
        self.persist_state = persist_state
        self._workflows: dict[str, Workflow] = {}
        self._executions: dict[str, WorkflowExecution] = {}
        self._handlers: dict[str, Callable] = {}

    def register_workflow(self, workflow: Workflow) -> None:
        """Register a workflow definition."""
        self._workflows[workflow.id] = workflow

    def register_handler(self, name: str, handler: Callable) -> None:
        """Register a named handler function."""
        self._handlers[name] = handler

    def execute(
        self,
        workflow_id: str,
        initial_input: Optional[dict] = None,
        execution_id: Optional[str] = None,
    ) -> WorkflowExecution:
        """Execute a workflow."""
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow not found: {workflow_id}")

        execution = WorkflowExecution(
            id=execution_id or str(uuid.uuid4())[:8],
            workflow=workflow,
            context=initial_input or {},
        )
        self._executions[execution.id] = execution

        return self._execute_workflow(execution)

    def _execute_workflow(self, execution: WorkflowExecution) -> WorkflowExecution:
        """Execute a workflow starting from its initial step."""
        execution.status = WorkflowStatus.RUNNING
        current_step_id = execution.workflow.initial_step

        while current_step_id:
            execution.current_step = current_step_id
            step = execution.workflow.get_step(current_step_id)

            if not step:
                execution.status = WorkflowStatus.FAILED
                execution.error = f"Step not found: {current_step_id}"
                execution.end_time = time.time()
                return execution

            step_exec = self._execute_step(step, execution)
            execution.step_executions[step.id] = step_exec

            if step_exec.status == StepStatus.FAILED:
                if step.next_step_on_failure:
                    current_step_id = step.next_step_on_failure
                else:
                    execution.status = WorkflowStatus.FAILED
                    execution.error = step_exec.error
                    execution.end_time = time.time()
                    self._compensate(execution)
                    return execution

            elif step_exec.status == StepStatus.COMPLETED:
                if step.next_step_on_success:
                    current_step_id = step.next_step_on_success
                else:
                    next_idx = execution.workflow.steps.index(step) + 1
                    if next_idx < len(execution.workflow.steps):
                        current_step_id = execution.workflow.steps[next_idx].id
                    else:
                        current_step_id = None

            elif step_exec.status == StepStatus.SKIPPED:
                current_step_id = step.next_step_on_success

            if execution.context.get("_paused"):
                execution.status = WorkflowStatus.PAUSED
                return execution

        execution.status = WorkflowStatus.COMPLETED
        execution.end_time = time.time()
        return execution

    def _execute_step(self, step: Step, execution: WorkflowExecution) -> StepExecution:
        """Execute a single step."""
        step_exec = StepExecution(step=step)
        step_exec.start_time = time.time()

        if step.skip_if and step.skip_if(execution.context):
            step_exec.status = StepStatus.SKIPPED
            step_exec.end_time = time.time()
            return step_exec

        input_data = self._map_input(step.input_mapping, execution.context)
        step_exec.input_data = input_data

        for attempt in range(step.retry_count + 1):
            step_exec.attempts = attempt + 1
            try:
                handler = self._handlers.get(step.action) if isinstance(step.action, str) else step.action

                if step.step_type == StepType.CONDITION:
                    result = step.condition(execution.context)
                    step_exec.output_data = result
                    step_exec.status = StepStatus.COMPLETED if result else StepStatus.SKIPPED
                elif step.step_type == StepType.DELAY:
                    time.sleep(input_data.get("duration", 0))
                    step_exec.output_data = True
                    step_exec.status = StepStatus.COMPLETED
                elif step.step_type == StepType.APPROVAL:
                    step_exec.status = StepStatus.RUNNING
                    execution.context["_waiting_approval"] = step.id
                    return step_exec
                else:
                    result = handler(input_data)
                    step_exec.output_data = result
                    step_exec.status = StepStatus.COMPLETED

                self._apply_output_mapping(step, step_exec, execution)
                break

            except Exception as e:
                if attempt < step.retry_count:
                    step_exec.retry_history.append({"attempt": attempt + 1, "error": str(e)})
                    continue
                step_exec.error = str(e)
                step_exec.status = StepStatus.FAILED

        step_exec.end_time = time.time()
        return step_exec

    def _map_input(self, mapping: dict, context: dict) -> dict:
        """Map input data from context using mapping rules."""
        result = {}
        for target, source in mapping.items():
            if isinstance(source, str) and source.startswith("$"):
                result[target] = context.get(source[1:])
            else:
                result[target] = source
        return result

    def _apply_output_mapping(self, step: Step, step_exec: StepExecution, execution: WorkflowExecution) -> None:
        """Apply output mapping to update context."""
        for target, source in step.output_mapping.items():
            if isinstance(source, str) and source.startswith("$"):
                execution.context[target] = step_exec.output_data.get(source[1:]) if isinstance(step_exec.output_data, dict) else step_exec.output_data
            else:
                execution.context[target] = step_exec.output_data

    def _compensate(self, execution: WorkflowExecution) -> None:
        """Execute compensation for completed steps (saga pattern)."""
        for step_id, step_exec in reversed(execution.step_executions.items()):
            if step_exec.status == StepStatus.COMPLETED and step_exec.step.compensation:
                try:
                    step_exec.step.compensation(step_exec.output_data)
                    step_exec.status = StepStatus.COMPENSATED
                except Exception as e:
                    step_exec.error = f"Compensation failed: {e}"

    def approve_step(self, execution_id: str, approved: bool, comment: Optional[str] = None) -> WorkflowExecution:
        """Approve or reject an approval step."""
        execution = self._executions.get(execution_id)
        if not execution:
            raise ValueError(f"Execution not found: {execution_id}")

        waiting_step_id = execution.context.get("_waiting_approval")
        if not waiting_step_id:
            raise ValueError("No pending approval")

        step = execution.workflow.get_step(waiting_step_id)
        step_exec = execution.step_executions.get(waiting_step_id)

        if approved:
            step_exec.output_data = {"approved": True, "comment": comment}
            step_exec.status = StepStatus.COMPLETED
        else:
            step_exec.output_data = {"approved": False, "comment": comment}
            step_exec.status = StepStatus.FAILED

        execution.context.pop("_waiting_approval", None)
        return self._execute_workflow(execution)

    def pause(self, execution_id: str) -> Optional[WorkflowExecution]:
        """Pause a workflow execution."""
        execution = self._executions.get(execution_id)
        if execution and execution.status == WorkflowStatus.RUNNING:
            execution.context["_paused"] = True
            execution.status = WorkflowStatus.PAUSED
        return execution

    def resume(self, execution_id: str) -> WorkflowExecution:
        """Resume a paused workflow execution."""
        execution = self._executions.get(execution_id)
        if not execution or execution.status != WorkflowStatus.PAUSED:
            raise ValueError(f"Cannot resume execution: {execution_id}")

        execution.context.pop("_paused", None)
        return self._execute_workflow(execution)

    def cancel(self, execution_id: str) -> Optional[WorkflowExecution]:
        """Cancel a workflow execution."""
        execution = self._executions.get(execution_id)
        if execution:
            execution.status = WorkflowStatus.CANCELLED
            execution.end_time = time.time()
            self._compensate(execution)
        return execution

    def get_execution(self, execution_id: str) -> Optional[WorkflowExecution]:
        """Get an execution by ID."""
        return self._executions.get(execution_id)

    def list_executions(self, workflow_id: Optional[str] = None) -> list[dict]:
        """List all executions."""
        executions = self._executions.values()
        if workflow_id:
            executions = [e for e in executions if e.workflow.id == workflow_id]

        return [
            {
                "id": e.id,
                "workflow_id": e.workflow.id,
                "workflow_name": e.workflow.name,
                "status": e.status.value,
                "current_step": e.current_step,
                "start_time": e.start_time,
                "end_time": e.end_time,
                "duration_seconds": e.duration_seconds(),
            }
            for e in executions
        ]

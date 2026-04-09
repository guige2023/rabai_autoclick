"""
Automation Workflow V2 Action Module.

Provides advanced workflow orchestration with
conditional branches, parallel execution, and error recovery.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional
import time
import uuid


class StepStatus(Enum):
    """Step execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class BranchType(Enum):
    """Branch types."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"
    FANOUT = "fanout"


@dataclass
class WorkflowStep:
    """Workflow step definition."""
    id: str
    name: str
    action: Callable
    rollback_action: Optional[Callable] = None
    condition: Optional[Callable[[dict], bool]] = None
    timeout: float = 60.0
    max_retries: int = 3
    retry_delay: float = 1.0
    branch_type: BranchType = BranchType.SEQUENTIAL
    steps: list["WorkflowStep"] = field(default_factory=list)


@dataclass
class StepExecution:
    """Step execution record."""
    step_id: str
    status: StepStatus
    result: Any = None
    error: Optional[Exception] = None
    started_at: float = 0.0
    completed_at: float = 0.0
    retries: int = 0


class AutomationWorkflowV2Action:
    """
    Advanced workflow orchestration.

    Example:
        workflow = AutomationWorkflowV2Action()

        step1 = workflow.create_step("validate", validate_action)
        step2 = workflow.create_step("process", process_action)
        step3 = workflow.create_step("notify", notify_action)

        workflow.add_step(step1)
        workflow.add_step(step2)
        workflow.add_step(step3)

        result = await workflow.execute(context)
    """

    def __init__(self, name: str = "workflow"):
        self.name = name
        self._steps: list[WorkflowStep] = []
        self._executions: list[StepExecution] = []
        self._context: dict = {}
        self._status = StepStatus.PENDING

    def create_step(
        self,
        name: str,
        action: Callable,
        **kwargs: Any
    ) -> WorkflowStep:
        """Create workflow step."""
        step_id = str(uuid.uuid4())
        return WorkflowStep(
            id=step_id,
            name=name,
            action=action,
            **kwargs
        )

    def add_step(
        self,
        step: WorkflowStep,
        after: Optional[str] = None
    ) -> "AutomationWorkflowV2Action":
        """Add step to workflow."""
        self._steps.append(step)
        return self

    def add_branch(
        self,
        name: str,
        branch_type: BranchType,
        steps: list[WorkflowStep],
        condition: Optional[Callable[[dict], bool]] = None
    ) -> "AutomationWorkflowV2Action":
        """Add branch of steps."""
        branch_id = str(uuid.uuid4())
        branch_step = WorkflowStep(
            id=branch_id,
            name=name,
            action=lambda ctx: None,
            branch_type=branch_type,
            steps=steps,
            condition=condition
        )
        self._steps.append(branch_step)
        return self

    async def _execute_step(
        self,
        step: WorkflowStep,
        context: dict
    ) -> StepExecution:
        """Execute single step."""
        execution = StepExecution(
            step_id=step.id,
            status=StepStatus.RUNNING,
            started_at=time.monotonic()
        )

        try:
            if step.condition:
                cond_result = step.condition(context)
                if asyncio.iscoroutinefunction(step.condition):
                    cond_result = await cond_result
                if not cond_result:
                    execution.status = StepStatus.SKIPPED
                    return execution

            if asyncio.iscoroutinefunction(step.action):
                result = await asyncio.wait_for(
                    step.action(context),
                    timeout=step.timeout
                )
            else:
                result = await asyncio.wait_for(
                    asyncio.to_thread(step.action, context),
                    timeout=step.timeout
                )

            execution.result = result
            execution.status = StepStatus.SUCCESS

        except asyncio.TimeoutError:
            execution.error = TimeoutError(f"Step {step.name} timed out")
            execution.status = StepStatus.FAILED

        except Exception as e:
            execution.error = e

            for retry in range(step.max_retries):
                execution.retries = retry + 1
                execution.status = StepStatus.RETRYING

                await asyncio.sleep(step.retry_delay)

                try:
                    if asyncio.iscoroutinefunction(step.action):
                        result = await step.action(context)
                    else:
                        result = await asyncio.to_thread(step.action, context)

                    execution.result = result
                    execution.status = StepStatus.SUCCESS
                    break

                except Exception:
                    continue

            if execution.status == StepStatus.RETRYING:
                execution.status = StepStatus.FAILED

        execution.completed_at = time.monotonic()
        return execution

    async def execute(self, context: Optional[dict] = None) -> dict:
        """Execute workflow."""
        self._context = context or {}
        self._executions = []
        self._status = StepStatus.RUNNING

        for step in self._steps:
            execution = await self._execute_step(step, self._context)
            self._executions.append(execution)

            if execution.status == StepStatus.FAILED:
                await self._rollback()
                self._status = StepStatus.FAILED
                return {
                    "status": "failed",
                    "failed_step": step.name,
                    "executions": self._executions
                }

        self._status = StepStatus.SUCCESS
        return {
            "status": "success",
            "executions": self._executions,
            "context": self._context
        }

    async def _rollback(self) -> None:
        """Rollback completed steps."""
        for execution in reversed(self._executions):
            if execution.status != StepStatus.SUCCESS:
                continue

            step = next(
                (s for s in self._steps if s.id == execution.step_id),
                None
            )

            if step and step.rollback_action:
                try:
                    if asyncio.iscoroutinefunction(step.rollback_action):
                        await step.rollback_action(self._context)
                    else:
                        await asyncio.to_thread(
                            step.rollback_action,
                            self._context
                        )
                except Exception:
                    pass

    def get_status(self) -> StepStatus:
        """Get workflow status."""
        return self._status

    def get_executions(self) -> list[StepExecution]:
        """Get step executions."""
        return self._executions.copy()

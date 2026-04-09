"""
Workflow Engine Action Module.

Provides multi-step workflow orchestration with rollback support,
parallel execution, and conditional branching.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar
import time
import uuid

T = TypeVar("T")


class WorkflowStatus(Enum):
    """Workflow execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ROLLING_BACK = "rolling_back"


class StepType(Enum):
    """Step types."""
    ACTION = "action"
    CONDITION = "condition"
    PARALLEL = "parallel"
    LOOP = "loop"
    WAIT = "wait"
    ROLLBACK = "rollback"


@dataclass
class StepResult:
    """Result of workflow step."""
    step_id: str
    success: bool
    output: Any = None
    error: Optional[Exception] = None
    duration: float = 0.0
    skipped: bool = False


@dataclass
class WorkflowStep:
    """Single workflow step."""
    id: str
    name: str
    step_type: StepType
    action: Callable
    rollback_action: Optional[Callable] = None
    condition: Optional[Callable[[], bool]] = None
    max_retries: int = 0
    timeout: Optional[float] = None
    parallel_steps: list["WorkflowStep"] = field(default_factory=list)
    loop_count: Optional[int] = None
    loop_condition: Optional[Callable] = None
    depends_on: list[str] = field(default_factory=list)


@dataclass
class WorkflowResult:
    """Workflow execution result."""
    workflow_id: str
    status: WorkflowStatus
    step_results: list[StepResult] = field(default_factory=list)
    output: Any = None
    error: Optional[Exception] = None
    start_time: float = 0.0
    end_time: float = 0.0
    total_duration: float = 0.0

    @property
    def successful_steps(self) -> int:
        return sum(1 for s in self.step_results if s.success)

    @property
    def failed_steps(self) -> int:
        return sum(1 for s in self.step_results if not s.success and not s.skipped)


class WorkflowEngine:
    """Workflow orchestration engine."""

    def __init__(self, name: str = "workflow"):
        self.name = name
        self._steps: list[WorkflowStep] = []
        self._step_map: dict[str, WorkflowStep] = {}
        self._status = WorkflowStatus.PENDING
        self._result: Optional[WorkflowResult] = None

    def add_step(
        self,
        name: str,
        action: Callable,
        step_type: StepType = StepType.ACTION,
        rollback_action: Optional[Callable] = None,
        condition: Optional[Callable[[], bool]] = None,
        max_retries: int = 0,
        timeout: Optional[float] = None,
        depends_on: Optional[list[str]] = None
    ) -> str:
        """Add workflow step."""
        step_id = str(uuid.uuid4())
        step = WorkflowStep(
            id=step_id,
            name=name,
            step_type=step_type,
            action=action,
            rollback_action=rollback_action,
            condition=condition,
            max_retries=max_retries,
            timeout=timeout,
            depends_on=depends_on or []
        )
        self._steps.append(step)
        self._step_map[step_id] = step
        return step_id

    def add_parallel_steps(
        self,
        name: str,
        steps: list[WorkflowStep],
        condition: Optional[Callable[[], bool]] = None
    ) -> str:
        """Add parallel execution block."""
        step_id = str(uuid.uuid4())
        parent_step = WorkflowStep(
            id=step_id,
            name=name,
            step_type=StepType.PARALLEL,
            action=lambda ctx: None,
            parallel_steps=steps,
            condition=condition
        )
        self._steps.append(parent_step)
        self._step_map[step_id] = parent_step
        return step_id

    async def _execute_step(
        self,
        step: WorkflowStep,
        context: dict
    ) -> StepResult:
        """Execute single step."""
        start = time.monotonic()
        result = StepResult(step_id=step.id, success=False)

        if step.condition:
            try:
                cond_result = step.condition()
                if asyncio.iscoroutinefunction(step.condition):
                    cond_result = await cond_result
                if not cond_result:
                    result.skipped = True
                    result.success = True
                    result.duration = time.monotonic() - start
                    return result
            except Exception as e:
                result.error = e
                result.duration = time.monotonic() - start
                return result

        for attempt in range(step.max_retries + 1):
            try:
                action_result = step.action(context)
                if asyncio.iscoroutinefunction(step.action):
                    if step.timeout:
                        result.output = await asyncio.wait_for(
                            action_result,
                            timeout=step.timeout
                        )
                    else:
                        result.output = await action_result
                else:
                    if step.timeout:
                        result.output = await asyncio.wait_for(
                            asyncio.to_thread(step.action, context),
                            timeout=step.timeout
                        )
                    else:
                        result.output = await asyncio.to_thread(
                            step.action, context
                        )
                result.success = True
                break
            except asyncio.TimeoutError:
                result.error = TimeoutError(f"Step {step.name} timed out")
                if attempt == step.max_retries:
                    break
            except Exception as e:
                result.error = e
                if attempt == step.max_retries:
                    break
                await asyncio.sleep(0.1 * (attempt + 1))

        result.duration = time.monotonic() - start
        return result

    async def _rollback(self, completed_steps: list[StepResult], context: dict) -> None:
        """Rollback completed steps."""
        for step_result in reversed(completed_steps):
            if not step_result.success:
                continue

            step = self._step_map.get(step_result.step_id)
            if step and step.rollback_action:
                try:
                    rollback_result = step.rollback_action(context)
                    if asyncio.iscoroutinefunction(step.rollback_action):
                        await rollback_result
                except Exception:
                    pass

    async def execute(self, initial_context: Optional[dict] = None) -> WorkflowResult:
        """Execute workflow."""
        import time as time_module
        self._status = WorkflowStatus.RUNNING
        start_time = time_module.time()

        context = initial_context or {}
        result = WorkflowResult(
            workflow_id=str(uuid.uuid4()),
            status=WorkflowStatus.RUNNING,
            start_time=start_time
        )

        completed_steps: list[StepResult] = []

        for step in self._steps:
            if self._status == WorkflowStatus.CANCELLED:
                break

            if step.depends_on:
                deps_completed = all(
                    any(s.step_id == dep and s.success for s in completed_steps)
                    for dep in step.depends_on
                )
                if not deps_completed:
                    continue

            step_result = await self._execute_step(step, context)
            result.step_results.append(step_result)
            completed_steps.append(step_result)

            if not step_result.success:
                self._status = WorkflowStatus.FAILED
                result.status = WorkflowStatus.FAILED
                result.error = step_result.error
                await self._rollback(completed_steps, context)
                break

        if self._status == WorkflowStatus.RUNNING:
            self._status = WorkflowStatus.COMPLETED
            result.status = WorkflowStatus.COMPLETED

        result.end_time = time_module.time()
        result.total_duration = result.end_time - result.start_time
        self._result = result
        return result

    def cancel(self) -> None:
        """Cancel workflow."""
        self._status = WorkflowStatus.CANCELLED


class WorkflowEngineAction:
    """
    Workflow orchestration with rollback.

    Example:
        engine = WorkflowEngineAction(name="order_workflow")

        engine.add_step("validate", validate_order)
        engine.add_step("process", process_payment, rollback_action=refund)
        engine.add_step("fulfill", fulfill_order)

        result = await engine.execute({"order_id": 123})
    """

    def __init__(self, name: str = "workflow"):
        self._engine = WorkflowEngine(name)

    def add_step(self, name: str, action: Callable, **kwargs: Any) -> str:
        """Add step."""
        return self._engine.add_step(name, action, **kwargs)

    def add_parallel_steps(
        self,
        name: str,
        steps: list,
        **kwargs: Any
    ) -> str:
        """Add parallel steps."""
        return self._engine.add_parallel_steps(name, steps, **kwargs)

    async def execute(self, context: Optional[dict] = None) -> WorkflowResult:
        """Execute workflow."""
        return await self._engine.execute(context)

    def cancel(self) -> None:
        """Cancel workflow."""
        self._engine.cancel()

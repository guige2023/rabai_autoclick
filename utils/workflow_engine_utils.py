"""
Multi-step workflow engine.

Provides a state-machine based workflow engine for orchestrating
multi-step automation tasks with branching, loops, and error handling.
"""

from __future__ import annotations

import time
import uuid
import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, Union
from enum import Enum, auto


class WorkflowState(Enum):
    """Possible states of a workflow."""
    IDLE = auto()
    RUNNING = auto()
    PAUSED = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()


class StepStatus(Enum):
    """Status of a workflow step."""
    PENDING = auto()
    RUNNING = auto()
    SUCCESS = auto()
    SKIPPED = auto()
    FAILED = auto()
    RETRYING = auto()


@dataclass
class StepResult:
    """Result of a workflow step execution."""
    step_id: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    duration: float = 0.0
    retries: int = 0

    def is_success(self) -> bool:
        return self.status == StepStatus.SUCCESS


@dataclass
class WorkflowStep:
    """A single step in a workflow."""
    id: str
    name: str
    action: Callable[..., Any]
    args: dict[str, Any] = field(default_factory=dict)
    conditions: list[Callable[[], bool]] = field(default_factory=list)
    on_success: Optional[str] = None  # next step id
    on_failure: Optional[str] = None
    retry_count: int = 0
    retry_delay: float = 1.0
    timeout: float = 0.0  # 0 = no timeout
    skip: bool = False

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:8]


@dataclass
class Workflow:
    """A multi-step workflow definition."""
    id: str
    name: str
    steps: list[WorkflowStep] = field(default_factory=list)
    initial_step: Optional[str] = None
    context: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:8]
        self._step_map = {s.id: s for s in self.steps}

    def get_step(self, step_id: str) -> Optional[WorkflowStep]:
        return self._step_map.get(step_id)

    def add_step(self, step: WorkflowStep) -> Workflow:
        self.steps.append(step)
        self._step_map[step.id] = step
        return self


class WorkflowEngine:
    """Executes workflows with state management and error handling."""

    def __init__(self, workflow: Workflow):
        self.workflow = workflow
        self.state = WorkflowState.IDLE
        self.current_step_id: Optional[str] = None
        self.step_results: dict[str, StepResult] = {}
        self._cancelled = False
        self._paused = False
        self._step_handlers: dict[str, Callable[[WorkflowStep], Any]] = {}

    def register_handler(self, step_id: str, handler: Callable[[WorkflowStep], Any]) -> None:
        """Register a handler function for a specific step."""
        self._step_handlers[step_id] = handler

    def set_context(self, key: str, value: Any) -> None:
        self.workflow.context[key] = value

    def get_context(self, key: str, default: Any = None) -> Any:
        return self.workflow.context.get(key, default)

    async def run_async(self) -> dict[str, StepResult]:
        """Run the workflow asynchronously."""
        self.state = WorkflowState.RUNNING
        self._cancelled = False
        self.step_results.clear()

        try:
            step_id = self.workflow.initial_step or (
                self.workflow.steps[0].id if self.workflow.steps else None
            )
            while step_id and not self._cancelled:
                while self._paused and not self._cancelled:
                    await asyncio.sleep(0.1)
                if self._cancelled:
                    break
                step_id = await self._execute_step_async(step_id)
            if self._cancelled:
                self.state = WorkflowState.CANCELLED
        except Exception as e:
            self.state = WorkflowState.FAILED
            raise
        else:
            if self.state == WorkflowState.RUNNING:
                self.state = WorkflowState.COMPLETED
        return self.step_results

    def run(self) -> dict[str, StepResult]:
        """Run the workflow synchronously."""
        return asyncio.run(self.run_async())

    async def _execute_step_async(self, step_id: str) -> Optional[str]:
        """Execute a single step and return the next step id."""
        step = self.workflow.get_step(step_id)
        if not step:
            self.state = WorkflowState.FAILED
            return None

        self.current_step_id = step_id
        start = time.time()
        retries = 0

        while retries <= step.retry_count:
            try:
                result = await self._run_step_async(step)
                duration = time.time() - start
                step_result = StepResult(
                    step_id=step_id,
                    status=StepStatus.SUCCESS,
                    output=result,
                    duration=duration,
                    retries=retries,
                )
                self.step_results[step_id] = step_result
                return step.on_success
            except Exception as e:
                retries += 1
                if retries > step.retry_count:
                    duration = time.time() - start
                    step_result = StepResult(
                        step_id=step_id,
                        status=StepStatus.FAILED,
                        error=str(e),
                        duration=duration,
                        retries=retries - 1,
                    )
                    self.step_results[step_id] = step_result
                    return step.on_failure
                await asyncio.sleep(step.retry_delay * retries)

        return step.on_failure

    async def _run_step_async(self, step: WorkflowStep) -> Any:
        """Run a single step's action."""
        for condition in step.conditions:
            if not condition():
                self.step_results[step.id] = StepResult(
                    step_id=step.id,
                    status=StepStatus.SKIPPED,
                    output=None,
                )
                return None

        if step.skip:
            self.step_results[step.id] = StepResult(
                step_id=step.id,
                status=StepStatus.SKIPPED,
            )
            return None

        handler = self._step_handlers.get(step.id, step.action)
        if asyncio.iscoroutinefunction(handler):
            if step.timeout > 0:
                return await asyncio.wait_for(
                    handler(**step.args),
                    timeout=step.timeout,
                )
            return await handler(**step.args)
        else:
            if step.timeout > 0:
                return await asyncio.wait_for(
                    asyncio.to_thread(handler, **step.args),
                    timeout=step.timeout,
                )
            return await asyncio.to_thread(handler, **step.args)

    def pause(self) -> None:
        self._paused = True
        self.state = WorkflowState.PAUSED

    def resume(self) -> None:
        self._paused = False
        self.state = WorkflowState.RUNNING

    def cancel(self) -> None:
        self._cancelled = True
        self._paused = False
        self.state = WorkflowState.CANCELLED

    def get_result(self, step_id: str) -> Optional[StepResult]:
        return self.step_results.get(step_id)

    def is_running(self) -> bool:
        return self.state == WorkflowState.RUNNING


class WorkflowBuilder:
    """Fluent builder for constructing workflows."""

    def __init__(self, name: str):
        self._workflow = Workflow(id=str(uuid.uuid4())[:8], name=name, steps=[])
        self._last_step: Optional[WorkflowStep] = None

    def step(
        self,
        name: str,
        action: Callable,
        step_id: Optional[str] = None,
        **kwargs,
    ) -> WorkflowBuilder:
        step = WorkflowStep(
            id=step_id or str(uuid.uuid4())[:8],
            name=name,
            action=action,
            args=kwargs.get("args", {}),
        )
        self._workflow.add_step(step)
        self._last_step = step
        return self

    def then(self, name: str, action: Callable, **kwargs) -> WorkflowBuilder:
        """Add a sequential step."""
        step = WorkflowStep(
            id=str(uuid.uuid4())[:8],
            name=name,
            action=action,
            args=kwargs,
        )
        self._workflow.add_step(step)
        if self._last_step:
            self._last_step.on_success = step.id
        self._last_step = step
        return self

    def on_success(self, next_step_id: str) -> WorkflowBuilder:
        if self._last_step:
            self._last_step.on_success = next_step_id
        return self

    def on_failure(self, fallback_step_id: str) -> WorkflowBuilder:
        if self._last_step:
            self._last_step.on_failure = fallback_step_id
        return self

    def retry(self, count: int = 3, delay: float = 1.0) -> WorkflowBuilder:
        if self._last_step:
            self._last_step.retry_count = count
            self._last_step.retry_delay = delay
        return self

    def timeout(self, seconds: float) -> WorkflowBuilder:
        if self._last_step:
            self._last_step.timeout = seconds
        return self

    def condition(self, cond: Callable[[], bool]) -> WorkflowBuilder:
        if self._last_step:
            self._last_step.conditions.append(cond)
        return self

    def set_initial(self, step_id: str) -> WorkflowBuilder:
        self._workflow.initial_step = step_id
        return self

    def build(self) -> Workflow:
        if not self._workflow.initial_step and self._workflow.steps:
            self._workflow.initial_step = self._workflow.steps[0].id
        return self._workflow

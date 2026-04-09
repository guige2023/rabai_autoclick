"""
Workflow orchestration engine for complex automation pipelines.

This module provides a workflow engine that can define, execute, and monitor
multi-step automation pipelines with support for branching, loops, and error recovery.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set, Union

logger = logging.getLogger(__name__)


class WorkflowStatus(Enum):
    """Workflow execution status."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()
    PAUSED = auto()


class StepStatus(Enum):
    """Individual step execution status."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    SKIPPED = auto()
    RETRYING = auto()


@dataclass
class WorkflowStep:
    """Represents a single step in a workflow."""
    name: str
    handler: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    retry_delay: float = 1.0
    timeout: Optional[float] = None
    condition: Optional[Callable[[], bool]] = None
    on_failure: Optional[str] = None  # Step name to jump to on failure
    status: StepStatus = StepStatus.PENDING
    result: Any = None
    error: Optional[Exception] = None
    attempts: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None

    @property
    def duration(self) -> Optional[float]:
        """Get step execution duration in seconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert step to dictionary representation."""
        return {
            "name": self.name,
            "status": self.status.name,
            "result": str(self.result)[:200] if self.result else None,
            "error": str(self.error) if self.error else None,
            "attempts": self.attempts,
            "duration": self.duration,
        }


@dataclass
class Workflow:
    """Represents a workflow definition."""
    name: str
    steps: List[WorkflowStep] = field(default_factory=list)
    max_parallel: int = 1
    allow_skip: bool = True
    continue_on_failure: bool = False
    status: WorkflowStatus = WorkflowStatus.PENDING
    context: Dict[str, Any] = field(default_factory=dict)
    errors: List[Exception] = field(default_factory=list)
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def duration(self) -> Optional[float]:
        """Get workflow execution duration in seconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None

    def add_step(self, name: str, handler: Callable[..., Any], **kwargs) -> WorkflowStep:
        """Add a step to the workflow."""
        step = WorkflowStep(name=name, handler=handler, kwargs=kwargs)
        self.steps.append(step)
        return step

    def get_step(self, name: str) -> Optional[WorkflowStep]:
        """Get a step by name."""
        for step in self.steps:
            if step.name == name:
                return step
        return None


class WorkflowEngine:
    """
    Workflow orchestration engine for executing multi-step automation pipelines.

    Features:
    - Sequential and parallel step execution
    - Step retry with configurable delays
    - Conditional step execution
    - Error recovery with fallback steps
    - Progress tracking and monitoring
    - Timeout handling
    - Context sharing between steps

    Example:
        >>> def step1(ctx): return {"value": 42}
        >>> def step2(ctx): return ctx["step1"]["value"] * 2
        >>> engine = WorkflowEngine()
        >>> wf = Workflow(name="test")
        >>> wf.add_step("s1", step1)
        >>> wf.add_step("s2", step2)
        >>> result = await engine.run(wf)
    """

    def __init__(
        self,
        max_concurrent: int = 4,
        default_timeout: float = 300.0,
        default_retry_delay: float = 1.0,
    ):
        """
        Initialize the workflow engine.

        Args:
            max_concurrent: Maximum number of steps to run in parallel
            default_timeout: Default timeout for each step in seconds
            default_retry_delay: Default delay between retries in seconds
        """
        self.max_concurrent = max_concurrent
        self.default_timeout = default_timeout
        self.default_retry_delay = default_retry_delay
        self._running: Set[str] = set()
        self._cancelled: Set[str] = set()
        self._paused: Set[str] = set()
        self._subscribers: List[Callable[[Workflow, WorkflowStep], None]] = []
        logger.info(
            f"WorkflowEngine initialized (max_concurrent={max_concurrent}, "
            f"default_timeout={default_timeout}s)"
        )

    def subscribe(self, callback: Callable[[Workflow, WorkflowStep], None]) -> None:
        """Subscribe to workflow execution events."""
        self._subscribers.append(callback)

    def _notify(self, workflow: Workflow, step: WorkflowStep) -> None:
        """Notify subscribers of workflow events."""
        for callback in self._subscribers:
            try:
                callback(workflow, step)
            except Exception as e:
                logger.warning(f"Subscriber callback failed: {e}")

    async def run(
        self,
        workflow: Workflow,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute a workflow.

        Args:
            workflow: The workflow to execute
            context: Initial context dictionary

        Returns:
            Final workflow context after all steps complete
        """
        workflow.context = context or {}
        workflow.status = WorkflowStatus.RUNNING
        workflow.start_time = time.time()
        self._running.add(workflow.id)
        logger.info(f"Starting workflow: {workflow.name} (id={workflow.id})")

        try:
            await self._execute_steps(workflow)
            if workflow.errors:
                workflow.status = WorkflowStatus.FAILED
                logger.error(
                    f"Workflow {workflow.name} completed with {len(workflow.errors)} errors"
                )
            else:
                workflow.status = WorkflowStatus.COMPLETED
                logger.info(
                    f"Workflow {workflow.name} completed successfully "
                    f"(duration={workflow.duration:.2f}s)"
                )
        except asyncio.CancelledError:
            workflow.status = WorkflowStatus.CANCELLED
            logger.warning(f"Workflow {workflow.name} cancelled")
            raise
        except Exception as e:
            workflow.status = WorkflowStatus.FAILED
            workflow.errors.append(e)
            logger.exception(f"Workflow {workflow.name} failed with exception")
        finally:
            workflow.end_time = time.time()
            self._running.discard(workflow.id)

        return workflow.context

    async def _execute_steps(self, workflow: Workflow) -> None:
        """Execute all steps in the workflow."""
        for step in workflow.steps:
            if workflow.id in self._cancelled:
                break

            while workflow.id in self._paused:
                await asyncio.sleep(0.5)

            await self._execute_step(workflow, step)

            if step.status == StepStatus.FAILED:
                if step.on_failure:
                    failure_step = workflow.get_step(step.on_failure)
                    if failure_step:
                        logger.info(f"Jumping to failure handler: {step.on_failure}")
                        await self._execute_step(workflow, failure_step)
                if not workflow.continue_on_failure:
                    break

    async def _execute_step(self, workflow: Workflow, step: WorkflowStep) -> None:
        """Execute a single workflow step with retry support."""
        if step.condition and not step.condition():
            step.status = StepStatus.SKIPPED
            logger.info(f"Step {step.name} skipped (condition not met)")
            return

        step.attempts += 1
        step.status = StepStatus.RUNNING
        step.start_time = time.time()
        self._notify(workflow, step)
        logger.debug(f"Executing step: {step.name} (attempt {step.attempts})")

        try:
            timeout = step.timeout or self.default_timeout
            result = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(
                    None, lambda: step.handler(*step.args, **{**workflow.context, **step.kwargs})
                ),
                timeout=timeout,
            )
            step.result = result
            workflow.context[step.name] = result
            step.status = StepStatus.COMPLETED
            step.error = None
            logger.debug(f"Step {step.name} completed successfully")

        except asyncio.TimeoutError:
            step.error = TimeoutError(f"Step {step.name} timed out after {timeout}s")
            step.status = StepStatus.FAILED
            logger.error(f"Step {step.name} timed out")
            workflow.errors.append(step.error)

        except Exception as e:
            step.error = e
            logger.warning(f"Step {step.name} failed: {e}")

            if step.attempts <= step.retry_count:
                step.status = StepStatus.RETRYING
                logger.info(
                    f"Retrying step {step.name} in {step.retry_delay}s "
                    f"(attempt {step.attempts}/{step.retry_count + 1})"
                )
                await asyncio.sleep(step.retry_delay)
                await self._execute_step(workflow, step)
            else:
                step.status = StepStatus.FAILED
                workflow.errors.append(e)

        finally:
            step.end_time = time.time()
            self._notify(workflow, step)

    def cancel(self, workflow_id: str) -> bool:
        """Cancel a running workflow."""
        if workflow_id in self._running:
            self._cancelled.add(workflow_id)
            logger.info(f"Workflow {workflow_id} cancellation requested")
            return True
        return False

    def pause(self, workflow_id: str) -> bool:
        """Pause a running workflow."""
        if workflow_id in self._running:
            self._paused.add(workflow_id)
            logger.info(f"Workflow {workflow_id} paused")
            return True
        return False

    def resume(self, workflow_id: str) -> bool:
        """Resume a paused workflow."""
        if workflow_id in self._paused:
            self._paused.discard(workflow_id)
            logger.info(f"Workflow {workflow_id} resumed")
            return True
        return False

    def get_status(self, workflow_id: str) -> Optional[WorkflowStatus]:
        """Get the status of a workflow."""
        if workflow_id in self._cancelled:
            return WorkflowStatus.CANCELLED
        if workflow_id in self._paused:
            return WorkflowStatus.PAUSED
        if workflow_id in self._running:
            return WorkflowStatus.RUNNING
        return None

    def list_running(self) -> List[str]:
        """List all running workflow IDs."""
        return list(self._running)


# Convenience function for synchronous workflow execution
def run_workflow_sync(
    workflow: Workflow,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Run a workflow synchronously (blocking).

    Args:
        workflow: The workflow to execute
        context: Initial context dictionary

    Returns:
        Final workflow context after all steps complete
    """
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop.run_until_complete(workflow.run(context=context))

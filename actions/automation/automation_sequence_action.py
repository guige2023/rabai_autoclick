"""
Automation Sequence Action Module.

Sequential task execution for automation workflows with dependency tracking,
step management, and rollback support.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class StepStatus(Enum):
    """Status of a sequence step."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ROLLED_BACK = "rolled_back"


@dataclass
class StepResult:
    """Result of a sequence step execution."""
    step_name: str
    success: bool
    output: Any = None
    error: Optional[str] = None
    started_at: float = 0.0
    completed_at: float = 0.0
    duration_ms: float = 0.0


@dataclass
class Step:
    """A single step in a sequence."""
    name: str
    func: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    required: bool = True
    retry_on_failure: bool = False
    max_retries: int = 3
    skip_if: Optional[Callable[[], bool]] = None
    rollback_func: Optional[Callable[[Any], None]] = None


class AutomationSequenceAction:
    """
    Sequential task execution with dependency management.

    Executes steps in order, respecting dependencies, with support
    for rollback on failure, skipping, and retry.

    Example:
        sequence = AutomationSequenceAction(name="deploy-pipeline")

        sequence.add_step("validate", validate_func)
        sequence.add_step("build", build_func, depends_on=["validate"])
        sequence.add_step("deploy", deploy_func, depends_on=["build"])

        results = await sequence.execute()
    """

    def __init__(
        self,
        name: str = "sequence",
        stop_on_failure: bool = True,
    ) -> None:
        self.name = name
        self.stop_on_failure = stop_on_failure
        self._steps: Dict[str, Step] = {}
        self._execution_order: List[str] = []
        self._results: Dict[str, StepResult] = {}
        self._running = False

    def add_step(
        self,
        name: str,
        func: Callable[..., T],
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        depends_on: Optional[List[str]] = None,
        required: bool = True,
        retry_on_failure: bool = False,
        max_retries: int = 3,
        skip_if: Optional[Callable[[], bool]] = None,
        rollback_func: Optional[Callable[[Any], None]] = None,
    ) -> "AutomationSequenceAction":
        """Add a step to the sequence."""
        self._steps[name] = Step(
            name=name,
            func=func,
            args=args,
            kwargs=kwargs or {},
            depends_on=depends_on or [],
            required=required,
            retry_on_failure=retry_on_failure,
            max_retries=max_retries,
            skip_if=skip_if,
            rollback_func=rollback_func,
        )
        return self

    def remove_step(self, name: str) -> bool:
        """Remove a step from the sequence."""
        if name in self._steps:
            del self._steps[name]
            return True
        return False

    def _resolve_order(self) -> List[str]:
        """Resolve step execution order based on dependencies (topological sort)."""
        visited: Dict[str, bool] = {name: False for name in self._steps}
        order: List[str] = []

        def visit(name: str) -> None:
            if visited[name]:
                return
            visited[name] = True
            step = self._steps[name]
            for dep in step.depends_on:
                if dep in self._steps:
                    visit(dep)
            order.append(name)

        for name in self._steps:
            visit(name)

        return order

    async def execute(
        self,
        context: Optional[Dict[str, Any]] = None,
    ) -> List[StepResult]:
        """Execute all steps in sequence."""
        self._results = {}
        self._execution_order = self._resolve_order()
        self._running = True
        ctx = context or {}

        logger.info(f"Starting sequence '{self.name}' with {len(self._execution_order)} steps")

        for step_name in self._execution_order:
            if not self._running:
                break

            step = self._steps[step_name]
            result = await self._execute_step(step_name, step, ctx)

            self._results[step_name] = result

            if not result.success:
                if step.required or self.stop_on_failure:
                    logger.error(f"Step '{step_name}' failed: {result.error}")
                    if self.stop_on_failure:
                        self._running = False
                        break
                else:
                    logger.warning(f"Optional step '{step_name}' failed, continuing")

        return list(self._results.values())

    async def _execute_step(
        self,
        step_name: str,
        step: Step,
        context: Dict[str, Any],
    ) -> StepResult:
        """Execute a single step with retries."""
        # Check skip condition
        if step.skip_if and step.skip_if():
            logger.info(f"Step '{step_name}' skipped")
            return StepResult(
                step_name=step_name,
                success=True,
                output=None,
            )

        # Check dependencies
        for dep_name in step.depends_on:
            if dep_name not in self._results:
                return StepResult(
                    step_name=step_name,
                    success=False,
                    error=f"Dependency '{dep_name}' not executed",
                )
            if not self._results[dep_name].success:
                return StepResult(
                    step_name=step_name,
                    success=False,
                    error=f"Dependency '{dep_name}' failed",
                )

        # Retry loop
        attempts = 0
        last_error: Optional[str] = None

        while attempts <= step.max_retries:
            attempts += 1
            started = time.time()

            try:
                logger.debug(f"Executing step '{step_name}' (attempt {attempts})")

                if asyncio.iscoroutinefunction(step.func):
                    output = await step.func(*step.args, **step.kwargs)
                else:
                    output = step.func(*step.args, **step.kwargs)

                completed = time.time()
                result = StepResult(
                    step_name=step_name,
                    success=True,
                    output=output,
                    started_at=started,
                    completed_at=completed,
                    duration_ms=(completed - started) * 1000,
                )

                # Store output in context for dependent steps
                context[step_name] = output

                logger.info(f"Step '{step_name}' completed in {result.duration_ms:.1f}ms")
                return result

            except Exception as e:
                last_error = str(e)
                logger.warning(f"Step '{step_name}' failed (attempt {attempts}): {e}")

                if attempts > step.max_retries:
                    break

                if step.retry_on_failure:
                    await asyncio.sleep(0.5 * attempts)  # Simple backoff

        completed = time.time()
        return StepResult(
            step_name=step_name,
            success=False,
            error=last_error,
            started_at=started,
            completed_at=completed,
            duration_ms=(completed - started) * 1000,
        )

    async def rollback(self) -> List[StepResult]:
        """Rollback completed steps in reverse order."""
        rollback_results: List[StepResult] = []

        for step_name in reversed(self._execution_order):
            if step_name not in self._results:
                continue

            result = self._results[step_name]
            if not result.success:
                continue

            step = self._steps[step_name]
            if not step.rollback_func:
                continue

            try:
                started = time.time()
                step.rollback_func(result.output)
                completed = time.time()

                rollback_results.append(StepResult(
                    step_name=f"{step_name}-rollback",
                    success=True,
                    duration_ms=(completed - started) * 1000,
                ))

                self._results[step_name].output = None
                logger.info(f"Rolled back step '{step_name}'")

            except Exception as e:
                logger.error(f"Rollback of '{step_name}' failed: {e}")

        return rollback_results

    def get_results(self) -> Dict[str, StepResult]:
        """Get results of all executed steps."""
        return self._results.copy()

    def get_result(self, step_name: str) -> Optional[StepResult]:
        """Get result of a specific step."""
        return self._results.get(step_name)

    def is_running(self) -> bool:
        """Check if sequence is still running."""
        return self._running

    def get_completed_steps(self) -> List[str]:
        """Get list of successfully completed step names."""
        return [
            name for name, result in self._results.items()
            if result.success
        ]

    def get_failed_steps(self) -> List[str]:
        """Get list of failed step names."""
        return [
            name for name, result in self._results.items()
            if not result.success
        ]

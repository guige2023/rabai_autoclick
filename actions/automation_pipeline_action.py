"""
Automation Pipeline Action Module.

Chains automation steps into pipelines with error handling,
 branching, and conditional execution support.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class StepStatus(Enum):
    """Status of a pipeline step."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


@dataclass
class PipelineStep:
    """A single step in the automation pipeline."""
    name: str
    func: Callable[..., Any]
    args: tuple[Any, ...] = field(default_factory=tuple)
    kwargs: dict[str, Any] = field(default_factory=dict)
    condition: Optional[Callable[..., bool]] = None
    retry_count: int = 0
    timeout: Optional[float] = None
    on_failure: Optional[str] = None
    continue_on_failure: bool = False


@dataclass
class StepResult:
    """Result of executing a pipeline step."""
    name: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    attempts: int = 1


@dataclass
class PipelineResult:
    """Result of a complete pipeline execution."""
    success: bool
    step_results: list[StepResult]
    total_duration_ms: float = 0.0
    failed_step: Optional[str] = None
    outputs: dict[str, Any] = field(default_factory=dict)


class AutomationPipelineAction:
    """
    Pipeline executor for chained automation steps.

    Supports sequential execution, conditional steps, retry logic,
    and configurable error handling strategies.

    Example:
        pipeline = AutomationPipelineAction()
        pipeline.add_step("login", login_func)
        pipeline.add_step("scrape", scrape_func, condition=lambda ctx: ctx["logged_in"])
        pipeline.add_step("logout", logout_func, on_failure="scrape")
        result = await pipeline.execute(context={"logged_in": False})
    """

    def __init__(
        self,
        name: str = "pipeline",
        continue_on_failure: bool = False,
        max_step_retries: int = 3,
    ) -> None:
        self.name = name
        self.continue_on_failure = continue_on_failure
        self.max_step_retries = max_step_retries
        self._steps: list[PipelineStep] = []
        self._step_map: dict[str, PipelineStep] = {}

    def add_step(
        self,
        name: str,
        func: Callable[..., Any],
        args: tuple[Any, ...] = (),
        kwargs: Optional[dict[str, Any]] = None,
        condition: Optional[Callable[..., bool]] = None,
        retry_count: int = 0,
        timeout: Optional[float] = None,
        on_failure: Optional[str] = None,
        continue_on_failure: bool = False,
    ) -> "AutomationPipelineAction":
        """Add a step to the pipeline."""
        step = PipelineStep(
            name=name,
            func=func,
            args=args,
            kwargs=kwargs or {},
            condition=condition,
            retry_count=retry_count,
            timeout=timeout,
            on_failure=on_failure,
            continue_on_failure=continue_on_failure,
        )
        self._steps.append(step)
        self._step_map[name] = step
        return self

    def add_branch(
        self,
        name: str,
        steps: list[tuple[str, Callable]],
        condition: Callable[..., bool],
    ) -> "AutomationPipelineAction":
        """Add a conditional branch of steps."""
        for step_name, func in steps:
            self.add_step(
                name=f"{name}.{step_name}",
                func=func,
                condition=condition,
            )
        return self

    async def execute(
        self,
        context: Optional[dict[str, Any]] = None,
        initial_input: Any = None,
    ) -> PipelineResult:
        """Execute the pipeline from start to finish."""
        import time
        start_time = time.monotonic()
        context = context or {}
        context["_pipeline_input"] = initial_input
        context["_pipeline_outputs"] = {}

        step_results: list[StepResult] = []
        failed_step: Optional[str] = None
        should_stop = False

        for step in self._steps:
            if should_stop and not step.continue_on_failure:
                result = StepResult(
                    name=step.name,
                    status=StepStatus.SKIPPED,
                    output=None,
                )
                step_results.append(result)
                continue

            if step.condition and not self._evaluate_condition(step.condition, context):
                logger.debug(f"Step {step.name} skipped: condition not met")
                result = StepResult(
                    name=step.name,
                    status=StepStatus.SKIPPED,
                    output=None,
                )
                step_results.append(result)
                continue

            step_start = time.monotonic()
            result = await self._execute_step(step, context)
            result.duration_ms = (time.monotonic() - step_start) * 1000
            step_results.append(result)

            context["_pipeline_outputs"][step.name] = result.output

            if result.status == StepStatus.FAILED:
                failed_step = step.name
                if step.on_failure:
                    await self._handle_failure(step.on_failure, context)
                if not step.continue_on_failure and not self.continue_on_failure:
                    should_stop = True

        success = failed_step is None

        return PipelineResult(
            success=success,
            step_results=step_results,
            total_duration_ms=(time.monotonic() - start_time) * 1000,
            failed_step=failed_step,
            outputs=context.get("_pipeline_outputs", {}),
        )

    async def _execute_step(
        self,
        step: PipelineStep,
        context: dict[str, Any],
    ) -> StepResult:
        """Execute a single step with retry logic."""
        import time

        for attempt in range(1, step.retry_count + 2):
            try:
                func = step.func
                args = step.args
                kwargs = step.kwargs.copy()
                kwargs["context"] = context

                if asyncio.iscoroutinefunction(func):
                    output = await func(*args, **kwargs)
                else:
                    output = func(*args, **kwargs)

                return StepResult(
                    name=step.name,
                    status=StepStatus.SUCCESS,
                    output=output,
                    attempts=attempt,
                )

            except Exception as e:
                logger.warning(f"Step {step.name} attempt {attempt} failed: {e}")
                if attempt < step.retry_count + 1:
                    await asyncio.sleep(0.5 * attempt)
                    continue

                return StepResult(
                    name=step.name,
                    status=StepStatus.FAILED,
                    error=str(e),
                    attempts=attempt,
                )

        return StepResult(name=step.name, status=StepStatus.FAILED)

    def _evaluate_condition(
        self,
        condition: Callable[..., bool],
        context: dict[str, Any],
    ) -> bool:
        """Evaluate a condition function."""
        try:
            return condition(context)
        except Exception as e:
            logger.warning(f"Condition evaluation failed: {e}")
            return False

    async def _handle_failure(
        self,
        target_step: str,
        context: dict[str, Any],
    ) -> None:
        """Handle step failure by executing recovery steps."""
        if target_step in self._step_map:
            step = self._step_map[target_step]
            logger.info(f"Executing failure handler: {target_step}")
            result = await self._execute_step(step, context)
            context["_pipeline_outputs"][step.name] = result.output


import asyncio

"""Automation Pipeline Action Module.

Provides pipeline execution framework for chaining automation steps
with support for parallel execution, error handling, and result passing.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class PipelineState(Enum):
    """Pipeline execution states."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepType(Enum):
    """Pipeline step types."""
    TASK = "task"
    PARALLEL = "parallel"
    MAP = "map"
    FILTER = "filter"
    REDUCE = "reduce"
    BRANCH = "branch"


@dataclass
class PipelineStep:
    """Represents a single step in a pipeline."""
    step_id: str
    name: str
    step_type: StepType = StepType.TASK
    handler: Optional[Callable] = None
    args: tuple = ()
    kwargs: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)
    timeout: Optional[float] = None
    retry_count: int = 0
    retry_delay: float = 1.0
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StepResult:
    """Result of a step execution."""
    step_id: str
    success: bool
    output: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    attempts: int = 1


@dataclass
class PipelineResult:
    """Result of a pipeline execution."""
    pipeline_id: str
    success: bool
    state: PipelineState
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    output: Any = None
    error: Optional[str] = None
    total_duration_ms: float = 0.0


class StepExecutor:
    """Executes individual pipeline steps."""

    def __init__(self):
        pass

    async def execute(
        self,
        step: PipelineStep,
        context: Dict[str, Any]
    ) -> StepResult:
        """Execute a single step."""
        start = time.time()
        attempts = 0
        last_error = None

        while attempts <= step.retry_count:
            attempts += 1
            try:
                if step.timeout:
                    result = await asyncio.wait_for(
                        self._execute_step(step, context),
                        timeout=step.timeout
                    )
                else:
                    result = await self._execute_step(step, context)

                return StepResult(
                    step_id=step.step_id,
                    success=True,
                    output=result,
                    duration_ms=(time.time() - start) * 1000,
                    attempts=attempts
                )

            except asyncio.TimeoutError:
                last_error = f"Step timed out after {step.timeout}s"
                logger.warning(f"Step {step.name} timed out")
            except Exception as e:
                last_error = str(e)
                logger.exception(f"Step {step.name} failed: {e}")

            if attempts <= step.retry_count:
                await asyncio.sleep(step.retry_delay * attempts)

        return StepResult(
            step_id=step.step_id,
            success=False,
            error=last_error,
            duration_ms=(time.time() - start) * 1000,
            attempts=attempts
        )

    async def _execute_step(
        self,
        step: PipelineStep,
        context: Dict[str, Any]
    ) -> Any:
        """Execute the step handler."""
        if step.handler is None:
            return context.get(step.step_id)

        if asyncio.iscoroutinefunction(step.handler):
            return await step.handler(*step.args, **{**step.kwargs, "context": context})
        else:
            return step.handler(*step.args, **{**step.kwargs, "context": context})


class Pipeline:
    """Represents an automation pipeline."""

    def __init__(self, pipeline_id: str, name: str):
        self._pipeline_id = pipeline_id
        self._name = name
        self._steps: Dict[str, PipelineStep] = {}
        self._step_order: List[str] = []
        self._state = PipelineState.IDLE
        self._results: Dict[str, StepResult] = {}
        self._executor = StepExecutor()

    @property
    def pipeline_id(self) -> str:
        return self._pipeline_id

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> PipelineState:
        return self._state

    def add_step(self, step: PipelineStep) -> None:
        """Add a step to the pipeline."""
        self._steps[step.step_id] = step
        if step.step_id not in self._step_order:
            self._step_order.append(step.step_id)

    def remove_step(self, step_id: str) -> bool:
        """Remove a step from the pipeline."""
        if step_id in self._steps:
            del self._steps[step_id]
            self._step_order.remove(step_id)
            return True
        return False

    def get_step(self, step_id: str) -> Optional[PipelineStep]:
        """Get a step by ID."""
        return self._steps.get(step_id)

    def get_execution_order(self) -> List[str]:
        """Get steps in topological order."""
        visited = set()
        order = []

        def visit(step_id: str):
            if step_id in visited:
                return
            visited.add(step_id)
            step = self._steps.get(step_id)
            if step:
                for dep in step.depends_on:
                    visit(dep)
                order.append(step_id)

        for step_id in self._step_order:
            visit(step_id)

        return order

    async def execute(
        self,
        initial_context: Optional[Dict[str, Any]] = None
    ) -> PipelineResult:
        """Execute the pipeline."""
        self._state = PipelineState.RUNNING
        self._results = {}
        start_time = time.time()
        context = initial_context or {}

        try:
            exec_order = self.get_execution_order()

            for step_id in exec_order:
                step = self._steps.get(step_id)
                if not step or not step.enabled:
                    continue

                # Check dependencies
                deps_satisfied = all(
                    self._results.get(dep_id, StepResult(dep_id, False)).success
                    for dep_id in step.depends_on
                )

                if not deps_satisfied:
                    self._state = PipelineState.FAILED
                    return PipelineResult(
                        pipeline_id=self._pipeline_id,
                        success=False,
                        state=self._state,
                        step_results=self._results,
                        error=f"Dependencies not satisfied for step {step_id}",
                        total_duration_ms=(time.time() - start_time) * 1000
                    )

                # Execute step
                if step.step_type == StepType.PARALLEL:
                    result = await self._execute_parallel(step, context)
                elif step.step_type == StepType.MAP:
                    result = await self._execute_map(step, context)
                else:
                    result = await self._executor.execute(step, context)

                self._results[step_id] = result
                context[step_id] = result.output

                if not result.success:
                    self._state = PipelineState.FAILED
                    return PipelineResult(
                        pipeline_id=self._pipeline_id,
                        success=False,
                        state=self._state,
                        step_results=self._results,
                        error=f"Step {step_id} failed: {result.error}",
                        total_duration_ms=(time.time() - start_time) * 1000
                    )

            self._state = PipelineState.COMPLETED
            return PipelineResult(
                pipeline_id=self._pipeline_id,
                success=True,
                state=self._state,
                step_results=self._results,
                output=context,
                total_duration_ms=(time.time() - start_time) * 1000
            )

        except Exception as e:
            self._state = PipelineState.FAILED
            logger.exception(f"Pipeline {self._name} failed: {e}")
            return PipelineResult(
                pipeline_id=self._pipeline_id,
                success=False,
                state=self._state,
                step_results=self._results,
                error=str(e),
                total_duration_ms=(time.time() - start_time) * 1000
            )

    async def _execute_parallel(
        self,
        step: PipelineStep,
        context: Dict[str, Any]
    ) -> StepResult:
        """Execute steps in parallel."""
        if not step.kwargs.get("steps"):
            return StepResult(step_id=step.step_id, success=True, output=[])

        start = time.time()
        tasks = []
        for sub_step in step.kwargs["steps"]:
            tasks.append(self._executor.execute(sub_step, context))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        duration = (time.time() - start) * 1000

        errors = [r for r in results if isinstance(r, Exception)]
        outputs = [r.output if hasattr(r, 'output') else r for r in results if not isinstance(r, Exception)]

        return StepResult(
            step_id=step.step_id,
            success=len(errors) == 0,
            output=outputs,
            error=str(errors) if errors else None,
            duration_ms=duration
        )

    async def _execute_map(
        self,
        step: PipelineStep,
        context: Dict[str, Any]
    ) -> StepResult:
        """Execute a map operation over items."""
        items = step.kwargs.get("items", [])
        handler = step.handler

        if not handler:
            return StepResult(step_id=step.step_id, success=True, output=items)

        start = time.time()
        results = []

        for item in items:
            try:
                result = handler(item, context=context)
                if asyncio.iscoroutine(result):
                    result = await result
                results.append(result)
            except Exception as e:
                logger.exception(f"Map operation failed for item: {e}")
                results.append(None)

        return StepResult(
            step_id=step.step_id,
            success=True,
            output=results,
            duration_ms=(time.time() - start) * 1000
        )


class AutomationPipelineAction:
    """Main action class for automation pipelines."""

    def __init__(self):
        self._pipelines: Dict[str, Pipeline] = {}

    def create_pipeline(self, name: str) -> Pipeline:
        """Create a new pipeline."""
        pipeline_id = str(uuid.uuid4())[:8]
        pipeline = Pipeline(pipeline_id, name)
        self._pipelines[pipeline_id] = pipeline
        return pipeline

    def get_pipeline(self, pipeline_id: str) -> Optional[Pipeline]:
        """Get a pipeline by ID."""
        return self._pipelines.get(pipeline_id)

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the automation pipeline action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - pipeline_id: Pipeline ID
                - Other operation-specific fields

        Returns:
            Dictionary with execution results.
        """
        operation = context.get("operation", "run")

        if operation == "create":
            pipeline = self.create_pipeline(context.get("name", "pipeline"))
            return {
                "success": True,
                "pipeline_id": pipeline.pipeline_id,
                "name": pipeline.name
            }

        elif operation == "add_step":
            pipeline_id = context.get("pipeline_id", "")
            pipeline = self.get_pipeline(pipeline_id)
            if not pipeline:
                return {"success": False, "error": "Pipeline not found"}

            step = PipelineStep(
                step_id=context.get("step_id", str(uuid.uuid4())[:8]),
                name=context.get("name", "step"),
                step_type=StepType(context.get("type", "task"))
            )
            pipeline.add_step(step)
            return {
                "success": True,
                "step_id": step.step_id
            }

        elif operation == "run":
            pipeline_id = context.get("pipeline_id", "")
            pipeline = self.get_pipeline(pipeline_id)
            if not pipeline:
                return {"success": False, "error": "Pipeline not found"}

            result = await pipeline.execute(context.get("context", {}))
            return {
                "success": result.success,
                "state": result.state.value,
                "step_results": {
                    sid: {"success": r.success, "output": r.output, "error": r.error}
                    for sid, r in result.step_results.items()
                },
                "total_duration_ms": round(result.total_duration_ms, 2)
            }

        elif operation == "list":
            pipelines = [
                {"pipeline_id": p.pipeline_id, "name": p.name, "state": p.state.value}
                for p in self._pipelines.values()
            ]
            return {"success": True, "pipelines": pipelines}

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

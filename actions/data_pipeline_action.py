"""
Data Pipeline Action Module

Provides data processing pipeline framework with support for
parallel execution, error handling, and checkpoint/resume.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import enum
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, Generic, List, Optional, TypeVar

import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")
U = TypeVar("U")


class PipelineState(enum.Enum):
    """Possible states of a pipeline."""

    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepType(enum.Enum):
    """Types of pipeline steps."""

    TRANSFORM = "transform"
    FILTER = "filter"
    BRANCH = "branch"
    MERGE = "merge"
    MAP = "map"
    REDUCE = "reduce"
    PARALLEL = "parallel"
    CONDITIONAL = "conditional"


@dataclass
class StepMetrics:
    """Metrics for a single pipeline step."""

    step_id: str
    name: str
    execution_count: int = 0
    total_duration_ms: float = 0.0
    min_duration_ms: float = float("inf")
    max_duration_ms: float = 0.0
    error_count: int = 0
    last_executed: Optional[float] = None

    @property
    def avg_duration_ms(self) -> float:
        """Average execution duration."""
        if self.execution_count == 0:
            return 0.0
        return self.total_duration_ms / self.execution_count

    def record_execution(self, duration_ms: float, error: bool = False) -> None:
        """Record a step execution."""
        self.execution_count += 1
        self.total_duration_ms += duration_ms
        self.min_duration_ms = min(self.min_duration_ms, duration_ms)
        self.max_duration_ms = max(self.max_duration_ms, duration_ms)
        self.last_executed = time.time()
        if error:
            self.error_count += 1


@dataclass
class PipelineMetrics:
    """Metrics for an entire pipeline execution."""

    pipeline_id: str
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    state: PipelineState = PipelineState.IDLE
    items_processed: int = 0
    items_succeeded: int = 0
    items_failed: int = 0
    step_metrics: Dict[str, StepMetrics] = field(default_factory=dict)
    total_wait_time_ms: float = 0.0

    @property
    def duration_ms(self) -> float:
        """Total pipeline duration."""
        if self.started_at is None:
            return 0.0
        end = self.completed_at or time.time()
        return (end - self.started_at) * 1000

    @property
    def success_rate(self) -> float:
        """Percentage of items processed successfully."""
        if self.items_processed == 0:
            return 0.0
        return (self.items_succeeded / self.items_processed) * 100

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "pipeline_id": self.pipeline_id,
            "state": self.state.value,
            "duration_ms": self.duration_ms,
            "items_processed": self.items_processed,
            "items_succeeded": self.items_succeeded,
            "items_failed": self.items_failed,
            "success_rate": self.success_rate,
            "step_metrics": {
                k: {
                    "name": v.name,
                    "executions": v.execution_count,
                    "avg_ms": v.avg_duration_ms,
                    "errors": v.error_count,
                }
                for k, v in self.step_metrics.items()
            },
        }


class PipelineStep(Generic[T, U]):
    """
    A single step in a data processing pipeline.

    Each step processes input data and produces output
    for the next step or final result.
    """

    def __init__(
        self,
        step_id: str,
        name: str,
        step_type: StepType,
        handler: Callable[[T], Awaitable[U]],
        error_handler: Optional[Callable[[T, Exception], Awaitable[U]]] = None,
        skip_on_error: bool = False,
        timeout: Optional[float] = None,
    ) -> None:
        self.step_id = step_id
        self.name = name
        self.step_type = step_type
        self.handler = handler
        self.error_handler = error_handler
        self.skip_on_error = skip_on_error
        self.timeout = timeout
        self._metrics = StepMetrics(step_id=step_id, name=name)

    @property
    def metrics(self) -> StepMetrics:
        """Get step metrics."""
        return self._metrics

    async def execute(self, input_data: T) -> U:
        """
        Execute the step with given input.

        Args:
            input_data: Data to process

        Returns:
            Processed output data

        Raises:
            Exception: If step fails and no error handler exists
        """
        start_time = time.time()
        error_occurred = False

        try:
            if self.timeout:
                result = await asyncio.wait_for(
                    self.handler(input_data),
                    timeout=self.timeout,
                )
            else:
                result = await self.handler(input_data)
        except Exception as e:
            error_occurred = True
            duration_ms = (time.time() - start_time) * 1000
            self._metrics.record_execution(duration_ms, error=True)

            if self.error_handler:
                logger.warning(f"Step {self.name} failed, using error handler: {e}")
                return await self.error_handler(input_data)

            if self.skip_on_error:
                logger.warning(f"Step {self.name} failed, skipping: {e}")
                raise

            logger.error(f"Step {self.name} failed: {e}")
            raise

        duration_ms = (time.time() - start_time) * 1000
        self._metrics.record_execution(duration_ms, error=error_occurred)

        return result  # type: ignore


class DataPipeline(Generic[T, U]):
    """
    Configurable data processing pipeline.

    Supports:
    - Sequential and parallel step execution
    - Error handling and recovery
    - Checkpointing for resume capability
    - Metrics collection
    - Conditional branching
    """

    def __init__(
        self,
        pipeline_id: Optional[str] = None,
        name: str = "DataPipeline",
    ) -> None:
        self._pipeline_id = pipeline_id or str(uuid.uuid4())
        self._name = name
        self._steps: List[PipelineStep[Any, Any]] = []
        self._state = PipelineState.IDLE
        self._metrics = PipelineMetrics(pipeline_id=self._pipeline_id)
        self._checkpoint_data: Dict[str, Any] = {}
        self._cancellation_event = asyncio.Event()

    @property
    def pipeline_id(self) -> str:
        """Get pipeline ID."""
        return self._pipeline_id

    @property
    def name(self) -> str:
        """Get pipeline name."""
        return self._name

    @property
    def state(self) -> PipelineState:
        """Get current pipeline state."""
        return self._state

    @property
    def metrics(self) -> PipelineMetrics:
        """Get pipeline metrics."""
        return self._metrics

    def add_step(
        self,
        step: PipelineStep[Any, Any],
    ) -> DataPipeline[T, U]:
        """
        Add a step to the pipeline.

        Args:
            step: PipelineStep to add

        Returns:
            Self for method chaining
        """
        self._steps.append(step)
        self._metrics.step_metrics[step.step_id] = step._metrics
        return self

    def add_transform_step(
        self,
        name: str,
        handler: Callable[[T], Awaitable[U]],
        **kwargs: Any,
    ) -> DataPipeline[T, U]:
        """
        Add a transform step.

        Args:
            name: Step name
            handler: Transform function
            **kwargs: Additional step options

        Returns:
            Self for method chaining
        """
        step = PipelineStep(
            step_id=str(uuid.uuid4()),
            name=name,
            step_type=StepType.TRANSFORM,
            handler=handler,  # type: ignore
            **kwargs,
        )
        return self.add_step(step)  # type: ignore

    def add_filter_step(
        self,
        name: str,
        predicate: Callable[[T], Awaitable[bool]],
        **kwargs: Any,
    ) -> DataPipeline[T, U]:
        """
        Add a filter step.

        Args:
            name: Step name
            predicate: Filter predicate, returns True to keep item
            **kwargs: Additional step options

        Returns:
            Self for method chaining
        """
        async def filter_handler(data: T) -> T:
            if await predicate(data):
                return data
            raise StopAsyncIteration()

        step = PipelineStep(
            step_id=str(uuid.uuid4()),
            name=name,
            step_type=StepType.FILTER,
            handler=filter_handler,  # type: ignore
            skip_on_error=True,
            **kwargs,
        )
        return self.add_step(step)  # type: ignore

    async def execute(
        self,
        input_data: T,
        checkpoint_key: Optional[str] = None,
    ) -> U:
        """
        Execute the pipeline with given input.

        Args:
            input_data: Initial input data
            checkpoint_key: Optional checkpoint key for resume

        Returns:
            Final pipeline output

        Raises:
            PipelineError: If pipeline execution fails
        """
        self._state = PipelineState.RUNNING
        self._metrics.started_at = time.time()
        self._metrics.state = PipelineState.RUNNING
        self._cancellation_event.clear()

        current_data: Any = input_data
        start_from_step = 0

        # Check for checkpoint resume
        if checkpoint_key and checkpoint_key in self._checkpoint_data:
            checkpoint = self._checkpoint_data[checkpoint_key]
            start_from_step = checkpoint.get("step_index", 0)
            current_data = checkpoint.get("data", input_data)
            logger.info(
                f"Resuming pipeline from step {start_from_step} "
                f"using checkpoint {checkpoint_key}"
            )

        try:
            for i, step in enumerate(self._steps[start_from_step:], start=start_from_step):
                # Check for cancellation
                if self._cancellation_event.is_set():
                    self._state = PipelineState.CANCELLED
                    self._metrics.state = PipelineState.CANCELLED
                    raise asyncio.CancelledError("Pipeline cancelled")

                # Check for pause
                while self._state == PipelineState.PAUSED:
                    await asyncio.sleep(0.1)

                logger.debug(f"Executing step {i + 1}/{len(self._steps)}: {step.name}")
                current_data = await step.execute(current_data)
                self._metrics.items_processed += 1

                # Auto-checkpoint
                if checkpoint_key and (i + 1) % 5 == 0:
                    self._checkpoint_data[checkpoint_key] = {
                        "step_index": i + 1,
                        "data": current_data,
                        "timestamp": time.time(),
                    }

            self._state = PipelineState.COMPLETED
            self._metrics.state = PipelineState.COMPLETED
            self._metrics.completed_at = time.time()
            self._metrics.items_succeeded = self._metrics.items_processed

            return current_data  # type: ignore

        except asyncio.CancelledError:
            self._state = PipelineState.CANCELLED
            self._metrics.state = PipelineState.CANCELLED
            raise

        except Exception as e:
            self._state = PipelineState.FAILED
            self._metrics.state = PipelineState.FAILED
            self._metrics.completed_at = time.time()
            self._metrics.items_failed += 1
            logger.error(f"Pipeline failed at step {start_from_step}: {e}")
            raise

    async def execute_parallel(
        self,
        input_items: List[T],
        max_concurrency: int = 5,
        checkpoint_key: Optional[str] = None,
    ) -> List[U]:
        """
        Execute pipeline in parallel on multiple input items.

        Args:
            input_items: List of items to process
            max_concurrency: Maximum concurrent executions
            checkpoint_key: Optional checkpoint key

        Returns:
            List of results in same order as input
        """
        self._state = PipelineState.RUNNING
        self._metrics.started_at = time.time()
        self._metrics.state = PipelineState.RUNNING
        self._metrics.items_processed = len(input_items)

        semaphore = asyncio.Semaphore(max_concurrency)
        results: List[Optional[U]] = [None] * len(input_items)
        errors: List[Optional[Exception]] = [None] * len(input_items)

        async def process_item(
            index: int,
            item: T,
        ) -> None:
            async with semaphore:
                try:
                    result = await self.execute(item, checkpoint_key)
                    results[index] = result
                    self._metrics.items_succeeded += 1
                except Exception as e:
                    errors[index] = e
                    self._metrics.items_failed += 1
                    logger.error(f"Parallel processing failed for item {index}: {e}")

        await asyncio.gather(
            *[process_item(i, item) for i, item in enumerate(input_items)],
            return_exceptions=True,
        )

        self._state = PipelineState.COMPLETED
        self._metrics.state = PipelineState.COMPLETED
        self._metrics.completed_at = time.time()

        # Raise first error if any failed
        first_error = next((e for e in errors if e is not None), None)
        if first_error:
            raise first_error  # type: ignore

        return results  # type: ignore

    def pause(self) -> None:
        """Pause pipeline execution."""
        if self._state == PipelineState.RUNNING:
            self._state = PipelineState.PAUSED
            self._metrics.state = PipelineState.PAUSED
            logger.info("Pipeline paused")

    def resume(self) -> None:
        """Resume paused pipeline."""
        if self._state == PipelineState.PAUSED:
            self._state = PipelineState.RUNNING
            self._metrics.state = PipelineState.RUNNING
            logger.info("Pipeline resumed")

    def cancel(self) -> None:
        """Cancel pipeline execution."""
        self._cancellation_event.set()
        logger.info("Pipeline cancellation requested")

    def set_checkpoint(self, key: str, data: Any) -> None:
        """
        Set a manual checkpoint.

        Args:
            key: Checkpoint identifier
            data: Data to checkpoint
        """
        self._checkpoint_data[key] = {
            "data": data,
            "timestamp": time.time(),
        }

    def get_checkpoint(self, key: str) -> Optional[Any]:
        """Get checkpoint data."""
        checkpoint = self._checkpoint_data.get(key)
        return checkpoint.get("data") if checkpoint else None

    def clear_checkpoints(self) -> None:
        """Clear all checkpoint data."""
        self._checkpoint_data.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics."""
        return self._metrics.to_dict()

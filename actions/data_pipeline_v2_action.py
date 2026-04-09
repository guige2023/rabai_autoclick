"""Data Pipeline v2 with parallel stages and error recovery.

This module provides a robust data pipeline with:
- Parallel stage execution
- Automatic retry and recovery
- Progress tracking and checkpoints
- Stage dependency management
- Error aggregation
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Awaitable, TypeVar, Generic

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class StageStatus(Enum):
    """Status of a pipeline stage."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class PipelineStrategy(Enum):
    """Execution strategy for pipeline stages."""

    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    PIPELINE = "pipeline"  # output of one feeds into next
    DAG = "dag"  # dependency graph


@dataclass
class StageResult(Generic[T]):
    """Result of a pipeline stage."""

    stage_name: str
    status: StageStatus
    data: T | None = None
    error: Exception | None = None
    duration: float = 0.0
    retry_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def is_success(self) -> bool:
        return self.status == StageStatus.COMPLETED


@dataclass
class PipelineStage(Generic[T, R]):
    """A single stage in the data pipeline."""

    name: str
    transform: Callable[[T], Awaitable[R]] | Callable[[T], R]
    input_type: type[T] | None = None
    output_type: type[R] | None = None
    retry_count: int = 3
    retry_delay: float = 1.0
    timeout: float | None = None
    condition: Callable[[Any], bool] | None = None  # skip if returns False
    on_error: Callable[[Exception, Any], Any] | None = None
    parallel: bool = False
    depends_on: list[str] = field(default_factory=list)

    async def execute(self, input_data: T) -> StageResult[R]:
        """Execute the stage with retry logic.

        Args:
            input_data: Input data for this stage

        Returns:
            StageResult with output data or error
        """
        start_time = time.time()
        last_error: Exception | None = None

        for attempt in range(self.retry_count + 1):
            try:
                if attempt > 0:
                    logger.info(f"Retry {attempt} for stage {self.name}")
                    await asyncio.sleep(self.retry_delay * (2 ** (attempt - 1)))

                # Check condition
                if self.condition and not self.condition(input_data):
                    return StageResult(
                        stage_name=self.name,
                        status=StageStatus.SKIPPED,
                        duration=time.time() - start_time,
                        metadata={"reason": "condition_not_met"},
                    )

                # Execute with optional timeout
                if self.timeout:
                    result = await asyncio.wait_for(
                        self._run_transform(input_data),
                        timeout=self.timeout,
                    )
                else:
                    result = await self._run_transform(input_data)

                return StageResult(
                    stage_name=self.name,
                    status=StageStatus.COMPLETED,
                    data=result,
                    duration=time.time() - start_time,
                    retry_count=attempt,
                )

            except asyncio.TimeoutError:
                last_error = TimeoutError(f"Stage {self.name} timed out after {self.timeout}s")
                logger.warning(f"Stage {self.name} timed out on attempt {attempt + 1}")

            except Exception as e:
                last_error = e
                logger.warning(f"Stage {self.name} failed on attempt {attempt + 1}: {e}")

                if self.on_error:
                    try:
                        input_data = self.on_error(e, input_data)
                    except Exception:
                        pass

        return StageResult(
            stage_name=self.name,
            status=StageStatus.FAILED,
            error=last_error,
            duration=time.time() - start_time,
            retry_count=self.retry_count,
        )

    async def _run_transform(self, input_data: T) -> R:
        """Run the transform function."""
        if asyncio.iscoroutinefunction(self.transform):
            return await self.transform(input_data)
        return self.transform(input_data)


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution."""

    strategy: PipelineStrategy = PipelineStrategy.SEQUENTIAL
    max_parallel: int = 4
    stop_on_error: bool = False
    checkpoint_enabled: bool = True
    checkpoint_interval: int = 5
    metrics_enabled: bool = True


@dataclass
class PipelineMetrics:
    """Pipeline execution metrics."""

    total_stages: int = 0
    completed_stages: int = 0
    failed_stages: int = 0
    skipped_stages: int = 0
    total_duration: float = 0.0
    stage_durations: dict[str, float] = field(default_factory=dict)
    errors: list[dict[str, Any]] = field(default_factory=list)


class DataPipelineV2(Generic[T]):
    """Advanced data pipeline with parallel execution and error recovery."""

    def __init__(
        self,
        name: str,
        config: PipelineConfig | None = None,
    ):
        """Initialize the pipeline.

        Args:
            name: Pipeline name
            config: Pipeline configuration
        """
        self.name = name
        self.config = config or PipelineConfig()
        self.stages: list[PipelineStage[Any, Any]] = []
        self.metrics = PipelineMetrics()
        self._results: dict[str, Any] = {}
        self._running = False

    def add_stage(
        self,
        name: str,
        transform: Callable[[Any], Any] | Callable[[Any], Awaitable[Any]],
        retry_count: int = 3,
        retry_delay: float = 1.0,
        timeout: float | None = None,
        condition: Callable[[Any], bool] | None = None,
        parallel: bool = False,
        depends_on: list[str] | None = None,
    ) -> "DataPipelineV2":
        """Add a stage to the pipeline.

        Args:
            name: Stage name (must be unique)
            transform: Transform function
            retry_count: Number of retries on failure
            retry_delay: Base delay between retries
            timeout: Optional stage timeout
            condition: Optional condition to skip stage
            parallel: Run in parallel with other parallel stages
            depends_on: List of stage names this depends on

        Returns:
            Self for chaining
        """
        stage = PipelineStage(
            name=name,
            transform=transform,
            retry_count=retry_count,
            retry_delay=retry_delay,
            timeout=timeout,
            condition=condition,
            parallel=parallel,
            depends_on=depends_on or [],
        )
        self.stages.append(stage)
        return self

    async def execute(self, input_data: T) -> tuple[bool, Any]:
        """Execute the pipeline.

        Args:
            input_data: Initial input data

        Returns:
            Tuple of (success, final_data)
        """
        self._running = True
        self._results = {"_input": input_data}
        self.metrics = PipelineMetrics(total_stages=len(self.stages))
        start_time = time.time()

        try:
            if self.config.strategy == PipelineStrategy.SEQUENTIAL:
                success = await self._execute_sequential(input_data)
            elif self.config.strategy == PipelineStrategy.PARALLEL:
                success = await self._execute_parallel(input_data)
            elif self.config.strategy == PipelineStrategy.PIPELINE:
                success = await self._execute_pipeline(input_data)
            elif self.config.strategy == PipelineStrategy.DAG:
                success = await self._execute_dag(input_data)
            else:
                success = await self._execute_sequential(input_data)

            self.metrics.total_duration = time.time() - start_time
            return success, self._results.get("_output", self._results.get("_last"))

        finally:
            self._running = False

    async def _execute_sequential(self, input_data: Any) -> bool:
        """Execute stages sequentially."""
        current_data = input_data
        success = True

        for stage in self.stages:
            if not self._running:
                break

            result = await stage.execute(current_data)
            self._process_stage_result(stage.name, result)

            if result.is_success:
                current_data = result.data
                self._results[stage.name] = result.data
                self._results["_last"] = result.data
            else:
                success = False
                self.metrics.failed_stages += 1
                if self.config.stop_on_error:
                    break

        return success

    async def _execute_parallel(self, input_data: Any) -> bool:
        """Execute parallel stages in groups."""
        current_data = input_data
        success = True

        # Group stages by parallel flag
        groups = self._group_parallel_stages()

        for group in groups:
            if not self._running:
                break

            if len(group) == 1:
                # Single stage - execute normally
                stage = group[0]
                result = await stage.execute(current_data)
                self._process_stage_result(stage.name, result)

                if result.is_success:
                    current_data = result.data
                    self._results[stage.name] = result.data
                    self._results["_last"] = result.data
                else:
                    success = False
                    if self.config.stop_on_error:
                        break
            else:
                # Multiple parallel stages - execute together
                tasks = [stage.execute(current_data) for stage in group]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for stage, result in zip(group, results):
                    if isinstance(result, Exception):
                        result = StageResult(
                            stage_name=stage.name,
                            status=StageStatus.FAILED,
                            error=result,
                        )

                    self._process_stage_result(stage.name, result)

                    if result.is_success:
                        self._results[stage.name] = result.data
                    else:
                        success = False

                # Update current_data from first successful result
                successful_results = [r for r in results if isinstance(r, StageResult) and r.is_success]
                if successful_results:
                    current_data = successful_results[0].data
                    self._results["_last"] = current_data

        return success

    async def _execute_pipeline(self, input_data: Any) -> bool:
        """Execute stages in pipeline mode (output feeds to next)."""
        return await self._execute_sequential(input_data)

    async def _execute_dag(self, input_data: Any) -> bool:
        """Execute stages based on dependency graph."""
        # Build adjacency list
        pending = {stage.name for stage in self.stages}
        completed: set[str] = set()
        current_data = input_data
        success = True

        while pending and self._running:
            # Find stages with all dependencies satisfied
            ready = [
                stage for stage in self.stages
                if stage.name in pending
                and all(dep in completed for dep in stage.depends_on)
            ]

            if not ready:
                if pending:
                    logger.error(f"Deadlock detected. Remaining stages: {pending}")
                    return False
                break

            # Execute ready stages
            for stage in ready:
                result = await stage.execute(current_data)
                self._process_stage_result(stage.name, result)

                if result.is_success:
                    completed.add(stage.name)
                    pending.discard(stage.name)
                    if result.data is not None:
                        current_data = result.data
                    self._results[stage.name] = result.data
                    self._results["_last"] = result.data
                else:
                    completed.add(stage.name)
                    pending.discard(stage.name)
                    success = False
                    if self.config.stop_on_error:
                        return False

        return success

    def _group_parallel_stages(self) -> list[list[PipelineStage]]:
        """Group stages for parallel execution."""
        groups: list[list[PipelineStage]] = []
        current_group: list[PipelineStage] = []

        for stage in self.stages:
            if stage.parallel:
                current_group.append(stage)
            else:
                if current_group:
                    groups.append(current_group)
                    current_group = []
                groups.append([stage])

        if current_group:
            groups.append(current_group)

        return groups

    def _process_stage_result(self, stage_name: str, result: StageResult) -> None:
        """Process a stage result and update metrics."""
        self.metrics.stage_durations[stage_name] = result.duration

        if result.status == StageStatus.COMPLETED:
            self.metrics.completed_stages += 1
        elif result.status == StageStatus.FAILED:
            self.metrics.failed_stages += 1
            self.metrics.errors.append({
                "stage": stage_name,
                "error": str(result.error),
                "retry_count": result.retry_count,
            })
        elif result.status == StageStatus.SKIPPED:
            self.metrics.skipped_stages += 1

    def get_metrics(self) -> dict[str, Any]:
        """Get pipeline execution metrics."""
        return {
            "pipeline_name": self.name,
            "total_stages": self.metrics.total_stages,
            "completed": self.metrics.completed_stages,
            "failed": self.metrics.failed_stages,
            "skipped": self.metrics.skipped_stages,
            "duration": self.metrics.total_duration,
            "stage_durations": self.metrics.stage_durations,
            "errors": self.metrics.errors,
        }

    def get_checkpoint(self) -> dict[str, Any]:
        """Get current pipeline checkpoint."""
        return {
            "name": self.name,
            "results": self._results.copy(),
            "metrics": self.get_metrics(),
        }


def create_pipeline(name: str, strategy: PipelineStrategy = PipelineStrategy.SEQUENTIAL) -> DataPipelineV2:
    """Create a new data pipeline.

    Args:
        name: Pipeline name
        strategy: Execution strategy

    Returns:
        New DataPipelineV2 instance
    """
    config = PipelineConfig(strategy=strategy)
    return DataPipelineV2(name=name, config=config)

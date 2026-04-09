"""
Data Pipeline Executor Action Module.

Executes complex data pipelines with parallel execution,
stage dependencies, and error recovery.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class PipelineStatus(Enum):
    """Pipeline execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class StageStatus(Enum):
    """Stage execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PipelineStage:
    """Represents a pipeline stage."""

    name: str
    processor: Callable
    depends_on: list[str] = field(default_factory=list)
    timeout: float = 60.0
    retry_count: int = 0
    status: StageStatus = StageStatus.PENDING
    result: Any = None
    error: str = ""
    started_at: float = 0.0
    completed_at: float = 0.0


@dataclass
class PipelineExecution:
    """Represents a pipeline execution."""

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = ""
    status: PipelineStatus = PipelineStatus.PENDING
    stages: list[PipelineStage] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    completed_at: float = 0.0
    context: dict[str, Any] = field(default_factory=dict)


class DataPipelineExecutorAction:
    """
    Executes complex data pipelines with parallel processing.

    Features:
    - Stage dependency management
    - Parallel execution when possible
    - Timeout and retry handling
    - Stage result caching
    - Execution pause/resume

    Example:
        executor = DataPipelineExecutorAction()
        executor.add_stage("extract", extract_fn)
        executor.add_stage("transform", transform_fn, depends_on=["extract"])
        result = await executor.execute()
    """

    def __init__(
        self,
        max_parallel: int = 4,
        default_timeout: float = 60.0,
        enable_checkpoint: bool = True,
    ) -> None:
        """
        Initialize pipeline executor.

        Args:
            max_parallel: Maximum parallel stage executions.
            default_timeout: Default stage timeout.
            enable_checkpoint: Enable stage result caching.
        """
        self.max_parallel = max_parallel
        self.default_timeout = default_timeout
        self.enable_checkpoint = enable_checkpoint
        self._stages: list[PipelineStage] = []
        self._executions: dict[str, PipelineExecution] = {}
        self._stage_results: dict[str, Any] = {}

    def add_stage(
        self,
        name: str,
        processor: Callable,
        *args: Any,
        depends_on: Optional[list[str]] = None,
        timeout: Optional[float] = None,
        retry_count: int = 0,
        **kwargs: Any,
    ) -> "DataPipelineExecutorAction":
        """
        Add a stage to the pipeline.

        Args:
            name: Stage name.
            processor: Processing function.
            *args: Positional args for processor.
            depends_on: List of stage names this depends on.
            timeout: Stage timeout in seconds.
            retry_count: Number of retries on failure.
            **kwargs: Keyword args for processor.

        Returns:
            Self for chaining.
        """
        stage = PipelineStage(
            name=name,
            processor=processor,
            depends_on=depends_on or [],
            timeout=timeout or self.default_timeout,
            retry_count=retry_count,
        )
        self._stages.append(stage)
        logger.debug(f"Added pipeline stage: {name}")
        return self

    async def execute(
        self,
        execution_id: Optional[str] = None,
        initial_data: Optional[Any] = None,
    ) -> PipelineExecution:
        """
        Execute the pipeline.

        Args:
            execution_id: Optional execution ID.
            initial_data: Initial data for pipeline.

        Returns:
            PipelineExecution with results.
        """
        exec_id = execution_id or str(uuid.uuid4())
        execution = PipelineExecution(
            id=exec_id,
            name=f"pipeline_{len(self._executions)}",
            stages=[self._create_stage_copy(s) for s in self._stages],
            context={"initial_data": initial_data},
        )
        self._executions[exec_id] = execution
        execution.status = PipelineStatus.RUNNING

        logger.info(f"Starting pipeline execution: {exec_id}")

        try:
            await self._execute_stages(execution)

            if all(s.status == StageStatus.COMPLETED for s in execution.stages):
                execution.status = PipelineStatus.COMPLETED
                logger.info(f"Pipeline completed: {exec_id}")
            else:
                execution.status = PipelineStatus.FAILED
                logger.error(f"Pipeline failed: {exec_id}")

        except Exception as e:
            execution.status = PipelineStatus.FAILED
            logger.error(f"Pipeline execution error: {e}")

        execution.completed_at = time.time()
        return execution

    async def _execute_stages(self, execution: PipelineExecution) -> None:
        """Execute pipeline stages respecting dependencies."""
        stage_map = {s.name: s for s in execution.stages}
        completed = set()

        while len(completed) < len(execution.stages):
            runnable = [
                s for s in execution.stages
                if s.name not in completed
                and s.status == StageStatus.PENDING
                and all(dep in completed for dep in s.depends_on)
            ]

            if not runnable:
                break

            batch = runnable[:self.max_parallel]
            tasks = [self._execute_stage(s, execution) for s in batch]

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for stage, result in zip(batch, results):
                if isinstance(result, Exception):
                    stage.status = StageStatus.FAILED
                    stage.error = str(result)
                elif stage.name in completed:
                    continue

            completed.update(s.name for s in batch)

    async def _execute_stage(
        self,
        stage: PipelineStage,
        execution: PipelineExecution,
    ) -> Any:
        """Execute a single stage."""
        stage.status = StageStatus.RUNNING
        stage.started_at = time.time()

        cache_key = f"{execution.id}:{stage.name}"
        if self.enable_checkpoint and cache_key in self._stage_results:
            stage.result = self._stage_results[cache_key]
            stage.status = StageStatus.COMPLETED
            stage.completed_at = time.time()
            return stage.result

        for attempt in range(stage.retry_count + 1):
            try:
                if asyncio.iscoroutinefunction(stage.processor):
                    result = await asyncio.wait_for(
                        stage.processor(execution.context),
                        timeout=stage.timeout,
                    )
                else:
                    result = stage.processor(execution.context)

                stage.result = result
                self._stage_results[cache_key] = result
                stage.status = StageStatus.COMPLETED
                stage.completed_at = time.time()
                execution.context[stage.name] = result

                logger.debug(f"Stage completed: {stage.name}")
                return result

            except asyncio.TimeoutError:
                stage.error = f"Timeout after {stage.timeout}s"
                logger.warning(f"Stage timeout: {stage.name}")

            except Exception as e:
                stage.error = str(e)
                logger.error(f"Stage error: {stage.name} - {e}")

        stage.status = StageStatus.FAILED
        stage.completed_at = time.time()
        return None

    def _create_stage_copy(self, stage: PipelineStage) -> PipelineStage:
        """Create a copy of a stage for execution."""
        return PipelineStage(
            name=stage.name,
            processor=stage.processor,
            depends_on=stage.depends_on.copy(),
            timeout=stage.timeout,
            retry_count=stage.retry_count,
        )

    def get_execution(self, execution_id: str) -> Optional[PipelineExecution]:
        """
        Get a pipeline execution by ID.

        Args:
            execution_id: Execution identifier.

        Returns:
            PipelineExecution or None.
        """
        return self._executions.get(execution_id)

    def clear_cache(self) -> None:
        """Clear stage result cache."""
        self._stage_results.clear()
        logger.info("Pipeline cache cleared")

    def get_stats(self) -> dict[str, Any]:
        """
        Get pipeline statistics.

        Returns:
            Statistics dictionary.
        """
        total = len(self._executions)
        completed = sum(1 for e in self._executions.values() if e.status == PipelineStatus.COMPLETED)
        failed = sum(1 for e in self._executions.values() if e.status == PipelineStatus.FAILED)

        return {
            "total_executions": total,
            "completed": completed,
            "failed": failed,
            "cached_results": len(self._stage_results),
        }

"""Data pipeline action module.

Provides data pipeline orchestration:
- PipelineBuilder: Build multi-stage data pipelines
- PipelineExecutor: Execute pipelines with error handling
- PipelineMonitor: Monitor pipeline execution
- Stage: Individual pipeline stage definition
"""

from __future__ import annotations

import time
import logging
from typing import Any, Callable, Dict, List, Optional, TypeVar
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)

T = TypeVar("T")


class StageStatus(Enum):
    """Pipeline stage execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class PipelineStatus(Enum):
    """Overall pipeline execution status."""
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class StageMetrics:
    """Metrics for a pipeline stage."""
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    input_count: int = 0
    output_count: int = 0
    error_count: int = 0
    retry_count: int = 0

    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


@dataclass
class Stage:
    """A single stage in a data pipeline."""
    name: str
    handler: Callable[[Any], Any]
    condition: Optional[Callable[[Any], bool]] = None
    retry_count: int = 0
    timeout: Optional[float] = None
    description: Optional[str] = None


@dataclass
class PipelineMetrics:
    """Metrics for a pipeline execution."""
    total_stages: int = 0
    completed_stages: int = 0
    failed_stages: int = 0
    total_duration: float = 0.0
    stage_metrics: Dict[str, StageMetrics] = field(default_factory=dict)


class Pipeline:
    """A data processing pipeline."""

    def __init__(
        self,
        name: str,
        description: Optional[str] = None,
    ):
        self.name = name
        self.description = description
        self._stages: List[Stage] = []
        self._error_handler: Optional[Callable[[Exception, Stage, Any], Any]] = None

    def add_stage(
        self,
        name: str,
        handler: Callable[[Any], Any],
        condition: Optional[Callable[[Any], bool]] = None,
        retry_count: int = 0,
        timeout: Optional[float] = None,
    ) -> "Pipeline":
        """Add a stage to the pipeline."""
        stage = Stage(
            name=name,
            handler=handler,
            condition=condition,
            retry_count=retry_count,
            timeout=timeout,
        )
        self._stages.append(stage)
        return self

    def set_error_handler(
        self,
        handler: Callable[[Exception, Stage, Any], Any],
    ) -> None:
        """Set error handler for pipeline failures."""
        self._error_handler = handler

    def execute(self, initial_input: Any) -> PipelineResult:
        """Execute the pipeline."""
        context = PipelineContext(
            pipeline_name=self.name,
            current_input=initial_input,
            stage_results={},
            metrics=PipelineMetrics(total_stages=len(self._stages)),
            status=PipelineStatus.RUNNING,
        )

        logger.info(f"Starting pipeline: {self.name}")
        start_time = time.time()

        for stage in self._stages:
            if context.status == PipelineStatus.CANCELLED:
                break

            result = self._execute_stage(stage, context)
            if not result.success:
                logger.error(f"Stage '{stage.name}' failed: {result.error}")
                if self._error_handler:
                    try:
                        self._error_handler(result.error, stage, context.current_input)
                    except Exception as e:
                        logger.warning(f"Error handler failed: {e}")
                if context.error_strategy == ErrorStrategy.STOP:
                    context.status = PipelineStatus.FAILED
                    break
            else:
                context.current_input = result.output

        context.metrics.total_duration = time.time() - start_time
        if context.status == PipelineStatus.RUNNING:
            context.status = PipelineStatus.COMPLETED

        logger.info(f"Pipeline '{self.name}' finished: {context.status.value}")
        return PipelineResult(
            success=context.status == PipelineStatus.COMPLETED,
            output=context.current_input,
            context=context,
        )

    def _execute_stage(self, stage: Stage, context: PipelineContext) -> StageResult:
        """Execute a single stage with retry logic."""
        metrics = StageMetrics()
        context.metrics.stage_metrics[stage.name] = metrics

        if stage.condition and not stage.condition(context.current_input):
            logger.info(f"Stage '{stage.name}' skipped (condition not met)")
            return StageResult(success=True, output=context.current_input, skipped=True)

        metrics.start_time = time.time()
        last_error: Optional[Exception] = None

        for attempt in range(stage.retry_count + 1):
            try:
                output = stage.handler(context.current_input)
                metrics.end_time = time.time()
                return StageResult(success=True, output=output)
            except Exception as e:
                last_error = e
                metrics.error_count += 1
                if attempt < stage.retry_count:
                    metrics.retry_count += 1
                    logger.warning(
                        f"Stage '{stage.name}' attempt {attempt + 1} failed: {e}"
                    )

        metrics.end_time = time.time()
        return StageResult(
            success=False,
            output=None,
            error=last_error,
        )


@dataclass
class PipelineContext:
    """Execution context for a pipeline."""
    pipeline_name: str
    current_input: Any
    stage_results: Dict[str, Any]
    metrics: PipelineMetrics
    status: PipelineStatus = PipelineStatus.IDLE
    error_strategy: Any = field(default="stop")


@dataclass
class PipelineResult:
    """Result of a pipeline execution."""
    success: bool
    output: Any
    context: PipelineContext


@dataclass
class StageResult:
    """Result of a stage execution."""
    success: bool
    output: Any
    error: Optional[Exception] = None
    skipped: bool = False


class ErrorStrategy(Enum):
    """Strategy for handling pipeline errors."""
    STOP = "stop"
    CONTINUE = "continue"
    RETRY_ALL = "retry_all"


class PipelineMonitor:
    """Monitor pipeline execution metrics."""

    def __init__(self):
        self._snapshots: List[PipelineMetrics] = []

    def capture(self, metrics: PipelineMetrics) -> None:
        """Capture a metrics snapshot."""
        self._snapshots.append(metrics)

    def get_summary(self) -> Dict[str, Any]:
        """Get monitoring summary."""
        if not self._snapshots:
            return {}
        latest = self._snapshots[-1]
        return {
            "total_runs": len(self._snapshots),
            "total_stages": latest.total_stages,
            "completed": latest.completed_stages,
            "failed": latest.failed_stages,
            "avg_duration": sum(s.total_duration for s in self._snapshots)
            / len(self._snapshots),
        }

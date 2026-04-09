"""
Automation Pipeline Executor Action.

Executes complex automation pipelines with support for
stages, checkpoints, rollback, and real-time monitoring.

Author: rabai_autoclick
License: MIT
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

logger = logging.getLogger(__name__)


class PipelineStatus(Enum):
    """Pipeline execution status."""
    PENDING = auto()
    RUNNING = auto()
    PAUSED = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()
    ROLLING_BACK = auto()


class StageStatus(Enum):
    """Individual stage status."""
    SKIPPED = auto()
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    ROLLED_BACK = auto()


@dataclass
class Checkpoint:
    """Execution checkpoint for recovery."""
    stage_index: int
    stage_name: str
    data_snapshot: Dict[str, Any]
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    label: str = ""


@dataclass
class StageResult:
    """Result of a single stage execution."""
    stage_name: str
    status: StageStatus
    output: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


@dataclass
class RollbackAction:
    """A single rollback action."""
    stage_name: str
    rollback_fn: Callable[[Any], None]
    order: int


@dataclass
class PipelineMetrics:
    """Real-time pipeline metrics."""
    stages_completed: int = 0
    stages_failed: int = 0
    stages_skipped: int = 0
    total_duration_ms: float = 0.0
    current_stage: Optional[str] = None
    current_stage_duration_ms: float = 0.0


class Stage:
    """A single stage in a pipeline."""

    def __init__(
        self,
        name: str,
        handler: Callable[[Any], Any],
        *,
        rollback_handler: Optional[Callable[[Any], None]] = None,
        timeout_seconds: Optional[float] = None,
        retry_count: int = 0,
        condition: Optional[Callable[[Any], bool]] = None,
        parallel: bool = False,
        executor: Optional[ThreadPoolExecutor] = None,
    ) -> None:
        self.name = name
        self.handler = handler
        self.rollback_handler = rollback_handler
        self.timeout_seconds = timeout_seconds
        self.retry_count = retry_count
        self.condition = condition
        self.parallel = parallel
        self.executor = executor

    async def execute(self, ctx: Any) -> StageResult:
        """Execute this stage."""
        import time
        started = datetime.now(timezone.utc)

        try:
            # Check condition
            if self.condition and not self.condition(ctx):
                return StageResult(
                    stage_name=self.name,
                    status=StageStatus.SKIPPED,
                    started_at=started,
                    completed_at=datetime.now(timezone.utc),
                )

            # Execute handler
            result = self.handler(ctx)
            if asyncio.iscoroutine(result):
                if self.timeout_seconds:
                    result = await asyncio.wait_for(result, timeout=self.timeout_seconds)
                else:
                    result = await result

            duration = (datetime.now(timezone.utc) - started).total_seconds() * 1000
            return StageResult(
                stage_name=self.name,
                status=StageStatus.COMPLETED,
                output=result,
                duration_ms=duration,
                started_at=started,
                completed_at=datetime.now(timezone.utc),
            )
        except asyncio.TimeoutError:
            return StageResult(
                stage_name=self.name,
                status=StageStatus.FAILED,
                error=f"Stage timed out after {self.timeout_seconds}s",
                duration_ms=(datetime.now(timezone.utc) - started).total_seconds() * 1000,
                started_at=started,
                completed_at=datetime.now(timezone.utc),
            )
        except Exception as exc:
            duration = (datetime.now(timezone.utc) - started).total_seconds() * 1000
            return StageResult(
                stage_name=self.name,
                status=StageStatus.FAILED,
                error=f"{type(exc).__name__}: {exc}",
                duration_ms=duration,
                started_at=started,
                completed_at=datetime.now(timezone.utc),
            )


class AutomationPipelineExecutor:
    """
    Executor for multi-stage automation pipelines with checkpointing and rollback.

    Example:
        pipeline = (AutomationPipelineExecutor("data-migration")
            .stage("extract", extract_data)
            .stage("transform", transform_data)
            .stage("load", load_data)
            .stage("verify", verify_data, rollback_handler=rollback_load)
            .build())
        result = await pipeline.execute(initial_data)
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._stages: List[Stage] = []
        self._rollback_stack: List[RollbackAction] = []
        self._checkpoints: List[Checkpoint] = []
        self._auto_checkpoint_every = 0  # 0 = disabled
        self._max_parallel_stages = 1
        self._cancel_requested = False

    def stage(
        self,
        name: str,
        handler: Callable[[Any], Any],
        *,
        rollback: Optional[Callable[[Any], None]] = None,
        timeout: Optional[float] = None,
        retries: int = 0,
        if_condition: Optional[Callable[[Any], bool]] = None,
        parallel: bool = False,
    ) -> Self:
        """Add a stage to the pipeline."""
        self._stages.append(Stage(
            name=name,
            handler=handler,
            rollback_handler=rollback,
            timeout_seconds=timeout,
            retry_count=retries,
            condition=if_condition,
            parallel=parallel,
        ))
        return self

    def checkpoint(self, label: str = "") -> Self:
        """Add a named checkpoint at current stage."""
        def checkpoint_handler(ctx: Any) -> Any:
            pass  # Checkpointing is handled by executor
        self._stages.append(Stage(name=f"_checkpoint_{label}", handler=checkpoint_handler))
        return self

    def auto_checkpoint(self, every_n_stages: int) -> Self:
        """Enable automatic checkpointing every N stages."""
        self._auto_checkpoint_every = every_n_stages
        return self

    def set_parallel(self, max_workers: int) -> Self:
        """Set maximum parallel stage workers."""
        self._max_parallel_stages = max_workers
        return self

    def build(self) -> AutomationPipeline:
        """Build the executable pipeline."""
        return AutomationPipeline(
            name=self.name,
            stages=self._stages,
            rollback_stack=self._rollback_stack,
            checkpoints=self._checkpoints,
            auto_checkpoint_every=self._auto_checkpoint_every,
            max_parallel=self._max_parallel_stages,
        )


class AutomationPipeline:
    """Executable pipeline with checkpoint and rollback support."""

    def __init__(
        self,
        name: str,
        stages: List[Stage],
        rollback_stack: List[RollbackAction],
        checkpoints: List[Checkpoint],
        auto_checkpoint_every: int,
        max_parallel: int,
    ) -> None:
        self.name = name
        self._stages = stages
        self._rollback_stack = rollback_stack
        self._checkpoints = checkpoints
        self._auto_checkpoint_every = auto_checkpoint_every
        self._max_parallel = max_parallel

    async def execute(self, initial_data: Any) -> Tuple[PipelineStatus, Any, PipelineMetrics]:
        """
        Execute the full pipeline.

        Returns:
            Tuple of (status, final_output, metrics)
        """
        import time
        from dataclasses import replace

        start_time = time.monotonic()
        status = PipelineStatus.RUNNING
        context = initial_data
        metrics = PipelineMetrics()
        stage_results: List[StageResult] = []
        rollback_queue: List[Callable[[Any], None]] = []

        try:
            for i, stage in enumerate(self._stages):
                if status == PipelineStatus.CANCELLED:
                    break

                metrics.current_stage = stage.name

                # Auto checkpoint
                if self._auto_checkpoint_every > 0 and (i + 1) % self._auto_checkpoint_every == 0:
                    self._checkpoints.append(Checkpoint(
                        stage_index=i,
                        stage_name=stage.name,
                        data_snapshot={"context": context} if isinstance(context, dict) else {"data": context},
                        label=f"auto_{i}",
                    ))

                # Execute stage
                logger.info("Executing stage %d/%d: %s", i + 1, len(self._stages), stage.name)
                result = await stage.execute(context)

                metrics.current_stage_duration_ms = result.duration_ms
                if result.status == StageStatus.COMPLETED:
                    metrics.stages_completed += 1
                    if stage.rollback_handler:
                        rollback_queue.append(stage.rollback_handler)
                elif result.status == StageStatus.FAILED:
                    metrics.stages_failed += 1
                    logger.error("Stage %s failed: %s", stage.name, result.error)
                    # Start rollback
                    status = PipelineStatus.ROLLING_BACK
                    await self._rollback(context, rollback_queue, stage_results)
                    status = PipelineStatus.FAILED
                    break
                elif result.status == StageStatus.SKIPPED:
                    metrics.stages_skipped += 1

                stage_results.append(result)
                if result.output is not None:
                    context = result.output

            if status == PipelineStatus.RUNNING:
                status = PipelineStatus.COMPLETED

        except Exception as exc:
            logger.error("Pipeline %s failed with exception: %s", self.name, exc)
            status = PipelineStatus.FAILED

        metrics.total_duration_ms = (time.monotonic() - start_time) * 1000
        metrics.current_stage = None
        metrics.current_stage_duration_ms = 0

        return status, context, metrics

    async def _rollback(
        self,
        context: Any,
        rollback_queue: List[Callable[[Any], None]],
        stage_results: List[StageResult],
    ) -> None:
        """Execute rollback for completed stages in reverse order."""
        logger.info("Starting rollback for %d stages", len(rollback_queue))
        for rollback_fn in reversed(rollback_queue):
            try:
                if asyncio.iscoroutinefunction(rollback_fn):
                    await rollback_fn(context)
                else:
                    rollback_fn(context)
            except Exception as exc:
                logger.error("Rollback action failed: %s", exc)

    def get_checkpoint(self, label: str) -> Optional[Checkpoint]:
        """Get a checkpoint by label."""
        for ck in self._checkpoints:
            if ck.label == label:
                return ck
        return None

    def list_checkpoints(self) -> List[Checkpoint]:
        """List all checkpoints."""
        return sorted(self._checkpoints, key=lambda c: c.stage_index)

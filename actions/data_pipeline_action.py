"""Data Pipeline Action Module.

Provides data pipeline orchestration with support for multiple stages,
error handling, checkpointing, and parallel execution.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class PipelineStatus(Enum):
    CREATED = "created"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class StageResult:
    stage_name: str
    success: bool
    output: Any = None
    error: Optional[str] = None
    duration_seconds: float = 0.0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineResult:
    pipeline_name: str
    status: PipelineStatus
    stage_results: List[StageResult] = field(default_factory=list)
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    total_duration_seconds: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        return self.status == PipelineStatus.COMPLETED

    def get_stage(self, name: str) -> Optional[StageResult]:
        for result in self.stage_results:
            if result.stage_name == name:
                return result
        return None


@dataclass
class PipelineStage:
    name: str
    processor: Callable[[Any], Any]
    error_handler: Optional[Callable[[Exception, Any], Any]] = None
    skip_on_error: bool = False
    timeout_seconds: Optional[float] = None
    retry_count: int = 0
    retry_delay_seconds: float = 1.0
    condition: Optional[Callable[[Any], bool]] = None
    parallel: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineConfig:
    name: str
    stages: List[PipelineStage] = field(default_factory=list)
    checkpoint_enabled: bool = True
    checkpoint_fn: Optional[Callable[[Any, int], None]] = None
    restore_fn: Optional[Callable[[], Any]] = None
    max_parallel_stages: int = 3
    continue_on_error: bool = True
    stop_on_stage_failure: bool = False


class DataPipeline:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self._running = False
        self._cancelled = False
        self._current_stage_index = 0
        self._checkpoint_data: Dict[int, Any] = {}

    async def run(self, initial_data: Any) -> PipelineResult:
        import time
        self._running = True
        self._cancelled = False
        result = PipelineResult(
            pipeline_name=self.config.name,
            status=PipelineStatus.RUNNING,
            start_time=datetime.now(),
        )

        data = initial_data

        if self.config.restore_fn and self.config.checkpoint_enabled:
            try:
                data = self.config.restore_fn()
                logger.info(f"Restored pipeline state from checkpoint")
            except Exception as e:
                logger.warning(f"Could not restore checkpoint: {e}")

        try:
            for i, stage in enumerate(self.config.stages):
                if self._cancelled:
                    result.status = PipelineStatus.CANCELLED
                    break

                self._current_stage_index = i

                if stage.condition and not stage.condition(data):
                    logger.info(f"Skipping stage '{stage.name}' - condition not met")
                    continue

                stage_result = await self._run_stage(stage, data, i)
                result.stage_results.append(stage_result)

                if self.config.checkpoint_enabled and self.config.checkpoint_fn:
                    try:
                        self.config.checkpoint_fn(data, i)
                        self._checkpoint_data[i] = data
                    except Exception as e:
                        logger.warning(f"Checkpoint failed: {e}")

                if not stage_result.success:
                    if stage.skip_on_error:
                        logger.warning(f"Stage '{stage.name}' failed, continuing: {stage_result.error}")
                        continue
                    elif self.config.stop_on_stage_failure:
                        result.status = PipelineStatus.FAILED
                        break
                    else:
                        raise RuntimeError(f"Stage '{stage.name}' failed: {stage_result.error}")

                data = stage_result.output

            result.status = PipelineStatus.COMPLETED if not self._cancelled else PipelineStatus.CANCELLED

        except Exception as e:
            result.status = PipelineStatus.FAILED
            logger.exception(f"Pipeline failed: {e}")
        finally:
            self._running = False
            result.end_time = datetime.now()
            result.total_duration_seconds = (result.end_time - result.start_time).total_seconds()

        return result

    async def _run_stage(self, stage: PipelineStage, data: Any, index: int) -> StageResult:
        import time
        result = StageResult(
            stage_name=stage.name,
            success=False,
            start_time=datetime.now(),
        )

        for attempt in range(stage.retry_count + 1):
            try:
                start = time.time()

                if stage.timeout_seconds:
                    result.output = await asyncio.wait_for(
                        asyncio.to_thread(stage.processor, data),
                        timeout=stage.timeout_seconds,
                    )
                else:
                    result.output = await asyncio.to_thread(stage.processor, data)

                result.duration_seconds = time.time() - start
                result.success = True
                result.end_time = datetime.now()
                return result

            except Exception as e:
                result.error = str(e)
                result.duration_seconds = time.time() - start

                if attempt < stage.retry_count:
                    logger.warning(f"Stage '{stage.name}' attempt {attempt + 1} failed, retrying...")
                    await asyncio.sleep(stage.retry_delay_seconds)
                else:
                    if stage.error_handler:
                        try:
                            result.output = stage.error_handler(e, data)
                            result.success = True
                            result.error = None
                        except Exception as handler_error:
                            result.error = f"Handler error: {handler_error}"
                            result.success = False
                    else:
                        result.success = False

        result.end_time = datetime.now()
        return result

    def cancel(self):
        self._cancelled = True
        self._running = False


def create_pipeline(name: str) -> DataPipeline:
    config = PipelineConfig(name=name)
    return DataPipeline(config)


def add_stage(
    pipeline: DataPipeline,
    name: str,
    processor: Callable[[Any], Any],
    **kwargs,
) -> None:
    stage = PipelineStage(name=name, processor=processor, **kwargs)
    pipeline.config.stages.append(stage)

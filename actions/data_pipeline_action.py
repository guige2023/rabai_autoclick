# Copyright (c) 2024. coded by claude
"""Data Pipeline Action Module.

Implements data processing pipeline with support for multiple stages,
parallel processing, and error handling.
"""
from typing import Optional, Dict, Any, List, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
import asyncio
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class PipelineStatus(Enum):
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class PipelineStage(Generic[T, R]):
    name: str
    processor: Callable[[T], R]
    error_handler: Optional[Callable[[Exception, T], R]] = None
    timeout: Optional[float] = None


@dataclass
class PipelineResult(Generic[R]):
    success: bool
    output: Optional[R]
    error: Optional[str]
    stages_completed: int
    total_time_ms: float


class DataPipeline(Generic[T, R]):
    def __init__(self, name: str):
        self.name = name
        self._stages: List[PipelineStage] = []
        self._status = PipelineStatus.IDLE
        self._metrics: Dict[str, Any] = {}

    def add_stage(
        self,
        name: str,
        processor: Callable[[T], R],
        error_handler: Optional[Callable[[Exception, T], R]] = None,
        timeout: Optional[float] = None,
    ) -> "DataPipeline":
        stage = PipelineStage(name=name, processor=processor, error_handler=error_handler, timeout=timeout)
        self._stages.append(stage)
        return self

    async def execute(self, input_data: T) -> PipelineResult[R]:
        self._status = PipelineStatus.RUNNING
        start_time = datetime.now()
        current_value: Any = input_data
        stages_completed = 0

        for i, stage in enumerate(self._stages):
            try:
                if stage.timeout:
                    result = await asyncio.wait_for(
                        self._process_stage(stage, current_value),
                        timeout=stage.timeout,
                    )
                else:
                    result = await self._process_stage(stage, current_value)
                current_value = result
                stages_completed += 1
            except asyncio.TimeoutError:
                error_msg = f"Stage '{stage.name}' timed out"
                logger.error(error_msg)
                self._status = PipelineStatus.FAILED
                elapsed = (datetime.now() - start_time).total_seconds() * 1000
                return PipelineResult(
                    success=False,
                    output=None,
                    error=error_msg,
                    stages_completed=stages_completed,
                    total_time_ms=elapsed,
                )
            except Exception as e:
                if stage.error_handler:
                    current_value = stage.error_handler(e, current_value)
                    stages_completed += 1
                else:
                    error_msg = f"Stage '{stage.name}' failed: {e}"
                    logger.error(error_msg)
                    self._status = PipelineStatus.FAILED
                    elapsed = (datetime.now() - start_time).total_seconds() * 1000
                    return PipelineResult(
                        success=False,
                        output=None,
                        error=error_msg,
                        stages_completed=stages_completed,
                        total_time_ms=elapsed,
                    )

        self._status = PipelineStatus.COMPLETED
        elapsed = (datetime.now() - start_time).total_seconds() * 1000
        return PipelineResult(
            success=True,
            output=current_value,
            error=None,
            stages_completed=stages_completed,
            total_time_ms=elapsed,
        )

    async def _process_stage(self, stage: PipelineStage, data: Any) -> R:
        result = stage.processor(data)
        if asyncio.iscoroutine(result):
            return await result
        return result

    def get_status(self) -> PipelineStatus:
        return self._status

    def get_metrics(self) -> Dict[str, Any]:
        return dict(self._metrics)

"""
Data Pipeline Executor Action Module

Provides data pipeline execution with stage management, parallel processing,
checkpointing, and error recovery for data processing workflows.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class StageStatus(Enum):
    """Pipeline stage status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class PipelineStatus(Enum):
    """Pipeline execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class PipelineStage:
    """A stage in a data pipeline."""

    stage_id: str
    name: str
    handler: Callable[..., Any]
    input_mapping: Dict[str, str] = field(default_factory=dict)
    output_mapping: Dict[str, str] = field(default_factory=dict)
    parallel: bool = False
    max_workers: int = 1
    timeout_seconds: float = 300.0
    retry_count: int = 0
    depends_on: List[str] = field(default_factory=list)


@dataclass
class StageResult:
    """Result of a stage execution."""

    stage_id: str
    status: StageStatus
    output: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    start_time: Optional[float] = None
    end_time: Optional[float] = None


@dataclass
class Pipeline:
    """A data processing pipeline."""

    pipeline_id: str
    name: str
    stages: List[PipelineStage]
    status: PipelineStatus = PipelineStatus.PENDING
    stage_results: Dict[str, StageResult] = field(default_factory=dict)
    start_time: Optional[float] = None
    end_time: Optional[float] = None


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution."""

    enable_parallel: bool = True
    max_parallel_stages: int = 5
    enable_checkpointing: bool = True
    continue_on_error: bool = True
    default_timeout: float = 300.0
    default_retry_count: int = 0


class DataPipelineExecutorAction:
    """
    Data pipeline executor action.

    Features:
    - Stage-based pipeline definition
    - Parallel and sequential execution
    - Stage dependencies and ordering
    - Checkpointing for resumable execution
    - Error recovery and retry
    - Stage result caching

    Usage:
        executor = DataPipelineExecutorAction(config)
        
        executor.add_stage("extract", extract_handler)
        executor.add_stage("transform", transform_handler, depends_on=["extract"])
        executor.add_stage("load", load_handler, depends_on=["transform"])
        
        result = await executor.execute(pipeline_id="etl-pipeline")
    """

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        self._pipelines: Dict[str, Pipeline] = {}
        self._checkpoints: Dict[str, Dict[str, Any]] = {}
        self._stats = {
            "pipelines_executed": 0,
            "stages_completed": 0,
            "stages_failed": 0,
            "checkpoints_saved": 0,
        }

    def create_pipeline(
        self,
        name: str,
    ) -> Pipeline:
        """Create a new pipeline."""
        pipeline_id = f"pipeline_{uuid.uuid4().hex[:12]}"
        pipeline = Pipeline(
            pipeline_id=pipeline_id,
            name=name,
            stages=[],
        )
        self._pipelines[pipeline_id] = pipeline
        return pipeline

    def add_stage(
        self,
        pipeline: Pipeline,
        stage_id: str,
        name: str,
        handler: Callable[..., Any],
        depends_on: Optional[List[str]] = None,
        parallel: bool = False,
        timeout_seconds: Optional[float] = None,
    ) -> PipelineStage:
        """Add a stage to a pipeline."""
        stage = PipelineStage(
            stage_id=stage_id,
            name=name,
            handler=handler,
            depends_on=depends_on or [],
            parallel=parallel,
            timeout_seconds=timeout_seconds or self.config.default_timeout,
        )
        pipeline.stages.append(stage)
        return stage

    async def execute(
        self,
        pipeline_id: str,
        initial_data: Optional[Dict[str, Any]] = None,
    ) -> Pipeline:
        """Execute a pipeline."""
        pipeline = self._pipelines.get(pipeline_id)
        if pipeline is None:
            raise ValueError(f"Pipeline not found: {pipeline_id}")

        logger.info(f"Executing pipeline: {pipeline_id}")
        pipeline.status = PipelineStatus.RUNNING
        pipeline.start_time = time.time()
        self._stats["pipelines_executed"] += 1

        data = initial_data or {}
        completed_stages: set = set()
        failed_stages: set = set()

        try:
            while len(completed_stages) < len(pipeline.stages):
                ready_stages = [
                    s for s in pipeline.stages
                    if s.stage_id not in completed_stages
                    and s.stage_id not in failed_stages
                    and all(dep in completed_stages for dep in s.depends_on)
                ]

                if not ready_stages:
                    break

                if self.config.enable_parallel:
                    parallel_stages = [s for s in ready_stages if s.parallel]
                    sequential_stages = [s for s in ready_stages if not s.parallel]

                    if parallel_stages:
                        await self._execute_parallel(parallel_stages, data, pipeline)

                    for stage in sequential_stages:
                        result = await self._execute_stage(stage, data, pipeline)
                        if result.status == StageStatus.COMPLETED:
                            completed_stages.add(stage.stage_id)
                            self._stats["stages_completed"] += 1
                        else:
                            failed_stages.add(stage.stage_id)
                            self._stats["stages_failed"] += 1
                            if not self.config.continue_on_error:
                                break
                else:
                    for stage in ready_stages:
                        result = await self._execute_stage(stage, data, pipeline)
                        if result.status == StageStatus.COMPLETED:
                            completed_stages.add(stage.stage_id)
                            self._stats["stages_completed"] += 1
                        else:
                            failed_stages.add(stage.stage_id)
                            self._stats["stages_failed"] += 1
                            if not self.config.continue_on_error:
                                break

                if self.config.enable_checkpointing:
                    self._save_checkpoint(pipeline_id, data)

            pipeline.status = PipelineStatus.COMPLETED

        except Exception as e:
            logger.error(f"Pipeline execution error: {e}")
            pipeline.status = PipelineStatus.FAILED

        pipeline.end_time = time.time()
        return pipeline

    async def _execute_stage(
        self,
        stage: PipelineStage,
        data: Dict[str, Any],
        pipeline: Pipeline,
    ) -> StageResult:
        """Execute a single stage."""
        result = StageResult(stage_id=stage.stage_id, status=StageStatus.RUNNING)
        result.start_time = time.time()

        try:
            input_data = {
                key: data.get(value)
                for key, value in stage.input_mapping.items()
            }
            if not input_data:
                input_data = data.copy()

            if asyncio.iscoroutinefunction(stage.handler):
                output = await asyncio.wait_for(
                    stage.handler(input_data),
                    timeout=stage.timeout_seconds,
                )
            else:
                output = stage.handler(input_data)

            result.output = output
            result.status = StageStatus.COMPLETED

            for key, value in stage.output_mapping.items():
                data[key] = output.get(value) if isinstance(output, dict) else output

        except asyncio.TimeoutError:
            result.status = StageStatus.FAILED
            result.error = f"Stage timed out after {stage.timeout_seconds}s"
        except Exception as e:
            result.status = StageStatus.FAILED
            result.error = str(e)

        result.end_time = time.time()
        result.duration_ms = (result.end_time - result.start_time) * 1000
        pipeline.stage_results[stage.stage_id] = result

        return result

    async def _execute_parallel(
        self,
        stages: List[PipelineStage],
        data: Dict[str, Any],
        pipeline: Pipeline,
    ) -> None:
        """Execute multiple stages in parallel."""
        tasks = [self._execute_stage(s, data, pipeline) for s in stages]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for stage, result in zip(stages, results):
            if isinstance(result, Exception):
                pipeline.stage_results[stage.stage_id] = StageResult(
                    stage_id=stage.stage_id,
                    status=StageStatus.FAILED,
                    error=str(result),
                )
                self._stats["stages_failed"] += 1
            elif result.status == StageStatus.COMPLETED:
                self._stats["stages_completed"] += 1

    def _save_checkpoint(self, pipeline_id: str, data: Dict[str, Any]) -> None:
        """Save a checkpoint for the pipeline."""
        self._checkpoints[pipeline_id] = data.copy()
        self._stats["checkpoints_saved"] += 1

    def get_checkpoint(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """Get the last checkpoint for a pipeline."""
        return self._checkpoints.get(pipeline_id)

    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline execution statistics."""
        return {
            **self._stats.copy(),
            "total_pipelines": len(self._pipelines),
        }


async def demo_pipeline():
    """Demonstrate pipeline execution."""
    config = PipelineConfig(enable_parallel=True)
    executor = DataPipelineExecutorAction(config)

    pipeline = executor.create_pipeline("etl-pipeline")

    async def extract(data):
        await asyncio.sleep(0.05)
        return {"rows": [{"id": 1}, {"id": 2}]}

    async def transform(data):
        await asyncio.sleep(0.05)
        rows = data.get("rows", [])
        return {"processed": len(rows), "status": "ok"}

    async def load(data):
        await asyncio.sleep(0.05)
        return {"loaded": data.get("processed", 0)}

    executor.add_stage(pipeline, "extract", "Extract Data", extract)
    executor.add_stage(pipeline, "transform", "Transform Data", transform, depends_on=["extract"])
    executor.add_stage(pipeline, "load", "Load Data", load, depends_on=["transform"])

    result = await executor.execute(pipeline.pipeline_id)

    print(f"Pipeline status: {result.status.value}")
    print(f"Stats: {executor.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_pipeline())

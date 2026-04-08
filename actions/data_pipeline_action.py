"""
Data pipeline module for building ETL/ELT data processing workflows.

Supports multi-stage pipelines, branching, merging, error handling,
checkpointing, and backpressure handling.
"""
from __future__ import annotations

import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Generator, Optional


class PipelineStatus(Enum):
    """Pipeline execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class StageType(Enum):
    """Type of pipeline stage."""
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    JOIN = "join"
    BRANCH = "branch"
    MERGE = "merge"


@dataclass
class PipelineStage:
    """A stage in a data pipeline."""
    id: str
    name: str
    stage_type: StageType
    processor: Callable
    input_key: str = "default"
    output_key: str = "default"
    error_handler: Optional[Callable] = None
    skip_on_error: bool = False
    timeout_seconds: int = 3600
    metadata: dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:8]


@dataclass
class PipelineBranch:
    """A branch in a pipeline."""
    condition: Callable
    output_key: str


@dataclass
class StageExecution:
    """Execution state of a pipeline stage."""
    stage: PipelineStage
    input_data: Any = None
    output_data: Any = None
    error: Optional[str] = None
    status: str = "pending"
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    records_in: int = 0
    records_out: int = 0

    def duration_ms(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time) * 1000
        return 0.0


@dataclass
class DataPipeline:
    """A complete data pipeline."""
    id: str
    name: str
    stages: list[PipelineStage]
    description: str = ""
    version: str = "1.0.0"
    checkpoint_enabled: bool = True
    parallel_execution: bool = False

    def __post_init__(self):
        if not self.id:
            self.id = str(uuid.uuid4())[:8]

    def get_stage(self, stage_id: str) -> Optional[PipelineStage]:
        for stage in self.stages:
            if stage.id == stage_id:
                return stage
        return None


@dataclass
class PipelineExecution:
    """A pipeline execution instance."""
    id: str
    pipeline: DataPipeline
    status: PipelineStatus = PipelineStatus.PENDING
    stage_executions: dict[str, StageExecution] = field(default_factory=dict)
    context: dict = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    error: Optional[str] = None
    checkpoint_data: dict = field(default_factory=dict)

    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0


class DataPipelineExecutor:
    """
    Data pipeline executor for ETL/ELT workflows.

    Supports multi-stage pipelines, branching, merging,
    error handling, and checkpointing.
    """

    def __init__(self):
        self._pipelines: dict[str, DataPipeline] = {}
        self._executions: dict[str, PipelineExecution] = {}
        self._checkpoints: dict[str, dict] = {}

    def create_pipeline(
        self,
        name: str,
        stages: list[PipelineStage],
        description: str = "",
        checkpoint_enabled: bool = True,
        parallel_execution: bool = False,
    ) -> DataPipeline:
        """Create a new data pipeline."""
        pipeline = DataPipeline(
            id=str(uuid.uuid4())[:12],
            name=name,
            stages=stages,
            description=description,
            checkpoint_enabled=checkpoint_enabled,
            parallel_execution=parallel_execution,
        )
        self._pipelines[pipeline.id] = pipeline
        return pipeline

    def execute(
        self,
        pipeline_id: str,
        initial_data: Any = None,
        execution_id: Optional[str] = None,
    ) -> PipelineExecution:
        """Execute a pipeline."""
        pipeline = self._pipelines.get(pipeline_id)
        if not pipeline:
            raise ValueError(f"Pipeline not found: {pipeline_id}")

        execution = PipelineExecution(
            id=execution_id or str(uuid.uuid4())[:8],
            pipeline=pipeline,
            context={"data": initial_data},
        )
        self._executions[execution.id] = execution

        return self._execute_pipeline(execution)

    def _execute_pipeline(self, execution: PipelineExecution) -> PipelineExecution:
        """Execute a pipeline's stages."""
        execution.status = PipelineStatus.RUNNING
        pipeline = execution.pipeline

        checkpoint = self._checkpoints.get(execution.id) if pipeline.checkpoint_enabled else None
        start_idx = checkpoint.get("last_completed_stage_idx", 0) if checkpoint else 0

        try:
            for i, stage in enumerate(pipeline.stages):
                if i < start_idx:
                    continue

                if execution.status == PipelineStatus.CANCELLED:
                    break

                stage_exec = self._execute_stage(stage, execution)
                execution.stage_executions[stage.id] = stage_exec

                if stage_exec.error and not stage.skip_on_error:
                    execution.status = PipelineStatus.FAILED
                    execution.error = stage_exec.error
                    execution.end_time = time.time()
                    return execution

                if pipeline.checkpoint_enabled and (i + 1) % 5 == 0:
                    self._save_checkpoint(execution, i)

            execution.status = PipelineStatus.COMPLETED

        except Exception as e:
            execution.status = PipelineStatus.FAILED
            execution.error = str(e)

        execution.end_time = time.time()
        return execution

    def _execute_stage(
        self,
        stage: PipelineStage,
        execution: PipelineExecution,
    ) -> StageExecution:
        """Execute a single pipeline stage."""
        stage_exec = StageExecution(stage=stage)
        stage_exec.start_time = time.time()

        input_data = execution.context.get(stage.input_key)
        stage_exec.input_data = input_data

        if input_data is not None and hasattr(input_data, "__len__"):
            stage_exec.records_in = len(input_data)

        try:
            if stage.stage_type == StageType.FILTER:
                result = [item for item in input_data if stage.processor(item)]
            elif stage.stage_type == StageType.AGGREGATE:
                result = stage.processor(input_data)
            elif stage.stage_type == StageType.JOIN:
                result = stage.processor(
                    execution.context.get("left", []),
                    execution.context.get("right", []),
                )
            elif stage.stage_type == StageType.BRANCH:
                branches = stage.processor(input_data)
                for branch_output in branches:
                    output_key = branch_output.get("key", "default")
                    execution.context[output_key] = branch_output.get("data")
                result = branches
            else:
                result = stage.processor(input_data)

            stage_exec.output_data = result
            execution.context[stage.output_key] = result

            if hasattr(result, "__len__"):
                stage_exec.records_out = len(result)

            stage_exec.status = "completed"

        except Exception as e:
            stage_exec.error = str(e)
            stage_exec.status = "failed"

            if stage.error_handler:
                stage_exec.output_data = stage.error_handler(input_data, e)
                stage_exec.status = "completed_with_error"

        stage_exec.end_time = time.time()
        return stage_exec

    def _save_checkpoint(self, execution: PipelineExecution, stage_idx: int) -> None:
        """Save a pipeline checkpoint."""
        self._checkpoints[execution.id] = {
            "last_completed_stage_idx": stage_idx,
            "context": execution.context,
            "timestamp": time.time(),
        }

    def resume(
        self,
        execution_id: str,
    ) -> PipelineExecution:
        """Resume a failed or paused pipeline execution."""
        execution = self._executions.get(execution_id)
        if not execution:
            raise ValueError(f"Execution not found: {execution_id}")

        execution.status = PipelineStatus.RUNNING
        return self._execute_pipeline(execution)

    def cancel(self, execution_id: str) -> bool:
        """Cancel a pipeline execution."""
        execution = self._executions.get(execution_id)
        if execution and execution.status == PipelineStatus.RUNNING:
            execution.status = PipelineStatus.CANCELLED
            execution.end_time = time.time()
            return True
        return False

    def get_execution(self, execution_id: str) -> Optional[PipelineExecution]:
        """Get a pipeline execution."""
        return self._executions.get(execution_id)

    def list_executions(
        self,
        pipeline_id: Optional[str] = None,
        status: Optional[PipelineStatus] = None,
    ) -> list[dict]:
        """List pipeline executions."""
        executions = list(self._executions.values())

        if pipeline_id:
            executions = [e for e in executions if e.pipeline.id == pipeline_id]
        if status:
            executions = [e for e in executions if e.status == status]

        return [
            {
                "id": e.id,
                "pipeline_id": e.pipeline.id,
                "pipeline_name": e.pipeline.name,
                "status": e.status.value,
                "start_time": e.start_time,
                "end_time": e.end_time,
                "duration_seconds": e.duration_seconds(),
            }
            for e in sorted(executions, key=lambda x: x.start_time, reverse=True)
        ]

    def get_pipeline(self, pipeline_id: str) -> Optional[DataPipeline]:
        """Get a pipeline by ID."""
        return self._pipelines.get(pipeline_id)

    def list_pipelines(self) -> list[DataPipeline]:
        """List all pipelines."""
        return list(self._pipelines.values())

    def delete_pipeline(self, pipeline_id: str) -> bool:
        """Delete a pipeline."""
        if pipeline_id in self._pipelines:
            del self._pipelines[pipeline_id]
            return True
        return False

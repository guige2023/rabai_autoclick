"""
Data Pipeline V2 Action Module.

Builds and executes multi-stage data pipelines with branching,
conditional execution, error handling, and checkpoint support.

Author: RabAi Team
"""

from __future__ import annotations

import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple


class StageStatus(Enum):
    """Stage execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PipelineStage:
    """A single stage in a pipeline."""
    id: str
    name: str
    fn: Callable
    input_keys: List[str] = field(default_factory=list)
    output_key: str = ""
    condition_fn: Optional[Callable[[Dict], bool]] = None
    retry_count: int = 0
    max_retries: int = 3
    timeout_seconds: Optional[float] = None
    on_error: str = "fail"  # fail, skip, continue
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "input_keys": self.input_keys,
            "output_key": self.output_key,
            "max_retries": self.max_retries,
            "timeout_seconds": self.timeout_seconds,
            "on_error": self.on_error,
        }


@dataclass
class StageResult:
    """Result of executing a stage."""
    stage_id: str
    status: StageStatus
    output: Any = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    retry_count: int = 0


@dataclass
class PipelineExecution:
    """Record of a pipeline execution."""
    execution_id: str
    pipeline_id: str
    status: StageStatus
    stage_results: Dict[str, StageResult] = field(default_factory=dict)
    context: Dict[str, Any] = field(default_factory=dict)
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    checkpoint_data: Dict[str, Any] = field(default_factory=dict)


class DataPipelineV2:
    """
    Advanced data pipeline builder and executor.

    Creates multi-stage pipelines with conditional branching,
    error handling, retry logic, and checkpoint support.

    Example:
        >>> pipeline = DataPipelineV2("etl_pipeline")
        >>> pipeline.add_stage("extract", extract_fn, output_key="raw_data")
        >>> pipeline.add_stage("transform", transform_fn, input_keys=["raw_data"])
        >>> pipeline.add_stage("load", load_fn, input_keys=["transformed_data"])
        >>> result = pipeline.execute(initial_context={})
    """

    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self._stages: List[PipelineStage] = []
        self._stage_map: Dict[str, PipelineStage] = {}
        self._execution: Optional[PipelineExecution] = None

    def add_stage(
        self,
        name: str,
        fn: Callable,
        input_keys: Optional[List[str]] = None,
        output_key: str = "",
        **kwargs,
    ) -> str:
        """Add a stage to the pipeline."""
        stage_id = str(uuid.uuid4())
        stage = PipelineStage(
            id=stage_id,
            name=name,
            fn=fn,
            input_keys=input_keys or [],
            output_key=output_key,
            **kwargs,
        )
        self._stages.append(stage)
        self._stage_map[stage_id] = stage
        return stage_id

    def add_branch(
        self,
        name: str,
        branches: Dict[str, Callable],
        condition_fn: Callable[[Dict], str],
        **kwargs,
    ) -> str:
        """Add a branching stage with multiple paths."""
        stage_id = str(uuid.uuid4())

        def branch_fn(context: Dict) -> Any:
            branch_key = condition_fn(context)
            branch_fn = branches.get(branch_key)
            if branch_fn:
                inputs = {k: context.get(k) for k in []}
                return branch_fn(inputs)
            return None

        stage = PipelineStage(
            id=stage_id,
            name=name,
            fn=branch_fn,
            **kwargs,
        )
        self._stages.append(stage)
        self._stage_map[stage_id] = stage
        return stage_id

    def execute(
        self,
        initial_context: Optional[Dict[str, Any]] = None,
        checkpoint: Optional[Dict[str, Any]] = None,
    ) -> PipelineExecution:
        """Execute the pipeline."""
        execution_id = str(uuid.uuid4())
        self._execution = PipelineExecution(
            execution_id=execution_id,
            pipeline_id=self.pipeline_id,
            status=StageStatus.RUNNING,
            context=initial_context or {},
            checkpoint_data=checkpoint or {},
        )

        for stage in self._stages:
            result = self._execute_stage(stage)

            self._execution.stage_results[stage.id] = result

            if result.output is not None and stage.output_key:
                self._execution.context[stage.output_key] = result.output

            if result.status == StageStatus.FAILED:
                if stage.on_error == "fail":
                    self._execution.status = StageStatus.FAILED
                    break
                elif stage.on_error == "skip":
                    continue
                elif stage.on_error == "continue":
                    pass
            elif result.status == StageStatus.SKIPPED:
                continue

        if self._execution.status == StageStatus.RUNNING:
            self._execution.status = StageStatus.COMPLETED

        self._execution.completed_at = datetime.now()
        return self._execution

    def _execute_stage(self, stage: PipelineStage) -> StageResult:
        """Execute a single stage."""
        started = datetime.now()

        if stage.condition_fn and not stage.condition_fn(self._execution.context):
            return StageResult(
                stage_id=stage.id,
                status=StageStatus.SKIPPED,
                started_at=started,
                completed_at=datetime.now(),
            )

        inputs = {k: self._execution.context.get(k) for k in stage.input_keys}

        for attempt in range(stage.max_retries + 1):
            try:
                output = stage.fn(inputs)
                return StageResult(
                    stage_id=stage.id,
                    status=StageStatus.COMPLETED,
                    output=output,
                    started_at=started,
                    completed_at=datetime.now(),
                    duration_ms=(datetime.now() - started).total_seconds() * 1000,
                )
            except Exception as e:
                if attempt < stage.max_retries:
                    stage.retry_count = attempt + 1
                    continue
                return StageResult(
                    stage_id=stage.id,
                    status=StageStatus.FAILED,
                    error=str(e),
                    started_at=started,
                    completed_at=datetime.now(),
                    duration_ms=(datetime.now() - started).total_seconds() * 1000,
                    retry_count=attempt,
                )

        return StageResult(
            stage_id=stage.id,
            status=StageStatus.FAILED,
            error="Max retries exceeded",
            started_at=started,
            completed_at=datetime.now(),
        )

    def get_execution(self) -> Optional[PipelineExecution]:
        """Get current execution state."""
        return self._execution

    def get_stage_status(self) -> Dict[str, StageStatus]:
        """Get status of all stages."""
        if not self._execution:
            return {s.id: StageStatus.PENDING for s in self._stages}
        return {sid: r.status for sid, r in self._execution.stage_results.items()}


def create_pipeline(pipeline_id: str) -> DataPipelineV2:
    """Factory to create a data pipeline."""
    return DataPipelineV2(pipeline_id=pipeline_id)

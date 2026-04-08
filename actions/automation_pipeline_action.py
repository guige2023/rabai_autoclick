"""Automation Pipeline Action Module.

Provides pipeline-based automation with stages,
filters, and transformations for data processing workflows.
"""

import time
import hashlib
from typing import Any, Dict, List, Optional, Callable, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


T = TypeVar('T')


class PipelineStatus(Enum):
    """Pipeline execution status."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StageType(Enum):
    """Pipeline stage type."""
    SOURCE = "source"
    TRANSFORM = "transform"
    FILTER = "filter"
    ACTION = "action"
    SINK = "sink"


@dataclass
class PipelineStage:
    """A stage in the pipeline."""
    stage_id: str
    name: str
    stage_type: StageType
    handler: Callable
    params: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    timeout_seconds: Optional[float] = None
    enabled: bool = True
    error_handler: Optional[Callable] = None


@dataclass
class PipelineContext:
    """Context passed through pipeline stages."""
    data: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineExecution:
    """Pipeline execution record."""
    execution_id: str
    pipeline_id: str
    status: PipelineStatus
    started_at: float
    completed_at: Optional[float] = None
    stages_executed: int = 0
    items_processed: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)


class Pipeline(Generic[T]):
    """Data processing pipeline."""

    def __init__(self, pipeline_id: str, name: str):
        self.pipeline_id = pipeline_id
        self.name = name
        self._stages: List[PipelineStage] = []
        self._execution_history: List[PipelineExecution] = []

    def add_stage(
        self,
        stage_id: str,
        name: str,
        stage_type: StageType,
        handler: Callable,
        params: Optional[Dict[str, Any]] = None,
        retry_count: int = 0,
        error_handler: Optional[Callable] = None
    ) -> "Pipeline":
        """Add a stage to the pipeline."""
        stage = PipelineStage(
            stage_id=stage_id,
            name=name,
            stage_type=stage_type,
            handler=handler,
            params=params or {},
            retry_count=retry_count,
            error_handler=error_handler
        )
        self._stages.append(stage)
        return self

    def remove_stage(self, stage_id: str) -> bool:
        """Remove a stage from pipeline."""
        for i, stage in enumerate(self._stages):
            if stage.stage_id == stage_id:
                self._stages.pop(i)
                return True
        return False

    def get_stage(self, stage_id: str) -> Optional[PipelineStage]:
        """Get stage by ID."""
        for stage in self._stages:
            if stage.stage_id == stage_id:
                return stage
        return None

    def enable_stage(self, stage_id: str, enabled: bool = True) -> None:
        """Enable or disable a stage."""
        stage = self.get_stage(stage_id)
        if stage:
            stage.enabled = enabled

    def execute(
        self,
        data: T,
        params: Optional[Dict[str, Any]] = None
    ) -> tuple[T, PipelineExecution]:
        """Execute pipeline on data."""
        execution_id = hashlib.md5(
            f"{self.pipeline_id}{time.time()}".encode()
        ).hexdigest()[:8]

        execution = PipelineExecution(
            execution_id=execution_id,
            pipeline_id=self.pipeline_id,
            status=PipelineStatus.RUNNING,
            started_at=time.time()
        )

        context = PipelineContext(
            data=data,
            metadata=params or {}
        )

        for stage in self._stages:
            if not stage.enabled:
                continue

            try:
                start_time = time.time()
                context = self._execute_stage(stage, context)
                duration = time.time() - start_time

                context.metrics[stage.stage_id] = {
                    "duration_ms": duration * 1000,
                    "success": True
                }

                execution.stages_executed += 1

            except Exception as e:
                error_info = {
                    "stage_id": stage.stage_id,
                    "error": str(e),
                    "timestamp": time.time()
                }
                context.errors.append(error_info)
                execution.errors.append(error_info)

                if stage.error_handler:
                    try:
                        context = stage.error_handler(context, e)
                    except Exception:
                        execution.status = PipelineStatus.FAILED
                        break
                else:
                    execution.status = PipelineStatus.FAILED
                    break

        execution.completed_at = time.time()
        execution.items_processed = self._count_items(context.data)

        if execution.status == PipelineStatus.RUNNING:
            execution.status = PipelineStatus.COMPLETED

        self._execution_history.append(execution)
        return context.data, execution

    def _execute_stage(
        self,
        stage: PipelineStage,
        context: PipelineContext
    ) -> PipelineContext:
        """Execute a single pipeline stage."""
        data_in = context.data

        if stage.stage_type == StageType.FILTER:
            should_continue = stage.handler(data_in, stage.params, context)
            if not should_continue:
                context.data = None
                return context

        elif stage.stage_type == StageType.TRANSFORM:
            context.data = stage.handler(data_in, stage.params, context)

        elif stage.stage_type == StageType.ACTION:
            context = stage.handler(data_in, stage.params, context)

        elif stage.stage_type == StageType.SINK:
            stage.handler(data_in, stage.params, context)

        return context

    def _count_items(self, data: Any) -> int:
        """Count items in data."""
        if isinstance(data, list):
            return len(data)
        elif isinstance(data, dict):
            return len(data)
        return 1

    def execute_batch(
        self,
        data_batch: List[T],
        params: Optional[Dict[str, Any]] = None
    ) -> tuple[List[T], PipelineExecution]:
        """Execute pipeline on a batch of data."""
        results = []
        total_errors = []

        for item in data_batch:
            try:
                result, _ = self.execute(item, params)
                results.append(result)
            except Exception as e:
                total_errors.append({
                    "item": str(item)[:100],
                    "error": str(e)
                })

        execution_id = hashlib.md5(
            f"{self.pipeline_id}{time.time()}".encode()
        ).hexdigest()[:8]

        execution = PipelineExecution(
            execution_id=execution_id,
            pipeline_id=self.pipeline_id,
            status=PipelineStatus.COMPLETED if not total_errors else PipelineStatus.FAILED,
            started_at=time.time(),
            completed_at=time.time(),
            stages_executed=len(self._stages),
            items_processed=len(results),
            errors=total_errors
        )

        self._execution_history.append(execution)
        return results, execution

    def get_history(self, limit: int = 100) -> List[PipelineExecution]:
        """Get execution history."""
        return self._execution_history[-limit:]

    def clear_history(self) -> None:
        """Clear execution history."""
        self._execution_history.clear()


class AutomationPipelineAction(BaseAction):
    """Action for pipeline automation operations."""

    def __init__(self):
        super().__init__("automation_pipeline")
        self._pipelines: Dict[str, Pipeline] = {}

    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute pipeline automation action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create_pipeline(params)
            elif operation == "add_stage":
                return self._add_stage(params)
            elif operation == "remove_stage":
                return self._remove_stage(params)
            elif operation == "enable_stage":
                return self._enable_stage(params)
            elif operation == "execute":
                return self._execute(params)
            elif operation == "execute_batch":
                return self._execute_batch(params)
            elif operation == "get_pipeline":
                return self._get_pipeline(params)
            elif operation == "history":
                return self._get_history(params)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}"
                )

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create_pipeline(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new pipeline."""
        pipeline_id = params.get("pipeline_id")
        name = params.get("name", "Unnamed Pipeline")

        if not pipeline_id:
            return ActionResult(success=False, message="pipeline_id required")

        pipeline = Pipeline(pipeline_id=pipeline_id, name=name)
        self._pipelines[pipeline_id] = pipeline

        return ActionResult(
            success=True,
            message=f"Pipeline created: {pipeline_id}"
        )

    def _add_stage(self, params: Dict[str, Any]) -> ActionResult:
        """Add a stage to pipeline."""
        pipeline_id = params.get("pipeline_id")
        stage_id = params.get("stage_id")
        name = params.get("name", "")
        stage_type = StageType(params.get("stage_type", "transform"))

        if not pipeline_id or pipeline_id not in self._pipelines:
            return ActionResult(success=False, message="Invalid pipeline_id")

        if not stage_id:
            return ActionResult(success=False, message="stage_id required")

        pipeline = self._pipelines[pipeline_id]

        def placeholder_handler(data, params, context):
            return data

        pipeline.add_stage(
            stage_id=stage_id,
            name=name or stage_id,
            stage_type=stage_type,
            handler=placeholder_handler,
            params=params.get("params", {}),
            retry_count=params.get("retry_count", 0)
        )

        return ActionResult(
            success=True,
            message=f"Stage added: {stage_id}"
        )

    def _remove_stage(self, params: Dict[str, Any]) -> ActionResult:
        """Remove stage from pipeline."""
        pipeline_id = params.get("pipeline_id")
        stage_id = params.get("stage_id")

        if not pipeline_id or pipeline_id not in self._pipelines:
            return ActionResult(success=False, message="Invalid pipeline_id")

        pipeline = self._pipelines[pipeline_id]
        success = pipeline.remove_stage(stage_id)

        return ActionResult(
            success=success,
            message=f"Stage removed: {stage_id}" if success else "Stage not found"
        )

    def _enable_stage(self, params: Dict[str, Any]) -> ActionResult:
        """Enable or disable a stage."""
        pipeline_id = params.get("pipeline_id")
        stage_id = params.get("stage_id")
        enabled = params.get("enabled", True)

        if not pipeline_id or pipeline_id not in self._pipelines:
            return ActionResult(success=False, message="Invalid pipeline_id")

        pipeline = self._pipelines[pipeline_id]
        pipeline.enable_stage(stage_id, enabled)

        return ActionResult(
            success=True,
            message=f"Stage {stage_id} {'enabled' if enabled else 'disabled'}"
        )

    def _execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute pipeline."""
        pipeline_id = params.get("pipeline_id")
        data = params.get("data")

        if not pipeline_id or pipeline_id not in self._pipelines:
            return ActionResult(success=False, message="Invalid pipeline_id")

        pipeline = self._pipelines[pipeline_id]

        result_data, execution = pipeline.execute(
            data=data,
            params=params.get("params")
        )

        return ActionResult(
            success=execution.status == PipelineStatus.COMPLETED,
            data={
                "execution_id": execution.execution_id,
                "status": execution.status.value,
                "stages_executed": execution.stages_executed,
                "items_processed": execution.items_processed,
                "errors": execution.errors,
                "duration_ms": (
                    execution.completed_at - execution.started_at
                ) * 1000 if execution.completed_at else None
            }
        )

    def _execute_batch(self, params: Dict[str, Any]) -> ActionResult:
        """Execute pipeline on batch."""
        pipeline_id = params.get("pipeline_id")
        data_batch = params.get("data_batch", [])

        if not pipeline_id or pipeline_id not in self._pipelines:
            return ActionResult(success=False, message="Invalid pipeline_id")

        pipeline = self._pipelines[pipeline_id]

        results, execution = pipeline.execute_batch(
            data_batch=data_batch,
            params=params.get("params")
        )

        return ActionResult(
            success=execution.status == PipelineStatus.COMPLETED,
            data={
                "execution_id": execution.execution_id,
                "status": execution.status.value,
                "items_processed": execution.items_processed,
                "errors": execution.errors
            }
        )

    def _get_pipeline(self, params: Dict[str, Any]) -> ActionResult:
        """Get pipeline details."""
        pipeline_id = params.get("pipeline_id")

        if not pipeline_id:
            return ActionResult(
                success=True,
                data={
                    "pipelines": [
                        {"pipeline_id": p.pipeline_id, "name": p.name}
                        for p in self._pipelines.values()
                    ]
                }
            )

        if pipeline_id not in self._pipelines:
            return ActionResult(success=False, message="Pipeline not found")

        pipeline = self._pipelines[pipeline_id]

        return ActionResult(
            success=True,
            data={
                "pipeline_id": pipeline.pipeline_id,
                "name": pipeline.name,
                "stage_count": len(pipeline._stages),
                "stages": [
                    {
                        "stage_id": s.stage_id,
                        "name": s.name,
                        "type": s.stage_type.value,
                        "enabled": s.enabled
                    }
                    for s in pipeline._stages
                ]
            }
        )

    def _get_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get execution history."""
        pipeline_id = params.get("pipeline_id")
        limit = params.get("limit", 100)

        if pipeline_id and pipeline_id in self._pipelines:
            history = self._pipelines[pipeline_id].get_history(limit)
        else:
            history = []
            for pipeline in self._pipelines.values():
                history.extend(pipeline.get_history(limit))

        return ActionResult(
            success=True,
            data={
                "history": [
                    {
                        "execution_id": e.execution_id,
                        "pipeline_id": e.pipeline_id,
                        "status": e.status.value,
                        "started_at": e.started_at
                    }
                    for e in history
                ]
            }
        )

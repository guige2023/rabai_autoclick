"""Data pipeline action module for RabAI AutoClick.

Provides data pipeline operations:
- PipelineBuilderAction: Build data processing pipelines
- PipelineExecutorAction: Execute data pipelines
- PipelineMonitorAction: Monitor pipeline execution
- PipelineCheckpointAction: Checkpoint and recovery for pipelines
"""

import sys
import os
import time
import logging
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

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
    """Individual stage status."""
    WAITING = "waiting"
    PROCESSING = "processing"
    DONE = "done"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PipelineStage:
    """A single stage in a data pipeline."""
    name: str
    processor: Callable[[Any], Any]
    condition: Optional[Callable[[Any], bool]] = None
    retry_count: int = 0
    timeout: float = 300.0
    status: StageStatus = StageStatus.WAITING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    error: Optional[str] = None


@dataclass
class Pipeline:
    """Data processing pipeline."""
    pipeline_id: str
    name: str
    stages: List[PipelineStage] = field(default_factory=list)
    status: PipelineStatus = PipelineStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    total_processed: int = 0
    total_failed: int = 0


class PipelineRegistry:
    """Registry for managing pipelines."""

    def __init__(self) -> None:
        self._pipelines: Dict[str, Pipeline] = {}

    def create(self, pipeline_id: str, name: str) -> Pipeline:
        pipeline = Pipeline(pipeline_id=pipeline_id, name=name)
        self._pipelines[pipeline_id] = pipeline
        return pipeline

    def get(self, pipeline_id: str) -> Optional[Pipeline]:
        return self._pipelines.get(pipeline_id)

    def update(self, pipeline: Pipeline) -> None:
        self._pipelines[pipeline.pipeline_id] = pipeline

    def list_all(self) -> List[Pipeline]:
        return list(self._pipelines.values())

    def delete(self, pipeline_id: str) -> bool:
        if pipeline_id in self._pipelines:
            del self._pipelines[pipeline_id]
            return True
        return False


_registry = PipelineRegistry()


class PipelineBuilderAction(BaseAction):
    """Build a data processing pipeline."""
    action_type = "data_pipeline_builder"
    display_name = "构建数据管道"
    description = "构建数据处理管道的阶段和配置"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        pipeline_id = params.get("pipeline_id", "")
        name = params.get("name", "")
        stages_config = params.get("stages", [])

        if not pipeline_id or not name:
            return ActionResult(success=False, message="pipeline_id和name是必需的")

        pipeline = _registry.create(pipeline_id, name)

        for i, stage_config in enumerate(stages_config):
            stage_name = stage_config.get("name", f"stage_{i}")
            retry = stage_config.get("retry_count", 0)
            timeout = stage_config.get("timeout", 300.0)

            stage = PipelineStage(
                name=stage_name,
                processor=self._dummy_processor,
                retry_count=retry,
                timeout=timeout
            )
            pipeline.stages.append(stage)

        return ActionResult(
            success=True,
            message=f"管道 {name} 已构建，包含 {len(pipeline.stages)} 个阶段",
            data={
                "pipeline_id": pipeline_id,
                "stage_count": len(pipeline.stages)
            }
        )

    def _dummy_processor(self, data: Any) -> Any:
        return data


class PipelineExecutorAction(BaseAction):
    """Execute a data pipeline."""
    action_type = "data_pipeline_executor"
    display_name = "执行数据管道"
    description = "执行已构建的数据处理管道"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        pipeline_id = params.get("pipeline_id", "")
        input_data = params.get("input_data")
        stop_on_error = params.get("stop_on_error", True)

        pipeline = _registry.get(pipeline_id)
        if not pipeline:
            return ActionResult(success=False, message=f"管道 {pipeline_id} 不存在")

        if not pipeline.stages:
            return ActionResult(success=False, message="管道没有阶段")

        pipeline.status = PipelineStatus.RUNNING
        pipeline.started_at = datetime.now()
        _registry.update(pipeline)

        current_data = input_data
        results = []

        for stage in pipeline.stages:
            stage.status = StageStatus.PROCESSING
            stage.start_time = datetime.now()

            try:
                start = time.time()
                output = stage.processor(current_data)
                elapsed = time.time() - start

                stage.status = StageStatus.DONE
                stage.end_time = datetime.now()
                current_data = output
                pipeline.total_processed += 1

                results.append({
                    "stage": stage.name,
                    "status": "success",
                    "elapsed_ms": round(elapsed * 1000, 2),
                    "output_type": type(output).__name__
                })

            except Exception as e:
                stage.status = StageStatus.FAILED
                stage.error = str(e)
                stage.end_time = datetime.now()
                pipeline.total_failed += 1

                results.append({
                    "stage": stage.name,
                    "status": "failed",
                    "error": str(e)
                })

                if stop_on_error:
                    pipeline.status = PipelineStatus.FAILED
                    _registry.update(pipeline)
                    return ActionResult(
                        success=False,
                        message=f"管道在阶段 {stage.name} 失败: {e}",
                        data={"pipeline_id": pipeline_id, "results": results}
                    )

        pipeline.status = PipelineStatus.COMPLETED
        pipeline.completed_at = datetime.now()
        _registry.update(pipeline)

        return ActionResult(
            success=True,
            message=f"管道执行完成，处理 {pipeline.total_processed} 条数据",
            data={
                "pipeline_id": pipeline_id,
                "results": results,
                "total_processed": pipeline.total_processed,
                "total_failed": pipeline.total_failed
            }
        )


class PipelineMonitorAction(BaseAction):
    """Monitor pipeline execution status."""
    action_type = "data_pipeline_monitor"
    display_name = "监控数据管道"
    description = "监控数据管道的执行状态和性能"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        pipeline_id = params.get("pipeline_id", "")

        if pipeline_id:
            pipeline = _registry.get(pipeline_id)
            if not pipeline:
                return ActionResult(success=False, message=f"管道 {pipeline_id} 不存在")

            stages_data = []
            for s in pipeline.stages:
                duration = 0.0
                if s.start_time and s.end_time:
                    duration = (s.end_time - s.start_time).total_seconds()
                stages_data.append({
                    "name": s.name,
                    "status": s.status.value,
                    "duration_seconds": round(duration, 4),
                    "error": s.error
                })

            return ActionResult(
                success=True,
                message=f"管道 {pipeline.name} 状态: {pipeline.status.value}",
                data={
                    "pipeline_id": pipeline_id,
                    "name": pipeline.name,
                    "status": pipeline.status.value,
                    "created_at": pipeline.created_at.isoformat(),
                    "started_at": pipeline.started_at.isoformat() if pipeline.started_at else None,
                    "completed_at": pipeline.completed_at.isoformat() if pipeline.completed_at else None,
                    "total_processed": pipeline.total_processed,
                    "total_failed": pipeline.total_failed,
                    "stages": stages_data
                }
            )

        pipelines = _registry.list_all()
        summary = [
            {
                "pipeline_id": p.pipeline_id,
                "name": p.name,
                "status": p.status.value,
                "stage_count": len(p.stages),
                "total_processed": p.total_processed,
                "total_failed": p.total_failed
            }
            for p in pipelines
        ]

        return ActionResult(
            success=True,
            message=f"共 {len(summary)} 个管道",
            data={"pipelines": summary}
        )


class PipelineCheckpointAction(BaseAction):
    """Checkpoint and recovery for pipelines."""
    action_type = "data_pipeline_checkpoint"
    display_name = "数据管道检查点"
    description = "保存和恢复数据管道的检查点状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        pipeline_id = params.get("pipeline_id", "")
        operation = params.get("operation", "save")

        pipeline = _registry.get(pipeline_id)
        if not pipeline:
            return ActionResult(success=False, message=f"管道 {pipeline_id} 不存在")

        checkpoint_data = {
            "pipeline_id": pipeline.pipeline_id,
            "name": pipeline.name,
            "status": pipeline.status.value,
            "stages": [
                {
                    "name": s.name,
                    "status": s.status.value,
                    "error": s.error
                }
                for s in pipeline.stages
            ],
            "total_processed": pipeline.total_processed,
            "total_failed": pipeline.total_failed,
            "timestamp": datetime.now().isoformat()
        }

        if operation == "save":
            try:
                checkpoint_file = f"/tmp/pipeline_checkpoint_{pipeline_id}.json"
                import json
                with open(checkpoint_file, "w") as f:
                    json.dump(checkpoint_data, f, indent=2, default=str)
                return ActionResult(
                    success=True,
                    message=f"检查点已保存到 {checkpoint_file}",
                    data={"checkpoint_file": checkpoint_file, "data": checkpoint_data}
                )
            except Exception as e:
                return ActionResult(success=False, message=f"保存检查点失败: {e}")

        if operation == "restore":
            try:
                checkpoint_file = f"/tmp/pipeline_checkpoint_{pipeline_id}.json"
                import json
                with open(checkpoint_file, "r") as f:
                    saved = json.load(f)

                pipeline.status = PipelineStatus(saved.get("status", "pending"))
                pipeline.total_processed = saved.get("total_processed", 0)
                pipeline.total_failed = saved.get("total_failed", 0)

                _registry.update(pipeline)
                return ActionResult(
                    success=True,
                    message="检查点已恢复",
                    data={"restored": saved}
                )
            except FileNotFoundError:
                return ActionResult(success=False, message="检查点文件不存在")
            except Exception as e:
                return ActionResult(success=False, message=f"恢复检查点失败: {e}")

        return ActionResult(success=False, message=f"未知操作: {operation}")

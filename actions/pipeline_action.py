"""Pipeline action module for RabAI AutoClick.

Provides pipeline processing operations:
- PipelineCreateAction: Create a processing pipeline
- PipelineRunAction: Run data through pipeline
- PipelineStageAction: Add stage to pipeline
- PipelineStatsAction: Get pipeline statistics
"""

import time
import threading
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime


import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class PipelineStage:
    """Represents a pipeline stage."""
    name: str
    processor: Optional[Callable] = None
    timeout: float = 60.0
    retry_count: int = 0
    enabled: bool = True


@dataclass
class PipelineStats:
    """Pipeline execution statistics."""
    total_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    total_items_processed: int = 0
    total_time_ms: float = 0.0
    stage_stats: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class Pipeline:
    """Data processing pipeline."""
    def __init__(self, name: str):
        self.name = name
        self._stages: List[PipelineStage] = []
        self._stats = PipelineStats()
        self._lock = threading.Lock()

    def add_stage(self, stage: PipelineStage) -> None:
        with self._lock:
            self._stages.append(stage)

    def run(self, data: Any, context: Optional[Any] = None) -> Dict[str, Any]:
        results = []
        errors = []
        start_time = time.time()

        with self._lock:
            self._stats.total_runs += 1
            stages_snapshot = list(self._stages)

        current = data
        for stage in stages_snapshot:
            if not stage.enabled:
                continue
            try:
                if stage.processor:
                    result = stage.processor(current, context)
                    current = result
                    results.append({"stage": stage.name, "success": True})
                else:
                    results.append({"stage": stage.name, "success": True, "skipped": True})
            except Exception as e:
                errors.append({"stage": stage.name, "error": str(e)})
                if stage.retry_count > 0:
                    for retry in range(stage.retry_count):
                        try:
                            if stage.processor:
                                current = stage.processor(current, context)
                            results.append({"stage": stage.name, "success": True, "retry": retry + 1})
                            errors.pop()
                            break
                        except Exception:
                            pass
                if errors:
                    with self._lock:
                        self._stats.failed_runs += 1
                    break

        elapsed_ms = (time.time() - start_time) * 1000
        with self._lock:
            self._stats.total_time_ms += elapsed_ms
            self._stats.successful_runs += 1 if not errors else 0

        return {
            "results": results,
            "errors": errors,
            "output": current,
            "elapsed_ms": elapsed_ms,
            "stages_run": len(results)
        }

    def get_stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "total_runs": self._stats.total_runs,
                "successful_runs": self._stats.successful_runs,
                "failed_runs": self._stats.failed_runs,
                "total_time_ms": self._stats.total_time_ms,
                "avg_time_ms": self._stats.total_time_ms / self._stats.total_runs if self._stats.total_runs > 0 else 0,
                "stage_count": len(self._stages)
            }


_pipelines: Dict[str, Pipeline] = {}
_pipelines_lock = threading.Lock()


class PipelineCreateAction(BaseAction):
    """Create a processing pipeline."""
    action_type = "pipeline_create"
    display_name = "创建流水线"
    description = "创建数据处理流水线"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            stages = params.get("stages", [])

            if not name:
                return ActionResult(success=False, message="name is required")

            with _pipelines_lock:
                if name in _pipelines:
                    return ActionResult(success=True, message=f"Pipeline '{name}' already exists")
                pipeline = Pipeline(name=name)

                for s in stages:
                    stage = PipelineStage(
                        name=s.get("name", ""),
                        timeout=s.get("timeout", 60.0),
                        retry_count=s.get("retry_count", 0),
                        enabled=s.get("enabled", True)
                    )
                    pipeline.add_stage(stage)

                _pipelines[name] = pipeline

            return ActionResult(
                success=True,
                message=f"Pipeline '{name}' created with {len(stages)} stages",
                data={"name": name, "stage_count": len(stages)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline create failed: {str(e)}")


class PipelineRunAction(BaseAction):
    """Run data through pipeline."""
    action_type = "pipeline_run"
    display_name = "运行流水线"
    description = "运行数据通过流水线"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            data = params.get("data", None)
            context_data = params.get("context", {})

            if not name:
                return ActionResult(success=False, message="name is required")

            with _pipelines_lock:
                if name not in _pipelines:
                    return ActionResult(success=False, message=f"Pipeline '{name}' not found")
                pipeline = _pipelines[name]

            result = pipeline.run(data, context_data)

            return ActionResult(
                success=len(result["errors"]) == 0,
                message=f"Pipeline '{name}' run: {result['stages_run']} stages, {len(result['errors'])} errors",
                data=result
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline run failed: {str(e)}")


class PipelineStageAction(BaseAction):
    """Add stage to pipeline."""
    action_type = "pipeline_stage"
    display_name = "添加工具阶段"
    description = "向流水线添加阶段"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "")
            stage_name = params.get("stage_name", "")
            timeout = params.get("timeout", 60.0)
            retry_count = params.get("retry_count", 0)
            enabled = params.get("enabled", True)

            if not name or not stage_name:
                return ActionResult(success=False, message="name and stage_name are required")

            with _pipelines_lock:
                if name not in _pipelines:
                    return ActionResult(success=False, message=f"Pipeline '{name}' not found")
                pipeline = _pipelines[name]

                stage = PipelineStage(
                    name=stage_name,
                    timeout=timeout,
                    retry_count=retry_count,
                    enabled=enabled
                )
                pipeline.add_stage(stage)

            return ActionResult(
                success=True,
                message=f"Stage '{stage_name}' added to pipeline '{name}'",
                data={"pipeline": name, "stage": stage_name}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline stage failed: {str(e)}")


class PipelineStatsAction(BaseAction):
    """Get pipeline statistics."""
    action_type = "pipeline_stats"
    display_name = "流水线统计"
    description = "获取流水线统计"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", None)

            with _pipelines_lock:
                if name:
                    if name not in _pipelines:
                        return ActionResult(success=False, message=f"Pipeline '{name}' not found")
                    stats = _pipelines[name].get_stats()
                else:
                    all_stats = {n: p.get_stats() for n, p in _pipelines.items()}
                    return ActionResult(
                        success=True,
                        message=f"{len(all_stats)} pipelines",
                        data={"pipelines": all_stats, "count": len(all_stats)}
                    )

            return ActionResult(
                success=True,
                message=f"Pipeline '{name}' stats",
                data=stats
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline stats failed: {str(e)}")

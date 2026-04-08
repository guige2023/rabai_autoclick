"""Data pipeline action module for RabAI AutoClick.

Provides data pipeline operations:
- PipelineBuilderAction: Build multi-stage data processing pipelines
- PipelineExecutorAction: Execute a configured pipeline
- PipelineMonitorAction: Monitor pipeline execution progress
"""

import time
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PipelineStage:
    """Represents a single stage in a data pipeline."""

    def __init__(self, name: str, handler: Callable, config: Optional[Dict] = None):
        self.name = name
        self.handler = handler
        self.config = config or {}
        self.input_key = self.config.get("input_key", "data")
        self.output_key = self.config.get("output_key", "data")
        self.error_handler = self.config.get("error_handler", None)

    def execute(self, context: Any, input_data: Any) -> Any:
        try:
            return self.handler(context, {"data": input_data, "config": self.config})
        except Exception as e:
            if self.error_handler:
                return self.error_handler(context, {"data": input_data, "error": str(e)})
            raise


class PipelineBuilderAction(BaseAction):
    """Build multi-stage data processing pipelines."""
    action_type = "pipeline_builder"
    display_name = "数据流水线构建器"
    description = "构建多阶段数据处理流水线"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stages = params.get("stages", [])
            name = params.get("name", "pipeline")

            if not stages:
                return ActionResult(success=False, message="stages is required")

            pipeline_stages: List[PipelineStage] = []
            for i, stage_def in enumerate(stages):
                stage_name = stage_def.get("name", f"stage_{i}")
                stage_type = stage_def.get("type", "passthrough")

                def make_handler(st_type: str, cfg: Dict):
                    def handler(ctx: Any, inp: Dict) -> Any:
                        data = inp.get("data")
                        if st_type == "passthrough":
                            return data
                        elif st_type == "filter":
                            predicate = cfg.get("predicate")
                            return data if predicate else None
                        elif st_type == "transform":
                            transform_fn = cfg.get("transform_fn", lambda x: x)
                            return transform_fn(data)
                        elif st_type == "map":
                            map_fn = cfg.get("map_fn", lambda x: x)
                            if isinstance(data, list):
                                return [map_fn(item) for item in data]
                            return map_fn(data)
                        elif st_type == "reduce":
                            reduce_fn = cfg.get("reduce_fn", lambda acc, x: acc)
                            initial = cfg.get("initial", None)
                            if isinstance(data, list):
                                result = initial
                                for item in data:
                                    result = reduce_fn(result, item)
                                return result
                            return data
                        elif st_type == "flatten":
                            if isinstance(data, list):
                                result = []
                                for item in data:
                                    if isinstance(item, list):
                                        result.extend(item)
                                    else:
                                        result.append(item)
                                return result
                            return [data]
                        elif st_type == "dedupe":
                            if isinstance(data, list):
                                seen = set()
                                result = []
                                for item in data:
                                    key = str(item)
                                    if key not in seen:
                                        seen.add(key)
                                        result.append(item)
                                return result
                            return data
                        return data
                    return handler

                stage = PipelineStage(
                    name=stage_name,
                    handler=make_handler(stage_type, stage_def.get("config", {})),
                    config=stage_def.get("config", {}),
                )
                pipeline_stages.append(stage)

            return ActionResult(
                success=True,
                message=f"Pipeline '{name}' built with {len(pipeline_stages)} stages",
                data={"pipeline": pipeline_stages, "name": name, "stage_count": len(pipeline_stages)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"PipelineBuilder error: {e}")


class PipelineExecutorAction(BaseAction):
    """Execute a configured pipeline."""
    action_type = "pipeline_executor"
    display_name = "数据流水线执行器"
    description = "执行已配置的数据流水线"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pipeline = params.get("pipeline", [])
            input_data = params.get("input_data")
            stop_on_error = params.get("stop_on_error", True)

            if not pipeline:
                return ActionResult(success=False, message="pipeline is required")

            if input_data is None:
                return ActionResult(success=False, message="input_data is required")

            data = input_data
            stage_results = []
            start_time = time.time()

            for i, stage in enumerate(pipeline):
                stage_name = getattr(stage, "name", f"stage_{i}")
                stage_start = time.time()

                try:
                    data = stage.execute(context, data)
                    stage_duration = time.time() - stage_start
                    stage_results.append({
                        "stage": stage_name,
                        "success": True,
                        "duration_ms": int(stage_duration * 1000),
                        "output_type": type(data).__name__,
                    })
                except Exception as e:
                    stage_duration = time.time() - stage_start
                    stage_results.append({
                        "stage": stage_name,
                        "success": False,
                        "error": str(e),
                        "duration_ms": int(stage_duration * 1000),
                    })
                    if stop_on_error:
                        break

            total_duration = time.time() - start_time
            success_count = sum(1 for r in stage_results if r.get("success", False))

            return ActionResult(
                success=success_count == len(stage_results),
                message=f"Pipeline executed: {success_count}/{len(stage_results)} stages succeeded",
                data={
                    "output": data,
                    "stages": stage_results,
                    "total_duration_ms": int(total_duration * 1000),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"PipelineExecutor error: {e}")


class PipelineMonitorAction(BaseAction):
    """Monitor pipeline execution progress."""
    action_type = "pipeline_monitor"
    display_name = "数据流水线监控器"
    description = "监控流水线执行进度"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pipeline_state = params.get("pipeline_state", {})
            include_metrics = params.get("include_metrics", True)

            if not pipeline_state:
                return ActionResult(success=False, message="pipeline_state is required")

            stages = pipeline_state.get("stages", [])
            total_stages = len(stages)
            completed = sum(1 for s in stages if s.get("success") or s.get("error"))
            failed = sum(1 for s in stages if s.get("error"))
            total_duration = sum(s.get("duration_ms", 0) for s in stages)
            avg_duration = total_duration / total_stages if total_stages > 0 else 0

            progress_pct = (completed / total_stages * 100) if total_stages > 0 else 0

            return ActionResult(
                success=True,
                message=f"Pipeline progress: {progress_pct:.1f}% ({completed}/{total_stages})",
                data={
                    "total_stages": total_stages,
                    "completed": completed,
                    "failed": failed,
                    "progress_percent": round(progress_pct, 2),
                    "total_duration_ms": total_duration,
                    "avg_stage_duration_ms": int(avg_duration),
                    "stages": stages if include_metrics else [],
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"PipelineMonitor error: {e}")

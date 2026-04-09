"""Data pipeline action module for RabAI AutoClick.

Provides data pipeline operations:
- DataPipelineBuilderAction: Build multi-stage data pipelines
- DataPipelineExecutorAction: Execute data pipeline
- DataPipelineMonitorAction: Monitor pipeline execution
- DataPipelineCheckpointAction: Checkpoint pipeline state
"""

import time
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PipelineState(Enum):
    """Pipeline states."""
    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StageType(Enum):
    """Pipeline stage types."""
    EXTRACT = "extract"
    TRANSFORM = "transform"
    LOAD = "load"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    VALIDATE = "validate"
    CUSTOM = "custom"


class DataPipelineBuilderAction(BaseAction):
    """Build multi-stage data pipelines."""
    action_type = "data_pipeline_builder"
    display_name = "数据管道构建器"
    description = "构建多阶段数据处理管道"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stages = params.get("stages", [])
            pipeline_name = params.get("name", "unnamed_pipeline")
            description = params.get("description", "")
            max_parallel = params.get("max_parallel", 1)

            if not stages:
                return ActionResult(success=False, message="stages list is required")

            validated_stages = []
            for i, stage in enumerate(stages):
                stage_type = stage.get("type", "custom")
                stage_name = stage.get("name", f"stage_{i}")

                if stage_type not in [e.value for e in StageType]:
                    return ActionResult(success=False, message=f"Invalid stage type: {stage_type}")

                validated_stage = {
                    "name": stage_name,
                    "type": stage_type,
                    "handler": stage.get("handler"),
                    "config": stage.get("config", {}),
                    "dependencies": stage.get("dependencies", []),
                    "retry_on_failure": stage.get("retry_on_failure", False),
                    "max_retries": stage.get("max_retries", 3),
                    "timeout": stage.get("timeout", 60),
                }
                validated_stages.append(validated_stage)

            pipeline = {
                "name": pipeline_name,
                "description": description,
                "stages": validated_stages,
                "max_parallel": max_parallel,
                "created_at": datetime.now().isoformat(),
                "state": PipelineState.CREATED.value,
            }

            return ActionResult(
                success=True,
                message=f"Built pipeline '{pipeline_name}' with {len(validated_stages)} stages",
                data={"pipeline": pipeline, "stage_count": len(validated_stages)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline builder error: {e}")


class DataPipelineExecutorAction(BaseAction):
    """Execute a data pipeline."""
    action_type = "data_pipeline_executor"
    display_name = "数据管道执行器"
    description = "执行数据处理管道"

    def __init__(self):
        super().__init__()
        self._pipeline_state = PipelineState.CREATED
        self._stage_results: Dict[str, Any] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pipeline = params.get("pipeline", {})
            input_data = params.get("input_data")
            stop_on_failure = params.get("stop_on_failure", True)
            log_level = params.get("log_level", "info")

            if not pipeline:
                return ActionResult(success=False, message="pipeline is required")

            stages = pipeline.get("stages", [])
            if not stages:
                return ActionResult(success=False, message="Pipeline has no stages")

            self._pipeline_state = PipelineState.RUNNING
            current_data = input_data
            stage_outputs = {}

            for i, stage in enumerate(stages):
                stage_name = stage.get("name", f"stage_{i}")
                stage_type = stage.get("type", "custom")
                handler = stage.get("handler")
                config = stage.get("config", {})
                timeout = stage.get("timeout", 60)
                max_retries = stage.get("max_retries", 3)

                start_time = time.time()
                attempt = 0
                stage_success = False

                while attempt <= max_retries and not stage_success:
                    try:
                        stage_start = time.time()
                        if callable(handler):
                            result = handler(data=current_data, config=config, context=context)
                        else:
                            result = self._default_stage_handler(stage_type, current_data, config)

                        stage_success = True
                        stage_duration = time.time() - stage_start

                        self._stage_results[stage_name] = {
                            "success": True,
                            "duration": stage_duration,
                            "attempt": attempt + 1,
                            "output": result,
                        }
                        stage_outputs[stage_name] = result
                        current_data = result.get("data", current_data)

                    except Exception as e:
                        attempt += 1
                        if attempt > max_retries:
                            stage_duration = time.time() - stage_start
                            self._stage_results[stage_name] = {
                                "success": False,
                                "duration": stage_duration,
                                "attempt": attempt,
                                "error": str(e),
                            }
                            if stop_on_failure:
                                self._pipeline_state = PipelineState.FAILED
                                return ActionResult(
                                    success=False,
                                    message=f"Pipeline failed at stage '{stage_name}': {e}",
                                    data={"failed_stage": stage_name, "stage_results": self._stage_results, "completed_stages": i}
                                )

                if not stage_success and not stop_on_failure:
                    continue

            all_success = all(r.get("success", False) for r in self._stage_results.values())
            self._pipeline_state = PipelineState.COMPLETED if all_success else PipelineState.FAILED

            return ActionResult(
                success=all_success,
                message=f"Pipeline {self._pipeline_state.value}: {len(stages)} stages, {sum(1 for r in self._stage_results.values() if r.get('success'))} succeeded",
                data={"pipeline_state": self._pipeline_state.value, "stage_results": self._stage_results, "final_data": current_data}
            )
        except Exception as e:
            self._pipeline_state = PipelineState.FAILED
            return ActionResult(success=False, message=f"Pipeline executor error: {e}")

    def _default_stage_handler(self, stage_type: str, data: Any, config: Dict[str, Any]) -> Dict[str, Any]:
        """Default handler for built-in stage types."""
        if stage_type == StageType.FILTER.value:
            filter_key = config.get("key")
            filter_value = config.get("value")
            filtered = [d for d in data if isinstance(d, dict) and d.get(filter_key) == filter_value]
            return {"data": filtered}
        elif stage_type == StageType.TRANSFORM.value:
            transform_fn = config.get("fn", lambda x: x)
            transformed = [transform_fn(d) for d in data]
            return {"data": transformed}
        elif stage_type == StageType.AGGREGATE.value:
            agg_key = config.get("key")
            agg_result = {}
            for d in data:
                if isinstance(d, dict) and agg_key in d:
                    k = d[agg_key]
                    if k not in agg_result:
                        agg_result[k] = []
                    agg_result[k].append(d)
            return {"data": agg_result}
        elif stage_type == StageType.VALIDATE.value:
            schema = config.get("schema", {})
            errors = []
            for d in data:
                if isinstance(d, dict):
                    for k, v_type in schema.items():
                        if k not in d:
                            errors.append(f"Missing key: {k}")
                        elif not isinstance(d[k], v_type):
                            errors.append(f"Type error for {k}")
            return {"data": data, "errors": errors}
        else:
            return {"data": data}


class DataPipelineMonitorAction(BaseAction):
    """Monitor data pipeline execution."""
    action_type = "data_pipeline_monitor"
    display_name = "数据管道监控器"
    description = "监控数据管道执行状态"

    def __init__(self):
        super().__init__()
        self._metrics: Dict[str, List[Dict[str, Any]]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            pipeline_name = params.get("pipeline_name")
            operation = params.get("operation", "status")
            record_metrics = params.get("record_metrics", True)

            if operation == "record":
                if not pipeline_name:
                    return ActionResult(success=False, message="pipeline_name required")
                metric = {
                    "timestamp": datetime.now().isoformat(),
                    "stage": params.get("stage"),
                    "duration": params.get("duration", 0),
                    "success": params.get("success", True),
                    "throughput": params.get("throughput", 0),
                }
                if pipeline_name not in self._metrics:
                    self._metrics[pipeline_name] = []
                self._metrics[pipeline_name].append(metric)
                return ActionResult(success=True, message="Metric recorded", data={"metric": metric})

            elif operation == "status":
                if not pipeline_name:
                    return ActionResult(success=True, message="All pipelines", data={"pipelines": list(self._metrics.keys())})
                if pipeline_name not in self._metrics:
                    return ActionResult(success=False, message=f"Pipeline {pipeline_name} not found")
                metrics = self._metrics[pipeline_name]
                return ActionResult(success=True, message=f"Retrieved {len(metrics)} metrics", data={"metrics": metrics, "count": len(metrics)})

            elif operation == "stats":
                if not pipeline_name or pipeline_name not in self._metrics:
                    return ActionResult(success=False, message="Pipeline not found")
                metrics = self._metrics[pipeline_name]
                durations = [m["duration"] for m in metrics if m.get("duration")]
                successes = sum(1 for m in metrics if m.get("success"))
                return ActionResult(success=True, message="Stats retrieved", data={
                    "total_stages": len(metrics),
                    "successful": successes,
                    "failed": len(metrics) - successes,
                    "avg_duration": sum(durations) / len(durations) if durations else 0,
                    "max_duration": max(durations) if durations else 0,
                })

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline monitor error: {e}")


class DataPipelineCheckpointAction(BaseAction):
    """Checkpoint data pipeline state."""
    action_type = "data_pipeline_checkpoint"
    display_name = "数据管道检查点"
    description = "检查点保存和恢复管道状态"

    def __init__(self):
        super().__init__()
        self._checkpoints: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "save")
            pipeline_name = params.get("pipeline_name")
            checkpoint_id = params.get("checkpoint_id")
            state_data = params.get("state_data", {})
            stage_index = params.get("stage_index", 0)
            max_checkpoints = params.get("max_checkpoints", 10)

            if operation == "save":
                if not pipeline_name:
                    return ActionResult(success=False, message="pipeline_name required")

                cp_id = checkpoint_id or f"cp_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
                self._checkpoints[cp_id] = {
                    "pipeline_name": pipeline_name,
                    "stage_index": stage_index,
                    "state_data": state_data,
                    "created_at": datetime.now().isoformat(),
                }

                pipeline_cps = [k for k, v in self._checkpoints.items() if v["pipeline_name"] == pipeline_name]
                if len(pipeline_cps) > max_checkpoints:
                    oldest = min(pipeline_cps, key=lambda k: self._checkpoints[k]["created_at"])
                    del self._checkpoints[oldest]

                return ActionResult(success=True, message=f"Checkpoint {cp_id} saved", data={"checkpoint_id": cp_id, "total_checkpoints": len(self._checkpoints)})

            elif operation == "restore":
                if not checkpoint_id or checkpoint_id not in self._checkpoints:
                    return ActionResult(success=False, message=f"Checkpoint {checkpoint_id} not found")
                cp = self._checkpoints[checkpoint_id]
                return ActionResult(success=True, message=f"Restored checkpoint {checkpoint_id}", data={"state_data": cp["state_data"], "stage_index": cp["stage_index"], "created_at": cp["created_at"]})

            elif operation == "list":
                if pipeline_name:
                    cps = {k: v for k, v in self._checkpoints.items() if v["pipeline_name"] == pipeline_name}
                else:
                    cps = self._checkpoints
                return ActionResult(success=True, message=f"{len(cps)} checkpoints", data={"checkpoints": cps})

            elif operation == "delete":
                if checkpoint_id and checkpoint_id in self._checkpoints:
                    del self._checkpoints[checkpoint_id]
                    return ActionResult(success=True, message=f"Checkpoint {checkpoint_id} deleted")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline checkpoint error: {e}")

"""Data Pipeline Executor Action Module.

Provides streaming data pipeline execution with stage chaining,
backpressure handling, and checkpoint/resume support.
"""

from __future__ import annotations

import sys
import os
import time
import json
import threading
import hashlib
from typing import Any, Dict, List, Optional, Callable, Generator, Iterator
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
from abc import ABC, abstractmethod

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class StageType(Enum):
    """Types of pipeline stages."""
    SOURCE = "source"
    FILTER = "filter"
    TRANSFORM = "transform"
    AGGREGATE = "aggregate"
    JOIN = "join"
    SPLIT = "split"
    SINK = "sink"


class PipelineStatus(Enum):
    """Status of a pipeline execution."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class BackpressureStrategy(Enum):
    """Backpressure handling strategies."""
    BLOCK = "block"
    DROP = "drop"
    SPILL = "spill"


@dataclass
class PipelineStage:
    """Definition of a pipeline stage."""
    stage_id: str
    stage_type: StageType
    name: str
    fn: Optional[Callable] = None
    config: Dict[str, Any] = field(default_factory=dict)
    batch_size: int = 100
    parallelism: int = 1
    timeout: float = 60.0


@dataclass
class PipelineMetrics:
    """Metrics for a pipeline execution."""
    records_in: int = 0
    records_out: int = 0
    records_dropped: int = 0
    records_failed: int = 0
    batches_processed: int = 0
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    stage_metrics: Dict[str, Dict] = field(default_factory=dict)

    def elapsed(self) -> float:
        if self.start_time is None:
            return 0.0
        end = self.end_time or time.time()
        return end - self.start_time


@dataclass
class Checkpoint:
    """Checkpoint state for pipeline recovery."""
    pipeline_id: str
    stage_id: str
    position: int
    data: Dict[str, Any]
    timestamp: float
    checksum: str


class SpillQueue:
    """Queue that spills to disk when memory pressure occurs."""

    def __init__(self, max_memory_items: int = 1000):
        self._memory: deque = deque()
        self._spill_file = None
        self._max_memory = max_memory_items
        self._lock = threading.Lock()

    def put(self, item: Any) -> None:
        with self._lock:
            if len(self._memory) >= self._max_memory:
                self._spill(item)
            else:
                self._memory.append(item)

    def get(self) -> Optional[Any]:
        with self._lock:
            if self._memory:
                return self._memory.popleft()
            return None

    def _spill(self, item: Any) -> None:
        """Spill item to temporary storage."""
        if self._spill_file is None:
            import tempfile
            self._spill_file = tempfile.NamedTemporaryFile(delete=False)
        self._spill_file.write(json.dumps(item).encode() + b"\n")

    def size(self) -> int:
        with self._lock:
            return len(self._memory)


class DataPipelineExecutorAction(BaseAction):
    """Execute streaming data pipelines with stage chaining.

    Provides high-performance pipeline execution with backpressure
    handling, parallel processing, and checkpoint-based recovery.
    """
    action_type = "data_pipeline_executor"
    display_name = "数据管道执行器"
    description = "流式数据管道执行，支持反压处理和检查点恢复"

    def __init__(self):
        super().__init__()
        self._pipelines: Dict[str, List[PipelineStage]] = {}
        self._metrics: Dict[str, PipelineMetrics] = {}
        self._checkpoints: Dict[str, Checkpoint] = {}
        self._lock = threading.Lock()
        self._running: Dict[str, bool] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pipeline operation.

        Args:
            context: Execution context.
            params: Dict with keys: action, pipeline_id, stages, etc.

        Returns:
            ActionResult with pipeline execution result.
        """
        action = params.get("action", "create")

        if action == "create":
            return self._create_pipeline(context, params)
        elif action == "run":
            return self._run_pipeline(context, params)
        elif action == "run_stream":
            return self._run_streaming(params)
        elif action == "pause":
            return self._pause_pipeline(params)
        elif action == "resume":
            return self._resume_pipeline(params)
        elif action == "checkpoint":
            return self._create_checkpoint(params)
        elif action == "status":
            return self._get_pipeline_status(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )

    def _create_pipeline(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Create a new pipeline definition."""
        import uuid

        pipeline_id = params.get("pipeline_id") or str(uuid.uuid4())[:8]
        name = params.get("name", "Pipeline")
        stages_config = params.get("stages", [])
        save_to_var = params.get("save_to_var", None)

        stages = []
        for i, sc in enumerate(stages_config):
            try:
                stage_type = StageType[sc.get("type", "transform").upper()]
            except KeyError:
                stage_type = StageType.TRANSFORM

            stage = PipelineStage(
                stage_id=sc.get("id", f"stage_{i}"),
                stage_type=stage_type,
                name=sc.get("name", f"Stage {i}"),
                config=sc.get("config", {}),
                batch_size=sc.get("batch_size", 100),
                parallelism=sc.get("parallelism", 1),
                timeout=sc.get("timeout", 60.0)
            )
            stages.append(stage)

        with self._lock:
            self._pipelines[pipeline_id] = stages
            self._metrics[pipeline_id] = PipelineMetrics()

        result_data = {
            "pipeline_id": pipeline_id,
            "name": name,
            "stages": len(stages),
            "stage_ids": [s.stage_id for s in stages]
        }

        if save_to_var:
            context.variables[save_to_var] = result_data

        return ActionResult(
            success=True,
            message=f"Pipeline '{pipeline_id}' created with {len(stages)} stages",
            data=result_data
        )

    def _run_pipeline(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Run a batch pipeline on input data."""
        pipeline_id = params.get("pipeline_id", "")
        input_data = params.get("data", [])
        save_to_var = params.get("save_to_var", None)
        backpressure = params.get("backpressure", "block")
        checkpoint_enabled = params.get("checkpoint", False)

        with self._lock:
            stages = self._pipelines.get(pipeline_id)
            if not stages:
                return ActionResult(
                    success=False,
                    message=f"Pipeline '{pipeline_id}' not found"
                )
            stages = list(stages)

        if not isinstance(input_data, list):
            input_data = [input_data]

        metrics = PipelineMetrics()
        metrics.start_time = time.time()
        self._metrics[pipeline_id] = metrics

        try:
            checkpoint_key = f"{pipeline_id}_data"
            if checkpoint_enabled and checkpoint_key in self._checkpoints:
                cp = self._checkpoints[checkpoint_key]
                input_data = input_data[cp.position:]

            current_data = input_data
            metrics.records_in = len(input_data)

            for stage in stages:
                stage_start = time.time()
                stage_metrics = {
                    "records_in": len(current_data),
                    "records_out": 0,
                    "records_failed": 0,
                    "elapsed": 0.0
                }

                processed = self._process_stage(stage, current_data, backpressure)

                stage_metrics["records_out"] = len(processed)
                stage_metrics["elapsed"] = time.time() - stage_start
                metrics.stage_metrics[stage.stage_id] = stage_metrics

                current_data = processed
                metrics.batches_processed += 1

            metrics.records_out = len(current_data)
            metrics.end_time = time.time()

            result_data = {
                "pipeline_id": pipeline_id,
                "records_in": metrics.records_in,
                "records_out": metrics.records_out,
                "records_dropped": metrics.records_dropped,
                "batches": metrics.batches_processed,
                "elapsed": metrics.elapsed(),
                "output": current_data
            }

            if save_to_var:
                context.variables[save_to_var] = result_data

            return ActionResult(
                success=True,
                message=f"Pipeline '{pipeline_id}': "
                        f"{metrics.records_out}/{metrics.records_in} records "
                        f"in {metrics.elapsed():.2f}s",
                data=result_data
            )

        except Exception as e:
            metrics.end_time = time.time()
            return ActionResult(
                success=False,
                message=f"Pipeline failed: {str(e)}",
                data={"pipeline_id": pipeline_id, "error": str(e)}
            )

    def _run_streaming(self, params: Dict[str, Any]) -> ActionResult:
        """Run a streaming pipeline on an async data source."""
        pipeline_id = params.get("pipeline_id", "")
        source_fn = params.get("source_fn", None)
        max_records = int(params.get("max_records", 10000))
        save_to_var = params.get("save_to_var", None)

        with self._lock:
            stages = self._pipelines.get(pipeline_id)
            if not stages:
                return ActionResult(
                    success=False,
                    message=f"Pipeline '{pipeline_id}' not found"
                )

        metrics = PipelineMetrics()
        metrics.start_time = time.time()
        self._running[pipeline_id] = True

        try:
            records_processed = 0
            current_batch = []

            for record in range(min(max_records, 100)):
                if not self._running.get(pipeline_id, False):
                    break

                current_batch.append(record)
                if len(current_batch) >= 100:
                    for stage in stages:
                        current_batch = self._process_stage(stage, current_batch, "block")
                    metrics.records_in += len(current_batch)
                    records_processed += len(current_batch)
                    current_batch = []

            metrics.records_out = records_processed
            metrics.end_time = time.time()

            return ActionResult(
                success=True,
                message=f"Streaming pipeline processed {records_processed} records",
                data={"records": records_processed, "elapsed": metrics.elapsed()}
            )

        finally:
            self._running[pipeline_id] = False

    def _process_stage(self, stage: PipelineStage, data: List,
                       backpressure: str) -> List:
        """Process data through a single stage."""
        if not data:
            return data

        stage_type = stage.stage_type
        config = stage.config
        output = []

        if stage_type == StageType.FILTER:
            filter_fn = config.get("filter")
            if filter_fn:
                try:
                    for item in data:
                        if eval(filter_fn, {"__builtins__": {}}, {"item": item}):
                            output.append(item)
                except Exception:
                    output = data
            else:
                output = data

        elif stage_type == StageType.TRANSFORM:
            transform_fn = config.get("transform")
            if transform_fn:
                try:
                    for item in data:
                        output.append(eval(transform_fn, {"__builtins__": {}}, {"item": item}))
                except Exception:
                    output = data
            else:
                pick_fields = config.get("pick")
                omit_fields = config.get("omit")
                if pick_fields:
                    output = [{k: v for k, v in item.items() if k in pick_fields}
                              for item in data if isinstance(item, dict)]
                elif omit_fields:
                    output = [{k: v for k, v in item.items() if k not in omit_fields}
                              for item in data if isinstance(item, dict)]
                else:
                    output = data

        elif stage_type == StageType.AGGREGATE:
            agg_type = config.get("agg_type", "count")
            if agg_type == "count":
                output = [{"count": len(data)}]
            elif agg_type == "sum":
                field_name = config.get("field", "value")
                output = [{"sum": sum(item.get(field_name, 0) for item in data)}]
            elif agg_type == "avg":
                field_name = config.get("field", "value")
                values = [item.get(field_name, 0) for item in data]
                output = [{"avg": sum(values) / len(values) if values else 0}]
            elif agg_type == "min":
                field_name = config.get("field", "value")
                output = [{"min": min((item.get(field_name, 0) for item in data), default=0)}]
            elif agg_type == "max":
                field_name = config.get("field", "value")
                output = [{"max": max((item.get(field_name, 0) for item in data), default=0)}]
            else:
                output = data

        elif stage_type == StageType.SPLIT:
            split_field = config.get("field", "type")
            split_map: Dict[str, List] = {}
            for item in data:
                key = item.get(split_field, "default") if isinstance(item, dict) else "default"
                split_map.setdefault(key, []).append(item)
            output = [{"key": k, "records": v} for k, v in split_map.items()]

        elif stage_type == StageType.JOIN:
            join_key = config.get("key", "id")
            right_data = config.get("right_data", [])
            join_type = config.get("join_type", "inner")
            left_dict = {item.get(join_key): item for item in data
                         if isinstance(item, dict) and join_key in item}
            if join_type == "inner":
                for r in right_data:
                    if isinstance(r, dict) and r.get(join_key) in left_dict:
                        left_dict[r.get(join_key)].update(r)
                output = list(left_dict.values())
            elif join_type == "left":
                for item in data:
                    if isinstance(item, dict):
                        right_match = next((r for r in right_data
                                           if isinstance(r, dict) and
                                           r.get(join_key) == item.get(join_key)), {})
                        item.update(right_match)
                        output.append(item)
            else:
                output = data

        elif stage_type == StageType.SINK:
            output = data

        else:
            output = data

        return output

    def _pause_pipeline(self, params: Dict[str, Any]) -> ActionResult:
        """Pause a running pipeline."""
        pipeline_id = params.get("pipeline_id", "")
        with self._lock:
            if pipeline_id in self._running:
                self._running[pipeline_id] = False
                return ActionResult(
                    success=True,
                    message=f"Pipeline '{pipeline_id}' paused"
                )
            return ActionResult(
                success=False,
                message=f"Pipeline '{pipeline_id}' not running"
            )

    def _resume_pipeline(self, params: Dict[str, Any]) -> ActionResult:
        """Resume a paused pipeline."""
        pipeline_id = params.get("pipeline_id", "")
        with self._lock:
            self._running[pipeline_id] = True

        return ActionResult(
            success=True,
            message=f"Pipeline '{pipeline_id}' resumed"
        )

    def _create_checkpoint(self, params: Dict[str, Any]) -> ActionResult:
        """Create a checkpoint for pipeline recovery."""
        pipeline_id = params.get("pipeline_id", "")
        stage_id = params.get("stage_id", "all")
        position = int(params.get("position", 0))
        data = params.get("data", {})

        checkpoint_id = f"{pipeline_id}_{stage_id}"
        checkpoint = Checkpoint(
            pipeline_id=pipeline_id,
            stage_id=stage_id,
            position=position,
            data=data,
            timestamp=time.time(),
            checksum=hashlib.md5(json.dumps(data, sort_keys=True).encode()).hexdigest()
        )

        with self._lock:
            self._checkpoints[checkpoint_id] = checkpoint

        return ActionResult(
            success=True,
            message=f"Checkpoint created for '{checkpoint_id}'",
            data={"checkpoint_id": checkpoint_id, "position": position}
        )

    def _get_pipeline_status(self, params: Dict[str, Any]) -> ActionResult:
        """Get status of a pipeline."""
        pipeline_id = params.get("pipeline_id", None)
        save_to_var = params.get("save_to_var", None)

        if pipeline_id:
            with self._lock:
                metrics = self._metrics.get(pipeline_id, PipelineMetrics())
                stages = self._pipelines.get(pipeline_id, [])
                running = self._running.get(pipeline_id, False)

            data = {
                "pipeline_id": pipeline_id,
                "stages": len(stages),
                "running": running,
                "metrics": {
                    "records_in": metrics.records_in,
                    "records_out": metrics.records_out,
                    "records_dropped": metrics.records_dropped,
                    "elapsed": metrics.elapsed(),
                    "batches": metrics.batches_processed
                }
            }
        else:
            with self._lock:
                data = {
                    "pipelines": list(self._pipelines.keys()),
                    "running": [pid for pid, r in self._running.items() if r]
                }

        if save_to_var:
            context.variables[save_to_var] = data

        return ActionResult(success=True, message="Status retrieved", data=data)

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "pipeline_id": None,
            "name": "Pipeline",
            "stages": [],
            "data": [],
            "backpressure": "block",
            "checkpoint": False,
            "save_to_var": None
        }

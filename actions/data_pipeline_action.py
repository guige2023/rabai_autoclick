"""Data pipeline action module for RabAI AutoClick.

Provides data pipeline processing:
- PipelineBuilder: Build processing pipelines
- PipelineExecutor: Execute pipeline stages
- DataValidator: Validate pipeline input/output
- PipelineMonitor: Monitor pipeline execution
- StreamProcessor: Process data streams
- BatchProcessor: Batch process large datasets
"""

import time
import json
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PipelineStageType(Enum):
    """Pipeline stage types."""
    VALIDATE = "validate"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    OUTPUT = "output"


@dataclass
class PipelineStage:
    """Represents a single pipeline stage."""
    name: str
    stage_type: PipelineStageType
    handler: Callable[[Any], Any]
    error_handler: Optional[Callable[[Exception], Any]] = None
    timeout: float = 60.0
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineStats:
    """Pipeline execution statistics."""
    total_stages: int = 0
    completed_stages: int = 0
    failed_stages: int = 0
    total_items: int = 0
    processed_items: int = 0
    failed_items: int = 0
    start_time: float = 0.0
    end_time: float = 0.0
    stage_durations: Dict[str, float] = field(default_factory=dict)


class PipelineBuilder:
    """Build data processing pipelines."""

    def __init__(self, name: str = "pipeline"):
        self.name = name
        self.stages: List[PipelineStage] = []
        self._global_error_handler: Optional[Callable] = None

    def add_stage(
        self,
        name: str,
        stage_type: PipelineStageType,
        handler: Callable[[Any], Any],
        error_handler: Optional[Callable[[Exception], Any]] = None,
        timeout: float = 60.0,
        retry_count: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "PipelineBuilder":
        """Add a stage to the pipeline."""
        stage = PipelineStage(
            name=name,
            stage_type=stage_type,
            handler=handler,
            error_handler=error_handler or self._global_error_handler,
            timeout=timeout,
            retry_count=retry_count,
            metadata=metadata or {},
        )
        self.stages.append(stage)
        return self

    def add_validation(self, name: str, validator: Callable[[Any], bool],
                      error_msg: str = "Validation failed") -> "PipelineBuilder":
        """Add validation stage."""
        def handler(data):
            if not validator(data):
                raise ValueError(error_msg)
            return data
        return self.add_stage(name, PipelineStageType.VALIDATE, handler)

    def add_transformation(self, name: str, transformer: Callable[[Any], Any]) -> "PipelineBuilder":
        """Add transformation stage."""
        return self.add_stage(name, PipelineStageType.TRANSFORM, transformer)

    def add_filter(self, name: str, filter_fn: Callable[[Any], bool]) -> "PipelineBuilder":
        """Add filter stage."""
        def handler(data):
            return [item for item in data if filter_fn(item)]
        return self.add_stage(name, PipelineStageType.FILTER, handler)

    def add_aggregation(self, name: str, aggregator: Callable[[List], Any]) -> "PipelineBuilder":
        """Add aggregation stage."""
        return self.add_stage(name, PipelineStageType.AGGREGATE, aggregator)

    def set_error_handler(self, handler: Callable[[Exception], Any]) -> "PipelineBuilder":
        """Set global error handler."""
        self._global_error_handler = handler
        return self

    def build(self) -> "DataPipelineAction":
        """Build the pipeline action."""
        return DataPipelineAction(name=self.name, stages=self.stages)


class DataPipelineAction(BaseAction):
    """Execute data processing pipelines."""
    action_type = "data_pipeline"
    display_name = "数据管道"
    description = "数据处理管道执行"

    def __init__(self, name: str = "pipeline", stages: Optional[List[PipelineStage]] = None):
        super().__init__()
        self.name = name
        self.stages = stages or []
        self.stats = PipelineStats()
        self._lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            parallel = params.get("parallel", False)
            stop_on_error = params.get("stop_on_error", True)

            self.stats = PipelineStats(
                total_stages=len(self.stages),
                start_time=time.time(),
            )

            if parallel:
                result = self._execute_parallel(data, stop_on_error)
            else:
                result = self._execute_sequential(data, stop_on_error)

            self.stats.end_time = time.time()
            duration = self.stats.end_time - self.stats.start_time

            return ActionResult(
                success=result["success"],
                message=f"Pipeline completed in {duration:.2f}s",
                data={
                    "stats": self._get_stats_dict(),
                    "output": result.get("output"),
                    "errors": result.get("errors", []),
                },
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Pipeline error: {str(e)}")

    def _execute_sequential(self, data: Any, stop_on_error: bool) -> Dict[str, Any]:
        """Execute pipeline stages sequentially."""
        output = data
        errors = []

        for stage in self.stages:
            stage_start = time.time()
            self.stats.completed_stages += 1

            try:
                output = self._execute_stage(stage, output)
            except Exception as e:
                self.stats.failed_stages += 1
                errors.append({"stage": stage.name, "error": str(e)})
                if stop_on_error:
                    break

            stage_duration = time.time() - stage_start
            self.stats.stage_durations[stage.name] = stage_duration

        return {
            "success": len(errors) == 0,
            "output": output,
            "errors": errors,
        }

    def _execute_parallel(self, data: List, stop_on_error: bool) -> Dict[str, Any]:
        """Execute pipeline stages in parallel threads."""
        output = data
        errors = []
        threads = []

        def run_stage(stage: PipelineStage, input_data: Any, results: Dict, idx: int):
            try:
                results[idx] = self._execute_stage(stage, input_data)
            except Exception as e:
                results[idx] = None
                errors.append({"stage": stage.name, "error": str(e)})

        for stage in self.stages:
            results = {}
            t = threading.Thread(target=run_stage, args=(stage, output, results, 0))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        return {
            "success": len(errors) == 0,
            "output": output,
            "errors": errors,
        }

    def _execute_stage(self, stage: PipelineStage, data: Any) -> Any:
        """Execute a single stage with retries."""
        last_error = None

        for attempt in range(stage.retry_count + 1):
            try:
                if hasattr(stage.handler, "__call__"):
                    return stage.handler(data)
                return data
            except Exception as e:
                last_error = e
                if stage.error_handler:
                    return stage.error_handler(e)
                if attempt < stage.retry_count:
                    time.sleep(0.1 * (attempt + 1))

        if last_error:
            raise last_error
        return data

    def _get_stats_dict(self) -> Dict[str, Any]:
        """Get statistics as dictionary."""
        return {
            "total_stages": self.stats.total_stages,
            "completed_stages": self.stats.completed_stages,
            "failed_stages": self.stats.failed_stages,
            "total_items": self.stats.total_items,
            "processed_items": self.stats.processed_items,
            "failed_items": self.stats.failed_items,
            "duration": self.stats.end_time - self.stats.start_time,
            "stage_durations": self.stats.stage_durations,
        }


class StreamProcessorAction(BaseAction):
    """Process data streams."""
    action_type = "stream_processor"
    display_name = "流处理器"
    description = "数据流实时处理"

    def __init__(self):
        super().__init__()
        self._handlers: Dict[str, Callable] = {}
        self._buffer: List[Any] = []
        self._buffer_size = 100

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            stream_data = params.get("data", [])
            flush = params.get("flush", False)

            if flush:
                result = self._flush_buffer()
            else:
                self._buffer.extend(stream_data)
                if len(self._buffer) >= self._buffer_size:
                    result = self._flush_buffer()
                else:
                    result = {"processed": len(stream_data), "buffered": len(self._buffer)}

            return ActionResult(success=True, message="Stream processed", data=result)

        except Exception as e:
            return ActionResult(success=False, message=f"Stream error: {str(e)}")

    def _flush_buffer(self) -> Dict[str, Any]:
        """Flush and process buffered data."""
        processed = len(self._buffer)
        self._buffer.clear()
        return {"processed": processed}


class BatchProcessorAction(BaseAction):
    """Batch process large datasets."""
    action_type = "batch_processor"
    display_name = "批处理器"
    description = "大批量数据分批处理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            data = params.get("data", [])
            batch_size = params.get("batch_size", 100)
            processor = params.get("processor", lambda x: x)

            batches = [data[i:i+batch_size] for i in range(0, len(data), batch_size)]
            results = []

            for batch in batches:
                try:
                    result = processor(batch)
                    results.append({"success": True, "data": result})
                except Exception as e:
                    results.append({"success": False, "error": str(e)})

            return ActionResult(
                success=True,
                message=f"Processed {len(batches)} batches",
                data={"total_batches": len(batches), "results": results},
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Batch error: {str(e)}")

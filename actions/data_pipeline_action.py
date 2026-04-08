"""Data pipeline action module for RabAI AutoClick.

Provides pipeline processing for data operations:
- DataPipeline: Multi-stage data processing pipeline
- PipelineStage: Individual pipeline stage
- DataFlowController: Control data flow through pipeline
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import time
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum
from collections import deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class PipelineMode(Enum):
    """Pipeline execution modes."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    FANOUT = "fanout"
    PIPELINE_PARALLEL = "pipeline_parallel"


class StageStatus(Enum):
    """Stage execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PipelineStageConfig:
    """Configuration for a pipeline stage."""
    name: str
    processor: Optional[Callable] = None
    timeout: Optional[float] = None
    retry_count: int = 0
    continue_on_error: bool = False
    condition: Optional[Callable] = None
    parallel_workers: int = 1


@dataclass
class PipelineConfig:
    """Configuration for data pipeline."""
    mode: PipelineMode = PipelineMode.SEQUENTIAL
    buffer_size: int = 100
    max_parallel_stages: int = 3
    stop_on_first_error: bool = False
    enable_profiling: bool = False
    checkpoint_enabled: bool = False


class PipelineStage:
    """Individual stage in pipeline."""
    
    def __init__(self, config: PipelineStageConfig):
        self.config = config
        self.status = StageStatus.PENDING
        self.input_data: Any = None
        self.output_data: Any = None
        self.error: Optional[Exception] = None
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.attempts = 0
        self._lock = threading.RLock()
    
    @property
    def duration(self) -> Optional[float]:
        """Get stage duration."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None
    
    def execute(self, input_data: Any) -> Any:
        """Execute stage."""
        with self._lock:
            if self.config.condition and not self.config.condition(input_data):
                self.status = StageStatus.SKIPPED
                return input_data
            
            self.input_data = input_data
            self.status = StageStatus.RUNNING
            self.start_time = time.time()
            self.attempts += 1
        
        try:
            if self.config.processor is None:
                result = input_data
            elif self.config.timeout:
                result = self._execute_with_timeout(input_data)
            else:
                result = self.config.processor(input_data)
            
            self.output_data = result
            self.status = StageStatus.COMPLETED
            self.end_time = time.time()
            return result
            
        except Exception as e:
            self.error = e
            self.status = StageStatus.FAILED
            self.end_time = time.time()
            
            if self.config.retry_count > 0 and self.attempts <= self.config.retry_count:
                return self.execute(input_data)
            
            raise
    
    def _execute_with_timeout(self, input_data: Any) -> Any:
        """Execute with timeout."""
        result = [None]
        error = [None]
        
        def worker():
            try:
                result[0] = self.config.processor(input_data)
            except Exception as e:
                error[0] = e
        
        thread = threading.Thread(target=worker)
        thread.daemon = True
        thread.start()
        thread.join(timeout=self.config.timeout)
        
        if thread.is_alive():
            raise TimeoutError(f"Stage {self.config.name} timed out after {self.config.timeout}s")
        if error[0]:
            raise error[0]
        return result[0]


class DataPipeline:
    """Data processing pipeline."""
    
    def __init__(self, name: str, config: Optional[PipelineConfig] = None):
        self.name = name
        self.config = config or PipelineConfig()
        self._stages: List[PipelineStage] = []
        self._checkpoints: Dict[str, Any] = {}
        self._results: List[Any] = []
        self._lock = threading.RLock()
        self._stats = {"total_items": 0, "processed_items": 0, "failed_items": 0, "skipped_items": 0}
    
    def add_stage(self, stage_config: PipelineStageConfig) -> "DataPipeline":
        """Add stage to pipeline."""
        with self._lock:
            self._stages.append(PipelineStage(stage_config))
        return self
    
    def add_stage_fn(self, name: str, processor: Callable, **kwargs) -> "DataPipeline":
        """Add stage with function."""
        config = PipelineStageConfig(name=name, processor=processor, **kwargs)
        return self.add_stage(config)
    
    def execute(self, input_data: Any) -> Tuple[bool, List[Any]]:
        """Execute pipeline on input data."""
        with self._lock:
            self._results = []
            current_data = input_data
            self._stats["total_items"] += 1
        
        for stage in self._stages:
            try:
                current_data = stage.execute(current_data)
                self._results.append(current_data)
            except Exception as e:
                if self.config.stop_on_first_error or not stage.config.continue_on_error:
                    with self._lock:
                        self._stats["failed_items"] += 1
                    return False, self._results
                with self._lock:
                    self._stats["failed_items"] += 1
        
        with self._lock:
            self._stats["processed_items"] += 1
        
        return True, self._results
    
    def execute_batch(self, inputs: List[Any]) -> List[Tuple[bool, List[Any]]]:
        """Execute pipeline on batch of inputs."""
        if self.config.mode == PipelineMode.PARALLEL:
            return self._execute_parallel(inputs)
        return [self.execute(inp) for inp in inputs]
    
    def _execute_parallel(self, inputs: List[Any]) -> List[Tuple[bool, List[Any]]]:
        """Execute pipeline in parallel."""
        results = [None] * len(inputs)
        threads = []
        
        def worker(idx: int, inp: Any):
            results[idx] = self.execute(inp)
        
        for i, inp in enumerate(inputs):
            t = threading.Thread(target=worker, args=(i, inp))
            threads.append(t)
            t.start()
            
            if len(threads) >= self.config.max_parallel_stages:
                for t in threads:
                    t.join()
                threads = []
        
        for t in threads:
            t.join()
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics."""
        with self._lock:
            stage_stats = []
            for stage in self._stages:
                stage_stats.append({
                    "name": stage.config.name,
                    "status": stage.status.value,
                    "duration": stage.duration,
                    "attempts": stage.attempts,
                    "error": str(stage.error) if stage.error else None,
                })
            
            return {
                "name": self.name,
                "stage_count": len(self._stages),
                **{k: v for k, v in self._stats.items()},
                "stages": stage_stats,
            }


class DataPipelineAction(BaseAction):
    """Data pipeline action."""
    action_type = "data_pipeline"
    display_name = "数据流水线"
    description = "多阶段数据处理流水线"
    
    def __init__(self):
        super().__init__()
        self._pipelines: Dict[str, DataPipeline] = {}
        self._lock = threading.Lock()
    
    def _get_pipeline(self, name: str, config: Optional[PipelineConfig] = None) -> DataPipeline:
        """Get or create pipeline."""
        with self._lock:
            if name not in self._pipelines:
                self._pipelines[name] = DataPipeline(name, config)
            return self._pipelines[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute pipeline operation."""
        try:
            pipeline_name = params.get("pipeline", "default")
            command = params.get("command", "execute")
            input_data = params.get("input_data")
            
            config = PipelineConfig(
                mode=PipelineMode[params.get("mode", "sequential").upper()],
                max_parallel_stages=params.get("max_parallel_stages", 3),
                stop_on_first_error=params.get("stop_on_first_error", False),
            )
            
            pipeline = self._get_pipeline(pipeline_name, config)
            
            if command == "add_stage":
                stage_name = params.get("stage_name")
                processor = params.get("processor")
                
                if stage_name and processor:
                    pipeline.add_stage_fn(stage_name, processor, 
                                         timeout=params.get("stage_timeout"),
                                         retry_count=params.get("retry_count", 0),
                                         continue_on_error=params.get("continue_on_error", False))
                return ActionResult(success=True, message=f"Stage {stage_name} added")
            
            elif command == "execute" and input_data is not None:
                success, results = pipeline.execute(input_data)
                return ActionResult(success=success, data={"results": results, "count": len(results)})
            
            elif command == "execute_batch" and input_data is not None:
                inputs = input_data if isinstance(input_data, list) else [input_data]
                batch_results = pipeline.execute_batch(inputs)
                return ActionResult(success=True, data={"results": batch_results, "count": len(batch_results)})
            
            elif command == "stats":
                stats = pipeline.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"DataPipelineAction error: {str(e)}")

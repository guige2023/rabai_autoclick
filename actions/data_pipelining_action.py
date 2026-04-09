"""
Data Pipelining Action Module.

Provides data pipeline construction and execution capabilities including
stages, transformations, branching, and error handling for data workflows.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, TypeVar, Generic
from dataclasses import dataclass, field
from enum import Enum
import threading
import time
from datetime import datetime
from collections import deque


T = TypeVar('T')
R = TypeVar('R')


class StageType(Enum):
    """Types of pipeline stages."""
    SOURCE = "source"
    TRANSFORM = "transform"
    FILTER = "filter"
    AGGREGATE = "aggregate"
    SINK = "sink"
    BRANCH = "branch"
    MERGE = "merge"


class PipelineStatus(Enum):
    """Pipeline execution status."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class PipelineStage:
    """Represents a stage in the pipeline."""
    id: str
    name: str
    stage_type: StageType
    func: Optional[Callable] = None
    params: Dict[str, Any] = field(default_factory=dict)
    error_handler: Optional[Callable] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution."""
    name: str
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout: Optional[float] = None
    buffer_size: int = 100
    parallel: bool = False
    max_workers: int = 4


@dataclass
class PipelineResult:
    """Result of pipeline execution."""
    pipeline_name: str
    status: PipelineStatus
    input_count: int
    output_count: int
    failed_count: int
    duration_seconds: float
    stage_results: Dict[str, Any]
    errors: List[str]
    started_at: datetime
    completed_at: datetime


class Pipeline:
    """
    Data pipeline with configurable stages.
    
    Example:
        pipeline = Pipeline(name="etl_pipeline")
        pipeline.add_stage("extract", StageType.SOURCE, extract_func)
        pipeline.add_stage("transform", StageType.TRANSFORM, transform_func)
        pipeline.add_stage("load", StageType.SINK, load_func)
        
        result = pipeline.execute(input_data)
    """
    
    def __init__(self, name: str, config: Optional[PipelineConfig] = None):
        self.name = name
        self.config = config or PipelineConfig(name=name)
        self.stages: List[PipelineStage] = []
        self.branches: Dict[str, List[PipelineStage]] = {}
        self._status = PipelineStatus.IDLE
        self._lock = threading.RLock()
    
    def add_stage(
        self,
        stage_id: str,
        name: str,
        stage_type: StageType,
        func: Optional[Callable] = None,
        params: Optional[Dict] = None,
        **kwargs
    ) -> "Pipeline":
        """Add a stage to the pipeline."""
        with self._lock:
            stage = PipelineStage(
                id=stage_id,
                name=name,
                stage_type=stage_type,
                func=func,
                params=params or {},
                **kwargs
            )
            self.stages.append(stage)
        return self
    
    def add_branch(
        self,
        branch_name: str,
        stages: List[tuple]
    ) -> "Pipeline":
        """Add a branch with multiple stages."""
        with self._lock:
            branch_stages = []
            for stage_id, name, stage_type, func in stages:
                branch_stages.append(PipelineStage(
                    id=stage_id,
                    name=name,
                    stage_type=stage_type,
                    func=func
                ))
            self.branches[branch_name] = branch_stages
        return self
    
    def execute(
        self,
        input_data: Any,
        context: Optional[Dict] = None
    ) -> PipelineResult:
        """Execute the pipeline."""
        start_time = datetime.now()
        
        with self._lock:
            self._status = PipelineStatus.RUNNING
        
        context = context or {}
        stage_results = {}
        errors = []
        current_data = input_data
        input_count = self._count_items(input_data)
        failed_count = 0
        
        try:
            for stage in self.stages:
                if self._status == PipelineStatus.CANCELLED:
                    break
                
                try:
                    result = self._execute_stage(stage, current_data, context)
                    stage_results[stage.id] = {"success": True, "output": result}
                    current_data = result
                    
                except Exception as e:
                    failed_count += 1
                    error_msg = f"Stage {stage.id} failed: {str(e)}"
                    errors.append(error_msg)
                    
                    if stage.error_handler:
                        try:
                            current_data = stage.error_handler(current_data, e)
                            stage_results[stage.id] = {"success": False, "error_handled": True}
                        except Exception as handler_err:
                            stage_results[stage.id] = {"success": False, "error": str(handler_err)}
                            if self.config.max_retries > 0:
                                current_data = self._retry_stage(stage, current_data, context)
                    else:
                        if self.config.max_retries > 0:
                            current_data = self._retry_stage(stage, current_data, context)
                        else:
                            raise
            
            status = PipelineStatus.COMPLETED if not errors else PipelineStatus.FAILED
            
        except Exception as e:
            status = PipelineStatus.FAILED
            errors.append(str(e))
        
        finally:
            with self._lock:
                self._status = status
        
        completed_at = datetime.now()
        output_count = self._count_items(current_data)
        
        return PipelineResult(
            pipeline_name=self.name,
            status=status,
            input_count=input_count,
            output_count=output_count,
            failed_count=failed_count,
            duration_seconds=(completed_at - start_time).total_seconds(),
            stage_results=stage_results,
            errors=errors,
            started_at=start_time,
            completed_at=completed_at
        )
    
    def _execute_stage(
        self,
        stage: PipelineStage,
        data: Any,
        context: Dict
    ) -> Any:
        """Execute a single stage."""
        if stage.func is None:
            return data
        
        # Apply stage based on type
        if stage.stage_type == StageType.FILTER:
            return [x for x in data if stage.func(x, stage.params, context)]
        else:
            return stage.func(data, stage.params, context)
    
    def _retry_stage(
        self,
        stage: PipelineStage,
        data: Any,
        context: Dict
    ) -> Any:
        """Retry a failed stage."""
        for attempt in range(self.config.max_retries):
            try:
                time.sleep(self.config.retry_delay * (attempt + 1))
                return self._execute_stage(stage, data, context)
            except Exception:
                if attempt == self.config.max_retries - 1:
                    raise
        return data
    
    def _count_items(self, data: Any) -> int:
        """Count items in data."""
        if isinstance(data, list):
            return len(data)
        elif isinstance(data, dict):
            return len(data)
        return 1
    
    def pause(self):
        """Pause pipeline execution."""
        with self._lock:
            self._status = PipelineStatus.PAUSED
    
    def resume(self):
        """Resume pipeline execution."""
        with self._lock:
            if self._status == PipelineStatus.PAUSED:
                self._status = PipelineStatus.RUNNING
    
    def cancel(self):
        """Cancel pipeline execution."""
        with self._lock:
            self._status = PipelineStatus.CANCELLED
    
    def get_status(self) -> PipelineStatus:
        """Get current status."""
        with self._lock:
            return self._status


class PipelineBuilder:
    """
    Builder for constructing pipelines fluently.
    
    Example:
        pipeline = (
            PipelineBuilder("etl")
            .source(extract_data)
            .transform(clean_data)
            .filter(validate_record)
            .sink(load_data)
            .build()
        )
    """
    
    def __init__(self, name: str):
        self._pipeline = Pipeline(name)
        self._stage_counter = 0
    
    def source(
        self,
        func: Callable,
        params: Optional[Dict] = None
    ) -> "PipelineBuilder":
        """Add a source stage."""
        self._stage_counter += 1
        self._pipeline.add_stage(
            stage_id=f"source_{self._stage_counter}",
            name="Source",
            stage_type=StageType.SOURCE,
            func=func,
            params=params
        )
        return self
    
    def transform(
        self,
        func: Callable,
        params: Optional[Dict] = None,
        name: str = "Transform"
    ) -> "PipelineBuilder":
        """Add a transform stage."""
        self._stage_counter += 1
        self._pipeline.add_stage(
            stage_id=f"transform_{self._stage_counter}",
            name=name,
            stage_type=StageType.TRANSFORM,
            func=func,
            params=params
        )
        return self
    
    def filter(
        self,
        predicate: Callable,
        params: Optional[Dict] = None
    ) -> "PipelineBuilder":
        """Add a filter stage."""
        self._stage_counter += 1
        self._pipeline.add_stage(
            stage_id=f"filter_{self._stage_counter}",
            name="Filter",
            stage_type=StageType.FILTER,
            func=predicate,
            params=params
        )
        return self
    
    def aggregate(
        self,
        func: Callable,
        params: Optional[Dict] = None
    ) -> "PipelineBuilder":
        """Add an aggregate stage."""
        self._stage_counter += 1
        self._pipeline.add_stage(
            stage_id=f"aggregate_{self._stage_counter}",
            name="Aggregate",
            stage_type=StageType.AGGREGATE,
            func=func,
            params=params
        )
        return self
    
    def sink(
        self,
        func: Callable,
        params: Optional[Dict] = None,
        name: str = "Sink"
    ) -> "PipelineBuilder":
        """Add a sink stage."""
        self._stage_counter += 1
        self._pipeline.add_stage(
            stage_id=f"sink_{self._stage_counter}",
            name=name,
            stage_type=StageType.SINK,
            func=func,
            params=params
        )
        return self
    
    def on_error(
        self,
        handler: Callable
    ) -> "PipelineBuilder":
        """Set error handler for last stage."""
        if self._pipeline.stages:
            self._pipeline.stages[-1].error_handler = handler
        return self
    
    def build(self) -> Pipeline:
        """Build the pipeline."""
        return self._pipeline


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class DataPipeliningAction(BaseAction):
    """
    Data pipelining action for ETL and data workflows.
    
    Parameters:
        operation: Operation type (create/execute/status)
        name: Pipeline name
        stages: List of stage definitions
        input_data: Input data for pipeline
    
    Example:
        action = DataPipeliningAction()
        result = action.execute({}, {
            "operation": "execute",
            "name": "etl_pipeline",
            "stages": [
                {"type": "source", "func": "extract"},
                {"type": "transform", "func": "clean"},
                {"type": "sink", "func": "load"}
            ]
        })
    """
    
    _pipelines: Dict[str, Pipeline] = {}
    _lock = threading.Lock()
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute pipeline operation."""
        operation = params.get("operation", "create")
        name = params.get("name", "pipeline")
        stages = params.get("stages", [])
        input_data = params.get("input_data", [])
        max_retries = params.get("max_retries", 3)
        
        if operation == "create":
            pipeline = Pipeline(name=name)
            
            for i, stage_def in enumerate(stages):
                def placeholder_func(data, params, ctx):
                    return data
                
                pipeline.add_stage(
                    stage_id=stage_def.get("id", f"stage_{i}"),
                    name=stage_def.get("name", f"Stage {i}"),
                    stage_type=StageType(stage_def.get("type", "transform")),
                    func=placeholder_func,
                    params=stage_def.get("params", {})
                )
            
            with self._lock:
                self._pipelines[name] = pipeline
            
            return {
                "success": True,
                "operation": "create",
                "name": name,
                "stage_count": len(stages),
                "created_at": datetime.now().isoformat()
            }
        
        elif operation == "execute":
            with self._lock:
                if name not in self._pipelines:
                    return {"success": False, "error": f"Pipeline '{name}' not found"}
                pipeline = self._pipelines[name]
            
            result = pipeline.execute(input_data)
            
            return {
                "success": result.status == PipelineStatus.COMPLETED,
                "operation": "execute",
                "name": name,
                "status": result.status.value,
                "input_count": result.input_count,
                "output_count": result.output_count,
                "failed_count": result.failed_count,
                "duration_seconds": result.duration_seconds,
                "started_at": result.started_at.isoformat(),
                "completed_at": result.completed_at.isoformat()
            }
        
        elif operation == "status":
            with self._lock:
                if name not in self._pipelines:
                    return {"success": False, "error": f"Pipeline '{name}' not found"}
                pipeline = self._pipelines[name]
            
            return {
                "success": True,
                "operation": "status",
                "name": name,
                "status": pipeline.get_status().value
            }
        
        elif operation == "cancel":
            with self._lock:
                if name in self._pipelines:
                    self._pipelines[name].cancel()
                    return {"success": True, "operation": "cancel", "name": name}
            return {"success": False, "error": "Pipeline not found"}
        
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}

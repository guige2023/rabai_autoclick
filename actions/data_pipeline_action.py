"""
Data Pipeline Action.

Provides data pipeline orchestration with support for:
- Stage-based processing
- Parallel and sequential execution
- Error handling and retries
- Progress tracking and monitoring
"""

from typing import Dict, List, Optional, Any, Callable, Awaitable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import threading
import asyncio
import logging
import json
import time
from concurrent.futures import ThreadPoolExecutor, Future

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
    """Stage execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


@dataclass
class StageResult:
    """Result of a stage execution."""
    stage_name: str
    status: StageStatus
    input_data: Any = None
    output_data: Any = None
    error: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> Optional[float]:
        """Get stage duration in milliseconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds() * 1000
        return None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "stage_name": self.stage_name,
            "status": self.status.value,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "retry_count": self.retry_count,
            "metadata": self.metadata
        }


@dataclass
class PipelineConfig:
    """Configuration for a data pipeline."""
    name: str
    stages: List[str]
    parallel_stages: Optional[Dict[str, List[str]]] = None
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout: Optional[float] = None
    continue_on_error: bool = False


@dataclass
class PipelineContext:
    """Context passed through pipeline stages."""
    data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    results: Dict[str, StageResult] = field(default_factory=dict)
    
    def get_result(self, stage_name: str) -> Optional[StageResult]:
        """Get the result of a specific stage."""
        return self.results.get(stage_name)
    
    def get_stage_output(self, stage_name: str) -> Any:
        """Get the output of a specific stage."""
        result = self.results.get(stage_name)
        return result.output_data if result else None


StageProcessor = Callable[[Any, PipelineContext], Awaitable[Any]]


class DataPipelineAction:
    """
    Data Pipeline Action.
    
    Orchestrates multi-stage data processing with support for:
    - Sequential and parallel stage execution
    - Configurable retry logic
    - Progress tracking and monitoring
    - Error handling with continue-on-error mode
    - Async and sync stage processors
    """
    
    def __init__(self, config: PipelineConfig):
        """
        Initialize the Data Pipeline Action.
        
        Args:
            config: Pipeline configuration
        """
        self.config = config
        self.stages: Dict[str, StageProcessor] = {}
        self.status = PipelineStatus.PENDING
        self.context: Optional[PipelineContext] = None
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self._cancel_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()  # Not paused by default
        self._executors: Dict[str, ThreadPoolExecutor] = {}
    
    def register_stage(
        self,
        stage_name: str,
        processor: StageProcessor,
        description: Optional[str] = None
    ) -> "DataPipelineAction":
        """
        Register a stage processor.
        
        Args:
            stage_name: Name of the stage
            processor: Async function to process the stage
            description: Optional stage description
        
        Returns:
            Self for chaining
        """
        if stage_name not in self.config.stages:
            raise ValueError(f"Stage '{stage_name}' not defined in pipeline config")
        
        self.stages[stage_name] = processor
        return self
    
    async def execute(
        self,
        initial_data: Any,
        metadata: Optional[Dict[str, Any]] = None
    ) -> PipelineContext:
        """
        Execute the pipeline.
        
        Args:
            initial_data: Initial data to pass to the first stage
            metadata: Optional metadata to include in context
        
        Returns:
            Pipeline execution context with all stage results
        """
        if self.status == PipelineStatus.RUNNING:
            raise RuntimeError("Pipeline is already running")
        
        self.status = PipelineStatus.RUNNING
        self.start_time = datetime.utcnow()
        self.context = PipelineContext(
            data={"initial": initial_data},
            metadata=metadata or {}
        )
        self._cancel_event.clear()
        self._pause_event.set()
        
        logger.info(f"Starting pipeline: {self.config.name}")
        
        try:
            await self._execute_stages(initial_data)
            
            if self._cancel_event.is_set():
                self.status = PipelineStatus.CANCELLED
            else:
                self.status = PipelineStatus.COMPLETED
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            self.status = PipelineStatus.FAILED
            if self.context:
                self.context.metadata["error"] = str(e)
        
        finally:
            self.end_time = datetime.utcnow()
            logger.info(f"Pipeline finished with status: {self.status.value}")
        
        return self.context
    
    async def _execute_stages(self, initial_data: Any) -> None:
        """Execute all pipeline stages."""
        current_data = initial_data
        
        for stage_name in self.config.stages:
            if self._cancel_event.is_set():
                break
            
            self._pause_event.wait()
            
            if stage_name not in self.stages:
                logger.warning(f"Stage '{stage_name}' has no processor, skipping")
                continue
            
            result = await self._execute_stage(stage_name, current_data)
            self.context.results[stage_name] = result
            
            if result.status == StageStatus.FAILED:
                if not self.config.continue_on_error:
                    raise RuntimeError(f"Stage '{stage_name}' failed: {result.error}")
                logger.warning(f"Stage '{stage_name}' failed, continuing: {result.error}")
            
            if result.output_data is not None:
                current_data = result.output_data
                self.context.data[stage_name] = current_data
    
    async def _execute_stage(
        self,
        stage_name: str,
        input_data: Any,
        retry_count: int = 0
    ) -> StageResult:
        """Execute a single stage with retry logic."""
        result = StageResult(
            stage_name=stage_name,
            status=StageStatus.RUNNING,
            input_data=input_data,
            start_time=datetime.utcnow()
        )
        
        try:
            if self.config.timeout:
                output = await asyncio.wait_for(
                    self.stages[stage_name](input_data, self.context),
                    timeout=self.config.timeout
                )
            else:
                output = await self.stages[stage_name](input_data, self.context)
            
            result.output_data = output
            result.status = StageStatus.COMPLETED
            logger.debug(f"Stage '{stage_name}' completed")
            
        except asyncio.TimeoutError:
            result.error = f"Stage timed out after {self.config.timeout}s"
            result.status = StageStatus.FAILED
            logger.error(f"Stage '{stage_name}' timed out")
            
        except Exception as e:
            result.error = str(e)
            
            if retry_count < self.config.max_retries:
                result.status = StageStatus.RETRYING
                result.retry_count = retry_count + 1
                logger.warning(
                    f"Stage '{stage_name}' failed, retrying "
                    f"({retry_count + 1}/{self.config.max_retries})"
                )
                await asyncio.sleep(self.config.retry_delay)
                return await self._execute_stage(
                    stage_name,
                    input_data,
                    retry_count + 1
                )
            
            result.status = StageStatus.FAILED
            logger.error(f"Stage '{stage_name}' failed after {retry_count} retries: {e}")
        
        result.end_time = datetime.utcnow()
        return result
    
    def cancel(self) -> None:
        """Cancel the pipeline execution."""
        self._cancel_event.set()
        logger.info(f"Pipeline '{self.config.name}' cancellation requested")
    
    def pause(self) -> None:
        """Pause the pipeline execution."""
        self._pause_event.clear()
        self.status = PipelineStatus.PAUSED
        logger.info(f"Pipeline '{self.config.name}' paused")
    
    def resume(self) -> None:
        """Resume a paused pipeline."""
        self._pause_event.set()
        self.status = PipelineStatus.RUNNING
        logger.info(f"Pipeline '{self.config.name}' resumed")
    
    def get_progress(self) -> Dict[str, Any]:
        """Get pipeline execution progress."""
        if not self.context:
            return {"status": "not_started"}
        
        total = len(self.config.stages)
        completed = sum(
            1 for r in self.context.results.values()
            if r.status == StageStatus.COMPLETED
        )
        
        return {
            "status": self.status.value,
            "name": self.config.name,
            "total_stages": total,
            "completed_stages": completed,
            "progress_percent": (completed / total * 100) if total > 0 else 0,
            "stage_results": {
                name: result.to_dict()
                for name, result in self.context.results.items()
            }
        }
    
    def get_summary(self) -> Dict[str, Any]:
        """Get pipeline execution summary."""
        progress = self.get_progress()
        
        duration_ms = None
        if self.start_time:
            end = self.end_time or datetime.utcnow()
            duration_ms = (end - self.start_time).total_seconds() * 1000
        
        return {
            "name": self.config.name,
            "status": self.status.value,
            "duration_ms": duration_ms,
            "stages": {
                "total": progress["total_stages"],
                "completed": progress["completed_stages"],
                "failed": sum(
                    1 for r in self.context.results.values()
                    if r.status == StageStatus.FAILED
                ) if self.context else 0
            },
            "progress_percent": progress["progress_percent"]
        }


# Example stage processors
async def extract_stage(data: Any, ctx: PipelineContext) -> Any:
    """Example: Extract data stage."""
    await asyncio.sleep(0.1)  # Simulate processing
    return {"extracted": data, "timestamp": datetime.utcnow().isoformat()}


async def transform_stage(data: Any, ctx: PipelineContext) -> Any:
    """Example: Transform data stage."""
    await asyncio.sleep(0.1)  # Simulate processing
    return {"transformed": True, "original": data}


async def load_stage(data: Any, ctx: PipelineContext) -> Any:
    """Example: Load data stage."""
    await asyncio.sleep(0.1)  # Simulate processing
    return {"loaded": True, "data": data}


# Standalone execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    async def main():
        # Create pipeline
        config = PipelineConfig(
            name="etl-pipeline",
            stages=["extract", "transform", "load"],
            max_retries=2,
            timeout=30.0
        )
        
        pipeline = DataPipelineAction(config)
        pipeline.register_stage("extract", extract_stage)
        pipeline.register_stage("transform", transform_stage)
        pipeline.register_stage("load", load_stage)
        
        # Execute
        result = await pipeline.execute({"source": "database", "query": "SELECT *"})
        
        print(f"Pipeline status: {pipeline.status.value}")
        print(f"Summary: {json.dumps(pipeline.get_summary(), indent=2, default=str)}")
        print(f"Progress: {json.dumps(pipeline.get_progress(), indent=2, default=str)}")
    
    asyncio.run(main())

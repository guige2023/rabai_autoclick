"""
Data Pipeline Action Module.

Provides a framework for building configurable data processing pipelines
with support for transformations, filtering, aggregation, and error handling.
"""

from typing import Optional, Dict, List, Any, Callable, TypeVar, Generic, Iterator
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import logging
import time
from concurrent.futures import ThreadPoolExecutor, Future
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class PipelineStatus(Enum):
    """Status of pipeline execution."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class PipelineStage(ABC, Generic[T, R]):
    """Abstract base class for pipeline stages."""
    name: str
    description: Optional[str] = None
    enabled: bool = True
    
    @abstractmethod
    def process(self, data: T) -> R:
        """Process input data and return transformed output."""
        pass
    
    def validate_input(self, data: T) -> bool:
        """Validate input data before processing."""
        return data is not None
        
    def handle_error(self, data: T, error: Exception) -> Optional[R]:
        """Handle errors during processing. Return fallback or None."""
        logger.error(f"Stage {self.name} error: {error}")
        return None
        
    def on_start(self) -> None:
        """Called before stage starts processing."""
        pass
        
    def on_complete(self, result: R) -> None:
        """Called after stage completes successfully."""
        pass


@dataclass
class PipelineMetrics:
    """Metrics collected during pipeline execution."""
    stages_executed: int = 0
    items_processed: int = 0
    items_succeeded: int = 0
    items_failed: int = 0
    total_process_time: float = 0.0
    stage_times: Dict[str, float] = field(default_factory=dict)
    errors: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution."""
    parallel: bool = False
    max_workers: int = 4
    batch_size: int = 100
    continue_on_error: bool = True
    timeout_seconds: Optional[float] = None
    buffer_size: int = 1000


class TransformStage(PipelineStage[T, R]):
    """Stage that transforms data from type T to type R."""
    
    def __init__(
        self,
        name: str,
        transform_fn: Callable[[T], R],
        description: Optional[str] = None,
    ):
        super().__init__(name, description)
        self.transform_fn = transform_fn
        
    def process(self, data: T) -> R:
        return self.transform_fn(data)


class FilterStage(PipelineStage[T, T]):
    """Stage that filters data items."""
    
    def __init__(
        self,
        name: str,
        predicate: Callable[[T], bool],
        description: Optional[str] = None,
    ):
        super().__init__(name, description)
        self.predicate = predicate
        
    def process(self, data: T) -> T:
        if self.predicate(data):
            return data
        return None  # None indicates filtered out


class BatchStage(PipelineStage[List[T], List[T]]):
    """Stage that batches items for bulk processing."""
    
    def __init__(
        self,
        name: str,
        batch_size: int = 100,
        description: Optional[str] = None,
    ):
        super().__init__(name, description)
        self.batch_size = batch_size
        self._buffer: List[T] = []
        
    def process(self, data: List[T]) -> List[List[T]]:
        result = []
        for item in data:
            self._buffer.append(item)
            if len(self._buffer) >= self.batch_size:
                result.append(self._buffer.copy())
                self._buffer.clear()
                
        if self._buffer:
            result.append(self._buffer.copy())
            self._buffer.clear()
            
        return result if result else [[]]


class AggregateStage(PipelineStage[List[T], Dict[str, Any]]):
    """Stage that aggregates data into summary statistics."""
    
    def __init__(
        self,
        name: str,
        aggregations: Optional[Dict[str, Callable]] = None,
        description: Optional[str] = None,
    ):
        super().__init__(name, description)
        self.aggregations = aggregations or {}
        self._values: List[Any] = []
        
    def process(self, data: List[T]) -> Dict[str, Any]:
        self._values.extend(data)
        
        result = {"count": len(self._values)}
        
        for agg_name, agg_fn in self.aggregations.items():
            try:
                result[agg_name] = agg_fn(self._values)
            except Exception as e:
                logger.warning(f"Aggregation {agg_name} failed: {e}")
                result[agg_name] = None
                
        return result


class DataPipeline(Generic[T]):
    """
    Configurable data processing pipeline.
    
    Example:
        pipeline = DataPipeline()
        
        pipeline.add_stage(FilterStage("valid_users", lambda u: u.get("active")))
        pipeline.add_stage(TransformStage("normalize", normalize_user))
        pipeline.add_stage(AggregateStage("stats", {"avg_age": lambda x: sum(x)/len(x)}))
        
        results = pipeline.run(user_data)
    """
    
    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or PipelineConfig()
        self.stages: List[PipelineStage] = []
        self.status = PipelineStatus.IDLE
        self.metrics = PipelineMetrics()
        
    def add_stage(self, stage: PipelineStage) -> "DataPipeline":
        """Add a stage to the pipeline."""
        self.stages.append(stage)
        return self
        
    def insert_stage(self, index: int, stage: PipelineStage) -> "DataPipeline":
        """Insert a stage at specific position."""
        self.stages.insert(index, stage)
        return self
        
    def remove_stage(self, name: str) -> bool:
        """Remove a stage by name."""
        for i, stage in enumerate(self.stages):
            if stage.name == name:
                self.stages.pop(i)
                return True
        return False
        
    def get_stage(self, name: str) -> Optional[PipelineStage]:
        """Get a stage by name."""
        for stage in self.stages:
            if stage.name == name:
                return stage
        return None
        
    def run(
        self,
        data: T,
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ) -> Any:
        """
        Execute the pipeline on input data.
        
        Args:
            data: Input data for pipeline
            progress_callback: Optional callback(stage_index, total_stages)
            
        Returns:
            Final output after all stages
            
        Raises:
            PipelineError: If pipeline fails and continue_on_error is False
        """
        if not self.stages:
            return data
            
        self.status = PipelineStatus.RUNNING
        self.metrics = PipelineMetrics()
        
        start_time = time.time()
        current_data = data
        total_stages = len([s for s in self.stages if s.enabled])
        
        try:
            for i, stage in enumerate(self.stages):
                if not stage.enabled:
                    continue
                    
                stage.on_start()
                stage_start = time.time()
                
                if not stage.validate_input(current_data):
                    if self.config.continue_on_error:
                        current_data = None
                        continue
                    raise PipelineError(f"Stage {stage.name}: invalid input")
                    
                try:
                    if self.config.parallel and isinstance(current_data, list):
                        current_data = self._process_parallel(stage, current_data)
                    else:
                        current_data = stage.process(current_data)
                        
                    stage_time = time.time() - stage_start
                    self.metrics.stage_times[stage.name] = stage_time
                    self.metrics.stages_executed += 1
                    self.metrics.items_processed += len(current_data) if isinstance(current_data, list) else 1
                    
                    stage.on_complete(current_data)
                    
                except Exception as e:
                    logger.error(f"Stage {stage.name} failed: {e}")
                    self.metrics.errors.append({
                        "stage": stage.name,
                        "error": str(e),
                        "timestamp": time.time(),
                    })
                    
                    if self.config.continue_on_error:
                        current_data = stage.handle_error(current_data, e)
                        if current_data is None and isinstance(data, list):
                            current_data = []
                    else:
                        raise PipelineError(f"Stage {stage.name} failed") from e
                        
                if progress_callback:
                    progress_callback(i + 1, total_stages)
                    
            self.metrics.total_process_time = time.time() - start_time
            self.status = PipelineStatus.COMPLETED
            return current_data
            
        except Exception as e:
            self.status = PipelineStatus.FAILED
            raise PipelineError(f"Pipeline failed: {e}") from e
            
    def _process_parallel(
        self,
        stage: PipelineStage,
        data: List[T],
    ) -> List[R]:
        """Process data in parallel using thread pool."""
        results = []
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = [executor.submit(stage.process, item) for item in data]
            for future in futures:
                try:
                    result = future.result(timeout=self.config.timeout_seconds)
                    if result is not None:
                        results.append(result)
                        self.metrics.items_succeeded += 1
                except Exception as e:
                    logger.warning(f"Parallel processing error: {e}")
                    self.metrics.items_failed += 1
                    if self.config.continue_on_error:
                        continue
                    raise
        return results
        
    def run_stream(
        self,
        data_stream: Iterator[T],
    ) -> Iterator[Any]:
        """
        Execute pipeline on streaming data.
        
        Args:
            data_stream: Iterator of input data items
            
        Yields:
            Processed data items as they complete each stage
        """
        self.status = PipelineStatus.RUNNING
        stage_buffers: List[List] = [ [] for _ in self.stages ]
        
        try:
            for item in data_stream:
                if self.status == PipelineStatus.CANCELLED:
                    break
                    
                current = item
                for i, stage in enumerate(self.stages):
                    if not stage.enabled:
                        continue
                    try:
                        current = stage.process(current)
                        if current is None:
                            break
                    except Exception as e:
                        if self.config.continue_on_error:
                            logger.warning(f"Stream stage {stage.name} error: {e}")
                            current = None
                            break
                        raise
                        
                if current is not None:
                    yield current
                    
            self.status = PipelineStatus.COMPLETED
            
        except Exception as e:
            self.status = PipelineStatus.FAILED
            raise PipelineError(f"Stream pipeline failed: {e}") from e
            
    def pause(self) -> None:
        """Pause pipeline execution."""
        if self.status == PipelineStatus.RUNNING:
            self.status = PipelineStatus.PAUSED
            
    def resume(self) -> None:
        """Resume paused pipeline."""
        if self.status == PipelineStatus.PAUSED:
            self.status = PipelineStatus.RUNNING
            
    def cancel(self) -> None:
        """Cancel pipeline execution."""
        self.status = PipelineStatus.CANCELLED
        
    def get_metrics(self) -> PipelineMetrics:
        """Get current pipeline metrics."""
        return self.metrics
        
    def get_summary(self) -> Dict[str, Any]:
        """Get human-readable pipeline summary."""
        return {
            "status": self.status.value,
            "stages": [s.name for s in self.stages],
            "metrics": {
                "stages_executed": self.metrics.stages_executed,
                "items_processed": self.metrics.items_processed,
                "items_succeeded": self.metrics.items_succeeded,
                "items_failed": self.metrics.items_failed,
                "total_time": f"{self.metrics.total_process_time:.2f}s",
                "stage_times": {
                    name: f"{time:.2f}s" 
                    for name, time in self.metrics.stage_times.items()
                },
                "error_count": len(self.metrics.errors),
            },
        }


class PipelineError(Exception):
    """Error raised when pipeline execution fails."""
    pass


def pipeline_retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
):
    """Decorator to add retry logic to a stage function."""
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            current_delay = delay
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < max_attempts - 1:
                        logger.warning(f"Retry {attempt + 1}/{max_attempts} after error: {e}")
                        time.sleep(current_delay)
                        current_delay *= backoff
            raise last_error
        return wrapper
    return decorator

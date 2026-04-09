"""
Data Pipeline Processing Module.

Provides streaming data pipeline with parallel stage execution,
backpressure handling, and error recovery.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, TypeVar
from collections import deque
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class StageType(Enum):
    """Type of pipeline stage."""
    MAP = "map"
    FILTER = "filter"
    FLATMAP = "flatmap"
    REDUCE = "reduce"
    WINDOW = "window"
    BATCH = "batch"
    BRANCH = "branch"


class BackpressureStrategy(Enum):
    """Backpressure handling strategy."""
    DROP = "drop"
    BLOCK = "block"
    BUFFER = "buffer"
    FAIL = "fail"


@dataclass
class PipelineStage:
    """Represents a single stage in the pipeline."""
    name: str
    stage_type: StageType
    func: Callable[[Any], Awaitable[Any]]
    parallelism: int = 1
    buffer_size: int = 100
    timeout: float = 30.0
    retry_count: int = 3
    retry_delay: float = 1.0
    
    
@dataclass
class PipelineConfig:
    """Configuration for the pipeline."""
    name: str = "pipeline"
    max_buffer_size: int = 1000
    default_timeout: float = 30.0
    default_retry: int = 3
    backpressure: BackpressureStrategy = BackpressureStrategy.BUFFER
    enable_metrics: bool = True
    error_strategy: str = "skip"  # skip, stop, retry


@dataclass
class StageMetrics:
    """Metrics for a pipeline stage."""
    name: str
    processed_count: int = 0
    error_count: int = 0
    total_latency: float = 0.0
    last_run: Optional[float] = None
    
    
@dataclass 
class PipelineStats:
    """Overall pipeline statistics."""
    total_processed: int = 0
    total_errors: int = 0
    total_latency: float = 0.0
    stage_metrics: Dict[str, StageMetrics] = field(default_factory=dict)


class Pipeline:
    """
    Streaming data pipeline with parallel stage execution.
    
    Example:
        pipeline = Pipeline(PipelineConfig(name="my_pipeline"))
        
        pipeline.stage("parse", StageType.MAP, parse_json)
        pipeline.stage("validate", StageType.FILTER, validate_schema)
        pipeline.stage("transform", StageType.MAP, transform_data)
        pipeline.stage("persist", StageType.MAP, save_to_db, parallelism=4)
        
        async for result in pipeline.stream(data_iterator):
            print(result)
    """
    
    def __init__(self, config: Optional[PipelineConfig] = None) -> None:
        """
        Initialize the pipeline.
        
        Args:
            config: Pipeline configuration.
        """
        self.config = config or PipelineConfig()
        self._stages: List[PipelineStage] = []
        self._stats = PipelineStats()
        self._running = False
        
    def stage(
        self,
        name: str,
        stage_type: StageType,
        func: Callable[[Any], Awaitable[Any]],
        parallelism: int = 1,
        buffer_size: int = 100,
        timeout: Optional[float] = None,
        retry_count: Optional[int] = None,
    ) -> Pipeline:
        """
        Add a stage to the pipeline.
        
        Args:
            name: Stage name (must be unique).
            stage_type: Type of processing.
            func: Async function to execute.
            parallelism: Number of parallel workers.
            buffer_size: Input buffer size.
            timeout: Stage timeout (uses default if None).
            retry_count: Retry count (uses default if None).
            
        Returns:
            Self for chaining.
        """
        # Check for duplicate names
        if any(s.name == name for s in self._stages):
            raise ValueError(f"Stage with name '{name}' already exists")
            
        stage = PipelineStage(
            name=name,
            stage_type=stage_type,
            func=func,
            parallelism=parallelism,
            buffer_size=buffer_size,
            timeout=timeout or self.config.default_timeout,
            retry_count=retry_count if retry_count is not None else self.config.default_retry,
        )
        
        self._stages.append(stage)
        self._stats.stage_metrics[name] = StageMetrics(name=name)
        
        logger.info(f"Added stage: {name} (type={stage_type.value}, parallelism={parallelism})")
        return self
        
    def map(self, name: str, func: Callable[[Any], Awaitable[R]]) -> Pipeline:
        """Add a map stage."""
        return self.stage(name, StageType.MAP, func)
        
    def filter(self, name: str, func: Callable[[Any], Awaitable[bool]]) -> Pipeline:
        """Add a filter stage."""
        return self.stage(name, StageType.FILTER, func)
        
    def flatmap(self, name: str, func: Callable[[Any], Awaitable[List[R]]]) -> Pipeline:
        """Add a flatmap stage (produces multiple outputs per input)."""
        return self.stage(name, StageType.FLATMAP, func)
        
    def batch(self, name: str, batch_size: int, timeout: Optional[float] = None) -> Pipeline:
        """Add a batch aggregation stage."""
        return self.stage(
            name, StageType.BATCH,
            self._batch_func(batch_size),
            buffer_size=batch_size * 2
        )
        
    async def stream(self, iterator: AsyncIterator[T]) -> AsyncIterator[R]:
        """
        Process data through the pipeline.
        
        Args:
            iterator: Input data iterator.
            
        Yields:
            Processed output items.
        """
        if not self._stages:
            async for item in iterator:
                yield item
            return
            
        self._running = True
        current_queues = [asyncio.Queue(maxsize=self.config.max_buffer_size)]
        
        try:
            # Start all stages
            stage_tasks = []
            for i, stage in enumerate(self._stages):
                input_queue = current_queues[-1]
                output_queue = asyncio.Queue(maxsize=self.config.max_buffer_size)
                current_queues.append(output_queue)
                
                task = asyncio.create_task(
                    self._run_stage(stage, input_queue, output_queue)
                )
                stage_tasks.append(task)
                
            # Feed input
            input_queue = current_queues[0]
            async for item in iterator:
                if not self._running:
                    break
                    
                if input_queue.full():
                    if self.config.backpressure == BackpressureStrategy.DROP:
                        logger.warning("Pipeline buffer full, dropping item")
                        continue
                    elif self.config.backpressure == BackpressureStrategy.FAIL:
                        raise RuntimeError("Pipeline buffer full")
                    else:
                        await input_queue.put(item)  # Block
                else:
                    await input_queue.put(item)
                    
            # Close input queue
            await input_queue.put(None)
            
            # Collect output from last queue
            output_queue = current_queues[-1]
            while True:
                item = await output_queue.get()
                if item is None:
                    break
                yield item
                output_queue.task_done()
                
        finally:
            self._running = False
            for task in stage_tasks:
                task.cancel()
                
    async def process(self, data: List[T]) -> List[R]:
        """
        Process a batch of data.
        
        Args:
            data: Input data list.
            
        Returns:
            List of processed results.
        """
        results = []
        async for result in self.stream(iter(data)):
            results.append(result)
        return results
        
    async def _run_stage(
        self,
        stage: PipelineStage,
        input_queue: asyncio.Queue,
        output_queue: asyncio.Queue,
    ) -> None:
        """Run a single stage with parallel workers."""
        workers = [
            asyncio.create_task(self._stage_worker(stage, input_queue, output_queue))
            for _ in range(stage.parallelism)
        ]
        
        try:
            # Wait for input to complete
            await input_queue.join()
            # Signal completion
            await output_queue.put(None)
        except Exception as e:
            logger.error(f"Stage {stage.name} error: {e}")
        finally:
            for worker in workers:
                worker.cancel()
                
    async def _stage_worker(
        self,
        stage: PipelineStage,
        input_queue: asyncio.Queue,
        output_queue: asyncio.Queue,
    ) -> None:
        """Worker coroutine for stage processing."""
        while True:
            try:
                item = await asyncio.wait_for(
                    input_queue.get(),
                    timeout=stage.timeout
                )
                
                if item is None:
                    input_queue.task_done()
                    break
                    
                start_time = time.time()
                result = None
                error = None
                
                for attempt in range(stage.retry_count + 1):
                    try:
                        result = await asyncio.wait_for(
                            stage.func(item),
                            timeout=stage.timeout
                        )
                        error = None
                        break
                    except Exception as e:
                        error = e
                        if attempt < stage.retry_count:
                            await asyncio.sleep(stage.retry_delay)
                            
                latency = time.time() - start_time
                metrics = self._stats.stage_metrics[stage.name]
                metrics.processed_count += 1
                metrics.total_latency += latency
                metrics.last_run = time.time()
                
                if error and self.config.error_strategy == "stop":
                    raise error
                    
                if error:
                    metrics.error_count += 1
                    self._stats.total_errors += 1
                    if self.config.error_strategy == "skip":
                        input_queue.task_done()
                        continue
                        
                # Handle different stage types
                if stage.stage_type == StageType.FILTER:
                    if result:
                        await output_queue.put(item)
                elif stage.stage_type == StageType.FLATMAP:
                    for r in result or []:
                        await output_queue.put(r)
                else:
                    if result is not None:
                        await output_queue.put(result)
                        
                self._stats.total_processed += 1
                self._stats.total_latency += latency
                input_queue.task_done()
                
            except asyncio.TimeoutError:
                logger.warning(f"Stage {stage.name} timeout")
                input_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Stage {stage.name} worker error: {e}")
                input_queue.task_done()
                
    async def _batch_func(self, batch_size: int) -> Callable[[Any], Awaitable[List[Any]]]:
        """Create a batch accumulation function."""
        buffer = []
        
        async def batch_processor(item: Any) -> List[Any]:
            nonlocal buffer
            buffer.append(item)
            if len(buffer) >= batch_size:
                result = buffer
                buffer = []
                return result
            return []
            
        return batch_processor
        
    def get_stats(self) -> PipelineStats:
        """Get pipeline statistics."""
        return self._stats
        
    def reset_stats(self) -> None:
        """Reset pipeline statistics."""
        self._stats = PipelineStats(
            stage_metrics={name: StageMetrics(name=name) for name in self._stats.stage_metrics}
        )

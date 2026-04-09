"""
Data Pipeline Builder Action Module

Provides a declarative pipeline builder with stage composition,
error handling, retry logic, and execution monitoring.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Dict, List, Optional, Union

import logging

logger = logging.getLogger(__name__)


class StageStatus(Enum):
    """Status of a pipeline stage."""

    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    SKIPPED = auto()
    RETRYING = auto()


@dataclass
class StageResult:
    """Result of a stage execution."""

    stage_id: str
    stage_name: str
    status: StageStatus
    input_data: Any
    output_data: Any = None
    error: Optional[str] = None
    start_time: float = 0.0
    end_time: float = 0.0
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def duration_ms(self) -> float:
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time) * 1000
        return 0.0


@dataclass
class PipelineStage:
    """A single stage in the pipeline."""

    stage_id: str
    name: str
    processor: Callable[[Any], Any]
    condition: Optional[Callable[[Any], bool]] = None
    error_handler: Optional[Callable[[Exception, Any], Any]] = None
    retry_count: int = 0
    retry_delay: float = 1.0
    timeout: Optional[float] = None
    skip_on_failure: bool = False
    parallel: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineConfig:
    """Configuration for pipeline execution."""

    name: str
    max_parallel: int = 1
    continue_on_failure: bool = True
    global_timeout: Optional[float] = None
    retry_failed_stages: bool = False
    enable_caching: bool = False


@dataclass
class PipelineResult:
    """Result of pipeline execution."""

    pipeline_name: str
    run_id: str
    status: StageStatus
    stage_results: List[StageResult]
    start_time: float
    end_time: float = 0.0
    total_records_processed: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def duration_ms(self) -> float:
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time) * 1000
        return 0.0

    def success_rate(self) -> float:
        if not self.stage_results:
            return 0.0
        completed = sum(1 for s in self.stage_results if s.status == StageStatus.COMPLETED)
        return completed / len(self.stage_results) * 100


class Pipeline:
    """Data pipeline with staged execution."""

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        self._stages: List[PipelineStage] = []
        self._cache: Dict[str, Any] = {}

    def add_stage(
        self,
        name: str,
        processor: Callable[[Any], Any],
        condition: Optional[Callable[[Any], bool]] = None,
        error_handler: Optional[Callable[[Exception, Any], Any]] = None,
        retry_count: int = 0,
        retry_delay: float = 1.0,
        timeout: Optional[float] = None,
        skip_on_failure: bool = False,
    ) -> Pipeline:
        """Add a stage to the pipeline."""
        stage = PipelineStage(
            stage_id=str(uuid.uuid4()),
            name=name,
            processor=processor,
            condition=condition,
            error_handler=error_handler,
            retry_count=retry_count,
            retry_delay=retry_delay,
            timeout=timeout,
            skip_on_failure=skip_on_failure,
        )
        self._stages.append(stage)
        return self

    def add_parallel_stage(
        self,
        name: str,
        processors: List[Callable[[Any], Any]],
        merge_fn: Optional[Callable[[List[Any]], Any]] = None,
    ) -> Pipeline:
        """Add a parallel stage that executes multiple processors concurrently."""
        def parallel_wrapper(data: Any) -> Any:
            results = []
            for proc in processors:
                results.append(proc(data))
            if merge_fn:
                return merge_fn(results)
            return results

        stage = PipelineStage(
            stage_id=str(uuid.uuid4()),
            name=name,
            processor=parallel_wrapper,
            parallel=True,
        )
        self._stages.append(stage)
        return self

    async def _execute_stage(
        self,
        stage: PipelineStage,
        input_data: Any,
        stage_results: List[StageResult],
    ) -> Any:
        """Execute a single stage with retries."""
        result = StageResult(
            stage_id=stage.stage_id,
            stage_name=stage.name,
            status=StageStatus.PENDING,
            input_data=input_data,
            start_time=time.time(),
        )

        if stage.condition and not stage.condition(input_data):
            result.status = StageStatus.SKIPPED
            result.end_time = time.time()
            stage_results.append(result)
            return input_data

        for attempt in range(stage.retry_count + 1):
            try:
                result.status = StageStatus.RUNNING
                if stage.timeout:
                    result.output_data = await asyncio.wait_for(
                        asyncio.to_thread(stage.processor, input_data),
                        timeout=stage.timeout,
                    )
                else:
                    result.output_data = await asyncio.to_thread(stage.processor, input_data)

                result.status = StageStatus.COMPLETED
                result.end_time = time.time()
                break

            except Exception as e:
                result.error = str(e)
                result.retry_count = attempt + 1

                if attempt < stage.retry_count:
                    result.status = StageStatus.RETRYING
                    await asyncio.sleep(stage.retry_delay)
                else:
                    result.status = StageStatus.FAILED
                    result.end_time = time.time()

                    if stage.error_handler:
                        try:
                            result.output_data = stage.error_handler(e, input_data)
                            result.status = StageStatus.COMPLETED
                        except Exception:
                            pass

        stage_results.append(result)

        if result.status == StageStatus.FAILED and stage.skip_on_failure:
            return input_data

        return result.output_data

    async def run(self, initial_data: Any) -> PipelineResult:
        """Run the pipeline with given input data."""
        run_id = str(uuid.uuid4())
        stage_results: List[StageResult] = []
        start_time = time.time()
        current_data = initial_data
        overall_status = StageStatus.COMPLETED

        for stage in self._stages:
            cache_key = f"{run_id}:{stage.stage_id}"
            if self.config.enable_caching and cache_key in self._cache:
                current_data = self._cache[cache_key]
                stage_results.append(StageResult(
                    stage_id=stage.stage_id,
                    stage_name=stage.name,
                    status=StageStatus.COMPLETED,
                    input_data=current_data,
                    output_data=current_data,
                    start_time=time.time(),
                    end_time=time.time(),
                ))
                continue

            prev_status = overall_status
            current_data = await self._execute_stage(stage, current_data, stage_results)

            if self.config.enable_caching:
                self._cache[cache_key] = current_data

            last_result = stage_results[-1]
            if last_result.status == StageStatus.FAILED:
                if not self.config.continue_on_failure:
                    overall_status = StageStatus.FAILED
                    break
                overall_status = StageStatus.FAILED

        return PipelineResult(
            pipeline_name=self.config.name,
            run_id=run_id,
            status=overall_status,
            stage_results=stage_results,
            start_time=start_time,
            end_time=time.time(),
        )

    def run_sync(self, initial_data: Any) -> PipelineResult:
        """Synchronous wrapper for pipeline execution."""
        return asyncio.run(self.run(initial_data))


class PipelineBuilder:
    """Builder class for constructing pipelines."""

    def __init__(self, name: str) -> None:
        self._config = PipelineConfig(name=name)
        self._stages: List[PipelineStage] = []

    def with_parallelism(self, max_parallel: int) -> PipelineBuilder:
        self._config.max_parallel = max_parallel
        return self

    def continue_on_failure(self, continue_on_failure: bool) -> PipelineBuilder:
        self._config.continue_on_failure = continue_on_failure
        return self

    def with_timeout(self, timeout: float) -> PipelineBuilder:
        self._config.global_timeout = timeout
        return self

    def enable_caching(self) -> PipelineBuilder:
        self._config.enable_caching = True
        return self

    def stage(
        self,
        name: str,
        processor: Callable[[Any], Any],
    ) -> PipelineBuilder:
        """Add a processing stage."""
        stage = PipelineStage(
            stage_id=str(uuid.uuid4()),
            name=name,
            processor=processor,
        )
        self._stages.append(stage)
        return self

    def build(self) -> Pipeline:
        """Build the pipeline."""
        pipeline = Pipeline(self._config)
        pipeline._stages = self._stages
        return pipeline


class PipelineAction:
    """Action class for data pipeline operations."""

    def __init__(self) -> None:
        self._pipelines: Dict[str, Pipeline] = {}

    def create_pipeline(self, name: str) -> PipelineBuilder:
        """Create a new pipeline builder."""
        return PipelineBuilder(name)

    def register_pipeline(self, pipeline: Pipeline) -> None:
        """Register a named pipeline."""
        self._pipelines[pipeline.config.name] = pipeline

    def run_pipeline(self, name: str, data: Any) -> Optional[PipelineResult]:
        """Run a registered pipeline."""
        pipeline = self._pipelines.get(name)
        if pipeline:
            return asyncio.run(pipeline.run(data))
        return None

    def list_pipelines(self) -> List[str]:
        """List all registered pipelines."""
        return list(self._pipelines.keys())

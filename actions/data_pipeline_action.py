"""
Data Pipeline Action Module.

Provides chained data transformations with parallel stages,
error handling, and result aggregation.
"""

import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Generic, TypeVar, Optional

T = TypeVar("T")
R = TypeVar("R")


class PipelineStatus(Enum):
    """Pipeline execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class StageConfig:
    """Configuration for a pipeline stage."""
    name: str
    transform: Callable[[Any], Any]
    error_handler: Optional[Callable[[Exception], Any]] = None
    skip_on_error: bool = False
    timeout: Optional[float] = None
    retry_count: int = 0
    parallel: bool = False


@dataclass
class StageResult(Generic[T]):
    """Result of a single stage."""
    name: str
    status: PipelineStatus
    input_data: Any
    output_data: Any = None
    error: Optional[Exception] = None
    duration: float = 0.0
    retries: int = 0


@dataclass
class PipelineResult(Generic[T]):
    """Result of pipeline execution."""
    status: PipelineStatus
    stages: list[StageResult] = field(default_factory=list)
    input_data: Any = None
    output_data: Any = None
    total_duration: float = 0.0

    @property
    def successful_stages(self) -> int:
        return sum(1 for s in self.stages if s.status == PipelineStatus.COMPLETED)

    @property
    def failed_stages(self) -> int:
        return sum(1 for s in self.stages if s.status == PipelineStatus.FAILED)


class PipelineStage:
    """Single stage in the pipeline."""

    def __init__(self, config: StageConfig):
        self.config = config
        self._result: Optional[StageResult] = None

    async def execute(self, input_data: Any) -> StageResult:
        """Execute the stage."""
        import time
        start = time.monotonic()

        self._result = StageResult(
            name=self.config.name,
            status=PipelineStatus.RUNNING,
            input_data=input_data
        )

        for attempt in range(self.config.retry_count + 1):
            try:
                if self.config.timeout:
                    output = await asyncio.wait_for(
                        asyncio.to_thread(self.config.transform, input_data),
                        timeout=self.config.timeout
                    )
                else:
                    output = await asyncio.to_thread(
                        self.config.transform,
                        input_data
                    )

                self._result.output_data = output
                self._result.status = PipelineStatus.COMPLETED
                self._result.duration = time.monotonic() - start
                return self._result

            except Exception as e:
                if self.config.error_handler:
                    try:
                        output = self.config.error_handler(e)
                        self._result.output_data = output
                        self._result.status = PipelineStatus.COMPLETED
                        self._result.duration = time.monotonic() - start
                        return self._result
                    except Exception:
                        pass

                if attempt < self.config.retry_count:
                    self._result.retries = attempt + 1
                    await asyncio.sleep(0.1 * (attempt + 1))
                    continue

                if self.config.skip_on_error:
                    self._result.output_data = input_data
                    self._result.status = PipelineStatus.COMPLETED
                else:
                    self._result.error = e
                    self._result.status = PipelineStatus.FAILED

                self._result.duration = time.monotonic() - start
                return self._result

        return self._result


class DataPipelineAction:
    """
    Data pipeline with chained transformations.

    Example:
        pipeline = DataPipelineAction()

        pipeline.add_stage("normalize", normalize_text)
        pipeline.add_stage("validate", validate_schema)
        pipeline.add_stage("enrich", enrich_data)

        result = await pipeline.execute(raw_data)
    """

    def __init__(self, name: str = "pipeline"):
        self.name = name
        self._stages: list[PipelineStage] = []
        self._status = PipelineStatus.PENDING
        self._cancel_event: Optional[asyncio.Event] = None

    def add_stage(
        self,
        name: str,
        transform: Callable[[Any], Any],
        error_handler: Optional[Callable[[Exception], Any]] = None,
        skip_on_error: bool = False,
        timeout: Optional[float] = None,
        retry_count: int = 0
    ) -> "DataPipelineAction":
        """Add a stage to the pipeline."""
        config = StageConfig(
            name=name,
            transform=transform,
            error_handler=error_handler,
            skip_on_error=skip_on_error,
            timeout=timeout,
            retry_count=retry_count
        )
        self._stages.append(PipelineStage(config))
        return self

    def add_parallel_stage(
        self,
        name: str,
        transform: Callable[[Any], Any],
        **kwargs: Any
    ) -> "DataPipelineAction":
        """Add a parallel-capable stage."""
        config = StageConfig(
            name=name,
            transform=transform,
            parallel=True,
            **kwargs
        )
        self._stages.append(PipelineStage(config))
        return self

    async def execute(self, input_data: Any) -> PipelineResult:
        """Execute the pipeline."""
        import time
        start = time.monotonic()

        self._status = PipelineStatus.RUNNING
        self._cancel_event = asyncio.Event()

        result = PipelineResult(
            status=PipelineStatus.RUNNING,
            input_data=input_data
        )

        current_data = input_data

        for stage in self._stages:
            if self._cancel_event and self._cancel_event.is_set():
                self._status = PipelineStatus.CANCELLED
                result.status = PipelineStatus.CANCELLED
                break

            stage_result = await stage.execute(current_data)
            result.stages.append(stage_result)

            if stage_result.status == PipelineStatus.FAILED:
                self._status = PipelineStatus.FAILED
                result.status = PipelineStatus.FAILED
                result.output_data = stage_result.input_data
                break

            current_data = stage_result.output_data

        if result.status != PipelineStatus.FAILED:
            self._status = PipelineStatus.COMPLETED
            result.status = PipelineStatus.COMPLETED
            result.output_data = current_data

        result.total_duration = time.monotonic() - start
        return result

    async def execute_parallel(
        self,
        input_data: list[Any]
    ) -> PipelineResult:
        """Execute pipeline in parallel for multiple inputs."""
        import time
        start = time.monotonic()

        self._status = PipelineStatus.RUNNING
        tasks = [self.execute(data) for data in input_data]
        results = await asyncio.gather(*tasks)

        self._status = PipelineStatus.COMPLETED

        all_stages = []
        for r in results:
            all_stages.extend(r.stages)

        return PipelineResult(
            status=PipelineStatus.COMPLETED,
            stages=all_stages,
            input_data=input_data,
            output_data=[r.output_data for r in results],
            total_duration=time.monotonic() - start
        )

    def cancel(self) -> None:
        """Cancel pipeline execution."""
        if self._cancel_event:
            self._cancel_event.set()
        self._status = PipelineStatus.CANCELLED

    @property
    def status(self) -> PipelineStatus:
        """Get current pipeline status."""
        return self._status

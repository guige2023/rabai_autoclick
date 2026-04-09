"""
Data Pipeline Action Module.

Composable data processing pipeline with stage management,
error handling, parallel execution, and result aggregation.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Generic, TypeVar, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)
T = TypeVar("T")
R = TypeVar("R")


class StageStatus(Enum):
    """Pipeline stage execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PipelineStage(Generic[T, R]):
    """
    Individual stage in a data pipeline.

    Attributes:
        name: Unique identifier for the stage.
        processor: Async function that transforms input to output.
        error_handler: Optional handler for stage errors.
        timeout: Optional timeout in seconds.
        retry_count: Number of retries on failure.
    """
    name: str
    processor: Callable[[T], R]
    error_handler: Optional[Callable[[Exception, T], R]] = None
    timeout: Optional[float] = None
    retry_count: int = 0

    status: StageStatus = field(default=StageStatus.PENDING, init=False)
    error: Optional[Exception] = field(default=None, init=False)


@dataclass
class PipelineResult:
    """
    Result of pipeline execution.

    Attributes:
        success: Whether all stages completed successfully.
        outputs: List of outputs from each stage.
        errors: Dict mapping stage names to errors.
        duration: Total execution time in seconds.
    """
    success: bool
    outputs: list = field(default_factory=list)
    errors: dict = field(default_factory=dict)
    duration: float = 0.0


class DataPipelineAction:
    """
    Orchestrates sequential and parallel data processing stages.

    Example:
        pipeline = DataPipelineAction(name="etl_pipeline")
        pipeline.add_stage("extract", extract_data)
        pipeline.add_stage("transform", transform_data)
        pipeline.add_stage("load", load_data)
        result = await pipeline.execute(input_data)
    """

    def __init__(self, name: str = "pipeline"):
        """
        Initialize data pipeline action.

        Args:
            name: Pipeline identifier for logging.
        """
        self.name = name
        self.stages: list[PipelineStage] = []
        self._results: list[Any] = []

    def add_stage(
        self,
        name: str,
        processor: Callable,
        error_handler: Optional[Callable] = None,
        timeout: Optional[float] = None,
        retry_count: int = 0
    ) -> "DataPipelineAction":
        """
        Add a stage to the pipeline.

        Args:
            name: Unique stage identifier.
            processor: Async function to process data.
            error_handler: Optional error handler function.
            timeout: Optional stage timeout.
            retry_count: Number of retries on failure.

        Returns:
            Self for method chaining.
        """
        stage = PipelineStage(
            name=name,
            processor=processor,
            error_handler=error_handler,
            timeout=timeout,
            retry_count=retry_count
        )
        self.stages.append(stage)
        return self

    async def execute(self, initial_input: Any) -> PipelineResult:
        """
        Execute all pipeline stages sequentially.

        Args:
            initial_input: Input data for first stage.

        Returns:
            PipelineResult with outputs and errors.
        """
        import time
        start_time = time.time()
        current_input = initial_input
        self._results = []

        for stage in self.stages:
            stage.status = StageStatus.RUNNING
            logger.info(f"[{self.name}] Running stage: {stage.name}")

            try:
                if stage.timeout:
                    output = await asyncio.wait_for(
                        stage.processor(current_input),
                        timeout=stage.timeout
                    )
                else:
                    output = await stage.processor(current_input)

                stage.status = StageStatus.COMPLETED
                self._results.append(output)
                current_input = output

            except Exception as e:
                logger.error(f"[{self.name}] Stage {stage.name} failed: {e}")

                if stage.error_handler:
                    try:
                        output = stage.error_handler(e, current_input)
                        stage.status = StageStatus.COMPLETED
                        self._results.append(output)
                        current_input = output
                        continue
                    except Exception as handler_error:
                        logger.error(f"Error handler also failed: {handler_error}")

                stage.status = StageStatus.FAILED
                stage.error = e

                if stage.retry_count > 0:
                    for attempt in range(stage.retry_count):
                        try:
                            await asyncio.sleep(2 ** attempt)
                            if stage.timeout:
                                output = await asyncio.wait_for(
                                    stage.processor(current_input),
                                    timeout=stage.timeout
                                )
                            else:
                                output = await stage.processor(current_input)

                            stage.status = StageStatus.COMPLETED
                            self._results.append(output)
                            current_input = output
                            break
                        except Exception as retry_error:
                            logger.warning(f"Retry {attempt + 1} failed: {retry_error}")
                            if attempt == stage.retry_count - 1:
                                stage.error = retry_error
                else:
                    break

        duration = time.time() - start_time
        success = all(s.status == StageStatus.COMPLETED for s in self.stages)

        errors = {s.name: s.error for s in self.stages if s.error}

        return PipelineResult(
            success=success,
            outputs=self._results,
            errors=errors,
            duration=duration
        )

    async def execute_parallel(
        self,
        inputs: list[Any],
        stage_index: int = 0
    ) -> list[Any]:
        """
        Execute a single stage in parallel across multiple inputs.

        Args:
            inputs: List of inputs for parallel processing.
            stage_index: Index of stage to execute.

        Returns:
            List of outputs from parallel execution.
        """
        if stage_index >= len(self.stages):
            raise IndexError(f"Stage index {stage_index} out of range")

        stage = self.stages[stage_index]

        async def process_one(item: Any) -> Any:
            return await stage.processor(item)

        tasks = [process_one(item) for item in inputs]
        outputs = await asyncio.gather(*tasks, return_exceptions=True)

        results = []
        for i, output in enumerate(outputs):
            if isinstance(output, Exception):
                logger.error(f"Parallel processing failed for item {i}: {output}")
                if stage.error_handler:
                    results.append(stage.error_handler(output, inputs[i]))
                else:
                    results.append(None)
            else:
                results.append(output)

        return results

    def clear(self) -> None:
        """Clear all stages and results."""
        self.stages.clear()
        self._results.clear()

    def get_stage_status(self, name: str) -> Optional[StageStatus]:
        """Get status of a specific stage."""
        for stage in self.stages:
            if stage.name == name:
                return stage.status
        return None

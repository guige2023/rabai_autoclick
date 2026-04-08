"""
Data Pipeline Action Module.

Pipeline execution engine for chained data transformations,
supports parallel branches, error handling, and stage skipping.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


class StageStatus(Enum):
    """Stage execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class PipelineStage:
    """Single stage in the pipeline."""
    name: str
    func: Callable[[Any], Any]
    condition: Optional[Callable[[Any], bool]] = None
    on_error: Optional[Callable[[Exception, Any], Any]] = None
    timeout: Optional[float] = None


@dataclass
class PipelineResult:
    """Result of pipeline execution."""
    success: bool
    output: Any
    stages_executed: list[str]
    stages_skipped: list[str]
    stages_failed: list[str]
    execution_time_ms: float
    errors: dict[str, str] = field(default_factory=dict)


class DataPipelineAction:
    """
    Chained data transformation pipeline.

    Executes stages sequentially with conditional execution,
    error handling, and parallel branching support.

    Example:
        pipeline = DataPipelineAction()
        pipeline.stage("validate", validate_func)
        pipeline.stage("transform", transform_func)
        pipeline.stage("load", load_func)
        result = pipeline.execute(input_data)
    """

    def __init__(
        self,
        name: str = "pipeline",
        stop_on_error: bool = True,
    ) -> None:
        self.name = name
        self.stop_on_error = stop_on_error
        self._stages: list[PipelineStage] = []
        self._parallel_branches: dict[str, list[PipelineStage]] = {}
        self._results: dict[str, Any] = {}

    def stage(
        self,
        name: str,
        func: Callable[[Any], Any],
        condition: Optional[Callable[[Any], bool]] = None,
        on_error: Optional[Callable[[Exception, Any], Any]] = None,
        timeout: Optional[float] = None,
    ) -> "DataPipelineAction":
        """Add a sequential stage to the pipeline."""
        self._stages.append(PipelineStage(
            name=name,
            func=func,
            condition=condition,
            on_error=on_error,
            timeout=timeout,
        ))
        return self

    def parallel_branch(
        self,
        branch_name: str,
        stages: list[tuple[str, Callable[[Any], Any]]],
    ) -> "DataPipelineAction":
        """Add a parallel branch to execute concurrently."""
        branch = [
            PipelineStage(name=name, func=func)
            for name, func in stages
        ]
        self._parallel_branches[branch_name] = branch
        return self

    def execute(
        self,
        input_data: Any,
    ) -> PipelineResult:
        """Execute pipeline on input data."""
        start_time = time.perf_counter()
        stages_executed = []
        stages_skipped = []
        stages_failed = []
        errors = {}

        current_data = input_data
        self._results["input"] = input_data

        for stage in self._stages:
            if stage.condition and not stage.condition(current_data):
                stages_skipped.append(stage.name)
                logger.debug("Stage '%s' skipped (condition not met)", stage.name)
                continue

            try:
                logger.debug("Executing stage '%s'", stage.name)
                stage_start = time.time()

                if stage.timeout:
                    import threading
                    result_container = [None]
                    exception_container = [None]

                    def target():
                        try:
                            result_container[0] = stage.func(current_data)
                        except Exception as e:
                            exception_container[0] = e

                    thread = threading.Thread(target=target)
                    thread.start()
                    thread.join(timeout=stage.timeout)

                    if thread.is_alive():
                        raise TimeoutError(f"Stage '{stage.name}' timed out after {stage.timeout}s")

                    if exception_container[0]:
                        raise exception_container[0]

                    current_data = result_container[0]
                else:
                    current_data = stage.func(current_data)

                stage_time = (time.time() - stage_start) * 1000
                self._results[stage.name] = current_data

                stages_executed.append(stage.name)
                logger.debug("Stage '%s' completed in %.2fms", stage.name, stage_time)

            except Exception as e:
                error_msg = str(e)
                errors[stage.name] = error_msg
                stages_failed.append(stage.name)
                logger.error("Stage '%s' failed: %s", stage.name, error_msg)

                if stage.on_error:
                    try:
                        current_data = stage.on_error(e, current_data)
                    except Exception as te:
                        logger.error("Stage '%s' on_error handler failed: %s", stage.name, te)

                if self.stop_on_error:
                    break

        execution_time_ms = (time.perf_counter() - start_time) * 1000

        return PipelineResult(
            success=len(stages_failed) == 0,
            output=current_data,
            stages_executed=stages_executed,
            stages_skipped=stages_skipped,
            stages_failed=stages_failed,
            execution_time_ms=execution_time_ms,
            errors=errors,
        )

    def execute_parallel(
        self,
        input_data: Any,
        max_workers: int = 4,
    ) -> dict[str, Any]:
        """Execute all parallel branches concurrently."""
        results = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}

            for branch_name, stages in self._parallel_branches.items():
                future = executor.submit(self._execute_branch, stages, input_data)
                futures[branch_name] = future

            for branch_name, future in as_completed(futures):
                try:
                    results[branch_name] = future.result()
                except Exception as e:
                    logger.error("Branch '%s' failed: %s", branch_name, e)
                    results[branch_name] = None

        return results

    def _execute_branch(
        self,
        stages: list[PipelineStage],
        input_data: Any,
    ) -> Any:
        """Execute a single branch."""
        current = input_data
        for stage in stages:
            current = stage.func(current)
        return current

    def clear(self) -> None:
        """Clear all stages."""
        self._stages.clear()
        self._parallel_branches.clear()
        self._results.clear()

    def get_stage_names(self) -> list[str]:
        """Get names of all stages in order."""
        return [s.name for s in self._stages]

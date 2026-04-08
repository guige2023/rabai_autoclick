"""
Automation Pipeline Action Module.

Builds and executes data processing pipelines with stages,
branching, error handling, and checkpointing for long-running workflows.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class PipelineStage:
    """A single stage in the pipeline."""
    name: str
    handler: Callable[[dict], dict]
    retry_count: int = 0
    timeout: Optional[float] = None
    on_error: Optional[str] = None  # next stage name on error


@dataclass
class PipelineResult:
    """Result of pipeline execution."""
    success: bool
    stages_completed: list[str]
    stages_failed: list[str]
    output: Any
    error: Optional[str] = None
    checkpoints: list[dict[str, Any]] = field(default_factory=list)


class AutomationPipelineAction(BaseAction):
    """Execute multi-stage data processing pipelines."""

    def __init__(self) -> None:
        super().__init__("automation_pipeline")
        self._stages: list[PipelineStage] = []
        self._checkpoints: list[dict[str, Any]] = []

    def execute(self, context: dict, params: dict) -> dict:
        """
        Execute a pipeline.

        Args:
            context: Execution context
            params: Parameters:
                - data: Input data for pipeline
                - stages: List of stage configs
                - checkpoint_enabled: Enable checkpointing (default: False)
                - stop_on_error: Stop pipeline on first error (default: True)

        Returns:
            PipelineResult with execution details
        """
        import time

        data = params.get("data")
        stage_configs = params.get("stages", [])
        checkpoint_enabled = params.get("checkpoint_enabled", False)
        stop_on_error = params.get("stop_on_error", True)

        stages = []
        for cfg in stage_configs:
            handler = cfg.get("handler")
            stages.append(PipelineStage(
                name=cfg.get("name", "unnamed"),
                handler=handler,
                retry_count=cfg.get("retry_count", 0),
                timeout=cfg.get("timeout"),
                on_error=cfg.get("on_error")
            ))

        completed = []
        failed = []
        current_data = data

        for stage in stages:
            start_time = time.time()
            try:
                if stage.timeout:
                    current_data = self._execute_with_timeout(stage.handler, current_data, stage.timeout)
                else:
                    current_data = stage.handler(current_data)

                elapsed = time.time() - start_time
                completed.append(stage.name)

                if checkpoint_enabled:
                    checkpoint = {"stage": stage.name, "data": current_data, "timestamp": start_time, "elapsed": elapsed}
                    self._checkpoints.append(checkpoint)

            except Exception as e:
                elapsed = time.time() - start_time
                if stage.retry_count > 0:
                    for retry in range(stage.retry_count):
                        try:
                            time.sleep(1 * (retry + 1))
                            current_data = stage.handler(current_data)
                            completed.append(stage.name)
                            break
                        except Exception:
                            if retry == stage.retry_count - 1:
                                if stop_on_error:
                                    return PipelineResult(
                                        success=False,
                                        stages_completed=completed,
                                        stages_failed=[stage.name],
                                        output=current_data,
                                        error=f"Stage {stage.name} failed after retries: {str(e)}",
                                        checkpoints=self._checkpoints
                                    )
                                else:
                                    failed.append(stage.name)
                            else:
                                continue
                else:
                    if stop_on_error:
                        return PipelineResult(
                            success=False,
                            stages_completed=completed,
                            stages_failed=[stage.name],
                            output=current_data,
                            error=f"Stage {stage.name} failed: {str(e)}",
                            checkpoints=self._checkpoints
                        )
                    else:
                        failed.append(stage.name)
                        if stage.on_error:
                            continue_stage_name = stage.on_error
                            next_stage = next((s for s in stages if s.name == continue_stage_name), None)
                            if next_stage:
                                idx = stages.index(next_stage)
                                stages = stages[idx:]

        return PipelineResult(
            success=len(failed) == 0,
            stages_completed=completed,
            stages_failed=failed,
            output=current_data,
            checkpoints=self._checkpoints
        )

    def _execute_with_timeout(self, handler: Callable, data: Any, timeout: float) -> Any:
        """Execute handler with timeout."""
        import time
        import threading

        result = [None]
        error = [None]

        def target():
            try:
                result[0] = handler(data)
            except Exception as e:
                error[0] = e

        t = threading.Thread(target=target)
        t.start()
        t.join(timeout)
        if t.is_alive():
            raise TimeoutError(f"Handler execution timed out after {timeout}s")
        if error[0]:
            raise error[0]
        return result[0]

    def add_stage(self, name: str, handler: Callable, retry_count: int = 0, timeout: Optional[float] = None) -> None:
        """Add a stage to the pipeline."""
        self._stages.append(PipelineStage(name=name, handler=handler, retry_count=retry_count, timeout=timeout))

    def get_checkpoints(self) -> list[dict[str, Any]]:
        """Get pipeline checkpoints."""
        return self._checkpoints.copy()

"""Data Pipeline Action Module.

Builds and executes multi-stage data processing pipelines with
stage composition, parallel branching, and error routing.
"""

import time
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


@dataclass
class PipelineStage:
    stage_id: str
    name: str
    processor_fn: Callable[[Any], Any]
    input_field: str = "data"
    output_field: str = "data"
    error_handler: Optional[Callable[[Exception, Any], Any]] = None
    timeout_sec: float = 30.0
    enabled: bool = True
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class PipelineResult:
    stage_id: str
    success: bool
    duration_ms: float
    output: Any = None
    error: Optional[str] = None
    attempt: int = 0


@dataclass
class PipelineConfig:
    stop_on_error: bool = True
    parallel_stages: bool = False
    context_sharing: bool = True


class DataPipelineAction:
    """Multi-stage data processing pipeline with error handling."""

    def __init__(self, config: Optional[PipelineConfig] = None) -> None:
        self._config = config or PipelineConfig()
        self._stages: List[PipelineStage] = []
        self._results: List[PipelineResult] = []
        self._context: Dict[str, Any] = {}

    def add_stage(
        self,
        stage_id: str,
        name: str,
        processor_fn: Callable[[Any], Any],
        input_field: str = "data",
        output_field: str = "data",
        error_handler: Optional[Callable[[Exception, Any], Any]] = None,
        timeout_sec: float = 30.0,
        max_retries: int = 3,
        enabled: bool = True,
    ) -> None:
        stage = PipelineStage(
            stage_id=stage_id,
            name=name,
            processor_fn=processor_fn,
            input_field=input_field,
            output_field=output_field,
            error_handler=error_handler,
            timeout_sec=timeout_sec,
            max_retries=max_retries,
            enabled=enabled,
        )
        self._stages.append(stage)

    def remove_stage(self, stage_id: str) -> bool:
        for i, s in enumerate(self._stages):
            if s.stage_id == stage_id:
                self._stages.pop(i)
                return True
        return False

    def execute(
        self,
        initial_data: Any,
    ) -> Tuple[bool, Any, List[PipelineResult]]:
        self._results.clear()
        if self._config.context_sharing:
            self._context.clear()
        data = initial_data
        for stage in self._stages:
            if not stage.enabled:
                continue
            result = self._execute_stage(stage, data)
            self._results.append(result)
            if result.success:
                data = result.output
                if self._config.context_sharing:
                    self._context[stage.stage_id] = result.output
            else:
                if self._config.stop_on_error:
                    return False, data, self._results
                if stage.error_handler:
                    try:
                        data = stage.error_handler(Exception(result.error), data)
                    except Exception as e:
                        logger.error(f"Error handler failed for {stage.stage_id}: {e}")
        success = all(r.success for r in self._results)
        return success, data, self._results

    def _execute_stage(
        self,
        stage: PipelineStage,
        data: Any,
    ) -> PipelineResult:
        start = time.time()
        input_data = data if stage.input_field == "data" else data.get(stage.input_field, data)
        for attempt in range(stage.max_retries + 1):
            try:
                output = stage.processor_fn(input_data)
                return PipelineResult(
                    stage_id=stage.stage_id,
                    success=True,
                    duration_ms=(time.time() - start) * 1000,
                    output=output,
                    attempt=attempt,
                )
            except Exception as e:
                if attempt < stage.max_retries:
                    continue
                return PipelineResult(
                    stage_id=stage.stage_id,
                    success=False,
                    duration_ms=(time.time() - start) * 1000,
                    output=None,
                    error=str(e),
                    attempt=attempt,
                )
        return PipelineResult(
            stage_id=stage.stage_id,
            success=False,
            duration_ms=(time.time() - start) * 1000,
            output=None,
            error="Max retries exceeded",
        )

    def get_stage_by_id(self, stage_id: str) -> Optional[PipelineStage]:
        for s in self._stages:
            if s.stage_id == stage_id:
                return s
        return None

    def enable_stage(self, stage_id: str) -> bool:
        stage = self.get_stage_by_id(stage_id)
        if stage:
            stage.enabled = True
            return True
        return False

    def disable_stage(self, stage_id: str) -> bool:
        stage = self.get_stage_by_id(stage_id)
        if stage:
            stage.enabled = False
            return True
        return False

    def get_stats(self) -> Dict[str, Any]:
        total = len(self._results)
        successful = sum(1 for r in self._results if r.success)
        return {
            "total_stages": len(self._stages),
            "completed": total,
            "successful": successful,
            "failed": total - successful,
            "success_rate": successful / total if total > 0 else 0,
            "total_duration_ms": sum(r.duration_ms for r in self._results),
        }

    def get_context(self) -> Dict[str, Any]:
        return dict(self._context)

    def get_results(self) -> List[PipelineResult]:
        return list(self._results)

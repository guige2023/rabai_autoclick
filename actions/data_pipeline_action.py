"""
Data Pipeline Processing Module.

Builds and executes multi-stage data processing pipelines with
parallel execution, error handling, backpressure, and monitoring.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple, TypeVar, Generic

logger = logging.getLogger(__name__)

T = TypeVar("T")
R = TypeVar("R")


class StageStatus(Enum):
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    SKIPPED = auto()


class BackpressureStrategy(Enum):
    BLOCK = auto()
    DROP = auto()
    QUEUE = auto()


@dataclass
class PipelineStage:
    stage_id: str
    name: str
    processor: Callable
    input_key: str = ""
    output_key: str = ""
    error_handler: Optional[Callable] = None
    timeout_seconds: float = 0.0
    retry_count: int = 0
    retry_delay: float = 1.0
    condition: Optional[Callable] = None
    parallel: bool = False
    max_batch_size: int = 100


@dataclass
class PipelineMetrics:
    stage_id: str
    processed_count: int = 0
    error_count: int = 0
    total_latency_ms: float = 0.0
    avg_latency_ms: float = 0.0
    last_run: Optional[datetime] = None


@dataclass
class PipelineContext:
    data: Dict[str, Any] = field(default_factory=dict)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    metrics: Dict[str, PipelineMetrics] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)


class StageExecutor:
    """Executes individual pipeline stages with error handling."""

    def __init__(self, stage: PipelineStage):
        self.stage = stage
        self.metrics = PipelineMetrics(stage_id=stage.stage_id)

    async def execute(self, context: PipelineContext) -> bool:
        start_time = time.time()
        self.metrics.last_run = datetime.utcnow()
        context.metrics[self.stage.stage_id] = self.metrics

        if self.stage.condition:
            try:
                if not self.stage.condition(context.data):
                    logger.debug("Stage %s skipped: condition false", self.stage.name)
                    return True
            except Exception as exc:
                logger.error("Condition error in %s: %s", self.stage.name, exc)
                return False

        input_data = context.data.get(self.stage.input_key)

        for attempt in range(self.stage.retry_count + 1):
            try:
                if asyncio.iscoroutinefunction(self.stage.processor):
                    result = await asyncio.wait_for(
                        self.stage.processor(input_data, context),
                        timeout=self.stage.timeout_seconds or None,
                    )
                else:
                    result = self.stage.processor(input_data, context)

                if self.stage.output_key:
                    context.data[self.stage.output_key] = result

                self.metrics.processed_count += 1
                latency = (time.time() - start_time) * 1000
                self.metrics.total_latency_ms += latency
                self.metrics.avg_latency_ms = (
                    self.metrics.total_latency_ms / self.metrics.processed_count
                )
                return True

            except asyncio.TimeoutError:
                error = f"Stage {self.stage.name} timed out"
                self._record_error(context, error, attempt)
            except Exception as exc:
                if attempt < self.stage.retry_count:
                    await asyncio.sleep(self.stage.retry_delay * (attempt + 1))
                    continue
                error = f"Stage {self.stage.name} failed: {exc}"
                self._record_error(context, error, attempt)

            if self.stage.error_handler:
                try:
                    self.stage.error_handler(exc, context)
                except Exception:
                    pass

            return False

        return False

    def _record_error(self, context: PipelineContext, error: str, attempt: int) -> None:
        context.errors.append({
            "stage_id": self.stage.stage_id,
            "stage_name": self.stage.name,
            "error": error,
            "attempt": attempt,
            "timestamp": datetime.utcnow().isoformat(),
        })
        self.metrics.error_count += 1


class DataPipeline:
    """
    Multi-stage data processing pipeline with parallel execution support.
    """

    def __init__(self, name: str, max_concurrency: int = 5):
        self.name = name
        self.max_concurrency = max_concurrency
        self._stages: List[PipelineStage] = []
        self._context: Optional[PipelineContext] = None
        self._semaphore = asyncio.Semaphore(max_concurrency)

    def add_stage(
        self,
        name: str,
        processor: Callable,
        input_key: str = "",
        output_key: str = "",
        **kwargs,
    ) -> PipelineStage:
        stage_id = f"stage_{len(self._stages)}"
        stage = PipelineStage(
            stage_id=stage_id,
            name=name,
            processor=processor,
            input_key=input_key,
            output_key=output_key,
            **kwargs,
        )
        self._stages.append(stage)
        logger.info("Added stage '%s' to pipeline '%s'", name, self.name)
        return stage

    def add_conditional(
        self,
        name: str,
        processor: Callable,
        condition: Callable[[Dict], bool],
        input_key: str = "",
        output_key: str = "",
    ) -> PipelineStage:
        return self.add_stage(
            name=name,
            processor=processor,
            input_key=input_key,
            output_key=output_key,
            condition=condition,
        )

    def add_parallel(
        self,
        name: str,
        processors: List[Tuple[str, Callable]],
        input_key: str = "",
    ) -> PipelineStage:
        async def parallel_processor(input_data: Any, ctx: PipelineContext) -> Dict[str, Any]:
            results = {}
            async with self._semaphore:
                tasks = []
                for key, proc in processors:
                    async def run(p=proc, k=key):
                        return (k, p(input_data))
                    tasks.append(run())
                outputs = await asyncio.gather(*tasks, return_exceptions=True)
                for output in outputs:
                    if isinstance(output, tuple):
                        results[output[0]] = output[1]
                    else:
                        logger.error("Parallel stage error: %s", output)
                return results

        return self.add_stage(
            name=name,
            processor=parallel_processor,
            input_key=input_key,
            output_key=name,
            parallel=True,
        )

    async def execute(
        self, initial_data: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, PipelineContext]:
        context = PipelineContext(
            data=initial_data or {},
            start_time=time.time(),
        )
        self._context = context
        success = True

        for stage in self._stages:
            executor = StageExecutor(stage)
            result = await executor.execute(context)
            if not result and stage.retry_count == 0:
                success = False
                break

        context.data["_duration_ms"] = (time.time() - context.start_time) * 1000
        return success, context

    def get_metrics_summary(self) -> Dict[str, Any]:
        if not self._context:
            return {}
        return {
            "total_stages": len(self._stages),
            "total_errors": len(self._context.errors),
            "duration_ms": (time.time() - self._context.start_time) * 1000,
            "stage_metrics": {
                sid: {
                    "processed": m.processed_count,
                    "errors": m.error_count,
                    "avg_latency_ms": m.avg_latency_ms,
                }
                for sid, m in self._context.metrics.items()
            },
        }


def create_etl_pipeline(
    name: str,
    extract: Callable,
    transform: Callable,
    load: Callable,
) -> DataPipeline:
    """Factory to create a simple ETL pipeline."""
    pipeline = DataPipeline(name)
    pipeline.add_stage("extract", extract, output_key="raw_data")
    pipeline.add_stage("transform", transform, input_key="raw_data", output_key="processed_data")
    pipeline.add_stage("load", load, input_key="processed_data")
    return pipeline

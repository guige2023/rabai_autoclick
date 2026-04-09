"""Automation Pipeline Executor Action.

Executes multi-stage automation pipelines with stage dependencies,
parallel execution, error handling, and result aggregation.
"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class StageStatus(Enum):
    """Status of a pipeline stage."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


@dataclass
class PipelineStage:
    """A single stage in a pipeline."""
    name: str
    fn: Callable[..., Any]
    depends_on: Set[str] = field(default_factory=set)
    timeout_sec: float = 60.0
    max_retries: int = 3
    retry_delay_sec: float = 1.0
    parallel: bool = False
    condition: Optional[Callable[[Dict[str, Any]], bool]] = None


@dataclass
class StageResult:
    """Result of a stage execution."""
    stage_name: str
    status: StageStatus
    output: Any = None
    error: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    duration_ms: float = 0.0
    retry_count: int = 0


@dataclass
class PipelineResult:
    """Result of a pipeline execution."""
    pipeline_name: str
    status: str
    total_stages: int
    completed_stages: int
    failed_stages: int
    skipped_stages: int
    results: Dict[str, StageResult] = field(default_factory=dict)
    total_duration_ms: float = 0.0
    start_time: float = 0.0
    end_time: float = 0.0


class AutomationPipelineExecutorAction:
    """Executes multi-stage automation pipelines."""

    def __init__(self, pipeline_name: str = "default") -> None:
        self.pipeline_name = pipeline_name
        self._stages: Dict[str, PipelineStage] = {}
        self._stage_order: List[str] = []
        self._results: Dict[str, StageResult] = {}

    def add_stage(
        self,
        name: str,
        fn: Callable[..., Any],
        depends_on: Optional[List[str]] = None,
        timeout_sec: float = 60.0,
        max_retries: int = 3,
        retry_delay_sec: float = 1.0,
        parallel: bool = False,
        condition: Optional[Callable[[Dict[str, Any]], bool]] = None,
    ) -> PipelineStage:
        """Add a stage to the pipeline."""
        stage = PipelineStage(
            name=name,
            fn=fn,
            depends_on=set(depends_on or []),
            timeout_sec=timeout_sec,
            max_retries=max_retries,
            retry_delay_sec=retry_delay_sec,
            parallel=parallel,
            condition=condition,
        )
        self._stages[name] = stage
        self._topological_sort()
        return stage

    def _topological_sort(self) -> None:
        """Compute topological order of stages."""
        in_degree: Dict[str, int] = {name: 0 for name in self._stages}
        graph: Dict[str, List[str]] = defaultdict(list)

        for name, stage in self._stages.items():
            for dep in stage.depends_on:
                if dep in self._stages:
                    graph[dep].append(name)
                    in_degree[name] += 1

        queue = [name for name, deg in in_degree.items() if deg == 0]
        self._stage_order = []

        while queue:
            current = queue.pop(0)
            self._stage_order.append(current)
            for neighbor in graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

    def _is_stage_ready(self, stage_name: str) -> bool:
        """Check if all dependencies of a stage are completed."""
        stage = self._stages.get(stage_name)
        if not stage:
            return False
        return all(
            self._results.get(dep, StageResult(dep, StageStatus.PENDING)).status == StageStatus.COMPLETED
            for dep in stage.depends_on
            if dep in self._stages
        )

    def _run_stage_sync(
        self,
        stage: PipelineStage,
        context: Dict[str, Any],
    ) -> StageResult:
        """Run a stage synchronously."""
        result = StageResult(stage_name=stage.name, status=StageStatus.RUNNING)
        result.start_time = time.time()

        retry_count = 0
        while retry_count <= stage.max_retries:
            try:
                output = stage.fn(context)
                if asyncio.iscoroutine(output):
                    loop = asyncio.get_event_loop()
                    output = loop.run_until_complete(output)

                result.output = output
                result.status = StageStatus.COMPLETED
                result.retry_count = retry_count
                break

            except Exception as e:
                retry_count += 1
                result.retry_count = retry_count

                if retry_count <= stage.max_retries:
                    result.status = StageStatus.RETRYING
                    time.sleep(stage.retry_delay_sec * (2 ** (retry_count - 1)))
                else:
                    result.status = StageStatus.FAILED
                    result.error = str(e)

        result.end_time = time.time()
        result.duration_ms = (result.end_time - result.start_time) * 1000
        return result

    def run(
        self,
        initial_context: Optional[Dict[str, Any]] = None,
        stop_on_failure: bool = True,
    ) -> PipelineResult:
        """Execute the pipeline."""
        start_time = time.time()
        context = dict(initial_context or {})
        self._results = {}

        for stage_name in self._stage_order:
            stage = self._stages[stage_name]

            if stage.condition and not stage.condition(context):
                self._results[stage_name] = StageResult(
                    stage_name=stage_name,
                    status=StageStatus.SKIPPED,
                )
                continue

            while not self._is_stage_ready(stage_name):
                time.sleep(0.01)

            result = self._run_stage_sync(stage, context)
            self._results[stage_name] = result

            if result.status == StageStatus.COMPLETED and result.output is not None:
                if isinstance(result.output, dict):
                    context.update(result.output)

            if stop_on_failure and result.status == StageStatus.FAILED:
                for remaining in self._stage_order[self._stage_order.index(stage_name) + 1:]:
                    self._results[remaining] = StageResult(
                        stage_name=remaining,
                        status=StageStatus.SKIPPED,
                    )
                break

        end_time = time.time()

        completed = sum(1 for r in self._results.values() if r.status == StageStatus.COMPLETED)
        failed = sum(1 for r in self._results.values() if r.status == StageStatus.FAILED)
        skipped = sum(1 for r in self._results.values() if r.status == StageStatus.SKIPPED)

        return PipelineResult(
            pipeline_name=self.pipeline_name,
            status="completed" if failed == 0 else "partial" if completed > 0 else "failed",
            total_stages=len(self._stages),
            completed_stages=completed,
            failed_stages=failed,
            skipped_stages=skipped,
            results=self._results,
            total_duration_ms=(end_time - start_time) * 1000,
            start_time=start_time,
            end_time=end_time,
        )

    def get_stage_result(self, stage_name: str) -> Optional[StageResult]:
        """Get the result of a specific stage."""
        return self._results.get(stage_name)

    def get_execution_order(self) -> List[str]:
        """Get the topological execution order."""
        return list(self._stage_order)

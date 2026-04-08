"""
Automation Pipeline Action Module.

Provides pipeline orchestration with stage management,
error handling, and progress tracking.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging
import uuid

logger = logging.getLogger(__name__)


class PipelineState(Enum):
    """Pipeline states."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StageState(Enum):
    """Stage execution states."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


@dataclass
class PipelineStage:
    """Pipeline stage definition."""
    stage_id: str
    name: str
    handler: Callable
    dependencies: List[str] = field(default_factory=list)
    timeout: Optional[float] = None
    retry_count: int = 0
    retry_delay: float = 1.0
    continue_on_error: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StageResult:
    """Stage execution result."""
    stage_id: str
    state: StageState
    output: Any = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retry_count: int = 0

    @property
    def duration(self) -> Optional[float]:
        """Get stage duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


@dataclass
class PipelineResult:
    """Pipeline execution result."""
    pipeline_id: str
    state: PipelineState
    stage_results: Dict[str, StageResult]
    started_at: datetime
    completed_at: Optional[datetime] = None
    output: Any = None
    error: Optional[str] = None

    @property
    def duration(self) -> Optional[float]:
        """Get pipeline duration in seconds."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


class PipelineExecutor:
    """Executes pipelines with stage management."""

    def __init__(self, pipeline_id: str):
        self.pipeline_id = pipeline_id
        self.stages: Dict[str, PipelineStage] = {}
        self.stage_results: Dict[str, StageResult] = {}
        self.state = PipelineState.IDLE
        self.started_at: Optional[datetime] = None
        self._cancelled = False
        self._paused = False

    def add_stage(self, stage: PipelineStage):
        """Add stage to pipeline."""
        self.stages[stage.stage_id] = stage
        self.stage_results[stage.stage_id] = StageResult(
            stage_id=stage.stage_id,
            state=StageState.PENDING
        )

    def remove_stage(self, stage_id: str) -> bool:
        """Remove stage from pipeline."""
        if stage_id in self.stages:
            del self.stages[stage_id]
            return True
        return False

    def get_execution_order(self) -> List[List[str]]:
        """Get stages in execution order (parallel within level)."""
        in_degree: Dict[str, int] = {
            sid: len(stage.dependencies)
            for sid, stage in self.stages.items()
        }

        levels = []
        remaining = set(self.stages.keys())
        completed = set()

        while remaining:
            current_level = [
                sid for sid in remaining
                if in_degree[sid] == 0
            ]

            if not current_level:
                break

            levels.append(current_level)

            for sid in current_level:
                remaining.remove(sid)
                completed.add(sid)

                for other_id, stage in self.stages.items():
                    if self.stages[sid].stage_id in stage.dependencies:
                        in_degree[other_id] -= 1

        return levels

    def can_execute(self, stage_id: str, completed_stages: Set[str]) -> bool:
        """Check if stage can execute."""
        stage = self.stages.get(stage_id)
        if not stage:
            return False

        return all(dep in completed_stages for dep in stage.dependencies)

    async def execute_stage(self, stage: PipelineStage) -> StageResult:
        """Execute a single stage."""
        result = StageResult(
            stage_id=stage.stage_id,
            state=StageState.RUNNING,
            started_at=datetime.now()
        )

        for attempt in range(stage.retry_count + 1):
            try:
                if asyncio.iscoroutinefunction(stage.handler):
                    if stage.timeout:
                        result.output = await asyncio.wait_for(
                            stage.handler(self.stage_results),
                            timeout=stage.timeout
                        )
                    else:
                        result.output = await stage.handler(self.stage_results)
                else:
                    result.output = stage.handler(self.stage_results)

                result.state = StageState.COMPLETED
                result.completed_at = datetime.now()
                return result

            except asyncio.TimeoutError:
                result.error = f"Stage timed out after {stage.timeout}s"
                result.state = StageState.FAILED

            except Exception as e:
                result.error = str(e)
                result.state = StageState.FAILED

            if attempt < stage.retry_count:
                result.state = StageState.RETRYING
                result.retry_count = attempt + 1
                await asyncio.sleep(stage.retry_delay * (attempt + 1))

        result.completed_at = datetime.now()
        return result

    async def execute(self) -> PipelineResult:
        """Execute entire pipeline."""
        self.state = PipelineState.RUNNING
        self.started_at = datetime.now()
        completed_stages: Set[str] = set()

        execution_order = self.get_execution_order()

        try:
            for level in execution_order:
                if self._cancelled:
                    self.state = PipelineState.CANCELLED
                    break

                while self._paused:
                    await asyncio.sleep(0.1)

                level_tasks = []
                for stage_id in level:
                    if self.can_execute(stage_id, completed_stages):
                        stage = self.stages[stage_id]
                        task = asyncio.create_task(self.execute_stage(stage))
                        level_tasks.append((stage_id, task))

                for stage_id, task in level_tasks:
                    result = await task
                    self.stage_results[stage_id] = result

                    if result.state == StageState.COMPLETED:
                        completed_stages.add(stage_id)
                    elif not self.stages[stage_id].continue_on_error:
                        self.state = PipelineState.FAILED
                        return PipelineResult(
                            pipeline_id=self.pipeline_id,
                            state=self.state,
                            stage_results=self.stage_results,
                            started_at=self.started_at,
                            completed_at=datetime.now(),
                            error=f"Stage {stage_id} failed: {result.error}"
                        )

            if self.state != PipelineState.FAILED and not self._cancelled:
                self.state = PipelineState.COMPLETED

        except Exception as e:
            self.state = PipelineState.FAILED
            return PipelineResult(
                pipeline_id=self.pipeline_id,
                state=self.state,
                stage_results=self.stage_results,
                started_at=self.started_at,
                completed_at=datetime.now(),
                error=str(e)
            )

        return PipelineResult(
            pipeline_id=self.pipeline_id,
            state=self.state,
            stage_results=self.stage_results,
            started_at=self.started_at,
            completed_at=datetime.now()
        )

    def cancel(self):
        """Cancel pipeline execution."""
        self._cancelled = True

    def pause(self):
        """Pause pipeline execution."""
        self._paused = True

    def resume(self):
        """Resume pipeline execution."""
        self._paused = False

    def get_progress(self) -> float:
        """Get pipeline progress (0.0 to 1.0)."""
        if not self.stages:
            return 0.0

        completed = sum(
            1 for r in self.stage_results.values()
            if r.state == StageState.COMPLETED
        )

        return completed / len(self.stages)


async def demo_stage(results: Dict[str, StageResult]) -> Dict[str, Any]:
    """Demo stage handler."""
    await asyncio.sleep(0.1)
    return {"status": "completed"}


async def main():
    """Demonstrate pipeline execution."""
    pipeline = PipelineExecutor("demo-pipeline")

    pipeline.add_stage(PipelineStage(
        stage_id="stage1",
        name="First Stage",
        handler=demo_stage
    ))

    pipeline.add_stage(PipelineStage(
        stage_id="stage2",
        name="Second Stage",
        handler=demo_stage,
        dependencies=["stage1"]
    ))

    result = await pipeline.execute()
    print(f"Pipeline state: {result.state.value}")
    print(f"Progress: {pipeline.get_progress():.1%}")


if __name__ == "__main__":
    asyncio.run(main())
